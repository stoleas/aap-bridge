"""Organization-scoped credential transfer using the Controllers' mounted keys.

No master key is exported. Decryption uses the installed Controller's own ORM
and encryption helper. Plan comparisons use per-run keyed fingerprints; apply
transfers source inputs sealed to a fresh, in-memory transport key.
"""

import base64
import json
import subprocess

from cryptography.fernet import Fernet


class CredentialSyncError(Exception):
    """Sanitized failure that contains no credential inputs or key material."""


# This constant program is passed as code; all request data and ephemeral keys
# travel through stdin. It never saves a model or prints plaintext inputs.
POD_PROGRAM = r"""
import base64, contextlib, hashlib, hmac, io, json, logging, sys
from cryptography.fernet import Fernet
logging.disable(logging.CRITICAL)
try:
    request = json.load(sys.stdin)
    from django.conf import settings
    from awx.main.models import Credential, Organization
    from awx.main.utils.encryption import decrypt_field
    if not settings.SECRET_KEY:
        raise ValueError()
    organization = Organization.objects.get(name=request["organization"])
    ids = request["ids"]
    if any(type(value) is not int or value <= 0 for value in ids) or len(ids) != len(set(ids)):
        raise ValueError()
    rows = list(Credential.objects.filter(organization_id=organization.pk, pk__in=ids))
    if {row.pk for row in rows} != set(ids):
        raise ValueError()
    if any(row.input_sources.exists() for row in rows):
        raise ValueError()
    values = {}
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        for row in rows:
            inputs = dict(row.inputs)
            for field in row.credential_type.secret_fields:
                if field in inputs:
                    inputs[field] = decrypt_field(row, field)
            values[str(row.pk)] = inputs
    key = request["key"].encode()
    if request["operation"] == "fingerprints":
        secret = base64.urlsafe_b64decode(key)
        result = {pk: {field: hmac.new(secret, json.dumps([field, value], sort_keys=True).encode(), hashlib.sha256).hexdigest()
                       for field, value in inputs.items()} for pk, inputs in values.items()}
    elif request["operation"] == "inputs":
        result = {"sealed": Fernet(key).encrypt(json.dumps(values).encode()).decode()}
    else:
        raise ValueError()
    print("BRIDGE_CREDENTIAL_RESULT:" + json.dumps({"ok": True, "key_loaded": True, "result": result}))
except BaseException:
    print('BRIDGE_CREDENTIAL_RESULT:{"ok":false}')
"""


class PodCredentialReader:
    def __init__(self, access, oc_binary="/usr/local/bin/oc", server=None, verify_ssl=True):
        self.access = access
        self.oc = [oc_binary]
        if server:
            self.oc.append("--server=" + server)
        if not verify_ssl:
            self.oc.append("--insecure-skip-tls-verify=true")
        self.oc += ["--request-timeout=30s", "-n", access.namespace]
        self.container = access.container
        self.secret_references = []

    def _run(self, args, request=None):
        try:
            result = subprocess.run(
                self.oc + args, input=request, capture_output=True, text=True, timeout=120
            )
        except Exception:
            raise CredentialSyncError(
                "Controller credential access failed; subprocess details withheld."
            ) from None
        if result.returncode:
            raise CredentialSyncError(
                "Controller credential access failed; check oc login, pod exec permission, and deployment readiness."
            )
        return result.stdout

    def discover(self):
        try:
            deployment = json.loads(
                self._run(["get", "deployment", self.access.deployment, "-o", "json"])
            )
            spec = deployment["spec"]["template"]["spec"]
            containers = [row["name"] for row in spec["containers"]]
            if not self.container:
                matches = [name for name in containers if name == "task" or name.endswith("-task")]
                if len(matches) != 1:
                    raise ValueError()
                self.container = matches[0]
            if self.container not in containers:
                raise ValueError()
            references = set()
            for volume in spec.get("volumes", []):
                if volume.get("secret"):
                    references.add(volume["secret"]["secretName"])
                for source in volume.get("projected", {}).get("sources", []):
                    if source.get("secret"):
                        references.add(source["secret"]["name"])
            for container in spec["containers"]:
                if container["name"] == self.container:
                    for item in container.get("env", []):
                        ref = item.get("valueFrom", {}).get("secretKeyRef")
                        if ref:
                            references.add(ref["name"])
            self.secret_references = sorted(references)
        except CredentialSyncError:
            raise
        except Exception:
            raise CredentialSyncError(
                "Cannot identify the task container; configure its exact name."
            ) from None

    async def read(self, organization, ids, key, operation="fingerprints"):
        if not ids:
            return {}
        if self.container is None:
            self.discover()
        request = json.dumps(
            {
                "organization": organization,
                "ids": sorted(ids),
                "key": key.decode(),
                "operation": operation,
            }
        )
        output = self._run(
            [
                "exec",
                "-i",
                "deployment/" + self.access.deployment,
                "-c",
                self.container,
                "--",
                "awx-manage",
                "shell",
                "-c",
                POD_PROGRAM,
            ],
            request,
        )
        try:
            lines = [
                line for line in output.splitlines() if line.startswith("BRIDGE_CREDENTIAL_RESULT:")
            ]
            if len(lines) != 1:
                raise ValueError()
            envelope = json.loads(lines[0].split(":", 1)[1])
            if not envelope.get("ok") or not envelope.get("key_loaded"):
                raise ValueError()
            data = envelope["result"]
            if operation == "inputs":
                data = json.loads(Fernet(key).decrypt(data["sealed"].encode()))
            if set(data) != {str(value) for value in ids}:
                raise ValueError()
            return data
        except Exception:
            raise CredentialSyncError(
                "Scoped credential read/decryption failed; values and diagnostic output withheld."
            ) from None


async def write_credential(client, endpoint, payload, method):
    """Bypass API payload loggers and suppress response bodies on failure."""
    try:
        if hasattr(client, "credential_write"):
            return await client.credential_write(endpoint, payload, method)
        response = await client.client.request(
            method, client._build_url(endpoint), json=payload, follow_redirects=False
        )
        if response.status_code not in (200, 201):
            raise ValueError()
        result = response.json()
        # Never retain returned credential inputs, even if an API fails to mask them.
        return {key: result[key] for key in ("id", "name", "organization", "credential_type")}
    except Exception:
        raise CredentialSyncError(
            "Target credential write failed; API response withheld to protect inputs."
        ) from None


class CredentialSession:
    def __init__(self, source_reader, target_reader):
        self.source_reader, self.target_reader = source_reader, target_reader
        self.key = Fernet.generate_key()
        self.rows, self.matches, self.types = [], {}, {}
        self.source_fingerprints, self.target_fingerprints = {}, {}
        self.organization = None

    async def prepare(self, source, target, organization, source_rows, target_rows):
        from aap_migration.org_sync import list_all

        self.organization = organization
        self.rows = source_rows
        for reader in (self.source_reader, self.target_reader):
            if hasattr(reader, "discover"):
                reader.discover()
        # Credential name is only unique together with organization and type.
        source_index, target_index = {}, {}
        for rows, client, index in (
            (source_rows, source, source_index),
            (target_rows, target, target_index),
        ):
            for row in rows:
                kind = await client.get(f"credential_types/{row['credential_type']}/")
                key = (row["name"], kind["name"], kind["kind"])
                if key in index:
                    raise CredentialSyncError(
                        "Ambiguous credential name/type in the selected organization."
                    )
                index[key] = (row, kind)
        for key, (row, source_type) in source_index.items():
            choices = [
                kind
                for kind in await list_all(
                    target, "credential_types/", name=source_type["name"], kind=source_type["kind"]
                )
                if kind["name"] == source_type["name"] and kind["kind"] == source_type["kind"]
            ]
            if len(choices) != 1:
                raise CredentialSyncError(
                    "A matching credential type must already exist in the target; no global types were created."
                )
            target_type = choices[0]
            source_fields = {
                field["id"]: field for field in source_type["inputs"].get("fields", [])
            }
            target_fields = {
                field["id"]: field for field in target_type["inputs"].get("fields", [])
            }
            if any(
                field not in target_fields
                or any(
                    source_fields[field].get(attr) != target_fields[field].get(attr)
                    for attr in ("type",)
                )
                or any(
                    bool(source_fields[field].get(attr)) != bool(target_fields[field].get(attr))
                    for attr in ("secret", "ask_at_runtime")
                )
                for field in source_fields
            ):
                raise CredentialSyncError(
                    "Source and target credential input schemas differ; inputs were not transferred."
                )
            if source_type.get("kind") == "external" or source_type.get(
                "injectors", {}
            ) != target_type.get("injectors", {}):
                raise CredentialSyncError(
                    "External credential sources or incompatible injectors require a separate migration path."
                )
            missing = set(target_type["inputs"].get("required", [])) - set(row.get("inputs", {}))
            if any("default" not in target_fields.get(field, {}) for field in missing):
                raise CredentialSyncError(
                    "Source inputs do not satisfy required target credential fields."
                )
            self.types[row["id"]] = target_type["id"]
            self.matches[row["id"]] = target_index.get(key, (None, None))[0]
        self.source_fingerprints = await self.source_reader.read(
            organization, [row["id"] for row in source_rows], self.key
        )
        self.target_fingerprints = await self.target_reader.read(
            organization, [row["id"] for row in target_rows], self.key
        )
        if any(
            set(row.get("inputs", {})) != set(self.source_fingerprints[str(row["id"])])
            for row in source_rows
        ):
            raise CredentialSyncError(
                "Source credential inputs changed during discovery; rerun the plan."
            )

    async def apply(self, target, organization_id, apply=False):
        changes, mappings = [], {}
        for row in self.rows:
            current = self.matches[row["id"]]
            metadata = {
                "name": row["name"],
                "description": row.get("description", ""),
                "organization": organization_id,
                "credential_type": self.types[row["id"]],
            }
            delta = {
                key: value
                for key, value in metadata.items()
                if current is None or current.get(key) != value
            }
            inputs_changed = (
                current is None
                or self.source_fingerprints[str(row["id"])]
                != self.target_fingerprints[str(current["id"])]
            )
            fields = sorted(set(delta) | ({"inputs"} if inputs_changed else set()))
            action = "create" if current is None else ("update" if fields else "unchanged")
            changes.append(
                {"resource": "credentials", "name": row["name"], "action": action, "fields": fields}
            )
            result = current or {"id": -100000 - row["id"]}
            if apply and fields:
                if inputs_changed:
                    values = await self.source_reader.read(
                        self.organization, [row["id"]], self.key, operation="inputs"
                    )
                    inputs = values[str(row["id"])]
                    # Ensure the secret-bearing read still matches the preflight snapshot.
                    import hashlib
                    import hmac

                    observed = {
                        field: hmac.new(
                            base64.urlsafe_b64decode(self.key),
                            json.dumps([field, value], sort_keys=True).encode(),
                            hashlib.sha256,
                        ).hexdigest()
                        for field, value in inputs.items()
                    }
                    if observed != self.source_fingerprints[str(row["id"])]:
                        raise CredentialSyncError(
                            "Source credential changed after preflight; rerun synchronization."
                        )
                    delta["inputs"] = inputs
                try:
                    result = await write_credential(
                        target,
                        "credentials/" if current is None else f"credentials/{current['id']}/",
                        {**metadata, **delta} if current is None else delta,
                        "POST" if current is None else "PATCH",
                    )
                finally:
                    if "inputs" in delta:
                        delta["inputs"].clear()
                if inputs_changed:
                    verified = await self.target_reader.read(
                        self.organization, [result["id"]], self.key
                    )
                    if verified[str(result["id"])] != self.source_fingerprints[str(row["id"])]:
                        raise CredentialSyncError(
                            "Target credential inputs did not verify after write; values withheld."
                        )
            mappings[row["id"]] = result["id"]
        return changes, mappings
