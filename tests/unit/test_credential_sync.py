"""Secret transfer, rotation, isolation, and redacted-failure behavior."""

import base64
import hashlib
import hmac
import io
import json
import logging
import shlex
import unittest
from copy import deepcopy
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from cryptography.fernet import Fernet
from pydantic import ValidationError
from test_org_sync import MemoryClient, fixture

from aap_migration.config import CredentialSyncConfig, SSHControllerCredentialAccess
from aap_migration.credential_sync import (
    POD_PROGRAM,
    CredentialSession,
    CredentialSyncError,
    PodCredentialReader,
    SSHCredentialReader,
    credential_reader,
    write_credential,
)
from aap_migration.org_sync import SyncError, synchronize

TYPE = {
    "id": 3,
    "name": "Machine",
    "kind": "ssh",
    "managed": True,
    "injectors": {},
    "inputs": {
        "fields": [
            {"id": "username", "type": "string"},
            {"id": "password", "type": "string", "secret": True, "ask_at_runtime": True},
        ]
    },
}


class CredentialClient(MemoryClient):
    def __init__(self, snapshots=(), writable=True, type_id=3):
        super().__init__(snapshots, writable)
        self.tables["credential_types"] = [{**deepcopy(TYPE), "id": type_id}]
        self.tables["credentials"] = []
        for snapshot in snapshots:
            self.tables["credentials"].extend(deepcopy(snapshot.get("credentials", [])))

    async def get(self, endpoint, params=None):
        if endpoint.startswith("job_templates/") and endpoint.endswith("/credentials/"):
            template_id = int(endpoint.split("/")[1])
            template = next(row for row in self.tables["job_templates"] if row["id"] == template_id)
            ids = template.get("_credential_ids", [])
            rows = [deepcopy(row) for row in self.tables["credentials"] if row["id"] in ids]
            result = {"results": rows, "next": None}
        else:
            result = await super().get(endpoint, params)
        if endpoint.startswith("credentials/") or endpoint.endswith("/credentials/"):
            for row in result.get("results", []):
                row["inputs"] = {
                    key: "$encrypted$" if key == "password" else value
                    for key, value in row["inputs"].items()
                }
        return result

    async def post(self, endpoint, json_data):
        if endpoint.startswith("job_templates/") and endpoint.endswith("/credentials/"):
            template_id = int(endpoint.split("/")[1])
            template = next(row for row in self.tables["job_templates"] if row["id"] == template_id)
            template.setdefault("_credential_ids", []).append(json_data["id"])
            self.writes.append(("POST", endpoint, deepcopy(json_data)))
            return {}
        return await super().post(endpoint, json_data)

    async def credential_write(self, endpoint, payload, method):
        row = await (
            self.post(endpoint, payload) if method == "POST" else self.patch(endpoint, payload)
        )
        return {key: row[key] for key in ("id", "name", "organization", "credential_type")}


class Reader:
    def __init__(self, client):
        self.client, self.calls = client, []

    async def read(self, organization, ids, key, operation="fingerprints"):
        self.calls.append((organization, list(ids), operation))
        if not ids:
            return {}
        org = next(
            row for row in self.client.tables["organizations"] if row["name"] == organization
        )
        rows = [
            row
            for row in self.client.tables["credentials"]
            if row["id"] in ids and row["organization"] == org["id"]
        ]
        if {row["id"] for row in rows} != set(ids):
            raise CredentialSyncError("Scoped credential read rejected.")
        if operation == "inputs":
            return {str(row["id"]): deepcopy(row["inputs"]) for row in rows}
        return {
            str(row["id"]): {
                field: hmac.new(
                    base64.urlsafe_b64decode(key),
                    json.dumps([field, value], sort_keys=True).encode(),
                    hashlib.sha256,
                ).hexdigest()
                for field, value in row["inputs"].items()
            }
            for row in rows
        }


def source_snapshot():
    data = fixture("revision-1")
    data["credentials"] = [
        {
            "id": 54,
            "name": "Bridge Demo Machine",
            "description": "demo",
            "organization": 1,
            "credential_type": 3,
            "inputs": {"username": "bridge-demo", "password": "test-only-secret-v1"},
        }
    ]
    data["job_templates"][0]["_credential_ids"] = [54]
    return data


class TestCredentialSync(unittest.IsolatedAsyncioTestCase):
    def clients(self):
        source = CredentialClient([source_snapshot()], writable=False)
        target = CredentialClient([fixture("control")], type_id=303)
        target.tables["credentials"].append(
            {
                "id": 950,
                "name": "Bridge Demo Machine",
                "description": "control",
                "organization": 2,
                "credential_type": 303,
                "inputs": {"username": "control", "password": "test-only-control-secret"},
            }
        )
        return source, target

    async def run_sync(self, source, target, apply=False):
        readers = (Reader(source), Reader(target))
        report = await synchronize(
            source,
            target,
            "aap-bridge",
            apply=apply,
            credential_session=CredentialSession(*readers),
        )
        return report, readers

    async def test_plan_never_transfers_plaintext_or_writes(self):
        source, target = self.clients()
        report, readers = await self.run_sync(source, target)
        self.assertEqual(target.writes, [])
        self.assertNotIn("test-only-secret-v1", json.dumps(report))
        self.assertTrue(
            all(call[2] == "fingerprints" for reader in readers for call in reader.calls)
        )
        self.assertIn("credentials", [row["resource"] for row in report["changes"]])

    async def test_create_rotation_noop_type_mapping_and_attachments_preserve_control(self):
        source, target = self.clients()
        control = deepcopy(target.tables["credentials"][0])
        await self.run_sync(source, target, apply=True)
        credential = next(row for row in target.tables["credentials"] if row["organization"] != 2)
        self.assertEqual(credential["credential_type"], 303)
        self.assertEqual(credential["inputs"]["password"], "test-only-secret-v1")
        template = target.tables["job_templates"][0]
        self.assertEqual(template["_credential_ids"], [credential["id"]])
        target.writes.clear()
        await self.run_sync(source, target, apply=True)
        self.assertEqual(target.writes, [])
        source.tables["credentials"][0]["inputs"]["password"] = "test-only-secret-v2"
        plan, readers = await self.run_sync(source, target)
        changed = [row for row in plan["changes"] if row["action"] != "unchanged"]
        self.assertEqual(len(changed), 1)
        self.assertEqual(changed[0]["fields"], ["inputs"])
        self.assertNotIn("test-only-secret-v2", json.dumps(plan))
        self.assertTrue(
            all(call[2] == "fingerprints" for reader in readers for call in reader.calls)
        )
        await self.run_sync(source, target, apply=True)
        self.assertEqual(credential["inputs"]["password"], "test-only-secret-v2")
        self.assertEqual(target.tables["credentials"][0], control)
        target.writes.clear()
        await self.run_sync(source, target, apply=True)
        self.assertEqual(target.writes, [])

    async def test_cross_organization_credential_reference_fails_before_writes(self):
        source, target = self.clients()
        source.tables["credentials"][0]["organization"] = 999
        with self.assertRaisesRegex(SyncError, "outside the selected organization"):
            await self.run_sync(source, target, apply=True)
        self.assertEqual(target.writes, [])

    async def test_incompatible_credential_type_fails_before_writes(self):
        source, target = self.clients()
        target.tables["credential_types"][0]["inputs"]["fields"][1]["secret"] = False
        with self.assertRaises(CredentialSyncError):
            await self.run_sync(source, target, apply=True)
        self.assertEqual(target.writes, [])

    async def test_secret_write_error_is_sanitized(self):
        class Broken:
            async def credential_write(self, endpoint, payload, method):
                raise RuntimeError("server echoed test-only-secret-v1")

        with self.assertRaises(CredentialSyncError) as failure:
            await write_credential(
                Broken(), "credentials/", {"inputs": {"password": "test-only-secret-v1"}}, "POST"
            )
        self.assertNotIn("test-only-secret-v1", str(failure.exception))

    async def test_new_required_target_input_is_rejected_before_writes(self):
        source, target = self.clients()
        target.tables["credential_types"][0]["inputs"]["required"] = ["new_required_field"]
        with self.assertRaises(CredentialSyncError):
            await self.run_sync(source, target, apply=True)
        self.assertEqual(target.writes, [])


class TestPodEnvelope(unittest.TestCase):
    def execute(self, operation="inputs", ids=None, dynamic=False, decrypt_error=False):
        key = Fernet.generate_key()
        ids = [54] if ids is None else ids
        row = SimpleNamespace(
            pk=54,
            inputs={"username": "demo", "password": "$encrypted$fake"},
            credential_type=SimpleNamespace(secret_fields=["password"]),
            input_sources=SimpleNamespace(exists=lambda: dynamic),
        )
        manager = SimpleNamespace(
            filter=lambda **filters: (
                [row] if filters == {"organization_id": 1, "pk__in": [54]} else []
            )
        )
        org_manager = SimpleNamespace(
            get=lambda **filters: (
                SimpleNamespace(pk=1) if filters == {"name": "aap-bridge"} else None
            )
        )
        modules = {
            name: ModuleType(name)
            for name in (
                "django",
                "django.conf",
                "awx",
                "awx.main",
                "awx.main.models",
                "awx.main.utils",
                "awx.main.utils.encryption",
            )
        }
        modules["django.conf"].settings = SimpleNamespace(SECRET_KEY="test-only-master-key")
        modules["awx.main.models"].Credential = SimpleNamespace(objects=manager)
        modules["awx.main.models"].Organization = SimpleNamespace(objects=org_manager)

        def decrypt(instance, field):
            if decrypt_error:
                raise ValueError("test-only-private-value must not appear in the error")
            return "test-only-private-value"

        modules["awx.main.utils.encryption"].decrypt_field = decrypt
        request = json.dumps(
            {"organization": "aap-bridge", "ids": ids, "key": key.decode(), "operation": operation}
        )
        stdout = io.StringIO()
        previous = logging.root.manager.disable
        try:
            with (
                patch.dict("sys.modules", modules),
                patch("sys.stdin", io.StringIO(request)),
                patch("sys.stdout", stdout),
            ):
                exec(POD_PROGRAM, {})
        finally:
            logging.disable(previous)
        output = stdout.getvalue()
        self.assertNotIn("test-only-private-value", output)
        self.assertNotIn("test-only-master-key", output)
        return key, json.loads(output.split(":", 1)[1])

    def test_pod_returns_only_sealed_values(self):
        key, envelope = self.execute()
        self.assertTrue(envelope["ok"])
        inputs = json.loads(Fernet(key).decrypt(envelope["result"]["sealed"].encode()))
        self.assertEqual(inputs["54"]["password"], "test-only-private-value")

    def test_plan_returns_fingerprints_not_values(self):
        _, envelope = self.execute(operation="fingerprints")
        self.assertTrue(envelope["ok"])
        self.assertEqual(len(envelope["result"]["54"]["password"]), 64)

    def test_out_of_scope_ids_and_external_input_sources_are_rejected(self):
        _, wrong_id = self.execute(ids=[55])
        _, external = self.execute(dynamic=True)
        self.assertFalse(wrong_id["ok"])
        self.assertFalse(external["ok"])

    def test_decryption_failures_return_no_values_or_exception_details(self):
        _, envelope = self.execute(decrypt_error=True)
        self.assertEqual(envelope, {"ok": False})

    def test_oc_arguments_do_not_contain_transport_key(self):
        access = SimpleNamespace(namespace="aap24", deployment="aap24-task", container="task")
        reader = PodCredentialReader(access)
        key = Fernet.generate_key()
        result = SimpleNamespace(
            returncode=0,
            stdout='BRIDGE_CREDENTIAL_RESULT:{"ok":true,"key_loaded":true,"result":{"54":{}}}',
        )
        with patch("subprocess.run", return_value=result) as command:
            import asyncio

            asyncio.run(reader.read("aap-bridge", [54], key))
        self.assertNotIn(key.decode(), repr(command.call_args.args))
        self.assertIn(key.decode(), command.call_args.kwargs["input"])


class TestSSHCredentialAccess(unittest.IsolatedAsyncioTestCase):
    def access(self, **overrides):
        return SSHControllerCredentialAccess(
            transport="ssh", host="controller.example.com", user="bridge", **overrides
        )

    async def test_sealed_inputs_and_remote_shell_quoting(self):
        key = Fernet.generate_key()
        values = {"54": {"password": "test-only-private-value"}}
        envelope = {
            "ok": True,
            "key_loaded": True,
            "result": {"sealed": Fernet(key).encrypt(json.dumps(values).encode()).decode()},
        }
        result = SimpleNamespace(
            returncode=0, stdout="BRIDGE_CREDENTIAL_RESULT:" + json.dumps(envelope)
        )
        reader = SSHCredentialReader(self.access(identity_file="/tmp/bridge key"))
        with patch("subprocess.run", return_value=result) as command:
            data = await reader.read("customer's organization", [54], key, "inputs")
        self.assertEqual(data, values)
        args = command.call_args.args[0]
        self.assertEqual(
            shlex.split(args[-1]),
            ["sudo", "-n", "-u", "awx", "--", "/usr/bin/awx-manage", "shell", "-c", POD_PROGRAM],
        )
        self.assertIn("StrictHostKeyChecking=yes", args)
        self.assertIn("BatchMode=yes", args)
        self.assertIn("/tmp/bridge key", args)
        self.assertNotIn(key.decode(), repr(args))
        self.assertNotIn("customer's organization", repr(args))
        request = json.loads(command.call_args.kwargs["input"])
        self.assertEqual(request["key"], key.decode())
        self.assertEqual(request["organization"], "customer's organization")

    async def test_direct_service_account_and_fingerprint_read(self):
        reader = SSHCredentialReader(self.access(become_user=None, port=2222))
        result = SimpleNamespace(
            returncode=0,
            stdout='BRIDGE_CREDENTIAL_RESULT:{"ok":true,"key_loaded":true,"result":{"54":{"password":"digest"}}}',
        )
        with patch("subprocess.run", return_value=result) as command:
            data = await reader.read("demo", [54], Fernet.generate_key())
        self.assertEqual(data, {"54": {"password": "digest"}})
        self.assertEqual(shlex.split(command.call_args.args[0][-1])[0], "/usr/bin/awx-manage")
        self.assertIn("2222", command.call_args.args[0])

    async def test_transport_and_response_errors_are_sanitized(self):
        reader = SSHCredentialReader(self.access())
        for result in (
            SimpleNamespace(returncode=1, stdout="test-only-secret", stderr="test-only-secret"),
            SimpleNamespace(returncode=0, stdout="test-only-secret"),
            SimpleNamespace(returncode=0, stdout='BRIDGE_CREDENTIAL_RESULT:{"ok":false}'),
            SimpleNamespace(
                returncode=0,
                stdout='BRIDGE_CREDENTIAL_RESULT:{"ok":true,"key_loaded":true,"result":{"99":{}}}',
            ),
        ):
            with patch("subprocess.run", return_value=result):
                with self.assertRaises(CredentialSyncError) as failure:
                    await reader.read("demo", [54], Fernet.generate_key())
            self.assertNotIn("test-only-secret", str(failure.exception))
        with patch("subprocess.run", side_effect=RuntimeError("test-only-secret")):
            with self.assertRaises(CredentialSyncError) as failure:
                await reader.read("demo", [54], Fernet.generate_key())
        self.assertNotIn("test-only-secret", str(failure.exception))

    async def test_no_command_for_empty_ids(self):
        with patch("subprocess.run") as command:
            self.assertEqual(
                await SSHCredentialReader(self.access()).read("demo", [], Fernet.generate_key()), {}
            )
        command.assert_not_called()

    def test_mixed_transports_and_legacy_openshift_config(self):
        pod = {"namespace": "aap", "deployment": "controller-task"}
        ssh = {"transport": "ssh", "host": "controller.example.com", "user": "bridge"}
        for source, target in ((ssh, pod), (pod, ssh), (pod, pod), (ssh, ssh)):
            settings = CredentialSyncConfig(source=source, target=target)
            for access in (settings.source, settings.target):
                reader = credential_reader(access, settings)
                expected = SSHCredentialReader if access.transport == "ssh" else PodCredentialReader
                self.assertIsInstance(reader, expected)

    def test_incomplete_and_option_like_ssh_settings_rejected(self):
        pod = {"namespace": "aap", "deployment": "controller-task"}
        for source in (
            {"transport": "ssh", "user": "bridge"},
            {"transport": "ssh", "host": "-oProxyCommand=bad", "user": "bridge"},
            {"transport": "ssh", "host": "controller.example.com", "user": "-root"},
            {"transport": "ssh", "host": "controller.example.com", "user": "bridge", "port": 0},
        ):
            with self.assertRaises(ValidationError):
                CredentialSyncConfig(source=source, target=pod)


if __name__ == "__main__":
    unittest.main()
