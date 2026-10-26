# Experimental organization synchronization

This feature branch adds an on-demand, one-way add/update sync for a named organization. It is work in progress for lab evaluation, with optional OpenShift-backed credential transfer. It does not extend the existing migration compatibility matrix or establish tested AAP 2.4 to 2.7 migration support.

## Plan and apply

Use the existing source and target API configuration and tokens. The target URL must use its Controller API prefix, including `/api/controller/v2` when accessed through Gateway.

```bash
aap-bridge --config config.yaml sync --organization demo-organization
aap-bridge --config config.yaml sync --organization demo-organization --apply
```

The default command only plans changes. Applying requires both `--apply` and `dry_run: false` in the configuration. Reports include resource names, actions, and changed field names, without field values.

Supported resources are organizations, ordinary static inventories, static hosts, Git projects, and job templates. The implementation manages the field allowlists in `org_sync.py` and remaps target IDs using scoped natural keys. It preserves target-only resources and fields outside those allowlists. It does not delete resources, launch jobs, synchronize groups or inventory sources, migrate RBAC/workflows/schedules/surveys/execution environments, or install a recurring schedule. Renames create new target resources.

## Optional credential access

Credential transfer requires privileged access to the Controller task pods in addition to the API tokens. Both source and target currently use the same configured Kubernetes API/context. Configure namespace and Deployment names for each Controller; the task container can be supplied explicitly if discovery is ambiguous.

```yaml
credential_sync:
  oc_binary: oc
  server: https://api.cluster.example:6443
  verify_ssl: true
  source:
    namespace: aap-source
    deployment: source-controller-task
  target:
    namespace: aap-target
    deployment: target-controller-task
logging:
  log_payloads: false
```

```bash
aap-bridge --config config.yaml sync --organization demo-organization --with-credentials
aap-bridge --config config.yaml sync --organization demo-organization --with-credentials --apply
```

The provider uses `oc exec` and the installed Controller's ORM/decryption helper with its mounted application key and database settings. It does not export or replace a Controller master key or write database rows directly. Plan comparisons use per-run keyed fingerprints. Changed inputs are sealed to an ephemeral transport key, decrypted in local process memory, and written through the target API. Target decrypted fingerprints are checked after writes. Payload logging must be disabled.

Credentials must belong to the selected organization, and a matching compatible credential type must already exist on the target. Global credential types are not created or updated. External secret-manager input sources are rejected. Project credential references are remapped; missing job-template credential associations are added, while target-only associations remain.

## Review findings still to resolve

Use synthetic lab credentials while these findings remain unresolved:

- API and pod-reader Controller identities are not bound together. A misconfigured pod deployment with colliding organization names and credential IDs can supply the wrong credential inputs and still pass the current fingerprint verification. Controller identity and credential metadata checks are required before broader use.
- A project with the same configured Git URL/branch but a different checkout revision is reported unchanged. The implementation does not request a fresh Git update before synchronizing templates that may reference new playbooks.
- Source and target pod access need separate Kubernetes contexts/API configuration for cross-cluster use.
- Live decryption, transfer, target re-encryption, and job-template interoperability have not been validated against the installed AAP versions. Mock tests are not evidence of that compatibility.

The standalone test-fixture dependency identified in review has been resolved by placing synthetic fixtures under `tests/fixtures/org_sync/`.

## Targeted tests

Run from the repository root in an installed development environment:

```bash
python -m unittest discover -s tests/unit -p test_org_sync\*.py -v
python -m unittest discover -s tests/unit -p test_credential_sync.py -v
python -m unittest discover -s tests/unit -p test_prep_untested_path.py -v
```

These tests cover scoped planning, create/update/no-op behavior, ID mapping, controls in another organization, credential rotation, secret-envelope handling, and sanitized failures. The existing full test suite and live integration checks remain necessary before merging this feature.
