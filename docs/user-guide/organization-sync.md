# Experimental organization synchronization

This feature branch adds an on-demand, one-way add/update sync for a named organization. It is work in progress for lab evaluation, with optional OpenShift-backed credential transfer. It does not extend the existing migration compatibility matrix or establish tested AAP 2.4 to 2.7 migration support.

## Plan and apply

Use the existing source and target API configuration and tokens. The target URL must use its Controller API prefix, including `/api/controller/v2` when accessed through Gateway.

```bash
aap-bridge --config config.yaml sync --organization demo-organization
aap-bridge --config config.yaml sync --organization demo-organization --apply
```

The default command only plans changes. Applying requires both `--apply` and `dry_run: false` in the configuration. Reports include resource names, actions, and changed field names, without field values.

Supported resources are organizations, ordinary static inventories, static hosts, Git projects, and job templates. The implementation manages the field allowlists in `org_sync.py` and remaps target IDs using scoped natural keys. It preserves target-only resources and fields outside those allowlists. It does not delete resources, launch jobs, synchronize groups or inventory sources, migrate RBAC/workflows/schedules/surveys/execution environments, or install a recurring schedule. Without an incremental baseline, renames create new target resources.

## Incremental modified_on synchronization

Add `--incremental` (also accepted as `--modified-on`) to record a successful baseline and process subsequent changes:

```bash
# First apply creates the baseline; existing matching assets are not recreated.
aap-bridge --config config.yaml sync --organization demo-organization --incremental --apply

# Later plans and applies reuse that same baseline.
aap-bridge --config config.yaml sync --organization demo-organization --incremental
aap-bridge --config config.yaml sync --organization demo-organization --incremental --apply
```

AAP's API field is `modified`. The checkpoint stores its normalized UTC value as `modified_on`, along with target modification time, source-to-target IDs, and dependency IDs. Compatible adapters may supply `modified_on` directly. The checkpoint contains no credential inputs, plaintext secrets, transport keys, or resource variable payloads.

The first successful apply establishes the baseline. Subsequent runs skip a resource only when both timestamps, its tracked target ID, and its dependency links match. New or changed source objects are reconciled; missing targets are recreated; changed target fields are checked and repaired. A source rename reuses its recorded target ID when that ID remains in the selected organization. Conflicting names are rejected. Missing or invalid timestamps fall back to normal comparison, never automatic skipping. Timestamp equality is checked per resource rather than using a global greater-than cursor, so equal timestamps on different resources and backwards timestamps are not lost.

Credential password rotation is detected through credential modification time. Unchanged credentials avoid pod decryption and input transfer. Credential and template association IDs are still checked, because a many-to-many association change need not update the parent timestamp. Normal credential schema checks remain in place.

State defaults to `.aap-bridge-sync/checkpoints.sqlite3`, independently of the original migration's PostgreSQL state. Use `--sync-state /path/to/checkpoints.sqlite3` with `--incremental` to choose another location. Each source/target API pair, organization, credential mode, and configured pod-access combination has a separate scope. Changing an organization identity requires an explicit full resync. API tokens are not part of the scope, so token rotation does not discard the baseline.

Plans never create or advance a checkpoint. Apply holds a SQLite write transaction and advances the baseline only after the run and final managed-field/dependency checks succeed. A failed or interrupted run retains its previous checkpoint; retry reconciles partially completed target changes rather than recreating them. Concurrent applies using the same state file are rejected. Checkpoint files are created with mode `0600`.

To deliberately recheck every supported resource and rebuild the successful baseline:

```bash
aap-bridge --config config.yaml sync --organization demo-organization --incremental --full-resync --apply
```

Use the same credential flag on repeated runs when credential verification is wanted. A full resync checks inputs through fingerprints even if a timestamp did not change. Run one for periodic audits or after administrative changes that bypass normal modification timestamps. Deletions still are not propagated.

This implementation continues to enumerate source/target resources and associations for scope checks, dependency mapping, missing targets, and drift detection. It reduces reconciliation and secret-decryption work; it does not yet replace all enumeration with server-side `modified__gte` queries. Git revision mismatches are not cached as incremental no-ops, but the Git checkout refresh limitation below remains. Keep the checkpoint if you want rename mappings and incremental progress preserved between runs.

Reports include `incremental.resources_skipped`, `resources_processed`, and `checkpoint_saved`. Without `--incremental`, the existing full-comparison behavior remains available.

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
python -m unittest discover -s tests/unit -p test_incremental_sync.py -v
```

These tests cover scoped planning, create/update/no-op behavior, ID mapping, controls in another organization, credential rotation, secret-envelope handling, sanitized failures, timestamp baselines, retry/rollback, dependency changes, target drift, and metadata-only state. The existing full test suite and live integration checks remain necessary before merging this feature.
