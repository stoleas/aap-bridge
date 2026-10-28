# Experimental organization synchronization

This feature branch adds an on-demand, one-way add/update sync for a named organization. It is work in progress for lab evaluation, with optional credential transfer through OpenShift or SSH to an RPM Controller. It does not extend the existing migration compatibility matrix or establish tested AAP 2.4 to 2.7 migration support.

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

Credential transfer requires privileged access to each Controller runtime in addition to the API tokens. Choose access independently for source and target: `transport: openshift` (the default for existing configurations) or `transport: ssh` for an RPM installation. OpenShift endpoints currently share the configured Kubernetes API/context. Configure namespace and Deployment names for each OpenShift Controller; the task container can be supplied explicitly if discovery is ambiguous.

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

For an AAP 2.4 RPM source and OpenShift target, use:

```yaml
credential_sync:
  ssh_binary: ssh
  oc_binary: oc
  source:
    transport: ssh
    host: controller24.example.com
    user: bridge
    port: 22
    identity_file: /path/to/bridge_ssh_key  # Optional; SSH agent/config also works
    become_user: awx
    awx_manage: /usr/bin/awx-manage
  target:
    transport: openshift
    namespace: aap-target
    deployment: target-controller-task
logging:
  log_payloads: false
```

SSH must reach a Controller node with the installed `awx-manage`, Controller settings, and database connectivity, rather than an execution-only node. Verify the executable path on the customer's installation and override `awx_manage` if needed. Provision SSH authentication and a verified host key in the runtime's known-hosts file before running. SSH uses batch mode, strict host-key checking, and no TTY. The login account needs non-interactive permission to run the helper as the Controller service account using `sudo -n -u awx`; set `become_user: null` when logging in as an account already able to run it. This is privileged access capable of reading Controller credentials; restrict it accordingly. No SSH password or private-key contents belong in this YAML.

The RPM helper loads the installation's Controller configuration, including its local application key (`/etc/tower/SECRET_KEY` on AAP 2.4 RPM). There is no need to copy that key into Bridge or Kubernetes. See [Red Hat's AAP 2.4 secret handling documentation](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.4/html/automation_controller_administration_guide/controller-secret-handling-and-connection-security). Both endpoints may use SSH, and the opposite OpenShift-to-RPM combination is also configurable.

```bash
aap-bridge --config config.yaml sync --organization demo-organization --with-credentials
aap-bridge --config config.yaml sync --organization demo-organization --with-credentials --apply
```

Both providers run the same installed Controller ORM/decryption helper with its local application key and database settings, using either `oc exec` or SSH. Neither exports or replaces a Controller master key or writes database rows directly. Request data and ephemeral keys travel through standard input rather than command arguments. Plan comparisons use per-run keyed fingerprints. Changed inputs are sealed to an ephemeral transport key, decrypted in local process memory, and written through the target API. Target decrypted fingerprints are checked after writes. Payload logging must be disabled.

Credentials must belong to the selected organization, and a matching compatible credential type must already exist on the target. Global credential types are not created or updated. External secret-manager input sources are rejected. Project credential references are remapped; missing job-template credential associations are added, while target-only associations remain.

## Review findings still to resolve

Use synthetic lab credentials while these findings remain unresolved:

- API and credential-reader Controller identities are not bound together. A misconfigured pod deployment or SSH host with colliding organization names and credential IDs can supply the wrong credential inputs and still pass the current fingerprint verification. Controller identity and credential metadata checks are required before broader use.
- A project with the same configured Git URL/branch but a different checkout revision is reported unchanged. The implementation does not request a fresh Git update before synchronizing templates that may reference new playbooks.
- Source and target pod access need separate Kubernetes contexts/API configuration for cross-cluster use.
- Live decryption, transfer, target re-encryption, and job-template interoperability have not been validated against the installed AAP versions, including AAP 2.4 RPM over SSH. Mock tests are not evidence of that compatibility.

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
