# Experimental organization synchronization

This feature branch adds an on-demand, one-way add/update sync for a named organization. It is work in progress for lab evaluation of credential-free static resources. It does not extend the existing migration compatibility matrix or establish tested AAP 2.4 to 2.7 migration support.

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

State defaults to `.aap-bridge-sync/checkpoints.sqlite3`, independently of the original migration's PostgreSQL state. Use `--sync-state /path/to/checkpoints.sqlite3` with `--incremental` to choose another location. Each source/target API pair, organization has a separate scope. Changing an organization identity requires an explicit full resync. API tokens are not part of the scope, so token rotation does not discard the baseline.

Plans never create or advance a checkpoint. Apply holds a SQLite write transaction and advances the baseline only after the run and final managed-field/dependency checks succeed. A failed or interrupted run retains its previous checkpoint; retry reconciles partially completed target changes rather than recreating them. Concurrent applies using the same state file are rejected. Checkpoint files are created with mode `0600`.

To deliberately recheck every supported resource and rebuild the successful baseline:

```bash
aap-bridge --config config.yaml sync --organization demo-organization --incremental --full-resync --apply
```

Use a full resync for periodic audits or after changes that bypass normal modification timestamps. Deletions are not propagated.

This implementation continues to enumerate source/target resources and associations for scope checks, dependency mapping, missing targets, and drift detection. It reduces reconciliation work; it does not yet replace all enumeration with server-side `modified__gte` queries. Git revision mismatches are not cached as incremental no-ops, but the Git checkout refresh limitation below remains. Keep the checkpoint if you want rename mappings and incremental progress preserved between runs.

Reports include `incremental.resources_skipped`, `resources_processed`, and `checkpoint_saved`. Without `--incremental`, the existing full-comparison behavior remains available.

## Known limitations

This path excludes credentials and does not establish tested AAP 2.4 to 2.7 compatibility.
A project with matching Git URL/branch but a different checkout revision may be
reported unchanged; sync does not request a fresh Git update before synchronizing templates.
Live integration and representative scale validation remain required.

## Targeted tests

```bash
python -m unittest discover -s tests/unit -p 'test_org_sync*.py' -v
```

```bash
python -m unittest discover -s tests/unit -p test_incremental_sync.py -v
```
