# Experimental organization synchronization

This feature branch adds an on-demand, one-way add/update sync for a named organization. It is work in progress for lab evaluation of credential-free static resources. It does not extend the existing migration compatibility matrix or establish tested AAP 2.4 to 2.7 migration support.

## Plan and apply

Use the existing source and target API configuration and tokens. The target URL must use its Controller API prefix, including `/api/controller/v2` when accessed through Gateway.

```bash
aap-bridge --config config.yaml sync --organization demo-organization
aap-bridge --config config.yaml sync --organization demo-organization --apply
```

The default command only plans changes. Applying requires both `--apply` and `dry_run: false` in the configuration. Reports include resource names, actions, and changed field names, without field values.

Supported resources are organizations, ordinary static inventories, static hosts, Git projects, and job templates. The implementation manages the field allowlists in `org_sync.py` and remaps target IDs using scoped natural keys. It preserves target-only resources and fields outside those allowlists. It does not delete resources, launch jobs, synchronize groups or inventory sources, migrate RBAC/workflows/schedules/surveys/execution environments, or install a recurring schedule. Renames create new target resources.

## Known limitations

This path excludes credentials and does not establish tested AAP 2.4 to 2.7 compatibility.
A project with matching Git URL/branch but a different checkout revision may be
reported unchanged; sync does not request a fresh Git update before synchronizing templates.
Live integration and representative scale validation remain required.

## Targeted tests

```bash
python -m unittest discover -s tests/unit -p 'test_org_sync*.py' -v
```
