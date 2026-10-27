"""Iterative sync checkpoints: timestamps, dependencies, failures, and secret rotation."""

import json
import sqlite3
import tempfile
import unittest
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from click.testing import CliRunner
from test_credential_sync import CredentialClient, Reader, source_snapshot
from test_org_sync import MemoryClient, fixture

from aap_migration.cli.commands.sync import sync as sync_command
from aap_migration.credential_sync import CredentialSession
from aap_migration.org_sync import SyncError, synchronize
from aap_migration.sync_checkpoint import (
    CheckpointError,
    SyncCheckpoint,
    modified_on,
    public_endpoint,
)


class Timestamped:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.clock = 100
        self.reads = []
        self.client = SimpleNamespace(aclose=AsyncMock())
        for rows in self.tables.values():
            for row in rows:
                row.setdefault("modified", "2026-10-27T10:00:00Z")

    def tick(self):
        self.clock += 1
        return (datetime(2026, 10, 27, 11, tzinfo=UTC) + timedelta(seconds=self.clock)).isoformat()

    def edit(self, kind, object_id, **changes):
        row = next(row for row in self.tables[kind] if row["id"] == object_id)
        row.update(deepcopy(changes))
        row["modified"] = self.tick()
        return row

    async def get(self, endpoint, params=None):
        self.reads.append(endpoint)
        return await super().get(endpoint, params)

    async def post(self, endpoint, json_data):
        result = await super().post(endpoint, json_data)
        if "id" in result:
            result = self.edit(endpoint.strip("/"), result["id"])
        return deepcopy(result)

    async def patch(self, endpoint, json_data):
        await super().patch(endpoint, json_data)
        kind, object_id = endpoint.strip("/").split("/")
        return deepcopy(self.edit(kind, int(object_id)))


class TimedClient(Timestamped, MemoryClient):
    pass


class TimedCredentialClient(Timestamped, CredentialClient):
    async def credential_write(self, endpoint, payload, method):
        result = await super().credential_write(endpoint, payload, method)
        row = next(row for row in self.tables["credentials"] if row["id"] == result["id"])
        result["modified"] = row["modified"]
        return result


class TestIncrementalSync(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "state/checkpoints.sqlite3"
        self.scope = {
            "source": "https://source/api/v2",
            "target": "https://target/api/controller/v2",
            "organization": "aap-bridge",
            "with_credentials": False,
        }
        self.source = TimedClient([fixture("revision-1")], writable=False)
        self.target = TimedClient([fixture("control")])

    async def run_sync(self, apply=True, full=False, credentials=False, scope=None):
        active_scope = {**(scope or self.scope), "with_credentials": credentials}
        session = None
        if credentials:
            session = CredentialSession(Reader(self.source), Reader(self.target))
        with SyncCheckpoint(self.path, active_scope, write=apply, full_resync=full) as checkpoint:
            result = await synchronize(
                self.source,
                self.target,
                "aap-bridge",
                apply=apply,
                checkpoint=checkpoint,
                credential_session=session,
            )
        return result, session

    def stored(self):
        with sqlite3.connect(self.path) as connection:
            return connection.execute("SELECT scope, data FROM org_sync ORDER BY scope").fetchall()

    async def test_first_run_baseline_then_noop_skips_every_resource(self):
        first, _ = await self.run_sync()
        self.assertEqual(first["incremental"], {"resources_skipped": 0, "resources_processed": 6})
        self.target.writes.clear()
        self.target.reads.clear()
        repeat, _ = await self.run_sync()
        self.assertEqual(repeat["incremental"], {"resources_skipped": 6, "resources_processed": 0})
        self.assertEqual(self.target.writes, [])
        self.assertFalse(
            any(
                endpoint.startswith("projects/") and endpoint != "projects/"
                for endpoint in self.target.reads
            )
        )
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)

    async def test_changed_host_only_is_processed(self):
        await self.run_sync()
        self.target.writes.clear()
        self.source.edit("hosts", 101, description="Changed after baseline")
        report, _ = await self.run_sync()
        self.assertEqual(report["incremental"]["resources_skipped"], 5)
        self.assertEqual(len(self.target.writes), 1)
        self.assertTrue(self.target.writes[0][1].startswith("hosts/"))

    async def test_new_template_uses_unchanged_parent_mappings(self):
        await self.run_sync()
        self.target.writes.clear()
        template = {
            **deepcopy(self.source.tables["job_templates"][0]),
            "id": 31,
            "name": "new-template",
            "modified": self.source.tick(),
        }
        self.source.tables["job_templates"].append(template)
        report, _ = await self.run_sync()
        self.assertEqual(report["incremental"]["resources_skipped"], 6)
        self.assertEqual(len(self.target.writes), 1)
        self.assertEqual(self.target.writes[0][1], "job_templates/")
        self.assertNotEqual(self.target.writes[0][2]["project"], 20)

    async def test_same_timestamp_on_multiple_updates_does_not_drop_either(self):
        await self.run_sync()
        self.target.writes.clear()
        for row in self.source.tables["hosts"]:
            row.update(description="same timestamp change", modified="2026-10-27T12:00:00Z")
        report, _ = await self.run_sync()
        self.assertEqual(report["incremental"]["resources_processed"], 2)
        self.assertEqual(len(self.target.writes), 2)

    async def test_timestamp_regression_is_still_a_change(self):
        await self.run_sync()
        self.target.writes.clear()
        self.source.tables["hosts"][0].update(
            description="restored snapshot", modified="2020-01-01T00:00:00Z"
        )
        await self.run_sync()
        self.assertEqual(len(self.target.writes), 1)

    async def test_missing_timestamp_always_falls_back_to_full_comparison(self):
        await self.run_sync()
        self.source.tables["hosts"][0].pop("modified")
        for _ in range(2):
            report, _ = await self.run_sync()
            self.assertEqual(report["incremental"]["resources_skipped"], 5)
            self.assertEqual(report["incremental"]["resources_processed"], 1)

    async def test_target_drift_is_repaired(self):
        await self.run_sync()
        host = self.target.tables["hosts"][-2]
        self.target.edit("hosts", host["id"], description="target drift")
        self.target.writes.clear()
        report, _ = await self.run_sync()
        self.assertEqual(report["incremental"]["resources_processed"], 1)
        self.assertEqual(len(self.target.writes), 1)
        self.assertNotEqual(host["description"], "target drift")

    async def test_deleted_target_host_is_recreated(self):
        await self.run_sync()
        removed = self.target.tables["hosts"].pop()
        self.target.writes.clear()
        report, _ = await self.run_sync()
        self.assertEqual(report["incremental"]["resources_processed"], 1)
        self.assertEqual(self.target.writes[0][0], "POST")
        self.assertNotEqual(self.target.tables["hosts"][-1]["id"], removed["id"])

    async def test_deleted_parent_is_recreated_and_template_reference_repaired(self):
        await self.run_sync()
        inventory = next(row for row in self.target.tables["inventories"] if row["id"] != 90)
        self.target.tables["inventories"].remove(inventory)
        self.target.tables["hosts"] = [
            row for row in self.target.tables["hosts"] if row["inventory"] != inventory["id"]
        ]
        self.target.tables["job_templates"][0]["inventory"] = None
        self.target.writes.clear()
        await self.run_sync()
        new_inventory = next(row for row in self.target.tables["inventories"] if row["id"] != 90)
        self.assertEqual(self.target.tables["job_templates"][0]["inventory"], new_inventory["id"])
        self.assertEqual(len(self.target.writes), 4)

    async def test_source_rename_reuses_tracked_target_id(self):
        await self.run_sync()
        target_id = next(
            row["id"]
            for row in self.target.tables["hosts"]
            if row["inventory"] != 90 and row["name"] == "app-01"
        )
        self.source.edit("hosts", 101, name="app-renamed")
        self.target.writes.clear()
        await self.run_sync()
        self.assertEqual(self.target.writes[0][1], f"hosts/{target_id}/")
        self.assertEqual(len(self.target.tables["hosts"]), 3)

    async def test_different_git_revision_is_never_cached_as_an_incremental_noop(self):
        await self.run_sync()
        self.source.tables["projects"][0]["scm_revision"] = "source-revision"
        selected = next(
            row for row in self.target.tables["projects"] if row["name"] == "Bridge Demo Playbooks"
        )
        selected["scm_revision"] = "different-target-revision"
        for _ in range(2):
            report, _ = await self.run_sync()
            self.assertEqual(report["incremental"]["resources_processed"], 1)
            self.assertEqual(report["incremental"]["resources_skipped"], 5)

    async def test_plan_never_creates_or_advances_checkpoint(self):
        await self.run_sync(apply=False)
        self.assertFalse(self.path.exists())
        self.assertFalse(self.path.parent.exists())
        await self.run_sync()
        before = self.stored()
        self.source.edit("hosts", 101, description="plan only")
        self.target.writes.clear()
        await self.run_sync(apply=False)
        self.assertEqual(self.stored(), before)
        self.assertEqual(self.target.writes, [])

    async def test_partial_failure_does_not_advance_checkpoint_or_repeat_successful_write(self):
        await self.run_sync()
        before = self.stored()
        for row in self.source.tables["hosts"]:
            row.update(description="changed", modified=self.source.tick())
        original_patch = self.target.patch
        count = 0

        async def fail_second(endpoint, json_data):
            nonlocal count
            count += 1
            if count == 2:
                raise RuntimeError("synthetic apply failure")
            return await original_patch(endpoint, json_data)

        self.target.patch = fail_second
        self.target.writes.clear()
        with self.assertRaises(RuntimeError):
            await self.run_sync()
        self.assertEqual(self.stored(), before)
        self.target.patch = original_patch
        self.target.writes.clear()
        await self.run_sync()
        self.assertEqual(len(self.target.writes), 1)

    async def test_full_resync_rechecks_values_even_if_timestamp_did_not_change(self):
        await self.run_sync()
        self.source.tables["hosts"][0]["description"] = "out-of-band edit"
        report, _ = await self.run_sync(full=True)
        self.assertEqual(report["incremental"]["resources_skipped"], 0)
        self.assertEqual(
            next(
                row
                for row in self.target.tables["hosts"]
                if row["inventory"] != 90 and row["name"] == "app-01"
            )["description"],
            "out-of-band edit",
        )

    async def test_checkpoint_scope_does_not_cross_target_endpoints(self):
        await self.run_sync()
        report, _ = await self.run_sync(
            scope={**self.scope, "target": "https://different-target/api/v2"}
        )
        self.assertEqual(report["incremental"]["resources_skipped"], 0)
        self.assertEqual(len(self.stored()), 2)

    async def test_new_organization_identity_requires_explicit_full_resync(self):
        await self.run_sync()
        org = next(
            row for row in self.target.tables["organizations"] if row["name"] == "aap-bridge"
        )
        old_id = org["id"]
        org["id"] = 9876
        for kind in ("inventories", "projects", "job_templates"):
            for row in self.target.tables[kind]:
                if row.get("organization") == old_id:
                    row["organization"] = 9876
        self.target.writes.clear()
        with self.assertRaises(CheckpointError):
            await self.run_sync()
        self.assertEqual(self.target.writes, [])
        report, _ = await self.run_sync(full=True)
        self.assertEqual(report["incremental"]["resources_skipped"], 0)

    async def test_unverified_target_write_cannot_advance_checkpoint(self):
        await self.run_sync()
        before = self.stored()
        self.source.edit("hosts", 101, description="source change")

        async def ignored_write(endpoint, json_data):
            return await self.target.get(endpoint)

        self.target.patch = ignored_write
        with self.assertRaisesRegex(SyncError, "did not verify"):
            await self.run_sync()
        self.assertEqual(self.stored(), before)

    async def test_credential_rotation_and_association_changes_are_incremental(self):
        self.source = TimedCredentialClient([source_snapshot()], writable=False)
        self.target = TimedCredentialClient([fixture("control")], type_id=303)
        await self.run_sync(credentials=True)
        self.target.writes.clear()
        report, session = await self.run_sync(credentials=True)
        self.assertEqual(report["incremental"]["resources_skipped"], 7)
        self.assertTrue(
            all(
                not ids
                for reader in (session.source_reader, session.target_reader)
                for _, ids, _ in reader.calls
            )
        )
        self.assertEqual(self.target.writes, [])
        self.source.edit(
            "credentials", 54, inputs={"username": "bridge-demo", "password": "test-only-rotated"}
        )
        report, session = await self.run_sync(credentials=True)
        self.assertEqual(report["incremental"]["resources_processed"], 1)
        self.assertEqual(report["incremental"]["resources_skipped"], 6)
        self.assertEqual(len(self.target.writes), 1)
        self.assertEqual(self.target.writes[0][0], "PATCH")
        template = self.target.tables["job_templates"][0]
        # A many-to-many edit need not bump the template's modified field.
        template["_credential_ids"] = []
        self.target.writes.clear()
        await self.run_sync(credentials=True)
        self.assertEqual(len(self.target.writes), 1)
        self.assertTrue(self.target.writes[0][1].endswith("/credentials/"))
        stored = str(self.stored())
        self.assertNotIn("test-only-rotated", stored)
        self.assertNotIn("test-only-secret-v1", stored)
        self.assertNotIn("username", stored)
        self.assertNotIn("inputs", stored)

    async def test_source_association_addition_is_detected_without_timestamp_change(self):
        data = source_snapshot()
        data["job_templates"][0]["_credential_ids"] = []
        self.source = TimedCredentialClient([data], writable=False)
        self.target = TimedCredentialClient([fixture("control")], type_id=303)
        await self.run_sync(credentials=True)
        self.source.tables["job_templates"][0]["_credential_ids"] = [54]
        self.target.writes.clear()
        report, session = await self.run_sync(credentials=True)
        self.assertEqual(report["incremental"]["resources_skipped"], 6)
        self.assertEqual(len(self.target.writes), 1)
        self.assertTrue(self.target.writes[0][1].endswith("/credentials/"))
        self.assertTrue(
            all(
                not ids
                for reader in (session.source_reader, session.target_reader)
                for _, ids, _ in reader.calls
            )
        )

    async def test_deleted_target_credential_is_recreated_and_reattached(self):
        self.source = TimedCredentialClient([source_snapshot()], writable=False)
        self.target = TimedCredentialClient([fixture("control")], type_id=303)
        await self.run_sync(credentials=True)
        removed_id = self.target.tables["credentials"].pop()["id"]
        self.target.writes.clear()
        await self.run_sync(credentials=True)
        created = self.target.tables["credentials"][0]
        self.assertNotEqual(created["id"], removed_id)
        self.assertEqual(
            self.target.tables["job_templates"][0]["_credential_ids"][-1], created["id"]
        )
        self.assertEqual(len(self.target.writes), 2)

    async def test_reused_credential_session_recomputes_skip_decisions(self):
        self.source = TimedCredentialClient([source_snapshot()], writable=False)
        self.target = TimedCredentialClient([fixture("control")], type_id=303)
        await self.run_sync(credentials=True)
        session = CredentialSession(Reader(self.source), Reader(self.target))
        scope = {**self.scope, "with_credentials": True}
        with SyncCheckpoint(self.path, scope, write=True) as checkpoint:
            await synchronize(
                self.source,
                self.target,
                "aap-bridge",
                apply=True,
                checkpoint=checkpoint,
                credential_session=session,
            )
        self.source.edit(
            "credentials", 54, inputs={"username": "bridge-demo", "password": "test-only-next-run"}
        )
        self.target.writes.clear()
        with SyncCheckpoint(self.path, scope, write=True) as checkpoint:
            report = await synchronize(
                self.source,
                self.target,
                "aap-bridge",
                apply=True,
                checkpoint=checkpoint,
                credential_session=session,
            )
        self.assertEqual(report["incremental"]["resources_processed"], 1)
        self.assertEqual(len(self.target.writes), 1)
        self.assertEqual(
            self.target.tables["credentials"][0]["inputs"]["password"], "test-only-next-run"
        )


class TestCheckpointUtilities(unittest.TestCase):
    def test_timestamp_normalization_and_alias(self):
        self.assertEqual(
            modified_on({"modified": "2026-10-27T10:00:00Z"}),
            modified_on({"modified_on": "2026-10-27T12:00:00+02:00"}),
        )
        for raw in (None, "garbage", "2026-10-27T10:00:00"):
            self.assertIsNone(modified_on({"modified": raw}))

    def test_endpoint_identity_never_stores_embedded_credentials(self):
        for url in ("https://user:password@source/api/v2", "https://source/api/v2?token=secret"):
            with self.assertRaises(CheckpointError):
                public_endpoint(url)

    def test_two_apply_runs_cannot_share_the_same_checkpoint_concurrently(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.sqlite3"
            with SyncCheckpoint(path, {"organization": "test"}, write=True):
                with self.assertRaises(CheckpointError):
                    with SyncCheckpoint(path, {"organization": "test"}, write=True):
                        self.fail("Concurrent apply was allowed")

    def test_invalid_state_path_does_not_change_existing_files_or_directories(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder) / "directory"
            directory.mkdir(mode=0o755)
            file = Path(folder) / "ordinary-file.txt"
            file.write_text("Not a checkpoint")
            for path in (directory, file):
                before = path.stat().st_mode
                with self.assertRaises(CheckpointError):
                    with SyncCheckpoint(path, {"organization": "test"}, write=True):
                        self.fail("Invalid state path accepted")
                self.assertEqual(path.stat().st_mode, before)
            self.assertEqual(file.read_text(), "Not a checkpoint")


class TestIncrementalCLI(unittest.TestCase):
    def test_alias_plan_is_read_only_and_apply_saves_a_baseline(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.sqlite3"
            source = TimedClient([fixture("revision-1")], writable=False)
            target = TimedClient()
            ctx = SimpleNamespace(
                config_path=Path("sync.yaml"),
                config=SimpleNamespace(
                    dry_run=False,
                    source=SimpleNamespace(url="https://source/api/v2"),
                    target=SimpleNamespace(url="https://target/api/v2"),
                ),
                source_client=source,
                target_client=target,
            )
            args = ["--organization", "aap-bridge", "--modified-on", "--sync-state", str(path)]
            plan = CliRunner().invoke(sync_command, args, obj=ctx)
            self.assertEqual(plan.exit_code, 0, plan.output)
            self.assertFalse(json.loads(plan.output)["incremental"]["checkpoint_saved"])
            self.assertFalse(path.exists())
            applied = CliRunner().invoke(sync_command, args + ["--apply"], obj=ctx)
            self.assertEqual(applied.exit_code, 0, applied.output)
            self.assertTrue(json.loads(applied.output)["incremental"]["checkpoint_saved"])

    def test_full_resync_cannot_be_requested_without_incremental(self):
        ctx = SimpleNamespace(config_path=Path("sync.yaml"), config=SimpleNamespace(dry_run=False))
        result = CliRunner().invoke(
            sync_command, ["--organization", "test", "--full-resync"], obj=ctx
        )
        self.assertEqual(result.exit_code, 1)
        self.assertIn("require --incremental", result.output)


if __name__ == "__main__":
    unittest.main()
