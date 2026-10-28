"""Transactional, metadata-only checkpoints for iterative organization sync."""

import hashlib
import json
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


class CheckpointError(Exception):
    """A checkpoint could not be used safely."""


def public_endpoint(url):
    parts = urlsplit(url)
    if parts.username or parts.password or parts.query or parts.fragment:
        raise CheckpointError(
            "Incremental API URLs must not contain credentials, queries, or fragments."
        )
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/"), "", ""))


def modified_on(row):
    """AAP calls the field modified; accept modified_on for compatible adapters."""
    raw = row.get("modified") or row.get("modified_on")
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(UTC).isoformat(timespec="microseconds")
    except ValueError:
        return None


def links(row):
    # Only foreign IDs and Git revision metadata, never inputs/variables/tokens.
    result = {
        key: row[key]
        for key in (
            "organization",
            "inventory",
            "project",
            "credential",
            "credential_type",
            "scm_revision",
        )
        if key in row
    }
    if "_credential_ids" in row:
        result["credentials"] = sorted(set(row["_credential_ids"]))
    return result


class SyncCheckpoint:
    """One successful baseline per endpoint pair, organization, and provider mode.

    SQLite serializes apply runs and rolls back on failure/crash. Plans open an
    existing checkpoint read-only and never create a checkpoint or its directory.
    """

    def __init__(self, path, scope, write=False, full_resync=False):
        self.path = Path(path).expanduser().absolute()
        self.scope = scope
        self.scope_key = hashlib.sha256(json.dumps(scope, sort_keys=True).encode()).hexdigest()
        self.write, self.full_resync = write, full_resync
        self.connection = None
        self.records = {}
        self.binding = None
        self.ready = self.committed = False

    def __enter__(self):
        self.ready = self.committed = False
        self.records, self.binding = {}, None
        try:
            if self.path.is_dir() or self.path.is_symlink():
                raise ValueError()
            if self.path.exists() and self.path.stat().st_size:
                with self.path.open("rb") as stream:
                    if stream.read(16) != b"SQLite format 3\x00":
                        raise ValueError()
            if self.write:
                self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                try:
                    fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                    os.close(fd)
                except FileExistsError:
                    pass
                os.chmod(self.path, 0o600)
                self.connection = sqlite3.connect(self.path, timeout=0, isolation_level=None)
                self.connection.execute("BEGIN IMMEDIATE")
                self.connection.execute(
                    "CREATE TABLE IF NOT EXISTS org_sync (scope TEXT PRIMARY KEY, data TEXT NOT NULL)"
                )
            elif self.path.exists():
                self.connection = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)
            if self.connection:
                try:
                    row = self.connection.execute(
                        "SELECT data FROM org_sync WHERE scope = ?", (self.scope_key,)
                    ).fetchone()
                except sqlite3.OperationalError as error:
                    if str(error) != "no such table: org_sync":
                        raise
                    row = None
                if row:
                    data = json.loads(row[0])
                    if data.get("version") != 1 or data.get("scope") != self.scope:
                        raise ValueError()
                    if not isinstance(data.get("records"), dict) or not isinstance(
                        data.get("binding"), dict
                    ):
                        raise ValueError()
                    self.records = data["records"]
                    self.binding = data["binding"]
            return self
        except Exception:
            if self.connection:
                self.connection.close()
                self.connection = None
            raise CheckpointError(
                "Cannot open sync checkpoint; check its format, permissions, or another running apply."
            ) from None

    def __exit__(self, error_type, error, traceback):
        try:
            if self.connection and self.write:
                if error_type is None and self.ready:
                    payload = {
                        "version": 1,
                        "scope": self.scope,
                        "binding": self.binding,
                        "records": self.records,
                    }
                    self.connection.execute(
                        "INSERT OR REPLACE INTO org_sync(scope, data) VALUES (?, ?)",
                        (self.scope_key, json.dumps(payload, sort_keys=True)),
                    )
                    self.connection.execute("COMMIT")
                    self.committed = True
                else:
                    self.connection.execute("ROLLBACK")
        except Exception:
            raise CheckpointError(
                "Could not save sync checkpoint; the previous successful baseline remains authoritative."
            ) from None
        finally:
            if self.connection:
                self.connection.close()
                self.connection = None

    def bind(self, source, target):
        current = {
            "source_organization_id": source["id"],
            "target_organization_id": target["id"] if target else None,
        }
        if self.binding and self.binding != current:
            if not self.full_resync:
                raise CheckpointError(
                    "Organization identity changed or target was removed; use --full-resync to rebuild the baseline."
                )
            self.records = {}
        self.binding = current

    def target_for(self, kind, source, target_rows):
        record = self.records.get(kind, {}).get(str(source["id"]))
        if record:
            if isinstance(target_rows, dict):
                return target_rows.get(record["target_id"])
            return next((row for row in target_rows if row["id"] == record["target_id"]), None)
        return None

    def can_skip(self, kind, source, target, parents=None):
        if self.full_resync or target is None:
            return False
        record = self.records.get(kind, {}).get(str(source["id"]))
        source_time, target_time = modified_on(source), modified_on(target)
        if not record or source_time is None or target_time is None:
            return False
        if kind == "projects" and target.get("status") != "successful":
            return False
        if (
            kind == "projects"
            and source.get("scm_revision")
            and target.get("scm_revision")
            and source["scm_revision"] != target["scm_revision"]
        ):
            return False
        return (
            record["target_id"] == target["id"]
            and record["modified_on"] == source_time
            and record["target_modified_on"] == target_time
            and record["source_links"] == links(source)
            and record["target_links"] == links(target)
            and all(target.get(key) == value for key, value in (parents or {}).items())
        )

    def complete(self, source, target, mappings):
        """Record only IDs, times and dependency metadata after a successful apply."""
        records = {}
        for kind, id_map in mappings.items():
            source_rows = [source[kind]] if kind == "organizations" else source.get(kind, [])
            target_rows = [target[kind]] if kind == "organizations" else target.get(kind, [])
            target_index = {row["id"]: row for row in target_rows}
            records[kind] = {}
            for row in source_rows:
                current = target_index.get(id_map[row["id"]])
                if current is None:
                    raise CheckpointError(
                        "A target resource disappeared during synchronization; checkpoint not advanced."
                    )
                records[kind][str(row["id"])] = {
                    "target_id": current["id"],
                    "modified_on": modified_on(row),
                    "target_modified_on": modified_on(current),
                    "source_links": links(row),
                    "target_links": links(current),
                }
        self.binding = {
            "source_organization_id": source["organizations"]["id"],
            "target_organization_id": target["organizations"]["id"],
        }
        self.records = records
        self.ready = True
