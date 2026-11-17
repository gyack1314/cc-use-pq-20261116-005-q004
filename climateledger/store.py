from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
import json
import sqlite3

from .registry import ContractError, UnitRule
from .intake import canonical_bytes


SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS indicators (key TEXT PRIMARY KEY, contract TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sources (key TEXT PRIMARY KEY, contract TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS unit_rules (
    indicator TEXT NOT NULL, source_unit TEXT NOT NULL, factor TEXT NOT NULL, offset TEXT NOT NULL,
    PRIMARY KEY(indicator, source_unit), FOREIGN KEY(indicator) REFERENCES indicators(key));
CREATE TABLE IF NOT EXISTS batches (
    id INTEGER PRIMARY KEY, source TEXT NOT NULL, external_id TEXT NOT NULL, revision INTEGER NOT NULL,
    payload_hash TEXT NOT NULL, status TEXT NOT NULL, row_count INTEGER NOT NULL,
    metadata TEXT NOT NULL, findings TEXT NOT NULL, received_at TEXT NOT NULL,
    approved_at TEXT, published_version INTEGER,
    UNIQUE(source, external_id, revision), FOREIGN KEY(source) REFERENCES sources(key));
CREATE INDEX IF NOT EXISTS batches_lookup ON batches(source, external_id, revision DESC);
CREATE TABLE IF NOT EXISTS measurements (
    batch_id INTEGER NOT NULL, line INTEGER NOT NULL, indicator TEXT NOT NULL, period TEXT NOT NULL,
    value TEXT NOT NULL, unit TEXT NOT NULL, raw_value TEXT NOT NULL, raw_unit TEXT NOT NULL,
    factor TEXT NOT NULL, offset TEXT NOT NULL,
    PRIMARY KEY(batch_id, line), FOREIGN KEY(batch_id) REFERENCES batches(id),
    FOREIGN KEY(indicator) REFERENCES indicators(key));
CREATE INDEX IF NOT EXISTS measurements_series ON measurements(indicator, period);
CREATE TABLE IF NOT EXISTS releases (
    version INTEGER PRIMARY KEY, created_at TEXT NOT NULL, manifest TEXT NOT NULL, digest TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS release_entries (
    version INTEGER NOT NULL, source TEXT NOT NULL, indicator TEXT NOT NULL, period TEXT NOT NULL,
    batch_id INTEGER NOT NULL, line INTEGER NOT NULL, value TEXT NOT NULL, unit TEXT NOT NULL,
    PRIMARY KEY(version, source, indicator, period), FOREIGN KEY(version) REFERENCES releases(version));
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY, at TEXT NOT NULL, kind TEXT NOT NULL, subject TEXT NOT NULL, detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS transfers (
    id INTEGER PRIMARY KEY, source TEXT NOT NULL, external_id TEXT NOT NULL, payload_hash TEXT NOT NULL,
    format TEXT NOT NULL, columns TEXT NOT NULL, row_count INTEGER NOT NULL, size INTEGER NOT NULL,
    segment_count INTEGER NOT NULL, metadata TEXT NOT NULL, status TEXT NOT NULL,
    batch_id INTEGER, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    UNIQUE(source, external_id, payload_hash),
    FOREIGN KEY(source) REFERENCES sources(key), FOREIGN KEY(batch_id) REFERENCES batches(id));
CREATE TABLE IF NOT EXISTS transfer_segments (
    transfer_id INTEGER NOT NULL, segment_index INTEGER NOT NULL, filename TEXT NOT NULL,
    offset INTEGER NOT NULL, length INTEGER NOT NULL, expected_digest TEXT NOT NULL,
    actual_digest TEXT NOT NULL, segment_blob BLOB NOT NULL, received_at TEXT NOT NULL,
    PRIMARY KEY(transfer_id, segment_index), FOREIGN KEY(transfer_id) REFERENCES transfers(id));
CREATE TABLE IF NOT EXISTS transfer_conflicts (
    id INTEGER PRIMARY KEY, transfer_id INTEGER NOT NULL, at TEXT NOT NULL, segment_index INTEGER,
    stage TEXT NOT NULL, code TEXT NOT NULL, detail TEXT NOT NULL, evidence TEXT NOT NULL,
    FOREIGN KEY(transfer_id) REFERENCES transfers(id));
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class LedgerStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(self.path), timeout=20, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA busy_timeout=20000")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript(SCHEMA)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield self.connection
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    def event(self, kind: str, subject: str, detail: dict[str, Any]) -> None:
        self.connection.execute("INSERT INTO events(at,kind,subject,detail) VALUES(?,?,?,?)",
                                (utc_now(), kind, subject, canonical_bytes(detail).decode()))

    def events(self, subject: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM events"
        args: tuple = ()
        if subject is not None:
            query += " WHERE subject=?"
            args = (subject,)
        query += " ORDER BY id"
        return [{**dict(row), "detail": json.loads(row["detail"])} for row in self.connection.execute(query, args)]

    def register_contract(self, table: str, key: str, contract: str) -> None:
        if table not in {"sources", "indicators"}:
            raise ValueError("unsupported contract table")
        with self.transaction():
            existing = self.get_contract(table, key)
            if existing is not None and existing != contract:
                raise ContractError("contract_immutable", f"{table} {key} already has a different contract")
            if existing is None:
                self.connection.execute(f"INSERT INTO {table}(key,contract) VALUES(?,?)", (key, contract))
                self.event("register_" + table, key, json.loads(contract))

    def get_contract(self, table: str, key: str) -> str | None:
        if table not in {"sources", "indicators"}:
            raise ValueError("unsupported contract table")
        row = self.connection.execute(f"SELECT contract FROM {table} WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def list_contracts(self, table: str) -> list[str]:
        if table not in {"sources", "indicators"}:
            raise ValueError("unsupported contract table")
        return [row[0] for row in self.connection.execute(f"SELECT contract FROM {table} ORDER BY key")]

    def register_unit(self, rule: UnitRule) -> None:
        with self.transaction():
            old = self.get_unit(rule.indicator, rule.source_unit)
            if old is not None and old != rule:
                raise ContractError("rule_immutable", "unit rule already differs")
            if old is None:
                self.connection.execute("INSERT INTO unit_rules VALUES(?,?,?,?)",
                                        (rule.indicator, rule.source_unit, rule.factor, rule.offset))
                self.event("register_unit", rule.indicator, asdict(rule))

    def get_unit(self, indicator: str, source_unit: str) -> UnitRule | None:
        row = self.connection.execute("SELECT * FROM unit_rules WHERE indicator=? AND source_unit=?",
                                      (indicator, source_unit)).fetchone()
        return UnitRule(*row) if row else None

    def latest_batch(self, source: str, external_id: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM batches WHERE source=? AND external_id=? ORDER BY revision DESC LIMIT 1",
                                      (source, external_id)).fetchone()
        return self._batch(row) if row else None

    def batch(self, batch_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if row is None:
            raise ContractError("unknown_batch", f"batch {batch_id} does not exist")
        return self._batch(row)

    def _batch(self, row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["metadata"] = json.loads(result["metadata"])
        result["findings"] = json.loads(result["findings"])
        return result

    def list_batches(self, status: str | None = None) -> list[dict[str, Any]]:
        if status:
            rows = self.connection.execute("SELECT * FROM batches WHERE status=? ORDER BY id", (status,))
        else:
            rows = self.connection.execute("SELECT * FROM batches ORDER BY id")
        return [self._batch(row) for row in rows]

    def insert_batch(self, envelope, assessment, measurements) -> dict[str, Any]:
        with self.transaction():
            old = self.latest_batch(envelope.source, envelope.batch_id)
            if old and old["payload_hash"] == envelope.payload_hash:
                return old
            revision = 1 if old is None else old["revision"] + 1
            cursor = self.connection.execute(
                "INSERT INTO batches(source,external_id,revision,payload_hash,status,row_count,metadata,findings,received_at) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (envelope.source, envelope.batch_id, revision, envelope.payload_hash,
                 assessment.status, envelope.declared_count,
                 canonical_bytes(envelope.metadata).decode(),
                 canonical_bytes([item.document() for item in assessment.findings]).decode(), utc_now()))
            batch_id = cursor.lastrowid
            self.connection.executemany(
                "INSERT INTO measurements VALUES(?,?,?,?,?,?,?,?,?,?)",
                [(batch_id, item.line, item.indicator, item.period, item.value, item.unit,
                  item.raw_value, item.raw_unit, item.rule_factor, item.rule_offset)
                 for item in measurements])
            self.event("ingest", f"batch:{batch_id}", {"status": assessment.status,
                       "revision": revision, "payload_hash": envelope.payload_hash})
            return self.batch(batch_id)

    def approve(self, batch_id: int, expected_revision: int) -> dict[str, Any]:
        with self.transaction():
            batch = self.batch(batch_id)
            latest = self.latest_batch(batch["source"], batch["external_id"])
            if latest["id"] != batch_id or batch["revision"] != expected_revision:
                raise ContractError("stale_revision", "batch is not the latest expected revision")
            if batch["status"] == "approved":
                return batch
            if batch["status"] != "review":
                raise ContractError("not_reviewable", "only review batches can be approved")
            self.connection.execute("UPDATE batches SET status='approved',approved_at=? WHERE id=?",
                                    (utc_now(), batch_id))
            self.event("approve", f"batch:{batch_id}", {"revision": expected_revision})
            return self.batch(batch_id)

    def measurements(self, batch_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM measurements WHERE batch_id=? ORDER BY line", (batch_id,))]

    def current_measurement(self, source: str, indicator: str, period: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT e.* FROM release_entries e JOIN releases r ON r.version=e.version "
            "WHERE e.source=? AND e.indicator=? AND e.period=? ORDER BY e.version DESC LIMIT 1",
            (source, indicator, period)).fetchone()
        return dict(row) if row else None

    def latest_release(self) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM releases ORDER BY version DESC LIMIT 1").fetchone()
        return self._release(row) if row else None

    def release(self, version: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM releases WHERE version=?", (version,)).fetchone()
        if row is None:
            raise ContractError("unknown_release", f"release {version} does not exist")
        return self._release(row)

    def _release(self, row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["manifest"] = json.loads(value["manifest"])
        return value

    def release_entries(self, version: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM release_entries WHERE version=? ORDER BY source,indicator,period", (version,))]

    def publish(self, batch_id: int, expected_revision: int, manifest: dict[str, Any],
                entries: list[dict[str, Any]], digest: str) -> dict[str, Any]:
        with self.transaction():
            batch = self.batch(batch_id)
            latest = self.latest_batch(batch["source"], batch["external_id"])
            if batch["published_version"]:
                return self.release(batch["published_version"])
            if latest["id"] != batch_id or batch["revision"] != expected_revision:
                raise ContractError("stale_revision", "cannot publish a superseded revision")
            if batch["status"] not in {"accepted", "approved"}:
                raise ContractError("quality_gate", "batch is not accepted or approved")
            current_version = self.latest_release()["version"] if self.latest_release() else 0
            version = current_version + 1
            if manifest["version"] != version:
                raise ContractError("release_race", "release version changed while preparing")
            self.connection.execute("INSERT INTO releases VALUES(?,?,?,?)",
                                    (version, utc_now(), canonical_bytes(manifest).decode(), digest))
            self.connection.executemany("INSERT INTO release_entries VALUES(?,?,?,?,?,?,?,?)",
                [(version, item["source"], item["indicator"], item["period"],
                  item["batch_id"], item["line"], item["value"], item["unit"]) for item in entries])
            self.connection.execute("UPDATE batches SET status='published',published_version=? WHERE id=?",
                                    (version, batch_id))
            self.event("publish", f"batch:{batch_id}", {"version": version, "digest": digest})
            return self.release(version)

    def _transfer(self, row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["columns"] = json.loads(result["columns"])
        result["metadata"] = json.loads(result["metadata"])
        return result

    def get_transfer(self, source: str, external_id: str, payload_hash: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM transfers WHERE source=? AND external_id=? AND payload_hash=?",
            (source, external_id, payload_hash)).fetchone()
        return self._transfer(row) if row else None

    def upsert_transfer(self, manifest) -> dict[str, Any]:
        with self.transaction():
            existing = self.get_transfer(manifest.source, manifest.batch_id, manifest.payload_hash)
            if existing is not None:
                self.connection.execute(
                    "UPDATE transfers SET row_count=?, size=?, segment_count=?, columns=?, metadata=?, updated_at=? "
                    "WHERE id=?",
                    (manifest.row_count, manifest.size, len(manifest.segments),
                     canonical_bytes(manifest.columns).decode(),
                     canonical_bytes(manifest.metadata).decode(), utc_now(), existing["id"]))
                return self.get_transfer(manifest.source, manifest.batch_id, manifest.payload_hash)
            cursor = self.connection.execute(
                "INSERT INTO transfers(source,external_id,payload_hash,format,columns,row_count,size,segment_count,"
                "metadata,status,batch_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (manifest.source, manifest.batch_id, manifest.payload_hash, manifest.format,
                 canonical_bytes(manifest.columns).decode(), manifest.row_count, manifest.size,
                 len(manifest.segments), canonical_bytes(manifest.metadata).decode(),
                 "receiving", None, utc_now(), utc_now()))
            transfer_id = cursor.lastrowid
            self.event("transfer_register", f"transfer:{transfer_id}",
                       {"source": manifest.source, "external_id": manifest.batch_id,
                        "payload_hash": manifest.payload_hash, "segment_count": len(manifest.segments)})
            return self.get_transfer(manifest.source, manifest.batch_id, manifest.payload_hash)

    def get_segment(self, transfer_id: int, index: int) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM transfer_segments WHERE transfer_id=? AND segment_index=?",
            (transfer_id, index)).fetchone()
        return dict(row) if row else None

    def put_segment(self, transfer_id: int, descriptor, actual_digest: str, blob: bytes) -> bool:
        with self.transaction():
            if self.get_segment(transfer_id, descriptor.index) is not None:
                return False
            self.connection.execute(
                "INSERT INTO transfer_segments VALUES(?,?,?,?,?,?,?,?,?)",
                (transfer_id, descriptor.index, descriptor.filename, descriptor.offset, descriptor.length,
                 descriptor.digest, actual_digest, blob, utc_now()))
            self.connection.execute("UPDATE transfers SET updated_at=? WHERE id=?", (utc_now(), transfer_id))
            self.event("segment_received", f"transfer:{transfer_id}",
                       {"segment_index": descriptor.index, "digest": actual_digest, "length": len(blob)})
            return True

    def list_segments(self, transfer_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            "SELECT transfer_id,segment_index,filename,offset,length,expected_digest,actual_digest,received_at "
            "FROM transfer_segments WHERE transfer_id=? ORDER BY segment_index", (transfer_id,))]

    def segment_blobs(self, transfer_id: int) -> dict[int, bytes]:
        return {row["segment_index"]: row["segment_blob"] for row in self.connection.execute(
            "SELECT segment_index, segment_blob FROM transfer_segments WHERE transfer_id=?", (transfer_id,))}

    def add_conflict(self, transfer_id: int, segment_index: int | None, stage: str,
                     code: str, detail: str, evidence: dict[str, Any]) -> None:
        with self.transaction():
            cursor = self.connection.execute(
                "INSERT INTO transfer_conflicts(transfer_id,at,segment_index,stage,code,detail,evidence) "
                "VALUES(?,?,?,?,?,?,?)",
                (transfer_id, utc_now(), segment_index, stage, code, detail,
                 canonical_bytes(evidence).decode()))
            self.event("transfer_conflict", f"transfer:{transfer_id}",
                       {"conflict_id": cursor.lastrowid, "segment_index": segment_index,
                        "stage": stage, "code": code})

    def list_conflicts(self, transfer_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM transfer_conflicts WHERE transfer_id=? ORDER BY id", (transfer_id,))
        return [{**dict(row), "evidence": json.loads(row["evidence"])} for row in rows]

    def mark_transfer_completed(self, transfer_id: int, batch_id: int) -> None:
        with self.transaction():
            self.connection.execute(
                "UPDATE transfers SET status='completed', batch_id=?, updated_at=? WHERE id=?",
                (batch_id, utc_now(), transfer_id))
            self.event("transfer_completed", f"transfer:{transfer_id}", {"batch_id": batch_id})

    def list_transfers(self, status: str | None = None) -> list[dict[str, Any]]:
        if status:
            rows = self.connection.execute("SELECT * FROM transfers WHERE status=? ORDER BY id", (status,))
        else:
            rows = self.connection.execute("SELECT * FROM transfers ORDER BY id")
        return [self._transfer(row) for row in rows]
