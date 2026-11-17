from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any

from .intake import (BatchEnvelope, TransferManifest, assemble_payload, load_transfer_manifest,
                     parse_envelope_rows, read_segment)
from .registry import ContractError


STRUCTURAL_FIELDS = ("format", "row_count", "size", "segment_count", "columns")


class TransferManager:
    """Drives segmented ingestion with durable, identity-bound checkpoints."""

    def __init__(self, store, registry, normalizer, quality):
        self.store = store
        self.registry = registry
        self.normalizer = normalizer
        self.quality = quality

    def register(self, manifest: TransferManifest) -> dict[str, Any]:
        existing = self.store.get_transfer(manifest.source, manifest.batch_id, manifest.payload_hash)
        if existing is None:
            self.registry.source(manifest.source)
            return self.store.upsert_transfer(manifest)
        self._require_same_structure(existing, manifest)
        return self.store.upsert_transfer(manifest)

    def receive_segment(self, manifest_path: str | Path, index: int) -> dict[str, Any]:
        manifest = load_transfer_manifest(manifest_path)
        transfer = self.register(manifest)
        if index not in range(len(manifest.segments)):
            raise ContractError("segment_index", f"segment {index} is outside the manifest")
        if transfer["status"] == "completed":
            return {"idempotent": True, "transfer": self.status_report(transfer["id"]),
                    "batch": self.store.batch(transfer["batch_id"])}
        descriptor = manifest.segments[index]
        wrote = self._store_if_absent(manifest_path, manifest, transfer, descriptor)
        result = self._finish_if_complete(transfer, manifest)
        return {"idempotent": not wrote, "transfer": result["report"],
                "batch": result.get("batch")}

    def ingest_segments(self, manifest_path: str | Path) -> dict[str, Any]:
        manifest = load_transfer_manifest(manifest_path)
        transfer = self.register(manifest)
        if transfer["status"] == "completed":
            return {"idempotent": True, "transfer": self.status_report(transfer["id"]),
                    "batch": self.store.batch(transfer["batch_id"])}
        for descriptor in manifest.segments:
            self._store_if_absent(manifest_path, manifest, transfer, descriptor)
        result = self._finish_if_complete(transfer, manifest)
        return {"idempotent": False, "transfer": result["report"], "batch": result.get("batch")}

    def _store_if_absent(self, manifest_path: str | Path, manifest: TransferManifest,
                         transfer: dict[str, Any], descriptor) -> bool:
        """Persist one segment checkpoint. Returns True when a new checkpoint was written."""
        checkpoint = self.store.get_segment(transfer["id"], descriptor.index)
        if checkpoint is not None:
            if checkpoint["expected_digest"] != descriptor.digest:
                self.store.add_conflict(
                    transfer["id"], descriptor.index, "checkpoint", "segment_hash",
                    f"segment {descriptor.index}: checkpoint bound to a different descriptor",
                    {"index": descriptor.index, "stored_digest": checkpoint["expected_digest"],
                     "incoming_digest": descriptor.digest})
                raise ContractError("segment_hash",
                                    "segment descriptor changed; resend the full corrected transfer")
            return False
        try:
            content = read_segment(manifest_path, descriptor)
        except ContractError as exc:
            evidence = getattr(exc, "evidence", {"index": descriptor.index})
            self.store.add_conflict(transfer["id"], descriptor.index,
                                    evidence.get("stage", "read"), exc.code, exc.detail, evidence)
            raise
        self.store.put_segment(transfer["id"], descriptor, sha256(content).hexdigest(), content)
        return True

    def _finish_if_complete(self, transfer: dict[str, Any], manifest: TransferManifest) -> dict[str, Any]:
        if transfer["status"] == "completed":
            return {"report": self.status_report(transfer["id"]),
                    "batch": self.store.batch(transfer["batch_id"])}
        report = self.status_report(transfer["id"])
        if not report["complete"]:
            return {"report": report}
        try:
            batch = self._materialize(transfer, manifest)
        except ContractError as exc:
            self.store.add_conflict(transfer["id"], None, "completion", exc.code, exc.detail,
                                    {"code": exc.code, "detail": exc.detail})
            raise
        self.store.mark_transfer_completed(transfer["id"], batch["id"])
        return {"report": self.status_report(transfer["id"]), "batch": batch}

    def _materialize(self, transfer: dict[str, Any], manifest: TransferManifest) -> dict[str, Any]:
        stored = self.store.segment_blobs(transfer["id"])
        if set(stored) != {item.index for item in manifest.segments}:
            raise ContractError("segment_missing", "not every declared segment has a durable checkpoint")
        payload = assemble_payload(None, manifest, stored)
        rows = parse_envelope_rows(payload, manifest.format, manifest.columns)
        if len(rows) != manifest.row_count:
            raise ContractError("row_count",
                                f"manifest declares {manifest.row_count} rows, source contains {len(rows)}")
        envelope = BatchEnvelope(manifest.source, manifest.batch_id, manifest.format, tuple(rows),
                                 manifest.payload_hash, manifest.row_count, manifest.metadata)
        previous = self.store.latest_batch(envelope.source, envelope.batch_id)
        if previous and previous["payload_hash"] == envelope.payload_hash:
            return previous
        source = self.registry.source(envelope.source)
        if not source.active:
            raise ContractError("inactive_source", f"source {source.key} is inactive")
        measurements, rejected = self.normalizer.batch(source, envelope.rows)
        assessment = self.quality.assess(source.key, measurements, rejected)
        return self.store.insert_batch(envelope, assessment, measurements)

    def _require_same_structure(self, existing: dict[str, Any], manifest: TransferManifest) -> None:
        incoming = {"format": manifest.format, "row_count": manifest.row_count, "size": manifest.size,
                    "segment_count": len(manifest.segments), "columns": manifest.columns}
        current = {field: existing[field] for field in STRUCTURAL_FIELDS}
        if current != incoming:
            evidence = {"stored": current, "incoming": incoming}
            self.store.add_conflict(existing["id"], None, "manifest", "transfer_manifest_conflict",
                                    "same source identity carries a different transfer descriptor", evidence)
            raise ContractError("transfer_manifest_conflict",
                                "manifest structure changed without a new source identity")

    def status_report(self, transfer_id: int) -> dict[str, Any]:
        transfer = self.store.connection.execute(
            "SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
        if transfer is None:
            raise ContractError("unknown_transfer", f"transfer {transfer_id} does not exist")
        transfer = dict(transfer)
        segments = self.store.list_segments(transfer_id)
        received = sorted(row["segment_index"] for row in segments)
        expected = list(range(transfer["segment_count"]))
        missing = [index for index in expected if index not in set(received)]
        contiguous = 0
        while contiguous in received:
            contiguous += 1
        conflicts = self.store.list_conflicts(transfer_id)
        complete = not missing
        blocked = None
        if transfer["status"] != "completed":
            if conflicts:
                blocked = f"{conflicts[-1]['code']}: {conflicts[-1]['detail']}"
            elif missing:
                blocked = f"awaiting {len(missing)} segment(s); next missing index {missing[0]}"
        return {"transfer_id": transfer_id, "source": transfer["source"],
                "external_id": transfer["external_id"], "payload_hash": transfer["payload_hash"],
                "status": transfer["status"], "format": transfer["format"],
                "declared_rows": transfer["row_count"], "size": transfer["size"],
                "segment_count": transfer["segment_count"], "received_count": len(received),
                "received_indices": received, "missing_indices": missing,
                "contiguous_prefix": contiguous, "next_missing_index": missing[0] if missing else None,
                "segments": segments, "complete": complete, "blocked_reason": blocked,
                "conflicts": conflicts, "batch_id": transfer["batch_id"],
                "updated_at": transfer["updated_at"]}

    def find_transfer(self, source: str, external_id: str, payload_hash: str) -> dict[str, Any]:
        transfer = self.store.get_transfer(source, external_id, payload_hash)
        if transfer is None:
            raise ContractError("unknown_transfer", "no transfer matches this source identity")
        return self.status_report(transfer["id"])
