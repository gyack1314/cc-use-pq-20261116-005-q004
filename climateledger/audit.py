from __future__ import annotations

from collections import Counter
from hashlib import sha256
from typing import Any

from .intake import canonical_bytes
from .registry import ContractError


class AuditInspector:
    def __init__(self, store, registry, releases):
        self.store = store
        self.registry = registry
        self.releases = releases

    def batch_trace(self, batch_id: int) -> dict[str, Any]:
        batch = self.store.batch(batch_id)
        rows = self.store.measurements(batch_id)
        events = self.store.events(f"batch:{batch_id}")
        if len(rows) > batch["row_count"]:
            raise ContractError("batch_corrupt", "stored measurements exceed declared rows")
        ordered_lines = [row["line"] for row in rows]
        if len(set(ordered_lines)) != len(ordered_lines):
            raise ContractError("batch_corrupt", "duplicate line numbers")
        if any(line < 1 or line > batch["row_count"] for line in ordered_lines):
            raise ContractError("batch_corrupt", "stored line exceeds declared row count")
        findings = Counter(item["code"] for item in batch["findings"])
        return {"batch": {key: batch[key] for key in (
                    "id", "source", "external_id", "revision", "payload_hash", "status", "row_count")},
                "measurement_count": len(rows), "finding_codes": dict(findings),
                "event_count": len(events), "events": events,
                "line_digest": sha256(canonical_bytes(ordered_lines)).hexdigest()}

    def release_trace(self, version: int) -> dict[str, Any]:
        bundle = self.releases.export(version)
        lineage = self.releases.lineage(version)
        batches = sorted({entry["batch_id"] for entry in bundle["entries"]})
        source_keys = sorted({entry["source"] for entry in bundle["entries"]})
        indicator_keys = sorted({entry["indicator"] for entry in bundle["entries"]})
        missing = []
        for entry in bundle["entries"]:
            batch = self.store.batch(entry["batch_id"])
            if batch["source"] != entry["source"]:
                missing.append({"entry": entry, "reason": "source_mismatch"})
                continue
            rows = self.store.measurements(entry["batch_id"])
            if not any(row["line"] == entry["line"] and row["indicator"] == entry["indicator"]
                       and row["period"] == entry["period"] and row["value"] == entry["value"]
                       for row in rows):
                missing.append({"entry": entry, "reason": "measurement_mismatch"})
        return {"version": version, "verified": not missing and self.releases.verify(bundle),
                "digest": bundle["digest"], "lineage": lineage,
                "batch_ids": batches, "sources": source_keys, "indicators": indicator_keys,
                "entry_count": len(bundle["entries"]), "broken_entries": missing}

    def transfer_trace(self) -> list[dict[str, Any]]:
        traces = []
        for transfer in self.store.list_transfers():
            segments = self.store.list_segments(transfer["id"])
            indices = sorted(row["segment_index"] for row in segments)
            expected = list(range(transfer["segment_count"]))
            conflicts = self.store.list_conflicts(transfer["id"])
            problems = []
            if len(set(indices)) != len(indices):
                problems.append("duplicate_segment_checkpoint")
            if any(row["expected_digest"] != row["actual_digest"] for row in segments):
                problems.append("segment_digest_mismatch")
            if transfer["status"] == "completed":
                if indices != expected:
                    problems.append("completed_with_missing_segments")
                if transfer["batch_id"] is None:
                    problems.append("completed_without_batch")
                else:
                    batch = self.store.batch(transfer["batch_id"])
                    if batch["payload_hash"] != transfer["payload_hash"]:
                        problems.append("batch_identity_mismatch")
                    if batch["row_count"] != transfer["row_count"]:
                        problems.append("batch_row_count_mismatch")
            traces.append({"transfer_id": transfer["id"], "source": transfer["source"],
                           "external_id": transfer["external_id"], "status": transfer["status"],
                           "payload_hash": transfer["payload_hash"], "segment_count": transfer["segment_count"],
                           "received_count": len(indices), "missing_indices": [i for i in expected if i not in indices],
                           "conflict_count": len(conflicts), "problems": problems,
                           "batch_id": transfer["batch_id"]})
        return traces

    def inspect_all(self) -> dict[str, Any]:
        batches = self.store.list_batches()
        latest = self.store.latest_release()
        batch_traces = [self.batch_trace(batch["id"]) for batch in batches]
        releases = []
        if latest:
            releases = [self.release_trace(version) for version in range(1, latest["version"] + 1)]
        violations = [entry for entry in releases if not entry["verified"]]
        transfer_traces = self.transfer_trace()
        transfer_violations = [entry for entry in transfer_traces if entry["problems"]]
        return {"batch_count": len(batches), "release_count": len(releases),
                "violations": violations, "batch_traces": batch_traces,
                "release_digests": [release["digest"] for release in releases],
                "transfer_count": len(transfer_traces),
                "transfer_violations": transfer_violations, "transfer_traces": transfer_traces}
