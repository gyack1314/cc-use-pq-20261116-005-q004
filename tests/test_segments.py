from __future__ import annotations

import json
import sqlite3
from hashlib import sha256
from pathlib import Path

from climateledger.intake import (assemble_payload, load_transfer_manifest, parse_envelope_rows,
                                  write_segmented_manifest)
from climateledger.registry import ContractError
from climateledger.service import LedgerService
from support import LedgerCase


class SegmentedCase(LedgerCase):
    def segmented_manifest(self, batch_id, rows=None, *, segment_size=64, source="lab_a",
                           format_name="json"):
        rows = rows if rows is not None else self.rows(428, 429, 430, 431, 432)
        folder = self.root / source / batch_id
        return write_segmented_manifest(folder, source=source, batch_id=batch_id, rows=rows,
                                        segment_size=segment_size, format_name=format_name,
                                        metadata={"synthetic": True}), rows

    def part_path(self, manifest_path, index):
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        return Path(manifest_path).parent / manifest["segments"][index]["payload"]

    def tamper_part(self, manifest_path, index, replacement):
        path = self.part_path(manifest_path, index)
        path.write_bytes(replacement if isinstance(replacement, bytes) else replacement.encode("utf-8"))

    def rewrite_manifest(self, manifest_path, mutate):
        path = Path(manifest_path)
        manifest = json.loads(path.read_text(encoding="utf-8"))
        mutate(manifest)
        path.write_text(json.dumps(manifest), encoding="utf-8")

    def feed_all(self, manifest_path, order=None):
        manifest = load_transfer_manifest(manifest_path)
        indices = order if order is not None else range(len(manifest.segments))
        last = None
        for index in indices:
            last = self.service.ingest_segment(manifest_path, index)
        return last


class ManifestValidationTests(SegmentedCase):
    def test_manifest_round_trip_describes_contiguous_segments(self):
        manifest_path, rows = self.segmented_manifest("segs", segment_size=32)
        manifest = load_transfer_manifest(manifest_path)
        self.assertEqual(manifest.row_count, len(rows))
        self.assertEqual(manifest.source, "lab_a")
        cursor = 0
        for index, descriptor in enumerate(manifest.segments):
            self.assertEqual(descriptor.index, index)
            self.assertEqual(descriptor.offset, cursor)
            cursor += descriptor.length
        self.assertEqual(cursor, manifest.size)

    def test_indices_must_be_contiguous(self):
        manifest_path, _ = self.segmented_manifest("segs")
        self.rewrite_manifest(manifest_path, lambda m: m["segments"].pop(1))
        with self.assertRaisesRegex(ContractError, "segment_index"):
            load_transfer_manifest(manifest_path)

    def test_offsets_must_be_contiguous(self):
        manifest_path, _ = self.segmented_manifest("segs")

        def break_offsets(manifest):
            manifest["segments"][1]["offset"] += 9

        self.rewrite_manifest(manifest_path, break_offsets)
        with self.assertRaisesRegex(ContractError, "segment_range"):
            load_transfer_manifest(manifest_path)

    def test_size_must_match_segments(self):
        manifest_path, _ = self.segmented_manifest("segs")
        self.rewrite_manifest(manifest_path, lambda m: m.update(size=m["size"] + 1))
        with self.assertRaisesRegex(ContractError, "payload_size"):
            load_transfer_manifest(manifest_path)

    def test_segment_payload_cannot_escape_directory(self):
        manifest_path, _ = self.segmented_manifest("segs")
        self.rewrite_manifest(manifest_path, lambda m: m["segments"][0].update(payload="../escape.bin"))
        with self.assertRaisesRegex(ContractError, "payload_path"):
            load_transfer_manifest(manifest_path)


class IngestionFlowTests(SegmentedCase):
    def test_segmented_ingestion_materializes_one_batch(self):
        manifest_path, rows = self.segmented_manifest("flow")
        result = self.feed_all(manifest_path)
        batch = result["batch"]
        self.assertEqual(batch["row_count"], len(rows))
        self.assertEqual(batch["status"], "accepted")
        self.assertEqual(result["transfer"]["status"], "completed")
        self.assertEqual(len(self.service.batches()), 1)
        self.assertEqual(len(self.service.batch(batch["id"])["measurements"]), len(rows))

    def test_out_of_order_segments_complete_when_all_arrive(self):
        manifest_path, _ = self.segmented_manifest("flow", segment_size=40)
        manifest = load_transfer_manifest(manifest_path)
        result = self.feed_all(manifest_path, order=list(range(len(manifest.segments)))[::-1])
        self.assertTrue(result["transfer"]["complete"])
        self.assertIsNotNone(result["batch"])

    def test_no_batch_exists_until_all_segments_received(self):
        manifest_path, _ = self.segmented_manifest("flow")
        self.service.ingest_segment(manifest_path, 0)
        report = self.service.transfer_status(manifest_path)
        self.assertFalse(report["complete"])
        self.assertEqual(self.service.batches(), [])
        self.assertEqual(report["missing_indices"], list(range(1, report["segment_count"])))
        self.assertEqual(report["next_missing_index"], 1)
        with self.assertRaisesRegex(ContractError, "unknown_batch"):
            self.service.publish(1, 1)
        self.assertIsNone(self.service.status()["latest_release"])

    def test_whole_file_hash_guards_reassembly(self):
        manifest_path, _ = self.segmented_manifest("flow")
        self.rewrite_manifest(manifest_path, lambda m: m.update(sha256="0" * 64))
        with self.assertRaisesRegex(ContractError, "payload_hash"):
            self.feed_all(manifest_path)
        self.assertEqual(self.service.batches(), [])

    def test_row_count_mismatch_blocks_materialization(self):
        manifest_path, _ = self.segmented_manifest("flow")
        self.rewrite_manifest(manifest_path, lambda m: m.update(row_count=m["row_count"] + 1))
        with self.assertRaisesRegex(ContractError, "row_count"):
            self.feed_all(manifest_path)
        report = self.service.transfer_status(manifest_path)
        self.assertEqual(report["status"], "receiving")
        self.assertIn("row_count", report["blocked_reason"])
        self.assertEqual(self.service.batches(), [])

    def test_csv_splits_mid_line_and_reassembles(self):
        rows = self.rows(428, 429, 430)
        manifest_path, _ = self.segmented_manifest("csv_flow", rows, segment_size=37, format_name="csv")
        result = self.feed_all(manifest_path)
        self.assertEqual(result["batch"]["status"], "accepted")
        manifest = load_transfer_manifest(manifest_path)
        payload = assemble_payload(manifest_path, manifest)
        self.assertEqual(len(parse_envelope_rows(payload, "csv", manifest.columns)), len(rows))

    def test_segments_and_whole_file_produce_identical_measurements(self):
        rows = self.rows(428, 429, 430)
        whole_batch = self.service.ingest(self.manifest("whole", rows))["batch"]
        manifest_path, _ = self.segmented_manifest("split", rows, segment_size=33)
        segmented_batch = self.feed_all(manifest_path)["batch"]
        whole_rows = self.service.batch(whole_batch["id"])["measurements"]
        split_rows = self.service.batch(segmented_batch["id"])["measurements"]
        self.assertEqual([row["value"] for row in split_rows], [row["value"] for row in whole_rows])
        self.assertEqual(segmented_batch["payload_hash"], whole_batch["payload_hash"])


class CheckpointTests(SegmentedCase):
    def test_duplicate_segment_is_idempotent(self):
        manifest_path, _ = self.segmented_manifest("dup")
        first = self.service.ingest_segment(manifest_path, 0)
        self.assertFalse(first["idempotent"])
        again = self.service.ingest_segment(manifest_path, 0)
        self.assertTrue(again["idempotent"])
        report = self.service.transfer_status(manifest_path)
        self.assertEqual(report["received_indices"], [0])
        self.assertEqual(self.store_count("transfer_segments"), 1)

    def test_checkpoints_survive_restart_and_resume(self):
        manifest_path, rows = self.segmented_manifest("restart", segment_size=40)
        self.service.ingest_segment(manifest_path, 0)
        self.service.close()
        with LedgerService(self.db) as reopened:
            report = reopened.transfer_status(manifest_path)
            self.assertEqual(report["received_indices"], [0])
            self.assertEqual(report["next_missing_index"], 1)
            manifest = load_transfer_manifest(manifest_path)
            for index in range(1, len(manifest.segments)):
                reopened.ingest_segment(manifest_path, index)
            completed = reopened.transfer_status(manifest_path)
            self.assertEqual(completed["status"], "completed")
            self.assertEqual(reopened.batch(completed["batch_id"])["row_count"], len(rows))

    def test_replaced_segment_bytes_are_conflict_not_checkpoint(self):
        manifest_path, _ = self.segmented_manifest("conflict")
        original_one = self.part_path(manifest_path, 1).read_bytes()
        self.tamper_part(manifest_path, 1, b"x" * len(original_one))
        with self.assertRaisesRegex(ContractError, "segment_hash"):
            self.service.ingest_segment(manifest_path, 1)
        report = self.service.transfer_status(manifest_path)
        self.assertNotIn(1, report["received_indices"])
        conflict = report["conflicts"][-1]
        self.assertEqual(conflict["code"], "segment_hash")
        self.assertIn("actual_digest", conflict["evidence"])
        self.assertIn("segment_hash", report["blocked_reason"])
        self.part_path(manifest_path, 1).write_bytes(original_one)
        self.feed_all(manifest_path)
        self.assertEqual(self.service.transfer_status(manifest_path)["status"], "completed")

    def test_received_segment_keeps_checkpoint_and_ignores_bad_resend(self):
        manifest_path, _ = self.segmented_manifest("conflict2")
        self.service.ingest_segment(manifest_path, 0)
        original_zero = self.part_path(manifest_path, 0).read_bytes()
        self.tamper_part(manifest_path, 0, b"y" * len(original_zero))
        result = self.service.ingest_segment(manifest_path, 0)
        self.assertTrue(result["idempotent"])
        self.part_path(manifest_path, 0).write_bytes(original_zero)
        self.feed_all(manifest_path)
        self.assertEqual(self.service.transfer_status(manifest_path)["status"], "completed")

    def test_truncated_segment_records_length_conflict(self):
        manifest_path, _ = self.segmented_manifest("trunc")
        self.part_path(manifest_path, 1).write_bytes(b"short")
        with self.assertRaisesRegex(ContractError, "segment_length"):
            self.service.ingest_segment(manifest_path, 1)
        report = self.service.transfer_status(manifest_path)
        self.assertEqual(report["conflicts"][-1]["code"], "segment_length")
        self.assertEqual(report["received_indices"], [])

    def test_missing_segment_file_is_reported_as_pending(self):
        manifest_path, _ = self.segmented_manifest("missing")
        self.part_path(manifest_path, 1).unlink()
        with self.assertRaisesRegex(ContractError, "missing_file"):
            self.service.ingest_segment(manifest_path, 1)
        report = self.service.transfer_status(manifest_path)
        self.assertIn(1, report["missing_indices"])
        self.assertEqual(report["conflicts"][-1]["code"], "missing_file")

    def test_new_file_identity_does_not_reuse_old_checkpoints(self):
        first_rows = self.rows(428, 429, 430)
        first_folder = self.root / "lab_a" / "replaced_v1"
        manifest_path = write_segmented_manifest(first_folder, source="lab_a", batch_id="replaced",
                                                 rows=first_rows, segment_size=40)
        self.service.ingest_segment(manifest_path, 0)
        second_rows = self.rows(500, 501, 502)
        second_folder = self.root / "lab_a" / "replaced_v2"
        new_path = write_segmented_manifest(second_folder, source="lab_a", batch_id="replaced",
                                            rows=second_rows, segment_size=40)
        self.service.ingest_segment(new_path, 0)
        new_manifest = load_transfer_manifest(new_path)
        report = self.service.transfer_status(new_path)
        self.assertEqual(report["received_indices"], [0])
        self.assertEqual(report["segment_count"], len(new_manifest.segments))
        old_manifest = load_transfer_manifest(manifest_path)
        old_report = self.service.transfer_manager.find_transfer(
            old_manifest.source, old_manifest.batch_id, old_manifest.payload_hash)
        self.assertEqual(old_report["received_indices"], [0])
        self.assertNotEqual(report["transfer_id"], old_report["transfer_id"])
        result = self.feed_all(new_path)
        self.assertEqual(result["batch"]["payload_hash"], new_manifest.payload_hash)
        self.assertEqual(len(self.service.list_transfers()), 2)

    def test_structural_manifest_change_under_same_identity_conflicts(self):
        manifest_path, _ = self.segmented_manifest("structural", segment_size=40)
        self.service.ingest_segment(manifest_path, 0)
        self.rewrite_manifest(manifest_path, lambda m: m.update(row_count=m["row_count"] + 1))
        with self.assertRaisesRegex(ContractError, "transfer_manifest_conflict"):
            self.service.ingest_segment(manifest_path, 1)
        report = self.service.transfer_status(manifest_path)
        self.assertEqual(report["conflicts"][-1]["code"], "transfer_manifest_conflict")

    def test_changed_segment_descriptor_under_same_index_conflicts(self):
        manifest_path, _ = self.segmented_manifest("descriptor")
        self.service.ingest_segment(manifest_path, 0)
        self.rewrite_manifest(manifest_path,
                              lambda m: m["segments"][0].update(sha256="a" * 64))
        with self.assertRaisesRegex(ContractError, "segment_hash"):
            self.service.ingest_segment(manifest_path, 0)
        report = self.service.transfer_status(manifest_path)
        self.assertEqual(report["conflicts"][-1]["stage"], "checkpoint")

    def test_completed_transfer_is_idempotent(self):
        manifest_path, _ = self.segmented_manifest("done")
        first = self.feed_all(manifest_path)
        second = self.feed_all(manifest_path)
        self.assertTrue(second["idempotent"])
        self.assertEqual(second["batch"]["id"], first["batch"]["id"])
        self.assertEqual(self.store_count("transfer_segments"),
                         len(load_transfer_manifest(manifest_path).segments))
        self.assertEqual(len(self.service.batches()), 1)

    def test_ingest_segments_resumes_and_completes(self):
        manifest_path, _ = self.segmented_manifest("bulk", segment_size=40)
        self.service.ingest_segment(manifest_path, 0)
        result = self.service.ingest_segments(manifest_path)
        self.assertTrue(result["transfer"]["complete"])
        self.assertIsNotNone(result["batch"])
        again = self.service.ingest_segments(manifest_path)
        self.assertTrue(again["idempotent"])

    def store_count(self, table):
        row = self.service.store.connection.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
        return row["n"]


class QualityAndReleaseTests(SegmentedCase):
    def test_rejected_segmented_batch_never_publishes(self):
        bad_rows = self.rows(1001, 1002)
        manifest_path, _ = self.segmented_manifest("bad", bad_rows)
        result = self.feed_all(manifest_path)
        self.assertEqual(result["batch"]["status"], "rejected")
        with self.assertRaisesRegex(ContractError, "quality_gate"):
            self.service.publish(result["batch"]["id"], result["batch"]["revision"])
        self.assertIsNone(self.service.status()["latest_release"])

    def test_completed_segmented_batch_publishes_and_exports(self):
        manifest_path, rows = self.segmented_manifest("publish")
        result = self.feed_all(manifest_path)
        release = self.service.publish(result["batch"]["id"], result["batch"]["revision"])
        bundle = self.service.export(release["version"])
        self.assertTrue(self.service.verify(bundle))
        self.assertEqual(len(bundle["entries"]), len(rows))

    def test_released_snapshot_never_contains_half_batch(self):
        manifest_path, _ = self.segmented_manifest("half")
        self.service.ingest_segment(manifest_path, 0)
        self.assertEqual(self.service.status()["latest_release"], None)
        result = self.feed_all(manifest_path)
        release = self.service.publish(result["batch"]["id"], result["batch"]["revision"])
        entries = self.service.export(release["version"])["entries"]
        self.assertEqual(len(entries), result["batch"]["row_count"])

    def test_segmented_revision_supersedes_like_whole_ingest(self):
        manifest_path, _ = self.segmented_manifest("rev", self.rows(428))
        first = self.feed_all(manifest_path)["batch"]
        new_path = write_segmented_manifest(Path(manifest_path).parent, source="lab_a",
                                            batch_id="rev", rows=self.rows(429), segment_size=40)
        second = self.feed_all(new_path)["batch"]
        self.assertEqual(second["revision"], 2)
        with self.assertRaisesRegex(ContractError, "stale_revision"):
            self.service.publish(first["id"], 1)
        second_release = self.service.publish(second["id"], second["revision"])
        self.assertEqual(len(self.service.export(second_release["version"])["entries"]), 1)


class TransactionTests(SegmentedCase):
    def test_crash_between_segments_leaves_durable_partial_state(self):
        manifest_path, _ = self.segmented_manifest("crash", segment_size=40)
        self.service.ingest_segment(manifest_path, 0)
        with self.assertRaises(sqlite3.IntegrityError):
            with self.service.store.transaction():
                self.service.store.connection.execute(
                    "INSERT INTO transfer_segments(transfer_id,segment_index,filename,offset,length,"
                    "expected_digest,actual_digest,segment_blob,received_at) "
                    "SELECT transfer_id,segment_index,filename,offset,length,expected_digest,actual_digest,"
                    "segment_blob,received_at FROM transfer_segments")
        report = self.service.transfer_status(manifest_path)
        self.assertEqual(report["received_indices"], [0])
        self.feed_all(manifest_path)
        self.assertEqual(self.service.transfer_status(manifest_path)["status"], "completed")

    def test_audit_reports_partial_and_completed_transfers(self):
        partial_path, _ = self.segmented_manifest("partial_audit", segment_size=40)
        self.service.ingest_segment(partial_path, 0)
        complete_path, rows = self.segmented_manifest("complete_audit", segment_size=40)
        completed = self.feed_all(complete_path)
        report = self.service.audit_all()
        self.assertEqual(report["transfer_count"], 2)
        self.assertEqual(report["transfer_violations"], [])
        by_status = {row["status"]: row for row in report["transfer_traces"]}
        self.assertFalse(by_status["receiving"]["problems"])
        self.assertEqual(by_status["receiving"]["missing_indices"],
                         list(range(1, by_status["receiving"]["segment_count"])))
        done = by_status["completed"]
        self.assertEqual(done["batch_id"], completed["batch"]["id"])
        self.assertEqual(done["received_count"], done["segment_count"])
        self.assertEqual(done["payload_hash"],
                         self.service.batch(completed["batch"]["id"])["payload_hash"])

    def test_audit_flags_checkpoint_digest_tampering(self):
        manifest_path, _ = self.segmented_manifest("tamper_audit", segment_size=40)
        self.feed_all(manifest_path)
        self.service.store.connection.execute(
            "UPDATE transfer_segments SET actual_digest=? WHERE segment_index=0", ("0" * 64,))
        report = self.service.audit_all()
        self.assertIn("segment_digest_mismatch", report["transfer_violations"][0]["problems"])
