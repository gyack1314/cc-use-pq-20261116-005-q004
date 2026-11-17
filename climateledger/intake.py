from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from io import StringIO
from pathlib import Path
from typing import Any
import csv
import json

from .registry import ContractError, identifier


MAX_TRANSFER_SIZE = 32_000_000
MAX_SEGMENT_COUNT = 1000


@dataclass(frozen=True)
class BatchEnvelope:
    source: str
    batch_id: str
    format: str
    rows: tuple[dict[str, Any], ...]
    payload_hash: str
    declared_count: int
    metadata: dict[str, Any]


@dataclass(frozen=True)
class SegmentDescriptor:
    index: int
    filename: str
    offset: int
    length: int
    digest: str


@dataclass(frozen=True)
class TransferManifest:
    source: str
    batch_id: str
    format: str
    columns: dict[str, str]
    row_count: int
    payload_hash: str
    size: int
    segments: tuple[SegmentDescriptor, ...]
    metadata: dict[str, Any]


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def file_bytes(path: Path, maximum: int = 8_000_000) -> bytes:
    if not path.is_file() or path.is_symlink():
        raise ContractError("missing_file", f"local file not found: {path}")
    size = path.stat().st_size
    if size > maximum:
        raise ContractError("file_too_large", f"file exceeds {maximum} bytes")
    content = path.read_bytes()
    if len(content) != size:
        raise ContractError("file_changed", "file changed while reading")
    return content


def manifest_value(path: Path) -> dict[str, Any]:
    raw = file_bytes(path, 100_000)
    try:
        value = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeError, json.JSONDecodeError):
        raise ContractError("manifest_json", "manifest must be UTF-8 JSON") from None
    if not isinstance(value, dict):
        raise ContractError("manifest_shape", "manifest must be a JSON object")
    return value


def valid_mapping(mapping: Any) -> dict[str, str]:
    required = ("indicator", "period", "value", "unit")
    if not isinstance(mapping, dict) or set(mapping) != set(required):
        raise ContractError("column_mapping", "columns must map indicator, period, value and unit")
    for key, column in mapping.items():
        if not isinstance(column, str) or not column.strip():
            raise ContractError("column_mapping", f"empty mapping for {key}")
    if len(set(mapping.values())) != len(mapping):
        raise ContractError("column_mapping", "one column cannot serve two fields")
    return mapping


def parse_json_rows(data: bytes) -> list[dict[str, Any]]:
    try:
        value = json.loads(data.decode("utf-8-sig"))
    except (UnicodeError, json.JSONDecodeError):
        raise ContractError("payload_json", "payload must be UTF-8 JSON") from None
    if not isinstance(value, list):
        raise ContractError("payload_shape", "JSON payload must be a list")
    if any(not isinstance(row, dict) for row in value):
        raise ContractError("row_shape", "each JSON row must be an object")
    return value


def parse_csv_rows(data: bytes) -> list[dict[str, Any]]:
    try:
        content = data.decode("utf-8-sig")
    except UnicodeError:
        raise ContractError("payload_utf8", "CSV payload must be UTF-8") from None
    if "\x00" in content:
        raise ContractError("payload_nul", "CSV cannot contain NUL")
    reader = csv.DictReader(StringIO(content, newline=""), strict=True)
    if not reader.fieldnames or len(set(reader.fieldnames)) != len(reader.fieldnames):
        raise ContractError("csv_header", "CSV header missing or duplicated")
    try:
        rows = list(reader)
    except csv.Error as exc:
        raise ContractError("csv_parse", str(exc)) from None
    if any(None in row for row in rows):
        raise ContractError("csv_columns", "CSV row contains extra fields")
    return rows


def load_batch(manifest_path: str | Path) -> BatchEnvelope:
    path = Path(manifest_path)
    manifest = manifest_value(path)
    source = identifier(manifest.get("source"), "source")
    batch_id = identifier(manifest.get("batch_id"), "batch_id")
    format_name = manifest.get("format")
    if format_name not in {"csv", "json"}:
        raise ContractError("payload_format", "format must be csv or json")
    filename = manifest.get("payload")
    if not isinstance(filename, str) or not filename or Path(filename).is_absolute() or Path(filename).name != filename:
        raise ContractError("payload_path", "payload must be a sibling filename")
    if filename == path.name:
        raise ContractError("payload_path", "payload cannot be the manifest")
    mapping = valid_mapping(manifest.get("columns"))
    declared_count = manifest.get("row_count")
    if type(declared_count) is not int or declared_count < 1 or declared_count > 100_000:
        raise ContractError("row_count", "row_count must be 1-100000")
    metadata = manifest.get("metadata", {})
    if not isinstance(metadata, dict) or len(canonical_bytes(metadata)) > 8192:
        raise ContractError("metadata", "metadata must be a small object")
    payload = file_bytes(path.parent / filename)
    digest = sha256(payload).hexdigest()
    expected_hash = manifest.get("sha256")
    if not isinstance(expected_hash, str) or len(expected_hash) != 64 or expected_hash.lower() != digest:
        raise ContractError("payload_hash", "payload SHA-256 mismatch")
    raw_rows = parse_csv_rows(payload) if format_name == "csv" else parse_json_rows(payload)
    if len(raw_rows) != declared_count:
        raise ContractError("row_count", f"declared {declared_count}, found {len(raw_rows)}")
    rows = []
    for number, row in enumerate(raw_rows, 1):
        if any(column not in row for column in mapping.values()):
            raise ContractError("missing_column", f"row {number} missing mapped column")
        normalized = {key: row[column] for key, column in mapping.items()}
        normalized["line"] = number
        rows.append(normalized)
    return BatchEnvelope(source, batch_id, format_name, tuple(rows), digest, declared_count, metadata)


def write_manifest(directory: str | Path, *, source: str, batch_id: str,
                   rows: list[dict[str, Any]], format_name: str = "json",
                   metadata: dict[str, Any] | None = None) -> Path:
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    identifier(source, "source")
    identifier(batch_id, "batch_id")
    if format_name not in {"csv", "json"}:
        raise ContractError("payload_format", "format must be csv or json")
    if not rows:
        raise ContractError("empty_rows", "at least one row is required")
    filename = f"{batch_id}.{format_name}"
    payload_path = root / filename
    if format_name == "json":
        payload = canonical_bytes(rows)
    else:
        stream = StringIO(newline="")
        writer = csv.DictWriter(stream, fieldnames=["indicator", "period", "value", "unit"], lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        payload = stream.getvalue().encode("utf-8")
    payload_path.write_bytes(payload)
    manifest = {"source": source, "batch_id": batch_id, "format": format_name,
                "payload": filename, "columns": {key: key for key in ("indicator", "period", "value", "unit")},
                "row_count": len(rows), "sha256": sha256(payload).hexdigest(), "metadata": metadata or {}}
    manifest_path = root / f"{batch_id}.manifest.json"
    manifest_path.write_bytes(canonical_bytes(manifest))
    return manifest_path


def _segment_descriptor(value: Any) -> SegmentDescriptor:
    if not isinstance(value, dict):
        raise ContractError("segment_shape", "each segment must be an object")
    index = value.get("index")
    if type(index) is not int or index < 0 or index >= MAX_SEGMENT_COUNT:
        raise ContractError("segment_index", f"segment index must be 0-{MAX_SEGMENT_COUNT - 1}")
    filename = value.get("payload")
    if not isinstance(filename, str) or not filename or Path(filename).is_absolute() or Path(filename).name != filename:
        raise ContractError("payload_path", "segment payload must be a sibling filename")
    offset = value.get("offset")
    length = value.get("length")
    if type(offset) is not int or offset < 0 or type(length) is not int or length < 1:
        raise ContractError("segment_range", "segment offset must be >= 0 and length >= 1")
    digest = value.get("sha256")
    if not isinstance(digest, str) or len(digest) != 64:
        raise ContractError("segment_hash", "segment sha256 must be 64 hex characters")
    return SegmentDescriptor(index, filename, offset, length, digest.lower())


def load_transfer_manifest(manifest_path: str | Path) -> TransferManifest:
    path = Path(manifest_path)
    manifest = manifest_value(path)
    source = identifier(manifest.get("source"), "source")
    batch_id = identifier(manifest.get("batch_id"), "batch_id")
    format_name = manifest.get("format")
    if format_name not in {"csv", "json"}:
        raise ContractError("payload_format", "format must be csv or json")
    mapping = valid_mapping(manifest.get("columns"))
    declared_count = manifest.get("row_count")
    if type(declared_count) is not int or declared_count < 1 or declared_count > 100_000:
        raise ContractError("row_count", "row_count must be 1-100000")
    metadata = manifest.get("metadata", {})
    if not isinstance(metadata, dict) or len(canonical_bytes(metadata)) > 8192:
        raise ContractError("metadata", "metadata must be a small object")
    expected_hash = manifest.get("sha256")
    if not isinstance(expected_hash, str) or len(expected_hash) != 64:
        raise ContractError("payload_hash", "whole-payload sha256 must be 64 hex characters")
    size = manifest.get("size")
    if type(size) is not int or not 1 <= size <= MAX_TRANSFER_SIZE:
        raise ContractError("payload_size", f"size must be 1-{MAX_TRANSFER_SIZE} bytes")
    raw_segments = manifest.get("segments")
    if not isinstance(raw_segments, list) or not raw_segments or len(raw_segments) > MAX_SEGMENT_COUNT:
        raise ContractError("segments", f"segments must be a non-empty list of at most {MAX_SEGMENT_COUNT}")
    descriptors = tuple(sorted((_segment_descriptor(item) for item in raw_segments),
                               key=lambda item: item.index))
    indices = [item.index for item in descriptors]
    if len(set(indices)) != len(indices) or indices != list(range(len(descriptors))):
        raise ContractError("segment_index", "segment indices must be unique and start at zero")
    names = [item.filename for item in descriptors]
    if len(set(names)) != len(names) or path.name in names:
        raise ContractError("payload_path", "segment payload names must be unique and not the manifest")
    cursor = 0
    for item in descriptors:
        if item.offset != cursor:
            raise ContractError("segment_range", "segments must be contiguous and ordered by offset")
        cursor = item.offset + item.length
    if cursor != size:
        raise ContractError("payload_size", "segment offsets do not reassemble to declared size")
    return TransferManifest(source, batch_id, format_name, mapping, declared_count,
                            expected_hash.lower(), size, descriptors, metadata)


def read_segment(manifest_path: str | Path, descriptor: SegmentDescriptor) -> bytes:
    path = Path(manifest_path)
    try:
        content = file_bytes(path.parent / descriptor.filename, 8_000_000)
    except ContractError as exc:
        exc.evidence = {"index": descriptor.index, "stage": "read", "code": exc.code, "detail": exc.detail}
        raise
    if len(content) != descriptor.length:
        error = ContractError("segment_length",
                              f"segment {descriptor.index}: expected {descriptor.length} bytes, got {len(content)}")
        error.evidence = {"index": descriptor.index, "stage": "digest", "code": "segment_length",
                          "expected_length": descriptor.length, "actual_length": len(content)}
        raise error
    digest = sha256(content).hexdigest()
    if digest != descriptor.digest:
        error = ContractError("segment_hash", f"segment {descriptor.index}: SHA-256 mismatch")
        error.evidence = {"index": descriptor.index, "stage": "digest", "code": "segment_hash",
                          "expected_digest": descriptor.digest, "actual_digest": digest}
        raise error
    return content


def assemble_payload(manifest_path: str | Path | None, manifest: TransferManifest,
                     segment_bytes: dict[int, bytes] | None = None) -> bytes:
    pieces: list[bytes] = []
    for descriptor in manifest.segments:
        if segment_bytes is not None and descriptor.index in segment_bytes:
            content = segment_bytes[descriptor.index]
        else:
            content = read_segment(manifest_path, descriptor)
        if len(content) != descriptor.length or sha256(content).hexdigest() != descriptor.digest:
            error = ContractError("segment_hash", f"segment {descriptor.index}: stored checkpoint fails verification")
            error.evidence = {"index": descriptor.index, "stage": "reassemble", "code": "segment_hash"}
            raise error
        pieces.append(content)
    payload = b"".join(pieces)
    if len(payload) != manifest.size:
        raise ContractError("payload_size", "reassembled payload size differs from manifest")
    digest = sha256(payload).hexdigest()
    if digest != manifest.payload_hash:
        error = ContractError("payload_hash", "reassembled payload SHA-256 does not match source identity")
        error.evidence = {"stage": "reassemble", "code": "payload_hash",
                          "expected_digest": manifest.payload_hash, "actual_digest": digest}
        raise error
    return payload


def parse_envelope_rows(payload: bytes, format_name: str, mapping: dict[str, str]) -> list[dict[str, Any]]:
    raw_rows = parse_csv_rows(payload) if format_name == "csv" else parse_json_rows(payload)
    rows = []
    for number, row in enumerate(raw_rows, 1):
        if any(column not in row for column in mapping.values()):
            raise ContractError("missing_column", f"row {number} missing mapped column")
        normalized = {key: row[column] for key, column in mapping.items()}
        normalized["line"] = number
        rows.append(normalized)
    return rows


def write_segmented_manifest(directory: str | Path, *, source: str, batch_id: str,
                             rows: list[dict[str, Any]], segment_size: int = 4096,
                             format_name: str = "json",
                             metadata: dict[str, Any] | None = None) -> Path:
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    identifier(source, "source")
    identifier(batch_id, "batch_id")
    if format_name not in {"csv", "json"}:
        raise ContractError("payload_format", "format must be csv or json")
    if not rows:
        raise ContractError("empty_rows", "at least one row is required")
    if type(segment_size) is not int or segment_size < 1:
        raise ContractError("segment_size", "segment_size must be a positive integer")
    if format_name == "json":
        payload = canonical_bytes(rows)
    else:
        stream = StringIO(newline="")
        writer = csv.DictWriter(stream, fieldnames=["indicator", "period", "value", "unit"], lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        payload = stream.getvalue().encode("utf-8")
    if len(payload) > MAX_TRANSFER_SIZE:
        raise ContractError("file_too_large", f"payload exceeds {MAX_TRANSFER_SIZE} bytes")
    descriptors = []
    for index, offset in enumerate(range(0, len(payload), segment_size)):
        if index >= MAX_SEGMENT_COUNT:
            raise ContractError("segments", f"more than {MAX_SEGMENT_COUNT} segments at this size")
        chunk = payload[offset:offset + segment_size]
        filename = f"{batch_id}.part{index:04d}"
        (root / filename).write_bytes(chunk)
        descriptors.append({"index": index, "payload": filename, "offset": offset,
                            "length": len(chunk), "sha256": sha256(chunk).hexdigest()})
    manifest = {"source": source, "batch_id": batch_id, "format": format_name,
                "columns": {key: key for key in ("indicator", "period", "value", "unit")},
                "row_count": len(rows), "sha256": sha256(payload).hexdigest(),
                "size": len(payload), "segments": descriptors, "metadata": metadata or {}}
    manifest_path = root / f"{batch_id}.transfer.json"
    manifest_path.write_bytes(canonical_bytes(manifest))
    return manifest_path
