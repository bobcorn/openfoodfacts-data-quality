from __future__ import annotations

import gzip
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Protocol, cast

from migration.source.datasets import SourceSelection
from migration.source.models import (
    ProductDocument,
    SourceBatchRecord,
    SourceInputSummary,
    SourceSnapshotFormat,
)
from migration.source.snapshots import source_snapshot_id_for


class SourceSnapshotAdapter(Protocol):
    """Adapter surface used by the migration source facade."""

    def summarize_input(
        self,
        *,
        selection: SourceSelection | None = None,
    ) -> SourceInputSummary: ...

    def iter_batches(
        self,
        *,
        batch_size: int,
        selection: SourceSelection | None = None,
    ) -> Iterator[list[SourceBatchRecord]]: ...


class SupportsBinaryRead(Protocol):
    """Minimal binary reader protocol for signature sniffing helpers."""

    def read(self, size: int | None = -1, /) -> bytes: ...


def count_source_products(
    path: Path,
    *,
    selection: SourceSelection | None = None,
) -> int:
    """Return the number of products selected from one source snapshot."""
    return summarize_source_input(path, selection=selection).processed_product_count


def summarize_source_input(
    path: Path,
    *,
    selection: SourceSelection | None = None,
) -> SourceInputSummary:
    """Return source-input diagnostics for one selected snapshot view."""
    return source_snapshot_adapter_for(path).summarize_input(selection=selection)


def iter_source_batches(
    path: Path,
    *,
    batch_size: int,
    selection: SourceSelection | None = None,
) -> Iterator[list[SourceBatchRecord]]:
    """Yield selected source products and product documents in batches."""
    return source_snapshot_adapter_for(path).iter_batches(
        batch_size=batch_size,
        selection=selection,
    )


def source_snapshot_adapter_for(path: Path) -> SourceSnapshotAdapter:
    """Return the explicit adapter for one source snapshot path."""
    resolved_format = resolve_source_snapshot_format(path)
    if resolved_format == SourceSnapshotFormat.DUCKDB:
        from migration.source._product_documents_duckdb import (
            DuckDBProductDocumentAdapter,
        )

        return DuckDBProductDocumentAdapter(path)
    if resolved_format == SourceSnapshotFormat.JSONL:
        from migration.source._product_documents_jsonl import (
            JsonlProductDocumentAdapter,
        )

        return JsonlProductDocumentAdapter(path)
    raise ValueError(f"Unsupported source snapshot format: {resolved_format!r}.")


def resolve_source_snapshot_format(path: Path) -> SourceSnapshotFormat:
    """Resolve the source snapshot format by suffix or by file signature."""
    suffixes = tuple(suffix.lower() for suffix in path.suffixes)
    if suffixes[-2:] == (".jsonl", ".gz") or suffixes[-1:] in (
        (".jsonl",),
        (".ndjson",),
    ):
        return SourceSnapshotFormat.JSONL
    if suffixes[-1:] in ((".duckdb",), (".db",)):
        return SourceSnapshotFormat.DUCKDB
    inferred_format = _infer_source_snapshot_format_from_content(path)
    if inferred_format is not None:
        return inferred_format

    supported_suffixes = ".duckdb, .db, .jsonl, .ndjson, .jsonl.gz"
    raise ValueError(
        f"Cannot infer source snapshot format from {path}. "
        f"Use a supported suffix ({supported_suffixes}) or a file whose content "
        "starts like JSONL or DuckDB."
    )


def source_batch_record_from_document(
    document: ProductDocument | Mapping[str, object],
) -> SourceBatchRecord:
    """Return one batch record from a validated product document."""
    product_document = validate_product_document(document)
    return SourceBatchRecord(product_document=product_document)


def validate_product_document(document: object) -> ProductDocument:
    """Return one validated full product document."""
    if isinstance(document, ProductDocument):
        return document
    if not isinstance(document, Mapping):
        raise TypeError("ProductDocument input must be a mapping.")
    product_document: dict[str, object] = {}
    for key, value in cast(Mapping[object, object], document).items():
        if not isinstance(key, str):
            raise ValueError("ProductDocument input keys must be strings.")
        product_document[key] = value
    code = _required_product_code(product_document.get("code"))
    product_document["code"] = code
    return ProductDocument(code=code, document=product_document)


def _required_product_code(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("ProductDocument requires a string 'code' field.")
    code = value.strip()
    if not code:
        raise ValueError("ProductDocument requires a non-empty 'code' field.")
    return code


def _infer_source_snapshot_format_from_content(
    path: Path,
) -> SourceSnapshotFormat | None:
    with path.open("rb") as handle:
        header = handle.read(32)
    if _is_duckdb_header(header):
        return SourceSnapshotFormat.DUCKDB
    if _is_gzip_header(header):
        with gzip.open(path, "rb") as handle:
            first_nonblank_byte = _first_nonblank_byte(handle)
    else:
        with path.open("rb") as handle:
            first_nonblank_byte = _first_nonblank_byte(handle)
    if first_nonblank_byte == b"{":
        return SourceSnapshotFormat.JSONL
    return None


def _is_duckdb_header(header: bytes) -> bool:
    return header[7:13] == b"GDUCK@"


def _is_gzip_header(header: bytes) -> bool:
    return header[:2] == b"\x1f\x8b"


def _first_nonblank_byte(handle: SupportsBinaryRead) -> bytes | None:
    while True:
        chunk = handle.read(8192)
        if not chunk:
            return None
        for byte in chunk:
            if chr(byte).isspace():
                continue
            return bytes([byte])


__all__ = [
    "SourceSnapshotAdapter",
    "count_source_products",
    "iter_source_batches",
    "source_batch_record_from_document",
    "source_snapshot_adapter_for",
    "source_snapshot_id_for",
    "summarize_source_input",
    "validate_product_document",
]
