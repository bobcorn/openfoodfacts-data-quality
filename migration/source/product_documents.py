from __future__ import annotations

import gzip
import hashlib
import heapq
import json
from collections.abc import Collection, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Protocol, cast

import duckdb

from migration.source.datasets import SourceSelection
from migration.source.models import (
    ProductDocument,
    SkippedSourceRow,
    SourceBatchRecord,
    SourceInputSummary,
    SourceSnapshotFormat,
)
from migration.source.snapshots import source_snapshot_id_for
from off_data_quality.contracts.source_products import (
    SOURCE_PRODUCT_BASE_FIELD_TO_COLUMN,
    SOURCE_PRODUCT_NUTRIMENT_COLUMNS,
    SourceProduct,
    validate_source_product,
)

_SKIPPED_SOURCE_ROW_EXAMPLE_LIMIT = 20
_MISSING_OR_BLANK_CODE_REASON = "missing or blank code"


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


def source_snapshot_adapter_for(
    path: Path,
) -> SourceSnapshotAdapter:
    """Return the explicit adapter for one source snapshot path."""
    resolved_format = resolve_source_snapshot_format(path)
    if resolved_format == SourceSnapshotFormat.DUCKDB:
        return DuckDBProductDocumentAdapter(path)
    if resolved_format == SourceSnapshotFormat.JSONL:
        return JsonlProductDocumentAdapter(path)
    raise ValueError(f"Unsupported source snapshot format: {resolved_format!r}.")


def resolve_source_snapshot_format(
    path: Path,
) -> SourceSnapshotFormat:
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


class DuckDBProductDocumentAdapter:
    """Read full product documents from one DuckDB products table."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def summarize_input(
        self,
        *,
        selection: SourceSelection | None = None,
    ) -> SourceInputSummary:
        """Return source-input diagnostics for the selected DuckDB rows."""
        connection = duckdb.connect(str(self.path), read_only=True)
        try:
            _require_code_column(
                _available_source_columns(connection), source=self.path
            )
            if selection is None or selection.kind == "all_products":
                return _summarize_duckdb_all_products(
                    connection,
                    source=self.path,
                )

            query, parameters = _duckdb_source_query(
                selection=selection,
                select_list="count(*)",
                ordered=False,
            )
            result = connection.execute(query, parameters).fetchone()
            if result is None:
                raise RuntimeError(
                    "DuckDB did not return a row count for the products table."
                )
            return SourceInputSummary(processed_product_count=int(result[0]))
        finally:
            connection.close()

    def iter_batches(
        self,
        *,
        batch_size: int,
        selection: SourceSelection | None = None,
    ) -> Iterator[list[SourceBatchRecord]]:
        """Yield DuckDB product documents as source batch records."""
        if batch_size <= 0:
            raise ValueError("batch_size must be a positive integer.")

        connection = duckdb.connect(str(self.path), read_only=True)
        try:
            columns = tuple(sorted(_available_source_columns(connection)))
            _require_code_column(columns, source=self.path)
            select_list = ", ".join(_quote_identifier(column) for column in columns)
            query, parameters = _duckdb_source_query(
                selection=selection,
                select_list=select_list,
                ordered=True,
            )
            cursor = connection.execute(query, parameters)
            cursor_columns = [column[0] for column in cursor.description]
            while True:
                batch_rows = cursor.fetchmany(batch_size)
                if not batch_rows:
                    break
                yield [
                    source_batch_record_from_document(
                        dict(zip(cursor_columns, row, strict=True))
                    )
                    for row in batch_rows
                ]
        finally:
            connection.close()


class JsonlProductDocumentAdapter:
    """Read full product documents from one JSON Lines source snapshot."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def summarize_input(
        self,
        *,
        selection: SourceSelection | None = None,
    ) -> SourceInputSummary:
        """Return source-input diagnostics for the selected JSONL rows."""
        if selection is None or selection.kind == "all_products":
            return _summarize_jsonl_all_products(self.path)
        if selection.kind == "code_list":
            return SourceInputSummary(
                processed_product_count=len(
                    _selected_jsonl_code_list_documents(self.path, selection.codes)
                )
            )
        _, summary = _selected_jsonl_stable_sample_result(
            self.path,
            sample_size=_required_selection_sample_size(selection),
            seed=_required_selection_seed(selection),
        )
        return summary

    def iter_batches(
        self,
        *,
        batch_size: int,
        selection: SourceSelection | None = None,
    ) -> Iterator[list[SourceBatchRecord]]:
        """Yield JSONL product documents as source batch records."""
        if batch_size <= 0:
            raise ValueError("batch_size must be a positive integer.")

        batch: list[SourceBatchRecord] = []
        for document in self._iter_selected_documents(selection=selection):
            batch.append(source_batch_record_from_document(document))
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _iter_selected_documents(
        self,
        *,
        selection: SourceSelection | None,
    ) -> Iterator[ProductDocument]:
        if selection is None or selection.kind == "all_products":
            return _iter_valid_product_documents(self.path)
        if selection.kind == "code_list":
            return iter(_selected_jsonl_code_list_documents(self.path, selection.codes))
        documents, _ = _selected_jsonl_stable_sample_result(
            self.path,
            sample_size=_required_selection_sample_size(selection),
            seed=_required_selection_seed(selection),
        )
        return iter(documents)


def source_batch_record_from_document(
    document: ProductDocument | Mapping[str, object],
) -> SourceBatchRecord:
    """Return one batch record from a validated product document."""
    product_document = validate_product_document(document)
    return SourceBatchRecord(
        source_product=source_product_from_product_document(product_document),
        product_document=product_document,
    )


def validate_product_document(
    document: object,
) -> ProductDocument:
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


def source_product_from_product_document(document: ProductDocument) -> SourceProduct:
    """Project one full product document into the migrated source-product view."""
    payload = document.document
    source_product_row = {
        column: _source_product_base_value(payload, column)
        for column in SOURCE_PRODUCT_BASE_FIELD_TO_COLUMN.values()
        if column in payload
    }
    source_product_row.update(_source_product_nutriment_values(payload))
    return validate_source_product(source_product_row)


def _source_product_base_value(
    document: Mapping[str, object],
    column: str,
) -> object:
    value = document.get(column)
    if column in {"product_name", "ingredients_text"} and isinstance(value, list):
        return _localized_text(cast(list[object], value), column)
    return value


def _source_product_nutriment_values(
    document: Mapping[str, object],
) -> dict[str, object]:
    projected = {
        column: document[column]
        for column in SOURCE_PRODUCT_NUTRIMENT_COLUMNS
        if column in document
    }
    nutriments = document.get("nutriments")
    if nutriments is None:
        return projected
    if isinstance(nutriments, Mapping):
        nutriments_mapping = cast(Mapping[str, object], nutriments)
        projected.update(
            {
                column: nutriments_mapping[column]
                for column in SOURCE_PRODUCT_NUTRIMENT_COLUMNS
                if column in nutriments_mapping
            }
        )
        return projected
    if isinstance(nutriments, list):
        projected.update(_nutriment_list_values(cast(list[object], nutriments)))
        return projected
    raise ValueError(
        "ProductDocument field 'nutriments' must be an object, list, or null."
    )


def _nutriment_list_values(values: list[object]) -> dict[str, object]:
    projected: dict[str, object] = {}
    supported_columns = frozenset(SOURCE_PRODUCT_NUTRIMENT_COLUMNS)
    for item in values:
        if not isinstance(item, Mapping):
            raise ValueError("ProductDocument field 'nutriments' must contain objects.")
        item_mapping = cast(Mapping[str, object], item)
        name = _optional_text(item_mapping.get("name"))
        value_100g = item_mapping.get("100g")
        if name is None or value_100g is None:
            continue
        column = f"{name}_100g"
        if column in supported_columns:
            projected[column] = value_100g
    return projected


def _localized_text(values: list[object], column: str) -> str | None:
    localized_values: list[tuple[object, str]] = []
    for item in values:
        if not isinstance(item, Mapping):
            raise ValueError(f"ProductDocument field {column!r} must contain objects.")
        item_mapping = cast(Mapping[str, object], item)
        text = _optional_text(item_mapping.get("text"))
        if text is not None:
            localized_values.append((item_mapping.get("lang"), text))

    main_text = next(
        (text for language, text in localized_values if language == "main"),
        None,
    )
    if main_text is not None:
        return main_text
    if not localized_values:
        return None
    return localized_values[0][1]


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


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


def _iter_nonblank_jsonl_lines(path: Path) -> Iterator[tuple[int, str]]:
    if tuple(suffix.lower() for suffix in path.suffixes[-2:]) == (".jsonl", ".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if line.strip():
                    yield line_number, line
        return

    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if line.strip():
                yield line_number, line


def _iter_jsonl_documents(path: Path) -> Iterator[tuple[int, dict[str, object]]]:
    for line_number, line in _iter_nonblank_jsonl_lines(path):
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(
                f"JSONL source line {line_number} is not a product object."
            )
        yield line_number, cast(dict[str, object], payload)


def _iter_valid_product_documents(path: Path) -> Iterator[ProductDocument]:
    for line_number, document in _iter_jsonl_documents(path):
        product_document, skipped_row = _validated_jsonl_product_document(
            document,
            line_number=line_number,
        )
        if skipped_row is None:
            assert product_document is not None
            yield product_document


def _selected_jsonl_code_list_documents(
    path: Path,
    codes: Sequence[str],
) -> list[ProductDocument]:
    selected_codes = set(codes)
    documents = [
        validate_product_document(document)
        for _, document in _iter_jsonl_documents(path)
        if _raw_product_code(document) in selected_codes
    ]
    return sorted(documents, key=lambda document: document.code)


def _selected_jsonl_stable_sample_result(
    path: Path,
    *,
    sample_size: int,
    seed: int,
) -> tuple[list[ProductDocument], SourceInputSummary]:
    heap: list[tuple[int, str, int, ProductDocument]] = []
    skipped_row_count = 0
    skipped_row_examples: list[SkippedSourceRow] = []
    for line_number, document in _iter_jsonl_documents(path):
        product_document, skipped_row = _validated_jsonl_product_document(
            document,
            line_number=line_number,
        )
        if skipped_row is not None:
            skipped_row_count = _record_skipped_source_row(
                skipped_row,
                skipped_row_count=skipped_row_count,
                skipped_row_examples=skipped_row_examples,
            )
            continue
        assert product_document is not None
        index = line_number
        code = product_document.code
        stable_hash = _stable_sample_hash(code, seed)
        entry = (-stable_hash, code, index, product_document)
        if len(heap) < sample_size:
            heapq.heappush(heap, entry)
            continue
        if entry > heap[0]:
            heapq.heapreplace(heap, entry)
    documents = [
        document
        for _, _, _, document in sorted(
            heap,
            key=lambda entry: (-entry[0], entry[1], entry[2]),
        )
    ]
    return (
        documents,
        SourceInputSummary(
            processed_product_count=len(documents),
            skipped_row_count=skipped_row_count,
            skipped_row_examples=tuple(skipped_row_examples),
        ),
    )


def _stable_sample_hash(code: str, seed: int) -> int:
    digest = hashlib.sha256(f"{code}::{seed}".encode()).hexdigest()
    return int(digest, 16)


def _required_selection_sample_size(selection: SourceSelection) -> int:
    if selection.sample_size is None:
        raise ValueError("stable_sample selections must define sample_size.")
    return selection.sample_size


def _required_selection_seed(selection: SourceSelection) -> int:
    if selection.seed is None:
        raise ValueError("stable_sample selections must define seed.")
    return selection.seed


def _duckdb_source_query(
    *,
    selection: SourceSelection | None,
    select_list: str,
    ordered: bool,
) -> tuple[str, list[object]]:
    if selection is None or selection.kind == "all_products":
        query = f"""
            select {select_list}
            from products
            where code is not null
              and trim(code) != ''
        """
        if ordered:
            query += " order by code"
        return query, []

    if selection.kind == "stable_sample":
        if not ordered:
            return (
                """
                select least(count(*), ?)
                from products
                where code is not null
                  and trim(code) != ''
                """,
                [_required_selection_sample_size(selection)],
            )
        query = f"""
            select {select_list}
            from products
            where code is not null
              and trim(code) != ''
            order by hash(code || ?), code
            limit ?
        """
        return (
            query,
            [
                f"::{_required_selection_seed(selection)}",
                _required_selection_sample_size(selection),
            ],
        )

    placeholders = ", ".join("?" for _ in selection.codes)
    query = f"""
        select {select_list}
        from products
        where code in ({placeholders})
    """
    if ordered:
        query += " order by code"
    return query, list(selection.codes)


def _available_source_columns(
    connection: duckdb.DuckDBPyConnection,
) -> set[str]:
    return {
        row[0]
        for row in connection.execute("describe select * from products").fetchall()
    }


def _require_code_column(
    available_columns: Collection[str],
    *,
    source: Path,
) -> None:
    if "code" not in available_columns:
        raise ValueError(
            f"Source snapshot {source} must expose a products.code column."
        )


def _summarize_duckdb_all_products(
    connection: duckdb.DuckDBPyConnection,
    *,
    source: Path,
) -> SourceInputSummary:
    result = connection.execute(
        """
        select
            count(*) filter (where code is not null and trim(code) != ''),
            count(*) filter (where code is null or trim(code) = '')
        from products
        """
    ).fetchone()
    if result is None:
        raise RuntimeError(
            f"DuckDB source snapshot {source} did not return aggregate row counts."
        )
    processed_product_count = int(result[0] or 0)
    skipped_row_count = int(result[1] or 0)
    skipped_row_examples = tuple(
        SkippedSourceRow(
            location=f"duckdb rowid {int(rowid)}",
            reason=_MISSING_OR_BLANK_CODE_REASON,
        )
        for (rowid,) in connection.execute(
            """
            select rowid
            from products
            where code is null
               or trim(code) = ''
            order by rowid
            limit ?
            """,
            [_SKIPPED_SOURCE_ROW_EXAMPLE_LIMIT],
        ).fetchall()
    )
    return SourceInputSummary(
        processed_product_count=processed_product_count,
        skipped_row_count=skipped_row_count,
        skipped_row_examples=skipped_row_examples,
    )


def _summarize_jsonl_all_products(path: Path) -> SourceInputSummary:
    processed_product_count = 0
    skipped_row_count = 0
    skipped_row_examples: list[SkippedSourceRow] = []
    for line_number, document in _iter_jsonl_documents(path):
        _, skipped_row = _validated_jsonl_product_document(
            document,
            line_number=line_number,
        )
        if skipped_row is not None:
            skipped_row_count = _record_skipped_source_row(
                skipped_row,
                skipped_row_count=skipped_row_count,
                skipped_row_examples=skipped_row_examples,
            )
            continue
        processed_product_count += 1
    return SourceInputSummary(
        processed_product_count=processed_product_count,
        skipped_row_count=skipped_row_count,
        skipped_row_examples=tuple(skipped_row_examples),
    )


def _validated_jsonl_product_document(
    document: dict[str, object],
    *,
    line_number: int,
) -> tuple[ProductDocument | None, SkippedSourceRow | None]:
    try:
        return validate_product_document(document), None
    except ValueError as exc:
        reason = _skip_reason_from_product_document_error(exc)
        if reason is None:
            raise
        return (
            None,
            SkippedSourceRow(
                location=f"jsonl line {line_number}",
                reason=reason,
            ),
        )


def _record_skipped_source_row(
    skipped_row: SkippedSourceRow,
    *,
    skipped_row_count: int,
    skipped_row_examples: list[SkippedSourceRow],
) -> int:
    skipped_row_count += 1
    if len(skipped_row_examples) < _SKIPPED_SOURCE_ROW_EXAMPLE_LIMIT:
        skipped_row_examples.append(skipped_row)
    return skipped_row_count


def _skip_reason_from_product_document_error(
    error: ValueError,
) -> str | None:
    if str(error) not in {
        "ProductDocument requires a string 'code' field.",
        "ProductDocument requires a non-empty 'code' field.",
    }:
        return None
    return _MISSING_OR_BLANK_CODE_REASON


def _raw_product_code(document: Mapping[str, object]) -> str | None:
    value = document.get("code")
    if not isinstance(value, str):
        return None
    code = value.strip()
    return code or None


def _quote_identifier(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


__all__ = [
    "SourceSnapshotAdapter",
    "count_source_products",
    "iter_source_batches",
    "summarize_source_input",
    "source_batch_record_from_document",
    "source_product_from_product_document",
    "source_snapshot_adapter_for",
    "source_snapshot_id_for",
    "validate_product_document",
]
