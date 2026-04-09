from __future__ import annotations

import gzip
import hashlib
import heapq
import json
from collections.abc import Collection, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Protocol, cast

import duckdb

from app.source.datasets import SourceSelection
from app.source.models import ProductDocument, SourceBatchRecord, SourceSnapshotFormat
from app.source.snapshots import source_snapshot_id_for
from openfoodfacts_data_quality.contracts.source_products import (
    SOURCE_PRODUCT_BASE_FIELD_TO_COLUMN,
    SOURCE_PRODUCT_NUTRIMENT_COLUMNS,
    SourceProduct,
    validate_source_product,
)


class SourceSnapshotAdapter(Protocol):
    """Adapter surface used by the application source facade."""

    def count_products(self, *, selection: SourceSelection | None = None) -> int: ...

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
    return source_snapshot_adapter_for(path).count_products(selection=selection)


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

    def count_products(self, *, selection: SourceSelection | None = None) -> int:
        """Return the selected products count from the DuckDB products table."""
        connection = duckdb.connect(str(self.path), read_only=True)
        try:
            _require_code_column(
                _available_source_columns(connection), source=self.path
            )
            _require_nonblank_product_codes(connection, source=self.path)
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
            return int(result[0])
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
            _require_nonblank_product_codes(connection, source=self.path)
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

    def count_products(self, *, selection: SourceSelection | None = None) -> int:
        """Return the selected products count from the JSONL source snapshot."""
        return sum(1 for _ in self._iter_selected_documents(selection=selection))

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
            return _iter_product_documents(self.path)
        if selection.kind == "code_list":
            return iter(_selected_jsonl_code_list_documents(self.path, selection.codes))
        return iter(
            _selected_jsonl_stable_sample_documents(
                self.path,
                sample_size=_required_selection_sample_size(selection),
                seed=_required_selection_seed(selection),
            )
        )


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


def _iter_nonblank_jsonl_lines(path: Path) -> Iterator[str]:
    if tuple(suffix.lower() for suffix in path.suffixes[-2:]) == (".jsonl", ".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield line
        return

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield line


def _iter_jsonl_documents(path: Path) -> Iterator[dict[str, object]]:
    for line_number, line in enumerate(_iter_nonblank_jsonl_lines(path), start=1):
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(
                f"JSONL source line {line_number} is not a product object."
            )
        yield cast(dict[str, object], payload)


def _iter_product_documents(path: Path) -> Iterator[ProductDocument]:
    for document in _iter_jsonl_documents(path):
        yield validate_product_document(document)


def _selected_jsonl_code_list_documents(
    path: Path,
    codes: Sequence[str],
) -> list[ProductDocument]:
    selected_codes = set(codes)
    documents = [
        product_document
        for product_document in _iter_product_documents(path)
        if product_document.code in selected_codes
    ]
    return sorted(documents, key=lambda document: document.code)


def _selected_jsonl_stable_sample_documents(
    path: Path,
    *,
    sample_size: int,
    seed: int,
) -> list[ProductDocument]:
    heap: list[tuple[int, str, int, ProductDocument]] = []
    for index, product_document in enumerate(_iter_product_documents(path)):
        code = product_document.code
        stable_hash = _stable_sample_hash(code, seed)
        entry = (-stable_hash, code, index, product_document)
        if len(heap) < sample_size:
            heapq.heappush(heap, entry)
            continue
        if entry > heap[0]:
            heapq.heapreplace(heap, entry)
    return [
        document
        for _, _, _, document in sorted(
            heap,
            key=lambda entry: (-entry[0], entry[1], entry[2]),
        )
    ]


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
        query = f"select {select_list} from products"
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


def _require_nonblank_product_codes(
    connection: duckdb.DuckDBPyConnection,
    *,
    source: Path,
) -> None:
    result = connection.execute(
        """
        select count(*)
        from products
        where code is null
           or trim(code) = ''
        """
    ).fetchone()
    invalid_count = 0 if result is None else int(result[0])
    if invalid_count:
        raise ValueError(
            f"Source snapshot {source} contains {invalid_count} product(s) "
            "without a non-empty code."
        )


def _quote_identifier(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


__all__ = [
    "SourceSnapshotAdapter",
    "count_source_products",
    "iter_source_batches",
    "source_batch_record_from_document",
    "source_product_from_product_document",
    "source_snapshot_adapter_for",
    "source_snapshot_id_for",
    "validate_product_document",
]
