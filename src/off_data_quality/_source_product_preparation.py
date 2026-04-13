from __future__ import annotations

import json
import os
from collections.abc import Collection, Iterable, Mapping
from typing import Any, cast

from off_data_quality._source_input import (
    prepare_canonical_source_row,
    prepare_supported_source_row,
)
from off_data_quality.contracts.source_products import (
    SOURCE_PRODUCT_INPUT_COLUMNS,
    SourceProduct,
)

_CANONICAL_SOURCE_COLUMNS = frozenset(SOURCE_PRODUCT_INPUT_COLUMNS)


def prepare_source_products(
    rows: object,
    *,
    columns: Mapping[str, str] | None = None,
    operation_name: str = "checks.prepare()",
) -> list[SourceProduct]:
    """Prepare one user-provided table or row stream for row-based checks."""
    normalized_columns = _validate_column_mapping(
        columns,
        operation_name=operation_name,
    )
    prepared_rows: list[SourceProduct] = []
    for index, row in enumerate(_iter_input_rows(rows, operation_name=operation_name)):
        prepared_rows.append(
            _prepare_source_product_row(
                row,
                columns=normalized_columns,
                row_index=index,
                operation_name=operation_name,
            )
        )
    return prepared_rows


def _iter_input_rows(
    rows: object,
    *,
    operation_name: str,
    single_row_hint: str | None = None,
) -> Iterable[object]:
    if isinstance(rows, SourceProduct | Mapping):
        hint = f" Use {single_row_hint} for one row." if single_row_hint else ""
        raise TypeError(
            f"{operation_name} expects an iterable of rows or a table-like "
            f"object, not a single row.{hint}"
        )
    if isinstance(rows, str | bytes | os.PathLike):
        raise TypeError(
            f"{operation_name} does not read files. Load the file with csv, "
            "pandas, PyArrow, DuckDB, or another tool, then pass the loaded rows."
        )

    if _is_pyarrow_like_table(rows):
        return _pyarrow_like_rows(rows)
    if _is_pandas_like_table(rows):
        return _pandas_like_rows(rows)
    if _is_duckdb_like_relation(rows):
        return _duckdb_like_rows(rows)
    if not isinstance(rows, Iterable):
        raise TypeError(
            f"{operation_name} expects an iterable of rows or a supported "
            "table-like object."
        )
    return cast(Iterable[object], rows)


def _prepare_source_product_row(
    row: object,
    *,
    columns: Mapping[str, str],
    row_index: int,
    operation_name: str,
) -> SourceProduct:
    if isinstance(row, SourceProduct):
        if columns:
            raise ValueError(
                f"{operation_name} does not accept columns= when rows are already "
                "validated SourceProduct values."
            )
        return row
    if not isinstance(row, Mapping):
        raise TypeError(
            f"{operation_name} expects each row to be either a mapping or a "
            "SourceProduct instance."
        )

    mapping_row = cast(Mapping[str, Any], row)
    if columns:
        remapped_row = _remap_row_columns(
            mapping_row,
            columns=columns,
            row_index=row_index,
            operation_name=operation_name,
        )
        return prepare_canonical_source_row(remapped_row, row_index=row_index)
    return prepare_supported_source_row(mapping_row, row_index=row_index)


def _validate_column_mapping(
    columns: Mapping[str, str] | None,
    *,
    operation_name: str,
) -> dict[str, str]:
    if columns is None:
        return {}

    normalized_columns = {str(key): str(value) for key, value in columns.items()}
    unknown_columns = sorted(
        column
        for column in normalized_columns
        if column not in _CANONICAL_SOURCE_COLUMNS
    )
    if unknown_columns:
        raise ValueError(
            f"{operation_name} received unknown canonical columns in columns=: "
            f"{', '.join(unknown_columns)}."
        )

    duplicate_sources = _duplicate_values(normalized_columns.values())
    if duplicate_sources:
        raise ValueError(
            f"{operation_name} columns= maps multiple canonical columns to the "
            f"same source column: {', '.join(duplicate_sources)}."
        )

    blank_sources = sorted(
        canonical_column
        for canonical_column, source_column in normalized_columns.items()
        if not source_column.strip()
    )
    if blank_sources:
        raise ValueError(
            f"{operation_name} columns= includes blank source names for: "
            f"{', '.join(blank_sources)}."
        )

    return normalized_columns


def _remap_row_columns(
    row: Mapping[str, Any],
    *,
    columns: Mapping[str, str],
    row_index: int,
    operation_name: str,
) -> dict[str, Any]:
    normalized_row = dict(row)
    remapped_row: dict[str, Any] = dict(normalized_row)

    for canonical_column, source_column in columns.items():
        if source_column not in normalized_row:
            raise ValueError(
                f"{operation_name} row {row_index} is missing mapped source "
                f"column {source_column!r} for canonical column "
                f"{canonical_column!r}."
            )
        if canonical_column in normalized_row and source_column != canonical_column:
            raise ValueError(
                f"{operation_name} row {row_index} contains both canonical "
                f"column {canonical_column!r} and mapped source column "
                f"{source_column!r}. Remove the duplicate or columns= mapping."
            )
        remapped_row[canonical_column] = normalized_row[source_column]
        if source_column != canonical_column:
            remapped_row.pop(source_column, None)

    return remapped_row


def _is_pyarrow_like_table(value: object) -> bool:
    return callable(getattr(value, "to_pylist", None))


def _pyarrow_like_rows(value: object) -> Iterable[object]:
    rows = cast(Any, value).to_pylist()
    if not isinstance(rows, Iterable):
        raise TypeError("checks.run() to_pylist() did not return row records.")
    return cast(Iterable[object], rows)


def _is_pandas_like_table(value: object) -> bool:
    return callable(getattr(value, "to_dict", None))


def _pandas_like_rows(value: object) -> Iterable[object]:
    rows = cast(Any, value).to_dict(orient="records")
    if not isinstance(rows, Iterable):
        raise TypeError(
            "checks.run() to_dict(orient='records') did not return row records."
        )
    return cast(Iterable[object], rows)


def _is_duckdb_like_relation(value: object) -> bool:
    return callable(getattr(value, "fetchall", None)) and isinstance(
        getattr(value, "columns", None),
        Collection,
    )


def _duckdb_like_rows(value: object) -> Iterable[object]:
    relation = cast(Any, value)
    columns = [str(column) for column in relation.columns]
    relation_types = [str(type_name) for type_name in getattr(relation, "types", ())]
    normalized_rows: list[dict[str, object]] = []
    for row in relation.fetchall():
        normalized_rows.append(
            {
                column: _normalize_duckdb_value(cell, duckdb_type=duckdb_type)
                for column, cell, duckdb_type in zip(
                    columns,
                    row,
                    relation_types or [""] * len(columns),
                    strict=True,
                )
            }
        )
    return normalized_rows


def _normalize_duckdb_value(
    value: object,
    *,
    duckdb_type: str,
) -> object:
    if duckdb_type != "JSON" or value is None:
        return value
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _duplicate_values(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: list[str] = []
    for value in values:
        if value in seen and value not in duplicates:
            duplicates.append(value)
            continue
        seen.add(value)
    return duplicates


__all__ = [
    "prepare_source_products",
]
