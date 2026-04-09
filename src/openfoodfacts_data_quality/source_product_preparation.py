from __future__ import annotations

import os
from collections.abc import Collection, Iterable, Mapping
from typing import Any, cast

from openfoodfacts_data_quality.contracts.source_products import (
    SOURCE_PRODUCT_INPUT_COLUMNS,
    SOURCE_PRODUCT_NUTRIMENT_COLUMNS,
    SourceProduct,
    validate_source_product,
)

_CANONICAL_SOURCE_COLUMNS = frozenset(SOURCE_PRODUCT_INPUT_COLUMNS)
_CANONICAL_NUTRIMENT_COLUMNS = frozenset(SOURCE_PRODUCT_NUTRIMENT_COLUMNS)
OFF_PRODUCT_EXPORT_COLUMNS: tuple[str, ...] = (
    "code",
    "created_t",
    "product_name",
    "quantity",
    "product_quantity",
    "serving_size",
    "serving_quantity",
    "brands",
    "categories",
    "labels",
    "emb_codes",
    "ingredients_text",
    "ingredients_tags",
    "nutriscore_grade",
    "nutriscore_score",
    "categories_tags",
    "labels_tags",
    "countries_tags",
    "no_nutrition_data",
    "nutriments",
)


def prepare_source_products(
    rows: object,
    *,
    columns: Mapping[str, str] | None = None,
) -> list[SourceProduct]:
    """Prepare one user-provided table or row stream for row-based checks."""
    normalized_columns = _validate_column_mapping(columns)
    prepared_rows: list[SourceProduct] = []
    for index, row in enumerate(_iter_input_rows(rows)):
        prepared_rows.append(
            _prepare_source_product_row(
                row,
                columns=normalized_columns,
                row_index=index,
            )
        )
    return prepared_rows


def _iter_input_rows(rows: object) -> Iterable[object]:
    if isinstance(rows, SourceProduct | Mapping):
        raise TypeError(
            "checks.run() expects an iterable of rows or a table-like object, "
            "not a single row."
        )
    if isinstance(rows, str | bytes | os.PathLike):
        raise TypeError(
            "checks.run() does not read files. Load the file with csv, pandas, "
            "PyArrow, DuckDB, or another tool, then pass the loaded rows."
        )

    if _is_pyarrow_like_table(rows):
        return _pyarrow_like_rows(rows)
    if _is_pandas_like_table(rows):
        return _pandas_like_rows(rows)
    if _is_duckdb_like_relation(rows):
        return _duckdb_like_rows(rows)
    if not isinstance(rows, Iterable):
        raise TypeError(
            "checks.run() expects an iterable of rows or a supported table-like object."
        )
    return cast(Iterable[object], rows)


def _prepare_source_product_row(
    row: object,
    *,
    columns: Mapping[str, str],
    row_index: int,
) -> SourceProduct:
    if isinstance(row, SourceProduct):
        if columns:
            raise ValueError(
                "checks.run() does not accept columns= when rows are already "
                "validated SourceProduct values."
            )
        return row
    if not isinstance(row, Mapping):
        raise TypeError(
            "checks.run() expects each row to be either a mapping or a "
            "SourceProduct instance."
        )

    mapping_row = cast(Mapping[str, Any], row)
    if columns:
        return _prepare_canonical_mapping_row(
            mapping_row,
            columns=columns,
            row_index=row_index,
        )
    if "nutriments" in mapping_row:
        return _prepare_off_product_export_row(mapping_row, row_index=row_index)
    return _prepare_canonical_mapping_row(
        mapping_row,
        columns={},
        row_index=row_index,
    )


def _prepare_canonical_mapping_row(
    row: Mapping[str, Any],
    *,
    columns: Mapping[str, str],
    row_index: int,
) -> SourceProduct:
    remapped_row = _remap_row_columns(row, columns=columns, row_index=row_index)
    projected_row = {
        column: value
        for column, value in remapped_row.items()
        if column in _CANONICAL_SOURCE_COLUMNS
    }
    return validate_source_product(projected_row)


def _prepare_off_product_export_row(
    row: Mapping[str, Any],
    *,
    row_index: int,
) -> SourceProduct:
    missing_columns = tuple(
        column for column in OFF_PRODUCT_EXPORT_COLUMNS if column not in row
    )
    if missing_columns:
        raise ValueError(
            f"checks.run() row {row_index} includes 'nutriments' but does "
            "not match the Open Food Facts product export shape. Missing "
            f"columns: {', '.join(missing_columns)}."
        )

    overlapping_nutriments = sorted(
        column for column in _CANONICAL_NUTRIMENT_COLUMNS if column in row
    )
    if overlapping_nutriments:
        raise ValueError(
            f"checks.run() row {row_index} mixes structured nutriments with "
            "canonical nutriment columns: "
            f"{', '.join(overlapping_nutriments)}."
        )

    return project_off_product_export_row(row)


def project_off_product_export_row(row: Mapping[str, object]) -> SourceProduct:
    """Project one Open Food Facts product export row into SourceProduct."""
    projected_row: dict[str, object] = {
        "code": row.get("code"),
        "created_t": row.get("created_t"),
        "product_name": _localized_text(row.get("product_name"), "product_name"),
        "quantity": row.get("quantity"),
        "product_quantity": row.get("product_quantity"),
        "serving_size": row.get("serving_size"),
        "serving_quantity": row.get("serving_quantity"),
        "brands": row.get("brands"),
        "categories": row.get("categories"),
        "labels": row.get("labels"),
        "emb_codes": row.get("emb_codes"),
        "ingredients_text": _localized_text(
            row.get("ingredients_text"),
            "ingredients_text",
        ),
        "ingredients_tags": _tag_values(
            row.get("ingredients_tags"), "ingredients_tags"
        ),
        "nutriscore_grade": row.get("nutriscore_grade"),
        "nutriscore_score": row.get("nutriscore_score"),
        "categories_tags": _tag_values(row.get("categories_tags"), "categories_tags"),
        "labels_tags": _tag_values(row.get("labels_tags"), "labels_tags"),
        "countries_tags": _tag_values(row.get("countries_tags"), "countries_tags"),
        "no_nutrition_data": row.get("no_nutrition_data"),
    }
    projected_row.update(_nutriment_columns(row.get("nutriments")))
    return validate_source_product(projected_row)


def _validate_column_mapping(
    columns: Mapping[str, str] | None,
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
            "checks.run() received unknown canonical columns in columns=: "
            f"{', '.join(unknown_columns)}."
        )

    duplicate_sources = _duplicate_values(normalized_columns.values())
    if duplicate_sources:
        raise ValueError(
            "checks.run() columns= maps multiple canonical columns to the "
            f"same source column: {', '.join(duplicate_sources)}."
        )

    blank_sources = sorted(
        canonical_column
        for canonical_column, source_column in normalized_columns.items()
        if not source_column.strip()
    )
    if blank_sources:
        raise ValueError(
            "checks.run() columns= includes blank source names for: "
            f"{', '.join(blank_sources)}."
        )

    return normalized_columns


def _remap_row_columns(
    row: Mapping[str, Any],
    *,
    columns: Mapping[str, str],
    row_index: int,
) -> dict[str, Any]:
    normalized_row = dict(row)
    remapped_row: dict[str, Any] = dict(normalized_row)

    for canonical_column, source_column in columns.items():
        if source_column not in normalized_row:
            raise ValueError(
                f"checks.run() row {row_index} is missing mapped source "
                f"column {source_column!r} for canonical column "
                f"{canonical_column!r}."
            )
        if canonical_column in normalized_row and source_column != canonical_column:
            raise ValueError(
                f"checks.run() row {row_index} contains both canonical "
                f"column {canonical_column!r} and mapped source column "
                f"{source_column!r}. Remove the duplicate or columns= mapping."
            )
        remapped_row[canonical_column] = normalized_row[source_column]
        if source_column != canonical_column:
            remapped_row.pop(source_column, None)

    return remapped_row


def _localized_text(value: object, column: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError(
            f"Open Food Facts product export column {column!r} must be a list or null."
        )

    fallback: str | None = None
    for item in cast(list[object], value):
        if not isinstance(item, Mapping):
            raise ValueError(
                f"Open Food Facts product export column {column!r} must contain objects."
            )
        item_mapping = cast(Mapping[str, object], item)
        text = _optional_text(item_mapping.get("text"))
        if text is None:
            continue
        if item_mapping.get("lang") == "main":
            return text
        if fallback is None:
            fallback = text
    return fallback


def _tag_values(value: object, column: str) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError(
            f"Open Food Facts product export column {column!r} must be a list or null."
        )

    tags: list[str] = []
    for item in cast(list[object], value):
        if not isinstance(item, str):
            raise ValueError(
                f"Open Food Facts product export column {column!r} must contain strings."
            )
        tags.append(item)
    return tags


def _nutriment_columns(value: object) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, list):
        raise ValueError(
            "Open Food Facts product export column 'nutriments' must be a list or null."
        )

    flattened: dict[str, object] = {}
    for item in cast(list[object], value):
        if not isinstance(item, Mapping):
            raise ValueError(
                "Open Food Facts product export column 'nutriments' must contain objects."
            )
        item_mapping = cast(Mapping[str, object], item)
        name = _optional_text(item_mapping.get("name"))
        value_100g = item_mapping.get("100g")
        if name is None or value_100g is None:
            continue
        column_name = f"{name}_100g"
        if column_name in _CANONICAL_NUTRIMENT_COLUMNS:
            flattened[column_name] = value_100g
    return flattened


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


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
    return [dict(zip(columns, row, strict=True)) for row in relation.fetchall()]


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
    "OFF_PRODUCT_EXPORT_COLUMNS",
    "prepare_source_products",
    "project_off_product_export_row",
]
