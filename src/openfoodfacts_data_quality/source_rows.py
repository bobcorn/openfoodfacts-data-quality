from __future__ import annotations

import json
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from typing import cast

from openfoodfacts_data_quality.contracts.raw import (
    RAW_INPUT_COLUMNS,
    RAW_NUTRIMENT_COLUMNS,
    RawProductRow,
    validate_raw_product_row,
)
from openfoodfacts_data_quality.structured_values import (
    is_string_object_mapping,
    object_list_or_empty,
)

PUBLIC_SOURCE_SNAPSHOT_COLUMNS: tuple[str, ...] = (
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
PUBLIC_CSV_EXPORT_COLUMNS: tuple[str, ...] = RAW_INPUT_COLUMNS

_KNOWN_RAW_NUTRIMENT_COLUMNS = frozenset(RAW_NUTRIMENT_COLUMNS)
_STRUCTURED_SOURCE_COLUMNS = (
    "product_name",
    "ingredients_text",
    "ingredients_tags",
    "categories_tags",
    "labels_tags",
    "countries_tags",
    "nutriments",
)


@dataclass(frozen=True, slots=True)
class SupportedSourceContract:
    """One supported source row shape plus its normalization path."""

    name: str
    required_columns: tuple[str, ...]
    normalize_row: Callable[[Mapping[str, object]], RawProductRow]

    def missing_columns(self, available_columns: Collection[str]) -> tuple[str, ...]:
        """Return the required columns that are not present in one source table."""
        return tuple(
            column
            for column in self.required_columns
            if column not in available_columns
        )


def normalize_raw_input_row(
    value: RawProductRow | Mapping[str, object],
) -> RawProductRow:
    """Normalize one public raw input row into the internal runtime contract."""
    if isinstance(value, RawProductRow):
        return value
    if looks_like_public_source_snapshot_row(value):
        return normalize_public_source_row(value)
    return normalize_public_csv_export_row(value)


def normalize_public_source_row(row: Mapping[str, object]) -> RawProductRow:
    """Project one public Open Food Facts source row into the normalized raw runtime contract."""
    normalized_row: dict[str, object] = {
        "code": row.get("code"),
        "created_t": row.get("created_t"),
        "product_name": extract_localized_text(row.get("product_name")),
        "quantity": row.get("quantity"),
        "product_quantity": row.get("product_quantity"),
        "serving_size": row.get("serving_size"),
        "serving_quantity": row.get("serving_quantity"),
        "brands": row.get("brands"),
        "categories": row.get("categories"),
        "labels": row.get("labels"),
        "emb_codes": row.get("emb_codes"),
        "ingredients_text": extract_localized_text(row.get("ingredients_text")),
        "ingredients_tags": extract_tag_values(row.get("ingredients_tags")),
        "nutriscore_grade": row.get("nutriscore_grade"),
        "nutriscore_score": row.get("nutriscore_score"),
        "categories_tags": extract_tag_values(row.get("categories_tags")),
        "labels_tags": extract_tag_values(row.get("labels_tags")),
        "countries_tags": extract_tag_values(row.get("countries_tags")),
        "no_nutrition_data": row.get("no_nutrition_data"),
    }
    normalized_row.update(extract_nutriment_columns(row.get("nutriments")))
    return validate_raw_product_row(normalized_row)


def normalize_public_csv_export_row(row: Mapping[str, object]) -> RawProductRow:
    """Validate one flat public CSV export row."""
    return validate_raw_product_row(
        {
            column: row.get(column)
            for column in PUBLIC_CSV_EXPORT_COLUMNS
            if column in row
        }
    )


def looks_like_public_source_snapshot_row(row: Mapping[str, object]) -> bool:
    """Return whether one raw input row looks like the structured snapshot contract."""
    for column in _STRUCTURED_SOURCE_COLUMNS:
        if column not in row:
            continue
        parsed_value = _decode_structured_string(row.get(column))
        if isinstance(parsed_value, (list, dict)):
            return True
    return False


def extract_localized_text(value: object) -> str | None:
    """Pick one readable string from localized Open Food Facts text values."""
    parsed_value = _decode_structured_string(value)
    items = object_list_or_empty(parsed_value)
    if not isinstance(parsed_value, list):
        return _optional_text(parsed_value)

    fallback: str | None = None
    for item in items:
        if not is_string_object_mapping(item):
            continue
        text = _optional_text(item.get("text"))
        if text is None:
            continue
        if item.get("lang") == "main":
            return text
        if fallback is None:
            fallback = text
    return fallback


def extract_tag_values(value: object) -> object:
    """Return tag-like values as lists when they are serialized in CSV rows."""
    parsed_value = _decode_structured_string(value)
    if isinstance(parsed_value, list):
        return cast(list[object], parsed_value)
    return value


def extract_nutriment_columns(value: object) -> dict[str, object]:
    """Flatten supported Open Food Facts nutriment structs into normalized *_100g fields."""
    parsed_value = _decode_structured_string(value)
    items = object_list_or_empty(parsed_value)
    if not isinstance(parsed_value, list):
        return {}

    flattened: dict[str, object] = {}
    for item in items:
        if not is_string_object_mapping(item):
            continue
        name = _optional_text(item.get("name"))
        number = item.get("100g")
        if name is None or number is None:
            continue
        column_name = f"{name}_100g"
        if column_name not in _KNOWN_RAW_NUTRIMENT_COLUMNS:
            continue
        flattened[column_name] = number
    return flattened


def _decode_structured_string(value: object) -> object:
    """Decode one JSON serialized structured value used by CSV source rows."""
    if not isinstance(value, str):
        return value

    text = value.strip()
    if not text or text[0] not in "[{":
        return value

    try:
        return cast(object, json.loads(text))
    except json.JSONDecodeError:
        return value


def _optional_text(value: object) -> str | None:
    """Return one stripped optional text value."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


PUBLIC_SOURCE_SNAPSHOT_CONTRACT = SupportedSourceContract(
    name="public_source_snapshot",
    required_columns=PUBLIC_SOURCE_SNAPSHOT_COLUMNS,
    normalize_row=normalize_public_source_row,
)
PUBLIC_CSV_EXPORT_CONTRACT = SupportedSourceContract(
    name="public_csv_export",
    required_columns=PUBLIC_CSV_EXPORT_COLUMNS,
    normalize_row=normalize_public_csv_export_row,
)
SUPPORTED_SOURCE_CONTRACTS: tuple[SupportedSourceContract, ...] = (
    PUBLIC_SOURCE_SNAPSHOT_CONTRACT,
    PUBLIC_CSV_EXPORT_CONTRACT,
)


__all__ = [
    "PUBLIC_SOURCE_SNAPSHOT_COLUMNS",
    "PUBLIC_SOURCE_SNAPSHOT_CONTRACT",
    "PUBLIC_CSV_EXPORT_COLUMNS",
    "PUBLIC_CSV_EXPORT_CONTRACT",
    "SUPPORTED_SOURCE_CONTRACTS",
    "SupportedSourceContract",
    "extract_localized_text",
    "extract_nutriment_columns",
    "looks_like_public_source_snapshot_row",
    "normalize_public_csv_export_row",
    "normalize_public_source_row",
    "normalize_raw_input_row",
]
