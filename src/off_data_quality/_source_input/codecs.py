from __future__ import annotations

import ast
import json
from collections.abc import Mapping
from typing import cast

from off_data_quality._source_input.contracts import (
    OFF_FULL_DOCUMENT_CONTRACT_NAME,
    OFF_PRODUCT_EXPORT_CONTRACT_NAME,
)
from off_data_quality.contracts.source_products import SOURCE_PRODUCT_NUTRIMENT_COLUMNS

_CANONICAL_NUTRIMENT_COLUMNS = frozenset(SOURCE_PRODUCT_NUTRIMENT_COLUMNS)


def parse_raw_structured_string(
    value: object,
    *,
    column: str,
    contract_name: str,
) -> list[object] | Mapping[str, object] | None:
    """Parse one raw structured cell from a supported OFF carrier."""
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None
    if text[0] not in "[{" or text[-1] not in "]}":
        return None

    for decoder in (json.loads, ast.literal_eval):
        try:
            decoded = decoder(text)
        except ValueError, SyntaxError:
            continue
        if isinstance(decoded, list):
            return cast(list[object], decoded)
        if isinstance(decoded, Mapping):
            return cast(Mapping[str, object], decoded)
        raise ValueError(
            f"{contract_name} column {column!r} must decode to an object or list."
        )

    raise ValueError(
        f"{contract_name} column {column!r} contains an unsupported raw "
        "structured string."
    )


def decode_export_localized_text(
    value: object,
    *,
    column: str,
) -> str | None:
    """Decode one localized text field from an OFF product export row."""
    if value is None:
        return None
    if isinstance(value, list):
        return decode_localized_text_list(
            cast(list[object], value),
            column=column,
            contract_name=OFF_PRODUCT_EXPORT_CONTRACT_NAME,
        )

    decoded = parse_raw_structured_string(
        value,
        column=column,
        contract_name=OFF_PRODUCT_EXPORT_CONTRACT_NAME,
    )
    if decoded is None:
        raise ValueError(
            f"{OFF_PRODUCT_EXPORT_CONTRACT_NAME} column {column!r} must be a "
            "localized list or null."
        )
    return decode_localized_text_list(
        decoded,
        column=column,
        contract_name=OFF_PRODUCT_EXPORT_CONTRACT_NAME,
    )


def decode_full_document_localized_text(
    value: object,
    *,
    column: str,
) -> str | None:
    """Decode one localized text field from an OFF full document."""
    if value is None:
        return None
    if isinstance(value, str):
        return optional_text(value)
    if isinstance(value, list):
        return decode_localized_text_list(
            cast(list[object], value),
            column=column,
            contract_name=OFF_FULL_DOCUMENT_CONTRACT_NAME,
        )
    raise ValueError(
        f"{OFF_FULL_DOCUMENT_CONTRACT_NAME} column {column!r} must be a string, "
        "a localized list, or null."
    )


def decode_export_tag_values(
    value: object,
    *,
    column: str,
) -> list[str] | None:
    """Decode one tag field from an OFF product export row."""
    if value is None:
        return None
    if isinstance(value, list):
        return decode_tag_list(
            cast(list[object], value),
            column=column,
            contract_name=OFF_PRODUCT_EXPORT_CONTRACT_NAME,
        )

    decoded = parse_raw_structured_string(
        value,
        column=column,
        contract_name=OFF_PRODUCT_EXPORT_CONTRACT_NAME,
    )
    if decoded is None:
        raise ValueError(
            f"{OFF_PRODUCT_EXPORT_CONTRACT_NAME} column {column!r} must be a list "
            "or null."
        )
    return decode_tag_list(
        decoded,
        column=column,
        contract_name=OFF_PRODUCT_EXPORT_CONTRACT_NAME,
    )


def decode_full_document_tag_values(
    value: object,
    *,
    column: str,
) -> str | list[str] | None:
    """Decode one tag field from an OFF full document."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return decode_tag_list(
            cast(list[object], value),
            column=column,
            contract_name=OFF_FULL_DOCUMENT_CONTRACT_NAME,
        )
    raise ValueError(
        f"{OFF_FULL_DOCUMENT_CONTRACT_NAME} column {column!r} must be a string, "
        "a list, or null."
    )


def decode_export_nutriment_columns(value: object) -> dict[str, object]:
    """Decode OFF product-export nutriments into canonical columns."""
    if value is None:
        return {}
    if isinstance(value, list):
        return decode_nutriment_list(
            cast(list[object], value),
            contract_name=OFF_PRODUCT_EXPORT_CONTRACT_NAME,
        )

    decoded = parse_raw_structured_string(
        value,
        column="nutriments",
        contract_name=OFF_PRODUCT_EXPORT_CONTRACT_NAME,
    )
    if decoded is None:
        raise ValueError(
            f"{OFF_PRODUCT_EXPORT_CONTRACT_NAME} column 'nutriments' must be a "
            "list or null."
        )
    if isinstance(decoded, Mapping):
        raise ValueError(
            f"{OFF_PRODUCT_EXPORT_CONTRACT_NAME} column 'nutriments' must decode "
            "to a list."
        )
    return decode_nutriment_list(
        decoded,
        contract_name=OFF_PRODUCT_EXPORT_CONTRACT_NAME,
    )


def decode_full_document_nutriment_columns(value: object) -> dict[str, object]:
    """Decode OFF full-document nutriments into canonical columns."""
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(
            f"{OFF_FULL_DOCUMENT_CONTRACT_NAME} column 'nutriments' must be an "
            "object or null."
        )
    return decode_nutriment_mapping(cast(Mapping[str, object], value))


def decode_localized_text_list(
    value: object,
    *,
    column: str,
    contract_name: str,
) -> str | None:
    """Decode one list of localized-text objects."""
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError(f"{contract_name} column {column!r} must be a list or null.")

    fallback: str | None = None
    for item in cast(list[object], value):
        if not isinstance(item, Mapping):
            raise ValueError(f"{contract_name} column {column!r} must contain objects.")
        item_mapping = cast(Mapping[str, object], item)
        text = optional_text(item_mapping.get("text"))
        if text is None:
            continue
        if item_mapping.get("lang") == "main":
            return text
        if fallback is None:
            fallback = text
    return fallback


def decode_tag_list(
    value: object,
    *,
    column: str,
    contract_name: str,
) -> list[str] | None:
    """Decode one list of tag strings."""
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError(f"{contract_name} column {column!r} must be a list or null.")

    for item in cast(list[object], value):
        if not isinstance(item, str):
            raise ValueError(f"{contract_name} column {column!r} must contain strings.")
    return [cast(str, item) for item in cast(list[object], value)]


def decode_nutriment_mapping(
    value: Mapping[str, object],
) -> dict[str, object]:
    """Keep canonical nutriment columns from one mapping."""
    return {
        column: value[column]
        for column in _CANONICAL_NUTRIMENT_COLUMNS
        if column in value
    }


def decode_nutriment_list(
    value: list[object],
    *,
    contract_name: str,
) -> dict[str, object]:
    """Flatten one structured nutriments list into canonical columns."""
    flattened: dict[str, object] = {}
    for item in value:
        if not isinstance(item, Mapping):
            raise ValueError(
                f"{contract_name} column 'nutriments' must contain objects."
            )
        item_mapping = cast(Mapping[str, object], item)
        name = optional_text(item_mapping.get("name"))
        value_100g = item_mapping.get("100g")
        if name is None or value_100g is None:
            continue
        column_name = f"{name}_100g"
        if column_name in _CANONICAL_NUTRIMENT_COLUMNS:
            flattened[column_name] = value_100g
    return flattened


def optional_text(value: object) -> str | None:
    """Return stripped text when present."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


__all__ = [
    "decode_export_localized_text",
    "decode_export_nutriment_columns",
    "decode_export_tag_values",
    "decode_full_document_localized_text",
    "decode_full_document_nutriment_columns",
    "decode_full_document_tag_values",
    "decode_localized_text_list",
    "decode_nutriment_list",
    "decode_nutriment_mapping",
    "decode_tag_list",
    "optional_text",
    "parse_raw_structured_string",
]
