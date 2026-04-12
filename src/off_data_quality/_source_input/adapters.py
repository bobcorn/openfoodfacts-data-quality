from __future__ import annotations

from collections.abc import Callable, Mapping

from off_data_quality._source_input.codecs import (
    decode_export_localized_text,
    decode_export_nutriment_columns,
    decode_export_tag_values,
    decode_full_document_localized_text,
    decode_full_document_nutriment_columns,
    decode_full_document_tag_values,
)
from off_data_quality._source_input.contracts import (
    OFF_FULL_DOCUMENT_CONTRACT_NAME,
    OFF_FULL_DOCUMENT_LOCALIZED_COLUMNS,
    OFF_PRODUCT_EXPORT_COLUMNS,
    OFF_TAG_COLUMNS,
    SourceInputRow,
)
from off_data_quality.contracts.source_products import (
    SOURCE_PRODUCT_BASE_FIELD_TO_COLUMN,
    SOURCE_PRODUCT_INPUT_COLUMNS,
    SOURCE_PRODUCT_NUTRIMENT_COLUMNS,
    SourceProduct,
    validate_source_product,
)

_CANONICAL_SOURCE_COLUMNS = frozenset(SOURCE_PRODUCT_INPUT_COLUMNS)
_CANONICAL_NUTRIMENT_COLUMNS = frozenset(SOURCE_PRODUCT_NUTRIMENT_COLUMNS)


def prepare_canonical_source_row(
    row: SourceInputRow,
    *,
    row_index: int,
) -> SourceProduct:
    """Normalize one canonical-compatible row into `SourceProduct`."""
    del row_index
    projected_row = {
        column: value
        for column, value in row.items()
        if column in _CANONICAL_SOURCE_COLUMNS
    }
    return validate_source_product(projected_row)


def prepare_off_full_document_row(
    row: SourceInputRow,
    *,
    row_index: int,
) -> SourceProduct:
    """Normalize one OFF full-document row into `SourceProduct`."""
    projected_row: dict[str, object] = {
        column: _off_full_document_base_value(row, column)
        for column in SOURCE_PRODUCT_BASE_FIELD_TO_COLUMN.values()
        if column in row
    }
    projected_row.update(_off_full_document_nutriment_values(row))

    try:
        return validate_source_product(projected_row)
    except Exception as exc:
        raise ValueError(
            "checks.run() row "
            f"{row_index} does not match the {OFF_FULL_DOCUMENT_CONTRACT_NAME} "
            "shape supported by the checks facade."
        ) from exc


def prepare_off_product_export_row(
    row: SourceInputRow,
    *,
    row_index: int,
) -> SourceProduct:
    """Normalize one complete OFF product-export row into `SourceProduct`."""
    missing_columns = tuple(
        column for column in OFF_PRODUCT_EXPORT_COLUMNS if column not in row
    )
    if missing_columns:
        raise ValueError(
            f"checks.project_off_product_export_rows() row {row_index} does "
            "not match the Open Food Facts product export shape. Missing "
            f"columns: {', '.join(missing_columns)}."
        )

    overlapping_nutriments = sorted(
        column for column in _CANONICAL_NUTRIMENT_COLUMNS if column in row
    )
    if overlapping_nutriments:
        raise ValueError(
            "checks.project_off_product_export_rows() row "
            f"{row_index} mixes structured nutriments with canonical nutriment "
            "columns: "
            f"{', '.join(overlapping_nutriments)}."
        )

    return project_off_product_export_row(row)


def project_off_product_export_row(row: Mapping[str, object]) -> SourceProduct:
    """Project one OFF product-export row to `SourceProduct`."""
    projected_row: dict[str, object] = {
        "code": row.get("code"),
        "created_t": row.get("created_t"),
        "product_name": decode_export_localized_text(
            row.get("product_name"),
            column="product_name",
        ),
        "quantity": row.get("quantity"),
        "product_quantity": row.get("product_quantity"),
        "serving_size": row.get("serving_size"),
        "serving_quantity": row.get("serving_quantity"),
        "brands": row.get("brands"),
        "categories": row.get("categories"),
        "labels": row.get("labels"),
        "emb_codes": row.get("emb_codes"),
        "ingredients_text": decode_export_localized_text(
            row.get("ingredients_text"),
            column="ingredients_text",
        ),
        "ingredients_tags": decode_export_tag_values(
            row.get("ingredients_tags"),
            column="ingredients_tags",
        ),
        "nutriscore_grade": row.get("nutriscore_grade"),
        "nutriscore_score": row.get("nutriscore_score"),
        "categories_tags": decode_export_tag_values(
            row.get("categories_tags"),
            column="categories_tags",
        ),
        "labels_tags": decode_export_tag_values(
            row.get("labels_tags"),
            column="labels_tags",
        ),
        "countries_tags": decode_export_tag_values(
            row.get("countries_tags"),
            column="countries_tags",
        ),
        "no_nutrition_data": row.get("no_nutrition_data"),
    }
    projected_row.update(_off_product_export_nutriment_values(row))
    return validate_source_product(projected_row)


def _off_product_export_nutriment_values(
    row: Mapping[str, object],
) -> dict[str, object]:
    return _combined_nutriment_values(row, decoder=decode_export_nutriment_columns)


def _off_full_document_base_value(
    row: SourceInputRow,
    column: str,
) -> object:
    value = row.get(column)
    if column in OFF_FULL_DOCUMENT_LOCALIZED_COLUMNS:
        return decode_full_document_localized_text(
            value,
            column=column,
        )
    if column in OFF_TAG_COLUMNS:
        return decode_full_document_tag_values(
            value,
            column=column,
        )
    return value


def _off_full_document_nutriment_values(
    row: SourceInputRow,
) -> dict[str, object]:
    return _combined_nutriment_values(
        row,
        decoder=decode_full_document_nutriment_columns,
    )


def _combined_nutriment_values(
    row: Mapping[str, object],
    *,
    decoder: Callable[[object], dict[str, object]],
) -> dict[str, object]:
    projected = {
        column: row[column] for column in _CANONICAL_NUTRIMENT_COLUMNS if column in row
    }
    if "nutriments" not in row:
        return projected

    projected.update(decoder(row.get("nutriments")))
    return projected


__all__ = [
    "OFF_PRODUCT_EXPORT_COLUMNS",
    "prepare_canonical_source_row",
    "prepare_off_full_document_row",
    "prepare_off_product_export_row",
    "project_off_product_export_row",
]
