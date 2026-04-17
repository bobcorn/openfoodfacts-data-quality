from __future__ import annotations

from collections.abc import Mapping

from off_data_quality._source_input._adapter_common import (
    combine_canonical_nutriment_values,
    overlapping_canonical_nutriment_columns,
)
from off_data_quality._source_input.codecs import (
    decode_export_localized_text,
    decode_export_nutriment_columns,
    decode_export_tag_values,
    parse_raw_structured_string,
)
from off_data_quality._source_input.contracts import (
    OFF_FULL_DOCUMENT_LOCALIZED_COLUMNS,
    OFF_FULL_DOCUMENT_ONLY_COLUMNS,
    OFF_PRODUCT_EXPORT_COLUMNS,
    OFF_PRODUCT_EXPORT_CONTRACT_NAME,
    OFF_TAG_COLUMNS,
    SourceInputContractSpec,
    SourceInputProbeResult,
    SourceInputRow,
)
from off_data_quality.contracts.source_products import (
    SourceProduct,
    validate_source_product,
)


def prepare_off_product_export_row(
    row: SourceInputRow,
    row_index: int,
) -> SourceProduct:
    """Normalize one complete OFF product export row into `SourceProduct`."""
    missing_columns = tuple(
        column for column in OFF_PRODUCT_EXPORT_COLUMNS if column not in row
    )
    if missing_columns:
        raise ValueError(
            f"Open Food Facts product export row {row_index} does not match "
            "the expected product export shape. Missing "
            f"columns: {', '.join(missing_columns)}."
        )

    overlapping_nutriments = overlapping_canonical_nutriment_columns(row)
    if overlapping_nutriments:
        raise ValueError(
            "Open Food Facts product export row "
            f"{row_index} mixes structured nutriments with canonical nutriment "
            "columns: "
            f"{', '.join(overlapping_nutriments)}."
        )

    return _project_off_product_export_row(row)


def probe_off_product_export_row(
    row: SourceInputRow,
    row_index: int,
) -> SourceInputProbeResult:
    """Match complete OFF product export rows."""
    if any(column in row for column in OFF_FULL_DOCUMENT_ONLY_COLUMNS):
        return SourceInputProbeResult.no_match()

    has_complete_export_shape = all(
        column in row for column in OFF_PRODUCT_EXPORT_COLUMNS
    )
    partial_export_field = _partial_off_structured_export_field(row)
    if partial_export_field is not None and not has_complete_export_shape:
        return SourceInputProbeResult.invalid(
            _product_export_shape_error(
                row_index,
                f"Field {partial_export_field!r} requires a complete official OFF "
                "export row. Pass a complete official OFF export row or a "
                "canonical-compatible row.",
            )
        )
    if not has_complete_export_shape:
        return SourceInputProbeResult.no_match()

    try:
        decode_export_nutriment_columns(row.get("nutriments"))
        for column in OFF_FULL_DOCUMENT_LOCALIZED_COLUMNS:
            decode_export_localized_text(row.get(column), column=column)
        for column in OFF_TAG_COLUMNS:
            decode_export_tag_values(row.get(column), column=column)
    except ValueError as exc:
        return SourceInputProbeResult.invalid(
            _product_export_shape_error(row_index, str(exc))
        )
    return SourceInputProbeResult.matched()


def _project_off_product_export_row(row: Mapping[str, object]) -> SourceProduct:
    """Project one OFF product export row to `SourceProduct`."""
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
    return combine_canonical_nutriment_values(
        row,
        decoder=decode_export_nutriment_columns,
    )


def _partial_off_structured_export_field(row: SourceInputRow) -> str | None:
    nutriments_value = row.get("nutriments")
    if nutriments_value is not None:
        return "nutriments"

    for column in OFF_FULL_DOCUMENT_LOCALIZED_COLUMNS:
        value = row.get(column)
        if value is None:
            continue
        if not isinstance(value, str):
            return column
        if (
            parse_raw_structured_string(
                value,
                column=column,
                contract_name=OFF_PRODUCT_EXPORT_CONTRACT_NAME,
            )
            is not None
        ):
            return column

    for column in OFF_TAG_COLUMNS:
        value = row.get(column)
        if value is None:
            continue
        if isinstance(value, list):
            continue
        if not isinstance(value, str):
            return column
        if (
            parse_raw_structured_string(
                value,
                column=column,
                contract_name=OFF_PRODUCT_EXPORT_CONTRACT_NAME,
            )
            is not None
        ):
            return column
    return None


def _product_export_shape_error(
    row_index: int,
    detail: str,
) -> str:
    return (
        "checks.run() row "
        f"{row_index} does not match the {OFF_PRODUCT_EXPORT_CONTRACT_NAME} shape. "
        f"{detail}"
    )


OFF_PRODUCT_EXPORT_SOURCE_INPUT_CONTRACT = SourceInputContractSpec(
    id="off_product_export",
    name=OFF_PRODUCT_EXPORT_CONTRACT_NAME,
    probe=probe_off_product_export_row,
    prepare=prepare_off_product_export_row,
)


__all__ = [
    "OFF_PRODUCT_EXPORT_SOURCE_INPUT_CONTRACT",
    "prepare_off_product_export_row",
    "probe_off_product_export_row",
]
