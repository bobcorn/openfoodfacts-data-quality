from __future__ import annotations

from collections.abc import Mapping

from off_data_quality._source_input._adapter_common import (
    combine_canonical_nutriment_values,
)
from off_data_quality._source_input.codecs import (
    decode_full_document_localized_text,
    decode_full_document_nutriment_columns,
    decode_full_document_tag_values,
)
from off_data_quality._source_input.contracts import (
    OFF_FULL_DOCUMENT_CONTRACT_NAME,
    OFF_FULL_DOCUMENT_LOCALIZED_COLUMNS,
    OFF_FULL_DOCUMENT_ONLY_COLUMNS,
    OFF_TAG_COLUMNS,
    SourceInputContractSpec,
    SourceInputProbeResult,
    SourceInputRow,
)
from off_data_quality.contracts.source_products import (
    SOURCE_PRODUCT_BASE_FIELD_TO_COLUMN,
    SourceProduct,
    validate_source_product,
)


def prepare_off_full_document_row(
    row: SourceInputRow,
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


def probe_off_full_document_row(
    row: SourceInputRow,
    row_index: int,
) -> SourceInputProbeResult:
    """Match complete OFF full-document rows."""
    has_full_document_markers = any(
        column in row for column in OFF_FULL_DOCUMENT_ONLY_COLUMNS
    )
    uses_full_document_nutriments = isinstance(row.get("nutriments"), Mapping)

    if not has_full_document_markers and not uses_full_document_nutriments:
        return SourceInputProbeResult.no_match()
    if has_full_document_markers and not _has_supported_full_document_payload(row):
        return SourceInputProbeResult.invalid(
            _full_document_shape_error(
                row_index,
                "Pass a full official JSONL document row or a canonical-compatible "
                "row.",
            )
        )

    try:
        if "nutriments" in row:
            decode_full_document_nutriment_columns(row.get("nutriments"))
        for column in OFF_FULL_DOCUMENT_LOCALIZED_COLUMNS:
            if column in row:
                decode_full_document_localized_text(row.get(column), column=column)
        for column in OFF_TAG_COLUMNS:
            if column in row:
                decode_full_document_tag_values(row.get(column), column=column)
    except ValueError as exc:
        return SourceInputProbeResult.invalid(
            _full_document_shape_error(row_index, str(exc))
        )
    return SourceInputProbeResult.matched()


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
    return combine_canonical_nutriment_values(
        row,
        decoder=decode_full_document_nutriment_columns,
    )


def _has_supported_full_document_payload(row: SourceInputRow) -> bool:
    return (
        "nutriments" in row
        or any(column in row for column in OFF_FULL_DOCUMENT_LOCALIZED_COLUMNS)
        or any(column in row for column in OFF_TAG_COLUMNS)
    )


def _full_document_shape_error(
    row_index: int,
    detail: str,
) -> str:
    return (
        "checks.run() row "
        f"{row_index} does not match the {OFF_FULL_DOCUMENT_CONTRACT_NAME} shape. "
        f"{detail}"
    )


OFF_FULL_DOCUMENT_SOURCE_INPUT_CONTRACT = SourceInputContractSpec(
    id="off_full_document",
    name=OFF_FULL_DOCUMENT_CONTRACT_NAME,
    probe=probe_off_full_document_row,
    prepare=prepare_off_full_document_row,
)


__all__ = [
    "OFF_FULL_DOCUMENT_SOURCE_INPUT_CONTRACT",
    "prepare_off_full_document_row",
    "probe_off_full_document_row",
]
