from __future__ import annotations

from collections.abc import Mapping

from off_data_quality._source_input.adapters import (
    prepare_canonical_source_row,
    prepare_off_full_document_row,
    prepare_off_product_export_row,
)
from off_data_quality._source_input.codecs import (
    decode_export_localized_text,
    decode_export_nutriment_columns,
    decode_export_tag_values,
    decode_full_document_localized_text,
    decode_full_document_nutriment_columns,
    decode_full_document_tag_values,
    parse_raw_structured_string,
)
from off_data_quality._source_input.contracts import (
    CANONICAL_COMPATIBLE_CONTRACT_NAME,
    OFF_FULL_DOCUMENT_CONTRACT_NAME,
    OFF_FULL_DOCUMENT_LOCALIZED_COLUMNS,
    OFF_FULL_DOCUMENT_ONLY_COLUMNS,
    OFF_PRODUCT_EXPORT_COLUMNS,
    OFF_PRODUCT_EXPORT_CONTRACT_NAME,
    OFF_TAG_COLUMNS,
    SourceInputContractSpec,
    SourceInputProbeResult,
    SourceInputRow,
)
from off_data_quality.contracts.source_products import SourceProduct

SUPPORTED_SOURCE_INPUT_CONTRACTS: tuple[SourceInputContractSpec, ...] = (
    SourceInputContractSpec(
        id="off_full_document",
        name=OFF_FULL_DOCUMENT_CONTRACT_NAME,
        probe=lambda row, row_index: _probe_off_full_document_contract(
            row,
            row_index=row_index,
        ),
        prepare=lambda row, row_index: prepare_off_full_document_row(
            row,
            row_index=row_index,
        ),
    ),
    SourceInputContractSpec(
        id="off_product_export",
        name=OFF_PRODUCT_EXPORT_CONTRACT_NAME,
        probe=lambda row, row_index: _probe_off_product_export_contract(
            row,
            row_index=row_index,
        ),
        prepare=lambda row, row_index: prepare_off_product_export_row(
            row,
            row_index=row_index,
        ),
    ),
    SourceInputContractSpec(
        id="canonical_compatible",
        name=CANONICAL_COMPATIBLE_CONTRACT_NAME,
        probe=lambda row, row_index: _probe_canonical_compatible_contract(
            row,
            row_index=row_index,
        ),
        prepare=lambda row, row_index: prepare_canonical_source_row(
            row,
            row_index=row_index,
        ),
    ),
)


def prepare_supported_source_row(
    row: Mapping[str, object],
    *,
    row_index: int,
) -> SourceProduct:
    """Prepare one row with the supported `checks` input contracts."""
    probe_results = tuple(
        (contract, contract.probe(row, row_index))
        for contract in SUPPORTED_SOURCE_INPUT_CONTRACTS
    )
    matching_contracts = tuple(
        contract
        for contract, probe_result in probe_results
        if probe_result.decision == "matched"
    )
    if len(matching_contracts) == 1:
        return matching_contracts[0].prepare(row, row_index)
    if len(matching_contracts) > 1:
        matching_names = ", ".join(contract.name for contract in matching_contracts)
        raise ValueError(
            "checks.run() row "
            f"{row_index} matches multiple supported checks facade input contracts: "
            f"{matching_names}."
        )

    invalid_reasons = tuple(
        probe_result.reason
        for _, probe_result in probe_results
        if probe_result.decision == "invalid" and probe_result.reason is not None
    )
    if invalid_reasons:
        raise ValueError(invalid_reasons[0])

    raise ValueError(
        "checks.run() row "
        f"{row_index} does not match a supported checks facade input "
        "contract. Provide canonical-compatible source columns or one of the "
        "supported Open Food Facts structured representations."
    )


def _probe_canonical_compatible_contract(
    row: SourceInputRow,
    *,
    row_index: int,
) -> SourceInputProbeResult:
    if "code" not in row:
        return SourceInputProbeResult.no_match()
    if any(column in row for column in OFF_FULL_DOCUMENT_ONLY_COLUMNS):
        return SourceInputProbeResult.no_match()
    if all(column in row for column in OFF_PRODUCT_EXPORT_COLUMNS):
        return SourceInputProbeResult.no_match()

    invalid_reason = _invalid_partial_off_structured_reason(row, row_index=row_index)
    if invalid_reason is not None:
        return SourceInputProbeResult.invalid(invalid_reason)
    return SourceInputProbeResult.matched()


def _probe_off_full_document_contract(
    row: SourceInputRow,
    *,
    row_index: int,
) -> SourceInputProbeResult:
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


def _probe_off_product_export_contract(
    row: SourceInputRow,
    *,
    row_index: int,
) -> SourceInputProbeResult:
    if any(column in row for column in OFF_FULL_DOCUMENT_ONLY_COLUMNS):
        return SourceInputProbeResult.no_match()
    if not all(column in row for column in OFF_PRODUCT_EXPORT_COLUMNS):
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


def _invalid_partial_off_structured_reason(
    row: SourceInputRow,
    *,
    row_index: int,
) -> str | None:
    full_document_marker = next(
        (column for column in OFF_FULL_DOCUMENT_ONLY_COLUMNS if column in row),
        None,
    )
    if full_document_marker is not None or isinstance(row.get("nutriments"), Mapping):
        return _full_document_shape_error(
            row_index,
            "Pass a full official JSONL document row or a canonical-compatible row.",
        )

    partial_export_field = _partial_off_structured_export_field(row)
    if partial_export_field is not None:
        return _product_export_shape_error(
            row_index,
            f"Field {partial_export_field!r} requires a complete official OFF "
            "export row. Pass a complete official OFF export row or a "
            "canonical-compatible row.",
        )
    return None


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


def _has_supported_full_document_payload(row: SourceInputRow) -> bool:
    return (
        "nutriments" in row
        or any(column in row for column in OFF_FULL_DOCUMENT_LOCALIZED_COLUMNS)
        or any(column in row for column in OFF_TAG_COLUMNS)
    )


def _full_document_shape_error(row_index: int, detail: str) -> str:
    return (
        "checks.run() row "
        f"{row_index} does not match the {OFF_FULL_DOCUMENT_CONTRACT_NAME} shape. "
        f"{detail}"
    )


def _product_export_shape_error(row_index: int, detail: str) -> str:
    return (
        "checks.run() row "
        f"{row_index} does not match the {OFF_PRODUCT_EXPORT_CONTRACT_NAME} shape. "
        f"{detail}"
    )


__all__ = [
    "SUPPORTED_SOURCE_INPUT_CONTRACTS",
    "prepare_supported_source_row",
]
