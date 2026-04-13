from __future__ import annotations

from off_data_quality._source_input._adapter_common import (
    select_canonical_source_fields,
)
from off_data_quality._source_input.contracts import (
    CANONICAL_COMPATIBLE_CONTRACT_NAME,
    OFF_FULL_DOCUMENT_ONLY_COLUMNS,
    OFF_PRODUCT_EXPORT_COLUMNS,
    SourceInputContractSpec,
    SourceInputProbeResult,
    SourceInputRow,
)
from off_data_quality.contracts.source_products import (
    SourceProduct,
    validate_source_product,
)


def prepare_canonical_source_row(
    row: SourceInputRow,
    row_index: int,
) -> SourceProduct:
    """Normalize one canonical-compatible row into `SourceProduct`."""
    del row_index
    return validate_source_product(select_canonical_source_fields(row))


def probe_canonical_source_row(
    row: SourceInputRow,
    row_index: int,
) -> SourceInputProbeResult:
    """Match canonical-compatible rows after strict OFF contracts opt out."""
    del row_index
    if "code" not in row:
        return SourceInputProbeResult.no_match()
    if any(column in row for column in OFF_FULL_DOCUMENT_ONLY_COLUMNS):
        return SourceInputProbeResult.no_match()
    if all(column in row for column in OFF_PRODUCT_EXPORT_COLUMNS):
        return SourceInputProbeResult.no_match()
    return SourceInputProbeResult.matched()


CANONICAL_SOURCE_INPUT_CONTRACT = SourceInputContractSpec(
    id="canonical_compatible",
    name=CANONICAL_COMPATIBLE_CONTRACT_NAME,
    probe=probe_canonical_source_row,
    prepare=prepare_canonical_source_row,
)


__all__ = [
    "CANONICAL_SOURCE_INPUT_CONTRACT",
    "prepare_canonical_source_row",
    "probe_canonical_source_row",
]
