from __future__ import annotations

from collections.abc import Mapping

from off_data_quality._source_input._canonical_adapter import (
    CANONICAL_SOURCE_INPUT_CONTRACT,
)
from off_data_quality._source_input._off_full_document_adapter import (
    OFF_FULL_DOCUMENT_SOURCE_INPUT_CONTRACT,
)
from off_data_quality._source_input._off_product_export_adapter import (
    OFF_PRODUCT_EXPORT_SOURCE_INPUT_CONTRACT,
)
from off_data_quality._source_input.contracts import (
    SourceInputContractSpec,
)
from off_data_quality.contracts.source_products import SourceProduct

_STRICT_SOURCE_INPUT_CONTRACTS: tuple[SourceInputContractSpec, ...] = (
    OFF_FULL_DOCUMENT_SOURCE_INPUT_CONTRACT,
    OFF_PRODUCT_EXPORT_SOURCE_INPUT_CONTRACT,
)
_FALLBACK_SOURCE_INPUT_CONTRACTS: tuple[SourceInputContractSpec, ...] = (
    CANONICAL_SOURCE_INPUT_CONTRACT,
)
SUPPORTED_SOURCE_INPUT_CONTRACTS: tuple[SourceInputContractSpec, ...] = (
    _STRICT_SOURCE_INPUT_CONTRACTS + _FALLBACK_SOURCE_INPUT_CONTRACTS
)


def prepare_supported_source_row(
    row: Mapping[str, object],
    *,
    row_index: int,
) -> SourceProduct:
    """Prepare one row with the supported `checks` input contracts."""
    matched_contract = _resolve_contract_phase(
        row,
        row_index=row_index,
        contracts=_STRICT_SOURCE_INPUT_CONTRACTS,
    )
    if matched_contract is not None:
        return matched_contract.prepare(row, row_index)

    matched_contract = _resolve_contract_phase(
        row,
        row_index=row_index,
        contracts=_FALLBACK_SOURCE_INPUT_CONTRACTS,
    )
    if matched_contract is not None:
        return matched_contract.prepare(row, row_index)

    raise ValueError(
        "checks.run() row "
        f"{row_index} does not match a supported checks facade input "
        "contract. Provide canonical-compatible source columns or one of the "
        "supported Open Food Facts structured representations."
    )


def _resolve_contract_phase(
    row: Mapping[str, object],
    *,
    row_index: int,
    contracts: tuple[SourceInputContractSpec, ...],
) -> SourceInputContractSpec | None:
    probe_results = tuple(
        (contract, contract.probe(row, row_index)) for contract in contracts
    )
    matching_contracts = tuple(
        contract
        for contract, probe_result in probe_results
        if probe_result.decision == "matched"
    )
    if len(matching_contracts) == 1:
        return matching_contracts[0]
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
    return None


__all__ = [
    "SUPPORTED_SOURCE_INPUT_CONTRACTS",
    "prepare_supported_source_row",
]
