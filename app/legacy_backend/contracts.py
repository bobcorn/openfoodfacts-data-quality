from __future__ import annotations

from pydantic import ConfigDict, model_validator

from app.reference.models import ReferenceResult
from openfoodfacts_data_quality.contracts.mapping_view import MappingViewModel

LEGACY_BACKEND_RESULT_CONTRACT_KIND = (
    "openfoodfacts_data_quality.legacy_backend_reference_result"
)
LEGACY_BACKEND_RESULT_CONTRACT_VERSION = 1


class LegacyBackendContract(MappingViewModel):
    """Frozen internal legacy backend contract shared by input and output adapters."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class LegacyBackendResultEnvelope(LegacyBackendContract):
    """Versioned legacy backend envelope around the stable result owned by the Python runtime."""

    contract_kind: str
    contract_version: int
    reference_result: ReferenceResult

    @model_validator(mode="after")
    def validate_contract_version(self) -> LegacyBackendResultEnvelope:
        """Reject envelope versions that the current runtime does not understand."""
        if self.contract_kind != LEGACY_BACKEND_RESULT_CONTRACT_KIND:
            raise ValueError(
                "Unsupported legacy backend result contract kind. "
                f"Expected {LEGACY_BACKEND_RESULT_CONTRACT_KIND!r}, "
                f"got {self.contract_kind!r}."
            )
        if self.contract_version != LEGACY_BACKEND_RESULT_CONTRACT_VERSION:
            raise ValueError(
                "Unsupported legacy backend result contract version. "
                f"Expected {LEGACY_BACKEND_RESULT_CONTRACT_VERSION}, "
                f"got {self.contract_version}."
            )
        return self


def adapt_legacy_backend_result(
    payload: LegacyBackendResultEnvelope,
) -> ReferenceResult:
    """Validate and return the stable reference result owned by the Python runtime."""
    return payload.reference_result.model_copy(deep=True)
