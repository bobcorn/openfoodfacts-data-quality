from __future__ import annotations

from pydantic import ConfigDict, Field, model_validator

from app.reference.models import ReferenceResult
from openfoodfacts_data_quality.contracts.mapping_view import MappingViewModel
from openfoodfacts_data_quality.contracts.snapshot_fields import ProductCoreFields
from openfoodfacts_data_quality.contracts.structured import (
    NutritionInputSet,
)

LEGACY_BACKEND_RESULT_CONTRACT_KIND = (
    "openfoodfacts_data_quality.legacy_backend_reference_result"
)
LEGACY_BACKEND_RESULT_CONTRACT_VERSION = 1


class LegacyBackendContract(MappingViewModel):
    """Frozen internal legacy backend contract shared by input and output adapters."""

    model_config = ConfigDict(extra="forbid", frozen=True)


def _empty_input_ingredients() -> list[LegacyBackendInputIngredient]:
    """Build an empty backend input ingredient list with a concrete static type."""
    return []


def _empty_input_sets() -> list[NutritionInputSet]:
    """Build an empty backend nutrition-set list with a concrete static type."""
    return []


class LegacyBackendInputIngredient(LegacyBackendContract):
    """Minimal explicit ingredient payload accepted by the Perl backend."""

    id: str


class LegacyBackendInputNutrition(LegacyBackendContract):
    """Explicit nutrition payload accepted by the Perl backend."""

    input_sets: list[NutritionInputSet] = Field(default_factory=_empty_input_sets)
    no_nutrition_data_on_packaging: bool | None = None


class LegacyBackendInputPayload(ProductCoreFields):
    """Explicit backend input payload prepared from one raw row."""

    ingredients: list[LegacyBackendInputIngredient] = Field(
        default_factory=_empty_input_ingredients
    )
    nutrition: LegacyBackendInputNutrition | None = None


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
