from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

from openfoodfacts_data_quality.contracts.snapshot_fields import (
    CategoryPropsSnapshotFields,
    FlagsSnapshotFields,
    NutritionSnapshotFields,
    ProductSnapshotFields,
)


class EnrichedProductSnapshot(ProductSnapshotFields):
    """Stable enriched product payload owned by the Python runtime."""


class EnrichedFlagsSnapshot(FlagsSnapshotFields):
    """Stable enriched flags payload owned by the Python runtime."""


class EnrichedCategoryPropsSnapshot(CategoryPropsSnapshotFields):
    """Stable enriched category-properties payload owned by the Python runtime."""


class EnrichedNutritionSnapshot(NutritionSnapshotFields):
    """Stable enriched nutrition payload owned by the Python runtime."""


class EnrichedSnapshot(BaseModel):
    """Stable enriched product snapshot consumed by the enriched snapshots provider."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    product: EnrichedProductSnapshot = Field(default_factory=EnrichedProductSnapshot)
    flags: EnrichedFlagsSnapshot = Field(default_factory=EnrichedFlagsSnapshot)
    category_props: EnrichedCategoryPropsSnapshot = Field(
        default_factory=EnrichedCategoryPropsSnapshot
    )
    nutrition: EnrichedNutritionSnapshot = Field(
        default_factory=EnrichedNutritionSnapshot
    )


class EnrichedSnapshotRecord(BaseModel):
    """Stable enriched snapshot identified by product code."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: str
    enriched_snapshot: EnrichedSnapshot

    @model_validator(mode="after")
    def validate_snapshot_code(self) -> EnrichedSnapshotRecord:
        """Keep the outer code aligned with the embedded product code when present."""
        snapshot_code = self.enriched_snapshot.product.code
        if snapshot_code is not None and snapshot_code != self.code:
            raise ValueError(
                "Enriched snapshot code must match enriched_snapshot.product.code. "
                f"Got {self.code!r} and {snapshot_code!r}."
            )
        return self
