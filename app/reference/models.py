from __future__ import annotations

from pydantic import BaseModel, Field, model_validator

from openfoodfacts_data_quality.contracts.enrichment import (
    EnrichedSnapshot,
    EnrichedSnapshotResult,
)

REFERENCE_RESULT_SCHEMA_VERSION = 4


class LegacyCheckTags(BaseModel, frozen=True, extra="forbid"):
    """Legacy check tags emitted by the backend runner."""

    bug: list[str] = Field(default_factory=list)
    info: list[str] = Field(default_factory=list)
    completeness: list[str] = Field(default_factory=list)
    warning: list[str] = Field(default_factory=list)
    error: list[str] = Field(default_factory=list)


class ReferenceResult(BaseModel, frozen=True, extra="forbid"):
    """Explicit reference-path payload emitted by one legacy backend execution."""

    code: str
    enriched_snapshot: EnrichedSnapshot
    legacy_check_tags: LegacyCheckTags = Field(default_factory=LegacyCheckTags)

    @model_validator(mode="after")
    def validate_snapshot_code(self) -> ReferenceResult:
        """Keep the outer code aligned with the embedded product code when present."""
        snapshot_code = self.enriched_snapshot.product.code
        if snapshot_code is not None and snapshot_code != self.code:
            raise ValueError(
                "Reference result code must match enriched_snapshot.product.code. "
                f"Got {self.code!r} and {snapshot_code!r}."
            )
        return self


def enriched_snapshots_from_reference_results(
    reference_results: list[ReferenceResult],
) -> list[EnrichedSnapshotResult]:
    """Project reference results onto the library-facing enriched contract."""
    return [
        EnrichedSnapshotResult(
            code=result.code,
            enriched_snapshot=result.enriched_snapshot.model_copy(deep=True),
        )
        for result in reference_results
    ]
