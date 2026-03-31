from __future__ import annotations

from pydantic import BaseModel, Field

from openfoodfacts_data_quality.contracts.enrichment import EnrichedSnapshotResult

REFERENCE_RESULT_SCHEMA_VERSION = 2


class LegacyCheckTags(BaseModel):
    """Legacy check tags emitted by the backend runner."""

    bug: list[str] = Field(default_factory=list)
    info: list[str] = Field(default_factory=list)
    completeness: list[str] = Field(default_factory=list)
    warning: list[str] = Field(default_factory=list)
    error: list[str] = Field(default_factory=list)


class ReferenceResult(EnrichedSnapshotResult):
    """Explicit reference-path payload emitted by one legacy-backend execution."""

    legacy_check_tags: LegacyCheckTags = Field(default_factory=LegacyCheckTags)


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
