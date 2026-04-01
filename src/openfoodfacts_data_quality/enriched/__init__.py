"""Public enriched snapshot runtime surface."""

from __future__ import annotations

from typing import TYPE_CHECKING

from openfoodfacts_data_quality._surface_api import (
    list_surface_checks,
    run_surface_checks,
)
from openfoodfacts_data_quality.context.builder import build_enriched_contexts
from openfoodfacts_data_quality.contracts.enrichment import (
    EnrichedCategoryPropsSnapshot,
    EnrichedFlagsSnapshot,
    EnrichedNutritionSnapshot,
    EnrichedProductSnapshot,
    EnrichedSnapshot,
    EnrichedSnapshotResult,
)

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable

    from openfoodfacts_data_quality.checks.catalog import CheckCatalog
    from openfoodfacts_data_quality.contracts.checks import (
        CheckDefinition,
        CheckInputSurface,
        CheckJurisdiction,
    )
    from openfoodfacts_data_quality.contracts.findings import Finding

INPUT_SURFACE: CheckInputSurface = "enriched_products"


def list_checks(
    *,
    check_ids: Collection[str] | None = None,
    jurisdictions: Collection[CheckJurisdiction] | None = None,
    catalog: CheckCatalog | None = None,
) -> tuple[CheckDefinition, ...]:
    """Return the checks exposed on the enriched snapshot surface."""
    return list_surface_checks(
        input_surface=INPUT_SURFACE,
        check_ids=check_ids,
        jurisdictions=jurisdictions,
        catalog=catalog,
    )


def run_checks(
    snapshots: Iterable[EnrichedSnapshotResult],
    *,
    check_ids: Collection[str] | None = None,
    jurisdictions: Collection[CheckJurisdiction] | None = None,
    catalog: CheckCatalog | None = None,
) -> list[Finding]:
    """Run enriched surface checks on explicit enriched snapshots."""
    return run_surface_checks(
        snapshots,
        input_surface=INPUT_SURFACE,
        build_contexts=build_enriched_contexts,
        check_ids=check_ids,
        jurisdictions=jurisdictions,
        catalog=catalog,
    )


__all__ = [
    "EnrichedCategoryPropsSnapshot",
    "EnrichedFlagsSnapshot",
    "EnrichedNutritionSnapshot",
    "EnrichedProductSnapshot",
    "EnrichedSnapshot",
    "EnrichedSnapshotResult",
    "INPUT_SURFACE",
    "list_checks",
    "run_checks",
]
