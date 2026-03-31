"""Public raw-product runtime surface."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from openfoodfacts_data_quality._surface_api import (
    list_surface_checks,
    run_surface_checks,
)
from openfoodfacts_data_quality.context.builder import build_raw_contexts

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Mapping

    from openfoodfacts_data_quality.checks.catalog import CheckCatalog
    from openfoodfacts_data_quality.contracts.checks import (
        CheckDefinition,
        CheckInputSurface,
        CheckJurisdiction,
    )
    from openfoodfacts_data_quality.contracts.findings import Finding

INPUT_SURFACE: CheckInputSurface = "raw_products"


def list_checks(
    *,
    check_ids: Collection[str] | None = None,
    jurisdictions: Collection[CheckJurisdiction] | None = None,
    catalog: CheckCatalog | None = None,
) -> tuple[CheckDefinition, ...]:
    """Return the checks exposed on the raw-product surface."""
    return list_surface_checks(
        input_surface=INPUT_SURFACE,
        check_ids=check_ids,
        jurisdictions=jurisdictions,
        catalog=catalog,
    )


def run_checks(
    rows: Iterable[Mapping[str, Any]],
    *,
    check_ids: Collection[str] | None = None,
    jurisdictions: Collection[CheckJurisdiction] | None = None,
    catalog: CheckCatalog | None = None,
) -> list[Finding]:
    """Run raw-surface checks on public Open Food Facts product rows."""
    return run_surface_checks(
        rows,
        input_surface=INPUT_SURFACE,
        build_contexts=build_raw_contexts,
        check_ids=check_ids,
        jurisdictions=jurisdictions,
        catalog=catalog,
    )


__all__ = [
    "INPUT_SURFACE",
    "list_checks",
    "run_checks",
]
