from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

from openfoodfacts_data_quality.checks.catalog import (
    CheckCatalog,
    get_default_check_catalog,
)
from openfoodfacts_data_quality.checks.engine import (
    CheckRunOptions,
    run_checks_with_evaluators,
)
from openfoodfacts_data_quality.contracts.context import NormalizedContext

if TYPE_CHECKING:
    from collections.abc import Collection

    from openfoodfacts_data_quality.contracts.checks import (
        CheckDefinition,
        CheckInputSurface,
        CheckJurisdiction,
    )
    from openfoodfacts_data_quality.contracts.findings import Finding

type ContextBuilder[SurfaceInput] = Callable[
    [Iterable[SurfaceInput]],
    list[NormalizedContext],
]


def list_surface_checks(
    *,
    input_surface: CheckInputSurface,
    check_ids: Collection[str] | None = None,
    jurisdictions: Collection[CheckJurisdiction] | None = None,
    catalog: CheckCatalog | None = None,
) -> tuple[CheckDefinition, ...]:
    """Return the public checks available on one input surface."""
    selected_catalog = catalog or get_default_check_catalog()
    return selected_catalog.select_surface_checks(
        input_surface,
        active_check_ids=check_ids,
        jurisdictions=jurisdictions,
    )


def run_surface_checks[SurfaceInput](
    inputs: Iterable[SurfaceInput],
    *,
    input_surface: CheckInputSurface,
    build_contexts: ContextBuilder[SurfaceInput],
    check_ids: Collection[str] | None = None,
    jurisdictions: Collection[CheckJurisdiction] | None = None,
    catalog: CheckCatalog | None = None,
) -> list[Finding]:
    """Run the public checks available on one input surface."""
    selected_catalog = catalog or get_default_check_catalog()
    active_checks = list_surface_checks(
        input_surface=input_surface,
        check_ids=check_ids,
        jurisdictions=jurisdictions,
        catalog=selected_catalog,
    )
    active_check_ids = tuple(check.id for check in active_checks)
    contexts = build_contexts(inputs)
    return run_checks_with_evaluators(
        contexts,
        check_evaluators=selected_catalog.select_evaluators(active_check_ids),
        options=CheckRunOptions(
            catalog=selected_catalog,
            log_loaded=False,
            log_progress=False,
        ),
    )
