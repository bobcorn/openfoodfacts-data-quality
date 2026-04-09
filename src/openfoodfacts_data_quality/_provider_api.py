from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from openfoodfacts_data_quality.checks.catalog import (
    CheckCatalog,
    get_default_check_catalog,
)
from openfoodfacts_data_quality.checks.engine import (
    CheckRunOptions,
    run_checks_with_evaluators,
)
from openfoodfacts_data_quality.context.providers import ContextProvider
from openfoodfacts_data_quality.contracts.capabilities import resolve_check_capabilities
from openfoodfacts_data_quality.contracts.checks import (
    CheckSelection,
    normalize_check_jurisdictions,
)

if TYPE_CHECKING:
    from collections.abc import Collection

    from openfoodfacts_data_quality.contracts.checks import (
        CheckDefinition,
        CheckJurisdiction,
    )
    from openfoodfacts_data_quality.contracts.findings import Finding


def list_provider_checks[ProviderInput](
    *,
    provider: ContextProvider[ProviderInput],
    check_ids: Collection[str] | None = None,
    jurisdictions: Collection[CheckJurisdiction] | None = None,
    catalog: CheckCatalog | None = None,
) -> tuple[CheckDefinition, ...]:
    """Return the checks available through one context provider."""
    selected_catalog = catalog or get_default_check_catalog()
    selected_checks = selected_catalog.select_checks(
        check_ids,
        selection=CheckSelection(
            jurisdictions=normalize_check_jurisdictions(jurisdictions),
        ),
    )
    capability_report = resolve_check_capabilities(
        selected_checks,
        provider.availability,
    )
    unsupported_ids = tuple(
        capability.check_id for capability in capability_report.unsupported_capabilities
    )
    if check_ids is not None and unsupported_ids:
        raise ValueError(
            f"Checks not available for context provider {provider.name}: "
            f"{', '.join(sorted(unsupported_ids))}"
        )
    checks_by_id = {check.id: check for check in selected_checks}
    return tuple(
        checks_by_id[capability.check_id]
        for capability in capability_report.runnable_capabilities
    )


def run_provider_checks[ProviderInput](
    inputs: Iterable[ProviderInput],
    *,
    provider: ContextProvider[ProviderInput],
    check_ids: Collection[str] | None = None,
    jurisdictions: Collection[CheckJurisdiction] | None = None,
    catalog: CheckCatalog | None = None,
) -> list[Finding]:
    """Run the checks available on one context provider."""
    selected_catalog = catalog or get_default_check_catalog()
    active_checks = list_provider_checks(
        provider=provider,
        check_ids=check_ids,
        jurisdictions=jurisdictions,
        catalog=selected_catalog,
    )
    active_check_ids = tuple(check.id for check in active_checks)
    contexts = provider.build_contexts(inputs)
    return run_checks_with_evaluators(
        contexts,
        check_evaluators=selected_catalog.select_evaluators(active_check_ids),
        options=CheckRunOptions(
            catalog=selected_catalog,
            log_loaded=False,
            log_progress=False,
        ),
    )
