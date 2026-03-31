from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from openfoodfacts_data_quality.checks.catalog import (
    CheckCatalog,
    get_default_check_catalog,
    load_check_catalog,
)
from openfoodfacts_data_quality.contracts.findings import Finding
from openfoodfacts_data_quality.progress import iter_with_progress

if TYPE_CHECKING:
    from collections.abc import Collection
    from importlib.resources.abc import Traversable

    from openfoodfacts_data_quality.checks.registry import CheckEvaluator
    from openfoodfacts_data_quality.contracts.checks import (
        CheckEmission,
        CheckSelection,
    )
    from openfoodfacts_data_quality.contracts.context import NormalizedContext

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckRunOptions:
    """Optional controls for one quality-check execution pass."""

    active_check_ids: Collection[str] | None = None
    selection: CheckSelection | None = None
    catalog: CheckCatalog | None = None
    log_loaded: bool = True
    log_progress: bool = True


def run_checks(
    contexts: list[NormalizedContext],
    *,
    active_check_ids: Collection[str] | None = None,
    selection: CheckSelection | None = None,
    catalog: CheckCatalog | None = None,
) -> list[Finding]:
    """Run all selected check evaluators on the normalized contexts."""
    return run_checks_with_evaluators(
        contexts,
        options=CheckRunOptions(
            active_check_ids=active_check_ids,
            selection=selection,
            catalog=catalog,
        ),
    )


def run_checks_with_evaluators(
    contexts: list[NormalizedContext],
    check_evaluators: dict[str, CheckEvaluator] | None = None,
    *,
    options: CheckRunOptions | None = None,
) -> list[Finding]:
    """Run the selected check evaluators on the normalized contexts."""
    findings: list[Finding] = []
    resolved_options = options or CheckRunOptions()
    if check_evaluators is not None and resolved_options.active_check_ids is not None:
        raise ValueError("Pass either check_evaluators or active_check_ids, not both.")
    selected_catalog = resolved_options.catalog or get_default_check_catalog()
    active_evaluators = check_evaluators or selected_catalog.select_evaluators(
        resolved_options.active_check_ids,
        selection=resolved_options.selection,
    )
    python_count = sum(
        1
        for check_id in active_evaluators
        if selected_catalog.check_by_id(check_id).definition_language == "python"
    )
    dsl_count = len(active_evaluators) - python_count
    if resolved_options.log_loaded:
        LOGGER.info(
            "[Checks] Loaded %d Python checks and %d DSL checks.",
            python_count,
            dsl_count,
        )

    context_iterable = (
        iter_with_progress(
            contexts,
            desc="Checks | Run checks",
            unit="product",
            logger=LOGGER,
        )
        if resolved_options.log_progress
        else contexts
    )

    for context in context_iterable:
        findings.extend(
            _run_check_evaluators(context, active_evaluators, selected_catalog)
        )

    return sorted(
        findings,
        key=lambda finding: (
            finding.check_id,
            finding.product_id,
            finding.emitted_code or finding.check_id,
            finding.severity,
        ),
    )


def load_check_evaluators(
    definitions_path: Traversable | None = None,
    *,
    active_check_ids: Collection[str] | None = None,
    selection: CheckSelection | None = None,
    catalog: CheckCatalog | None = None,
) -> dict[str, CheckEvaluator]:
    """Load the unified evaluator registry in catalog order."""
    selected_catalog = catalog or load_check_catalog(definitions_path)
    return selected_catalog.select_evaluators(active_check_ids, selection=selection)


def _run_check_evaluators(
    context: NormalizedContext,
    check_evaluators: dict[str, CheckEvaluator],
    catalog: CheckCatalog,
) -> list[Finding]:
    """Run the unified evaluator registry for one product."""
    findings: list[Finding] = []
    for check_id, evaluator in check_evaluators.items():
        for emission in evaluator(context):
            findings.append(_build_finding(context.code, check_id, emission, catalog))
    return findings


def _build_finding(
    product_id: str,
    check_id: str,
    emission: CheckEmission,
    catalog: CheckCatalog,
) -> Finding:
    """Build a normalized finding from one check emission."""
    catalog.check_by_id(check_id)
    emitted_code = emission.raw_code or check_id
    return Finding(
        product_id=product_id,
        check_id=check_id,
        severity=emission.severity,
        emitted_code=emitted_code if emitted_code != check_id else None,
    )
