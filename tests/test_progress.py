from __future__ import annotations

import logging
from types import MappingProxyType
from typing import TYPE_CHECKING

import pytest

from off_data_quality.catalog import CheckCatalog
from off_data_quality.contracts.checks import CheckDefinition, CheckEmission
from off_data_quality.execution import (
    CheckEvaluator,
    CheckRunOptions,
    iter_check_findings_with_evaluators,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from off_data_quality.contracts.context import CheckContext


def _catalog_with_noop_check() -> CheckCatalog:
    check = CheckDefinition(
        id="en:test-check",
        definition_language="python",
        parity_baseline="legacy",
        jurisdictions=("global",),
        required_context_paths=("product.code",),
    )

    def noop_evaluator(_: CheckContext) -> list[CheckEmission]:
        return []

    evaluators: dict[str, CheckEvaluator] = {check.id: noop_evaluator}
    return CheckCatalog(
        checks=(check,),
        evaluators_by_id=MappingProxyType(evaluators),
        checks_by_id=MappingProxyType({check.id: check}),
    )


def test_execution_progress_logs_periodically(
    caplog: pytest.LogCaptureFixture,
    context_factory: Callable[..., CheckContext],
) -> None:
    caplog.set_level(logging.INFO)

    catalog = _catalog_with_noop_check()
    contexts = [context_factory(code=f"{index:03d}") for index in range(25)]
    findings = list(
        iter_check_findings_with_evaluators(
            contexts,
            catalog.select_evaluators({"en:test-check"}),
            options=CheckRunOptions(
                catalog=catalog,
                log_loaded=False,
                log_progress=True,
            ),
        )
    )

    assert findings == []
    messages = [record.getMessage() for record in caplog.records]
    assert "[Checks | Run checks] 2/25 products processed." in messages
    assert "[Checks | Run checks] 24/25 products processed." in messages
    assert "[Checks | Run checks] 25/25 products processed." in messages
