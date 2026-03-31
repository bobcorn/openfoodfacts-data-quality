from __future__ import annotations

from collections.abc import Callable

import pytest
from app.parity.models import ParityResult
from app.parity.serialization import build_parity_artifact
from app.report.renderer import build_report_payload

ParityResultFactory = Callable[[], ParityResult]


def test_build_report_payload_adds_ui_fields_without_mutating_parity_artifact(
    parity_result_factory: ParityResultFactory,
) -> None:
    parity_result = parity_result_factory()
    parity_artifact = build_parity_artifact(parity_result)

    report_payload = build_report_payload(
        parity_result,
        parity_artifact=parity_artifact,
    )

    machine_checks_by_id = {
        check["definition"]["id"]: check for check in parity_artifact["checks"]
    }
    report_checks_by_id = {
        check["definition"]["id"]: check for check in report_payload["checks"]
    }

    assert "parity_outcome" not in machine_checks_by_id["en:quantity-not-recognized"]
    assert (
        "code_snippet_panels" not in machine_checks_by_id["en:quantity-not-recognized"]
    )
    assert (
        report_checks_by_id["en:quantity-not-recognized"]["parity_outcome"]
        == "legacy_only"
    )
    assert report_checks_by_id["en:quantity-not-recognized"]["total_mismatches"] == 1


def test_build_report_payload_rejects_not_compared_checks(
    parity_result_factory: ParityResultFactory,
) -> None:
    parity_result = parity_result_factory()
    parity_result.checks[0].comparison_status = "not_compared"
    parity_result.checks[0].passed = None
    parity_result.not_compared_check_count = 1
    parity_result.compared_check_count = 1

    with pytest.raises(
        ValueError,
        match="Migration report supports only compared checks",
    ):
        build_report_payload(parity_result)
