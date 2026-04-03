from __future__ import annotations

from collections.abc import Callable

from app.report.payloads import build_report_payload
from app.run.serialization import (
    RUN_ARTIFACT_KIND,
    RUN_ARTIFACT_SCHEMA_VERSION,
    build_run_artifact,
)
from app.storage import CheckMismatchGovernanceSummary

from openfoodfacts_data_quality.contracts.run import RunResult

RunResultFactory = Callable[[], RunResult]


def test_build_report_payload_adds_ui_fields_without_mutating_run_artifact(
    run_result_factory: RunResultFactory,
) -> None:
    run_result = run_result_factory()
    run_artifact = build_run_artifact(run_result)

    report_payload = build_report_payload(
        run_result,
        run_artifact=run_artifact,
    )

    assert run_artifact["kind"] == RUN_ARTIFACT_KIND
    assert run_artifact["schema_version"] == RUN_ARTIFACT_SCHEMA_VERSION

    machine_checks_by_id = {
        check["definition"]["id"]: check for check in run_artifact["checks"]
    }
    report_checks_by_id = {
        check["definition"]["id"]: check for check in report_payload["checks"]
    }

    assert "run_outcome" not in machine_checks_by_id["en:quantity-not-recognized"]
    assert (
        "code_snippet_panels" not in machine_checks_by_id["en:quantity-not-recognized"]
    )
    assert (
        "snippet_issue_messages"
        not in machine_checks_by_id["en:quantity-not-recognized"]
    )
    assert (
        "legacy_snippet_status"
        not in machine_checks_by_id["en:quantity-not-recognized"]
    )
    assert (
        report_checks_by_id["en:quantity-not-recognized"]["run_outcome"]
        == "legacy_only"
    )
    assert report_checks_by_id["en:quantity-not-recognized"]["total_mismatches"] == 1
    assert (
        report_checks_by_id["en:quantity-not-recognized"]["legacy_snippet_status"]
        == "unavailable"
    )
    assert (
        report_checks_by_id["en:quantity-not-recognized"]["snippet_issue_messages"]
        == []
    )


def test_build_report_payload_supports_runtime_only_checks(
    run_result_factory: RunResultFactory,
) -> None:
    run_result = run_result_factory().model_copy(deep=True)
    run_result.checks[0].comparison_status = "runtime_only"
    run_result.checks[0].passed = None
    run_result.runtime_only_check_count = 1
    run_result.compared_check_count = 1

    report_payload = build_report_payload(
        run_result,
        run_artifact=build_run_artifact(run_result),
    )

    report_checks_by_id = {
        check["definition"]["id"]: check for check in report_payload["checks"]
    }

    assert (
        report_checks_by_id["en:product-name-to-be-completed"]["run_outcome"]
        == "runtime_only"
    )
    assert (
        report_checks_by_id["en:product-name-to-be-completed"]["legacy_snippet_status"]
        == "unavailable"
    )


def test_build_report_payload_includes_governance_counts(
    run_result_factory: RunResultFactory,
) -> None:
    run_result = run_result_factory()

    report_payload = build_report_payload(
        run_result,
        run_artifact=build_run_artifact(run_result),
        check_governance_by_id={
            "en:quantity-not-recognized": CheckMismatchGovernanceSummary(
                expected_missing_count=1,
            )
        },
        expected_differences_rule_count=2,
    )

    report_checks_by_id = {
        check["definition"]["id"]: check for check in report_payload["checks"]
    }

    assert report_payload["expected_differences_rule_count"] == 2
    assert report_payload["expected_mismatch_total"] == 1
    assert report_payload["unexpected_mismatch_total"] == 0
    assert (
        report_checks_by_id["en:quantity-not-recognized"]["expected_missing_count"] == 1
    )
    assert (
        report_checks_by_id["en:quantity-not-recognized"]["expected_total_mismatches"]
        == 1
    )
