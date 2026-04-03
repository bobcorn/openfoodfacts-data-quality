from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, cast

if TYPE_CHECKING:
    from collections.abc import Mapping

    from app.report.snippets import LegacySnippetStatus
    from app.storage import CheckMismatchGovernanceSummary
    from openfoodfacts_data_quality.contracts.run import RunCheckResult, RunResult

OutcomeId = Literal[
    "pass",
    "legacy_only",
    "migrated_only",
    "mixed",
    "runtime_only",
]
CardStatus = Literal["pass", "fail", "runtime_only"]
OUTCOME_IDS: tuple[OutcomeId, ...] = (
    "pass",
    "legacy_only",
    "migrated_only",
    "mixed",
    "runtime_only",
)


def build_report_payload(
    run_result: RunResult,
    *,
    run_artifact: dict[str, Any],
    check_governance_by_id: Mapping[str, CheckMismatchGovernanceSummary] | None = None,
    expected_differences_rule_count: int = 0,
    expected_mismatch_total: int | None = None,
    unexpected_mismatch_total: int | None = None,
    code_snippet_panels_by_check: dict[str, list[dict[str, str]]] | None = None,
    legacy_snippet_status_by_check: dict[str, LegacySnippetStatus] | None = None,
    snippet_issues: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build the report-only payload derived from the run result."""
    report_payload = dict(run_artifact)
    governance_by_id = (
        dict(check_governance_by_id) if check_governance_by_id is not None else {}
    )
    report_payload["checks"] = build_report_check_payloads(
        run_result,
        check_governance_by_id=governance_by_id,
        code_snippet_panels_by_check=(
            code_snippet_panels_by_check if code_snippet_panels_by_check else {}
        ),
        legacy_snippet_status_by_check=(
            legacy_snippet_status_by_check if legacy_snippet_status_by_check else {}
        ),
        snippet_issues=snippet_issues if snippet_issues else [],
    )
    report_payload["expected_differences_rule_count"] = expected_differences_rule_count
    report_payload["expected_mismatch_total"] = (
        sum(counts.expected_total_mismatches for counts in governance_by_id.values())
        if expected_mismatch_total is None
        else expected_mismatch_total
    )
    report_payload["unexpected_mismatch_total"] = (
        sum(counts.unexpected_total_mismatches for counts in governance_by_id.values())
        if unexpected_mismatch_total is None
        else unexpected_mismatch_total
    )
    return report_payload


def build_report_check_payloads(
    run_result: RunResult,
    *,
    check_governance_by_id: Mapping[str, CheckMismatchGovernanceSummary] | None = None,
    code_snippet_panels_by_check: dict[str, list[dict[str, str]]] | None = None,
    legacy_snippet_status_by_check: dict[str, LegacySnippetStatus] | None = None,
    snippet_issues: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build sorted check payloads for the report UI."""
    resolved_check_governance_by_id = (
        check_governance_by_id
        if check_governance_by_id
        else cast("Mapping[str, CheckMismatchGovernanceSummary]", {})
    )
    resolved_code_snippet_panels_by_check = (
        code_snippet_panels_by_check if code_snippet_panels_by_check else {}
    )
    resolved_legacy_snippet_status_by_check = (
        legacy_snippet_status_by_check if legacy_snippet_status_by_check else {}
    )
    snippet_issue_messages_by_check = _snippet_issue_messages_by_check(
        snippet_issues if snippet_issues else []
    )
    payloads = [
        _build_report_check_payload(
            check,
            resolved_check_governance_by_id,
            resolved_code_snippet_panels_by_check,
            resolved_legacy_snippet_status_by_check,
            snippet_issue_messages_by_check,
        )
        for check in run_result.checks
    ]
    return sorted(payloads, key=_default_check_sort_key)


def _build_report_check_payload(
    check: RunCheckResult,
    check_governance_by_id: Mapping[str, CheckMismatchGovernanceSummary],
    code_snippet_panels_by_check: dict[str, list[dict[str, str]]],
    legacy_snippet_status_by_check: dict[str, LegacySnippetStatus],
    snippet_issue_messages_by_check: dict[str, list[str]],
) -> dict[str, Any]:
    """Serialize one check result with derived UI metrics."""
    payload = check.model_dump(mode="json")
    governance = check_governance_by_id.get(check.definition.id)
    expected_missing_count = (
        governance.expected_missing_count if governance is not None else 0
    )
    unexpected_missing_count = (
        governance.unexpected_missing_count if governance is not None else 0
    )
    expected_extra_count = (
        governance.expected_extra_count if governance is not None else 0
    )
    unexpected_extra_count = (
        governance.unexpected_extra_count if governance is not None else 0
    )
    payload["run_outcome"] = check_outcome(check)
    payload["card_status"] = check_card_status(check)
    payload["total_mismatches"] = check.missing_count + check.extra_count
    payload["expected_missing_count"] = expected_missing_count
    payload["unexpected_missing_count"] = unexpected_missing_count
    payload["expected_extra_count"] = expected_extra_count
    payload["unexpected_extra_count"] = unexpected_extra_count
    payload["expected_total_mismatches"] = expected_missing_count + expected_extra_count
    payload["unexpected_total_mismatches"] = (
        unexpected_missing_count + unexpected_extra_count
    )
    payload["search_text"] = check.definition.id.lower()
    payload["missing_examples_count"] = len(check.missing)
    payload["extra_examples_count"] = len(check.extra)
    payload["code_snippet_panels"] = code_snippet_panels_by_check.get(
        check.definition.id, []
    )
    payload["legacy_snippet_status"] = legacy_snippet_status_by_check.get(
        check.definition.id,
        "not_applicable" if check.definition.legacy_identity is None else "unavailable",
    )
    payload["snippet_issue_messages"] = snippet_issue_messages_by_check.get(
        check.definition.id, []
    )
    return payload


def _snippet_issue_messages_by_check(
    snippet_issues: list[dict[str, Any]],
) -> dict[str, list[str]]:
    """Group snippet warning messages by canonical check id."""
    messages_by_check: dict[str, list[str]] = {}
    for issue in snippet_issues:
        message = issue.get("message")
        check_ids = issue.get("check_ids")
        if not isinstance(message, str) or not isinstance(check_ids, list):
            continue
        for raw_check_id in cast(list[object], check_ids):
            if not isinstance(raw_check_id, str):
                continue
            messages_by_check.setdefault(raw_check_id, []).append(message)
    return messages_by_check


def _default_check_sort_key(check: dict[str, Any]) -> tuple[int, int, int, int, str]:
    """Return the default report ordering: most actionable checks first."""
    outcome_priority = {
        "mixed": 0,
        "legacy_only": 1,
        "migrated_only": 2,
        "runtime_only": 3,
        "pass": 4,
    }
    return (
        -int(check["total_mismatches"]),
        outcome_priority[str(check["run_outcome"])],
        -int(check["missing_count"]),
        -int(check["extra_count"]),
        str(check["definition"]["id"]),
    )


def build_composition_segments(
    run_result: RunResult,
    *,
    outcome_terms: Mapping[str, Mapping[str, str]],
) -> list[dict[str, object]]:
    """Summarize check outcomes for the composition bar."""
    composition: dict[OutcomeId, int] = {
        "pass": 0,
        "legacy_only": 0,
        "migrated_only": 0,
        "mixed": 0,
        "runtime_only": 0,
    }
    for check in run_result.checks:
        composition[check_outcome(check)] += 1

    total_checks = max(len(run_result.checks), 1)
    return [
        {
            "id": outcome_id,
            "label": outcome_terms[outcome_id]["label"],
            "count": composition[outcome_id],
            "percentage": composition[outcome_id] / total_checks * 100,
            "tooltip": outcome_terms[outcome_id]["tooltip"],
        }
        for outcome_id in OUTCOME_IDS
    ]


def check_outcome(check: RunCheckResult) -> OutcomeId:
    """Map one check result to its report outcome bucket."""
    if check.comparison_status == "runtime_only":
        return "runtime_only"
    if check.passed:
        return "pass"
    if check.missing_count and check.extra_count:
        return "mixed"
    if check.missing_count:
        return "legacy_only"
    return "migrated_only"


def check_card_status(check: RunCheckResult) -> CardStatus:
    """Map one check result to its card visual state."""
    if check.comparison_status == "runtime_only":
        return "runtime_only"
    if check.passed:
        return "pass"
    return "fail"
