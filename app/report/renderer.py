from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict

from jinja2 import Environment, FileSystemLoader, select_autoescape

from app.parity.serialization import build_parity_artifact, write_parity_artifact
from app.report.downloads import write_json_export_archive
from app.report.snippets import (
    build_code_snippet_panels,
    build_snippet_artifact,
    write_snippet_artifact,
)

if TYPE_CHECKING:
    from app.parity.models import CheckParityResult, ParityResult

OutcomeId = Literal["pass", "legacy_only", "migrated_only", "mixed"]


class OutcomeTerms(TypedDict):
    label: str
    tooltip: str
    panel_tooltip: NotRequired[str]


OUTCOME_TERMS: dict[OutcomeId, OutcomeTerms] = {
    "pass": {
        "label": "Matching",
        "tooltip": "Checks whose reference and migrated outputs are identical under strict parity.",
    },
    "legacy_only": {
        "label": "Missing in Migrated",
        "tooltip": "Checks where migrated output is missing findings that are present in reference output, with no extra migrated findings.",
        "panel_tooltip": "Findings present in reference output but missing from migrated output.",
    },
    "migrated_only": {
        "label": "Extra in Migrated",
        "tooltip": "Checks where migrated output has extra findings not present in reference output, with no missing reference findings.",
        "panel_tooltip": "Findings produced by migrated output that are not present in reference output.",
    },
    "mixed": {
        "label": "Missing and Extra",
        "tooltip": "Checks where migrated output has both missing findings and extra findings compared with reference output.",
    },
}
OUTCOME_IDS: tuple[OutcomeId, ...] = (
    "pass",
    "legacy_only",
    "migrated_only",
    "mixed",
)

REPORT_TERMS = {
    "hero": {
        "eyebrow": "Data Quality Monitoring",
        "title": "Migration Report",
        "subtitle": "Parity evaluation for the evaluated Open Food Facts data quality checks in the current run.",
        "export_as": "Export as",
        "export_pdf": "PDF",
        "export_html": "HTML",
        "export_json": "JSON",
    },
    "stats": {
        "mismatching_label": "Checks Mismatching",
        "mismatching_tooltip": "Checks mismatch when they have at least one missing finding or at least one extra finding under strict parity.",
        "missing_in_migrated_label": "Missing Findings",
        "missing_in_migrated_tooltip": "Count of findings present in reference output but absent from migrated output under strict parity.",
        "extra_in_migrated_label": "Extra Findings",
        "extra_in_migrated_tooltip": "Count of findings present in migrated output but absent from reference output under strict parity.",
        "affected_products_label": "Affected Products",
        "affected_products_tooltip": "Unique product codes involved in at least one missing or extra finding, shown against the total products in the current run.",
    },
    "composition": {
        "title": "Parity Composition",
        "tooltip": "Parity composition shows how compared checks are distributed across parity outcomes.",
        "match_status": "Match",
        "mismatch_status": "Mismatch",
        "match_status_tooltip": "This check matches exactly under strict parity.",
        "mismatch_status_tooltip": "This check does not match under strict parity.",
    },
    "details": {
        "title": "Details",
        "examples_suffix": "showing up to",
        "columns": {
            "product": {
                "label": "Product",
                "tooltip": "Product code for the normalized finding.",
            },
            "severity": {
                "label": "Severity",
                "tooltip": "Normalized severity assigned to the finding.",
            },
            "observed_code": {
                "label": "Observed Code",
                "tooltip": "Concrete emitted code compared under strict parity.",
            },
        },
    },
    "controls": {
        "search_label": "Search check id",
        "search_placeholder": "Search check id",
        "filters_label": "Filter by",
        "clear_filters": "Clear all filters",
        "filter_groups": {
            "parity": "Parity",
            "definition_language": "Definition Language",
        },
        "sort_by_label": "Sort by",
        "sort_metric_label": "Metric",
        "sort_direction_label": "Direction",
        "ascending": "Ascending",
        "descending": "Descending",
        "sort_options": {
            "total_mismatches": "Total mismatches",
            "missing_count": "Missing in Migrated",
            "extra_count": "Extra in Migrated",
            "matched_count": "Matched findings",
            "check_id": "Check id",
        },
    },
    "empty_state": {
        "title": "No checks match the current view.",
        "body": "Try clearing some filters or searching for a different check.",
    },
    "definition_language": {
        "python": {
            "label": "Python",
        },
        "dsl": {
            "label": "DSL",
        },
    },
    "outcomes": OUTCOME_TERMS,
}


def render_report(
    parity_result: ParityResult,
    output_dir: Path,
    *,
    legacy_source_root: Path | None = None,
) -> None:
    """Render the static HTML report and companion artifacts."""
    _validate_supported_report_result(parity_result)
    output_dir.mkdir(parents=True, exist_ok=True)
    templates_dir = Path(__file__).resolve().parent / "templates"
    parity_artifact = build_parity_artifact(parity_result)
    snippet_artifact = build_snippet_artifact(
        {check.definition.id for check in parity_result.checks},
        legacy_source_root=legacy_source_root,
    )
    report_payload = build_report_payload(
        parity_result,
        parity_artifact=parity_artifact,
        code_snippet_panels_by_check=build_code_snippet_panels(snippet_artifact),
    )

    environment = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = environment.get_template("report.html.j2")
    context = {
        "report": report_payload,
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ"),
        "failing_checks": sum(
            1 for check in parity_result.checks if check.passed is False
        ),
        "compared_checks": parity_result.compared_check_count,
        "missing_findings": sum(check.missing_count for check in parity_result.checks),
        "extra_findings": sum(check.extra_count for check in parity_result.checks),
        "affected_products": len(
            {
                finding.product_id
                for check in parity_result.checks
                for finding in (*check.missing, *check.extra)
            }
        ),
        "checks_evaluated": len(parity_result.checks),
        "composition_segments": build_composition_segments(parity_result),
        "terms": REPORT_TERMS,
    }
    rendered_html = template.render(**context)

    (output_dir / "index.html").write_text(rendered_html, encoding="utf-8")
    (output_dir / "report.html").write_text(rendered_html, encoding="utf-8")
    parity_artifact_path = write_parity_artifact(parity_result, output_dir)
    snippets_artifact_path = write_snippet_artifact(snippet_artifact, output_dir)
    write_json_export_archive(
        output_dir=output_dir,
        artifact_paths=(parity_artifact_path, snippets_artifact_path),
    )
    (output_dir / "favicon.ico").write_bytes(
        (templates_dir / "favicon.ico").read_bytes()
    )


def build_report_payload(
    parity_result: ParityResult,
    *,
    parity_artifact: dict[str, Any] | None = None,
    code_snippet_panels_by_check: dict[str, list[dict[str, str]]] | None = None,
) -> dict[str, Any]:
    """Build the report-only payload derived from the parity domain result."""
    _validate_supported_report_result(parity_result)
    report_payload = dict(
        parity_artifact
        if parity_artifact is not None
        else build_parity_artifact(parity_result)
    )
    report_payload["checks"] = build_report_check_payloads(
        parity_result,
        code_snippet_panels_by_check=(
            code_snippet_panels_by_check if code_snippet_panels_by_check else {}
        ),
    )
    return report_payload


def build_report_check_payloads(
    parity_result: ParityResult,
    *,
    code_snippet_panels_by_check: dict[str, list[dict[str, str]]] | None = None,
) -> list[dict[str, Any]]:
    """Build sorted check payloads for the report UI."""
    resolved_code_snippet_panels_by_check = (
        code_snippet_panels_by_check if code_snippet_panels_by_check else {}
    )
    payloads = [
        _build_report_check_payload(
            check,
            resolved_code_snippet_panels_by_check,
        )
        for check in parity_result.checks
    ]
    return sorted(payloads, key=_default_check_sort_key)


def _build_report_check_payload(
    check: CheckParityResult,
    code_snippet_panels_by_check: dict[str, list[dict[str, str]]],
) -> dict[str, Any]:
    """Serialize one per-check parity result with derived UI metrics."""
    payload = check.model_dump(mode="json")
    payload["parity_outcome"] = check_outcome(check)
    payload["total_mismatches"] = check.missing_count + check.extra_count
    payload["search_text"] = check.definition.id.lower()
    payload["missing_examples_count"] = len(check.missing)
    payload["extra_examples_count"] = len(check.extra)
    payload["code_snippet_panels"] = code_snippet_panels_by_check.get(
        check.definition.id, []
    )
    return payload


def _default_check_sort_key(check: dict[str, Any]) -> tuple[int, int, int, int, str]:
    """Return the default report ordering: most actionable checks first."""
    outcome_priority = {
        "mixed": 0,
        "legacy_only": 1,
        "migrated_only": 2,
        "pass": 3,
    }
    return (
        -int(check["total_mismatches"]),
        outcome_priority[str(check["parity_outcome"])],
        -int(check["missing_count"]),
        -int(check["extra_count"]),
        str(check["definition"]["id"]),
    )


def build_composition_segments(parity_result: ParityResult) -> list[dict[str, object]]:
    """Summarize check outcomes for the composition bar."""
    composition: dict[OutcomeId, int] = {
        "pass": 0,
        "legacy_only": 0,
        "migrated_only": 0,
        "mixed": 0,
    }
    for check in parity_result.checks:
        composition[check_outcome(check)] += 1

    total_checks = max(len(parity_result.checks), 1)
    return [
        {
            "id": outcome_id,
            "label": OUTCOME_TERMS[outcome_id]["label"],
            "count": composition[outcome_id],
            "percentage": composition[outcome_id] / total_checks * 100,
            "tooltip": OUTCOME_TERMS[outcome_id]["tooltip"],
        }
        for outcome_id in OUTCOME_IDS
    ]


def check_outcome(check: CheckParityResult) -> OutcomeId:
    """Map a per-check parity result to its report outcome bucket."""
    if check.comparison_status != "compared":
        raise ValueError(
            "Migration report does not support checks without legacy comparison. "
            f"Unsupported check: {check.definition.id}"
        )
    if check.passed:
        return "pass"
    if check.missing_count and check.extra_count:
        return "mixed"
    if check.missing_count:
        return "legacy_only"
    return "migrated_only"


def _validate_supported_report_result(parity_result: ParityResult) -> None:
    """Reject parity results that contain checks outside migration-report scope."""
    unsupported_checks = [
        check.definition.id
        for check in parity_result.checks
        if check.comparison_status != "compared"
    ]
    if unsupported_checks:
        raise ValueError(
            "Migration report supports only compared checks. Unsupported checks: "
            + ", ".join(sorted(unsupported_checks))
        )
