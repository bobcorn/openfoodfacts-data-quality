from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from ui.rendering import UI_STATIC_ROOT, create_template_environment

from migration.report.downloads import write_json_export_archive
from migration.report.payloads import (
    build_composition_segments,
    build_report_payload,
)
from migration.report.snippets import (
    build_code_snippet_panels,
    build_snippet_artifact,
    legacy_snippet_status_by_check,
    write_snippet_artifact,
)
from migration.run.serialization import (
    build_run_artifact,
    write_run_artifact_payload,
)
from migration.storage import load_recorded_run_snapshot

if TYPE_CHECKING:
    from migration.source.models import SourceInputSummary
    from off_data_quality.contracts.run import RunResult


OUTCOME_TERMS = {
    "pass": {
        "label": "Matching",
        "tooltip": "Checks whose reference and migrated outputs are identical under strict comparison.",
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
    "runtime_only": {
        "label": "Runtime Only",
        "tooltip": "Checks that run in the migrated runtime without any legacy comparison baseline.",
    },
}

REPORT_TERMS = {
    "hero": {
        "eyebrow": "Data Quality Monitoring",
        "title": "Quality Run Report",
        "subtitle": "Run evaluation for the active Open Food Facts data quality checks.",
        "export_as": "Export as",
        "export_pdf": "PDF",
        "export_html": "HTML",
        "export_json": "JSON",
    },
    "stats": {
        "mismatching_label": "Mismatching",
        "mismatching_tooltip": "Compared checks mismatch when they have at least one missing finding or at least one extra finding under strict comparison.",
        "missing_in_migrated_label": "Missing Findings",
        "missing_in_migrated_tooltip": "Count of findings present in reference output but absent from migrated output under strict comparison.",
        "extra_in_migrated_label": "Extra Findings",
        "extra_in_migrated_tooltip": "Count of findings present in migrated output but absent from reference output under strict comparison.",
        "runtime_only_checks_label": "Runtime Only",
        "runtime_only_checks_tooltip": "Checks executed without a legacy comparison baseline.",
        "affected_products_label": "Affected Products",
        "affected_products_tooltip": "Unique product codes involved in at least one missing or extra finding, shown against the total products in the current run.",
        "skipped_source_rows_label": "Skipped Source Rows",
        "skipped_source_rows_tooltip": "Source rows skipped before execution because they did not contain a valid non-empty product code.",
    },
    "composition": {
        "title": "Run Composition",
        "tooltip": "Run composition shows how active checks are distributed across outcomes in the current run.",
        "match_status": "Match",
        "mismatch_status": "Mismatch",
        "runtime_only_status": "Runtime Only",
        "match_status_tooltip": "This check matches exactly under strict comparison.",
        "mismatch_status_tooltip": "This check does not match under strict comparison.",
        "runtime_only_status_tooltip": "This check runs without any legacy comparison baseline.",
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
                "tooltip": "Concrete emitted code compared under strict comparison.",
            },
        },
    },
    "controls": {
        "search_label": "Search check id",
        "search_placeholder": "Search check id",
        "filters_label": "Filter by",
        "clear_filters": "Clear all filters",
        "filter_groups": {
            "outcome": "Outcome",
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
            "migrated_count": "Migrated findings",
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
    run_result: RunResult,
    output_dir: Path,
    *,
    legacy_source_root: Path | None = None,
    source_input_summary: SourceInputSummary | None = None,
) -> None:
    """Render the static HTML report and companion artifacts."""
    _render_report_snapshot(
        run_result=run_result,
        run_artifact=build_run_artifact(
            run_result,
            source_input_summary=source_input_summary,
        ),
        output_dir=output_dir,
        legacy_source_root=legacy_source_root,
    )


def render_report_from_store(
    *,
    store_path: Path,
    run_id: str,
    output_dir: Path,
    legacy_source_root: Path | None = None,
) -> None:
    """Render the static HTML report from one recorded run snapshot."""
    snapshot = load_recorded_run_snapshot(store_path, run_id=run_id)
    _render_report_snapshot(
        run_result=snapshot.run_result,
        run_artifact=snapshot.run_artifact,
        output_dir=output_dir,
        legacy_source_root=legacy_source_root,
    )


def _render_report_snapshot(
    *,
    run_result: RunResult,
    run_artifact: dict[str, Any],
    output_dir: Path,
    legacy_source_root: Path | None,
) -> None:
    """Render the static HTML report from one resolved report snapshot."""
    output_dir.mkdir(parents=True, exist_ok=True)
    templates_dir = Path(__file__).resolve().parent / "templates"
    shared_ui_dir = UI_STATIC_ROOT
    snippet_artifact = build_snippet_artifact(
        {check.definition.id for check in run_result.checks},
        legacy_source_root=legacy_source_root,
    )
    snippet_issues = cast(
        list[dict[str, Any]],
        snippet_artifact.get("issues", []),
    )
    report_payload = build_report_payload(
        run_result,
        run_artifact=run_artifact,
        code_snippet_panels_by_check=build_code_snippet_panels(snippet_artifact),
        legacy_snippet_status_by_check=legacy_snippet_status_by_check(snippet_artifact),
        snippet_issues=snippet_issues,
    )
    report_payload["snippet_issues"] = snippet_issues

    environment = create_template_environment(templates_dir)
    template = environment.get_template("report.html.j2")
    context = {
        "report": report_payload,
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ"),
        "failing_checks": sum(
            1 for check in run_result.checks if check.passed is False
        ),
        "compared_checks": run_result.compared_check_count,
        "runtime_only_checks": run_result.runtime_only_check_count,
        "has_runtime_only_checks": run_result.runtime_only_check_count > 0,
        "missing_findings": sum(check.missing_count for check in run_result.checks),
        "extra_findings": sum(check.extra_count for check in run_result.checks),
        "affected_products": len(
            {
                finding.product_id
                for check in run_result.checks
                for finding in (*check.missing, *check.extra)
            }
        ),
        "checks_evaluated": len(run_result.checks),
        "composition_segments": build_composition_segments(
            run_result,
            outcome_terms=OUTCOME_TERMS,
        ),
        "terms": REPORT_TERMS,
    }
    rendered_html = template.render(**context)

    (output_dir / "index.html").write_text(rendered_html, encoding="utf-8")
    (output_dir / "report.html").write_text(rendered_html, encoding="utf-8")
    run_artifact_path = write_run_artifact_payload(run_artifact, output_dir)
    snippets_artifact_path = write_snippet_artifact(snippet_artifact, output_dir)
    write_json_export_archive(
        output_dir=output_dir,
        artifact_paths=(run_artifact_path, snippets_artifact_path),
    )
    (output_dir / "favicon.ico").write_bytes(
        (shared_ui_dir / "favicon.ico").read_bytes()
    )
