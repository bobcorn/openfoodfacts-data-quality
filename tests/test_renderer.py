from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from zipfile import ZipFile

from app.report.downloads import JSON_EXPORT_ARCHIVE_FILENAME
from app.report.renderer import render_report
from app.report.snippets import (
    SNIPPETS_ARTIFACT_FILENAME,
    SNIPPETS_ARTIFACT_KIND,
    SNIPPETS_ARTIFACT_SCHEMA_VERSION,
)
from app.run.serialization import (
    RUN_ARTIFACT_FILENAME,
    RUN_ARTIFACT_KIND,
    RUN_ARTIFACT_SCHEMA_VERSION,
)

from openfoodfacts_data_quality.contracts.run import RunResult

RunResultFactory = Callable[[], RunResult]


def test_render_report_writes_expected_artifacts(
    tmp_path: Path,
    run_result_factory: RunResultFactory,
    legacy_source_root_factory: Callable[[Path], Path],
) -> None:
    legacy_root = legacy_source_root_factory(tmp_path)

    render_report(
        run_result_factory(),
        tmp_path,
        legacy_source_root=legacy_root,
    )

    index_path = tmp_path / "index.html"
    report_path = tmp_path / "report.html"
    json_path = tmp_path / RUN_ARTIFACT_FILENAME
    snippets_path = tmp_path / SNIPPETS_ARTIFACT_FILENAME
    json_archive_path = tmp_path / JSON_EXPORT_ARCHIVE_FILENAME

    assert index_path.exists()
    assert report_path.exists()
    assert json_path.exists()
    assert snippets_path.exists()
    assert json_archive_path.exists()
    html = index_path.read_text(encoding="utf-8")
    assert "Quality Run Report" in html
    assert "Strict comparison is shown where a legacy baseline exists" in html
    assert "Mismatching" in html
    assert "Missing Findings" in html
    assert "Extra Findings" in html
    assert "Runtime Only" in html
    assert "Affected Products" in html
    assert "Compared Checks Mismatching" not in html
    assert "Runtime Only Checks" not in html
    assert 'value="runtime_only"' in html
    assert "1 / 2" in html
    assert 'class="stat stat--interactive stat--hero stat--detail stat--fail"' in html
    assert 'class="stat stat--hero stat--detail stat--missing stat--fail"' in html
    assert 'class="stat stat--hero stat--detail stat--extra stat--pass"' in html
    assert 'class="stat stat--hero stat--detail stat--fail"' in html
    assert (
        'class="stat stat--hero stat--detail">\n'
        '            <span class="stat__label"><span class="term" data-tooltip="Checks executed without a legacy comparison baseline.">Runtime Only</span></span>'
    ) in html
    assert "grid-template-columns: repeat(5, minmax(0, 1fr));" in html
    assert (
        html.index("Mismatching")
        < html.index("Missing Findings")
        < html.index("Extra Findings")
        < html.index("Affected Products")
        < html.index("Runtime Only")
    )
    assert ".stat--detail.stat--pass" in html
    assert ".stat--detail.stat--fail" in html
    assert "snippet-language" not in html
    assert ">JSON<" in html
    assert '<span class="disclosure-summary-label">Implementation</span>' not in html
    assert "Source Snapshot" in html
    assert 'id="visible-check-count">2</span>' in html
    assert 'id="visible-check-label">checks</span>' in html

    machine_payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert machine_payload["kind"] == RUN_ARTIFACT_KIND
    assert machine_payload["schema_version"] == RUN_ARTIFACT_SCHEMA_VERSION
    assert machine_payload["run_id"] == "test-run"
    assert machine_payload["source_snapshot_id"] == "source-snapshot"
    assert machine_payload["checks"][0]["definition"]["id"]
    assert "run_outcome" not in machine_payload["checks"][0]
    assert "total_mismatches" not in machine_payload["checks"][0]
    assert "code_snippet_panels" not in machine_payload["checks"][0]

    snippet_payload = json.loads(snippets_path.read_text(encoding="utf-8"))
    assert snippet_payload["kind"] == SNIPPETS_ARTIFACT_KIND
    assert snippet_payload["schema_version"] == SNIPPETS_ARTIFACT_SCHEMA_VERSION
    first_check_entry = next(iter(snippet_payload["checks"].values()))
    first_check_snippets = first_check_entry["snippets"]
    snippet_origins = {
        snippet["origin"]
        for check_entry in snippet_payload["checks"].values()
        for snippet in check_entry["snippets"]
    }
    assert first_check_entry["legacy_snippet_status"] in {
        "available",
        "not_applicable",
        "unavailable",
    }
    assert first_check_snippets[0]["check_id"]
    assert first_check_snippets[0]["origin"] in {"legacy", "implementation"}
    assert first_check_snippets[0]["definition_language"] in {"python", "dsl", None}
    assert first_check_snippets[0]["path"]
    assert first_check_snippets[0]["start_line"] >= 1
    assert first_check_snippets[0]["end_line"] >= first_check_snippets[0]["start_line"]
    assert first_check_snippets[0]["code"]
    assert "html" not in first_check_snippets[0]
    assert "implementation" in snippet_origins
    assert "Current Implementation" in html
    if "legacy" in snippet_origins:
        assert "Legacy Source" in html
        assert html.index("Current Implementation") < html.index("Legacy Source")
    else:
        assert "Legacy Source" not in html

    with ZipFile(json_archive_path) as archive:
        assert sorted(archive.namelist()) == sorted(
            [RUN_ARTIFACT_FILENAME, SNIPPETS_ARTIFACT_FILENAME]
        )


def test_render_report_surfaces_legacy_snippet_warnings_per_check(
    tmp_path: Path,
    run_result_factory: RunResultFactory,
) -> None:
    render_report(
        run_result_factory(),
        tmp_path,
        legacy_source_root=tmp_path / "missing-legacy-root",
    )

    html = (tmp_path / "index.html").read_text(encoding="utf-8")
    snippets_path = tmp_path / SNIPPETS_ARTIFACT_FILENAME
    snippet_payload = json.loads(snippets_path.read_text(encoding="utf-8"))

    assert "Current Implementation" in html
    assert "Legacy Source" not in html
    assert (
        "Legacy snippets unavailable because the legacy source tree does not contain the required Perl data-quality modules."
        in html
    )
    assert snippet_payload["issues"]
    assert all(
        check_entry["legacy_snippet_status"] == "unavailable"
        for check_entry in snippet_payload["checks"].values()
        if check_entry["snippets"]
    )
