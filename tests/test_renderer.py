from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from zipfile import ZipFile

from app.parity.models import ParityResult
from app.parity.serialization import PARITY_ARTIFACT_FILENAME
from app.report.downloads import JSON_EXPORT_ARCHIVE_FILENAME
from app.report.renderer import render_report
from app.report.snippets import SNIPPETS_ARTIFACT_FILENAME

ParityResultFactory = Callable[[], ParityResult]


def test_render_report_writes_expected_artifacts(
    tmp_path: Path,
    parity_result_factory: ParityResultFactory,
    legacy_source_root_factory: Callable[[Path], Path],
) -> None:
    legacy_root = legacy_source_root_factory(tmp_path)

    render_report(
        parity_result_factory(),
        tmp_path,
        legacy_source_root=legacy_root,
    )

    index_path = tmp_path / "index.html"
    report_path = tmp_path / "report.html"
    json_path = tmp_path / PARITY_ARTIFACT_FILENAME
    snippets_path = tmp_path / SNIPPETS_ARTIFACT_FILENAME
    json_archive_path = tmp_path / JSON_EXPORT_ARCHIVE_FILENAME

    assert index_path.exists()
    assert report_path.exists()
    assert json_path.exists()
    assert snippets_path.exists()
    assert json_archive_path.exists()
    html = index_path.read_text(encoding="utf-8")
    assert "Migration Report" in html
    assert "evaluated Open Food Facts data quality checks in the current run" in html
    assert "Missing Findings" in html
    assert "Extra Findings" in html
    assert "Affected Products" in html
    assert "Checks Not Compared" not in html
    assert 'value="not_compared"' not in html
    assert "Parity Mode" not in html
    assert 'aria-label="Current parity mode"' not in html
    assert "1 / 2" in html
    assert 'class="stat stat--interactive stat--hero stat--detail stat--fail"' in html
    assert 'class="stat stat--hero stat--detail stat--missing stat--fail"' in html
    assert 'class="stat stat--hero stat--detail stat--extra stat--pass"' in html
    assert 'class="stat stat--hero stat--detail stat--fail"' in html
    assert ".stat--detail.stat--pass" in html
    assert ".stat--detail.stat--fail" in html
    assert "snippet-language" not in html
    assert ">JSON<" in html
    assert '<span class="disclosure-summary-label">Implementation</span>' not in html
    assert "Source Snapshot" in html
    assert 'id="visible-check-count">2</span>' in html
    assert 'id="visible-check-label">checks</span>' in html

    machine_payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert machine_payload["run_id"] == "test-run"
    assert machine_payload["source_snapshot_id"] == "source-snapshot"
    assert machine_payload["checks"][0]["definition"]["id"]
    assert "parity_outcome" not in machine_payload["checks"][0]
    assert "total_mismatches" not in machine_payload["checks"][0]
    assert "code_snippet_panels" not in machine_payload["checks"][0]

    snippet_payload = json.loads(snippets_path.read_text(encoding="utf-8"))
    first_check_snippets = next(iter(snippet_payload["checks"].values()))
    snippet_origins = {
        snippet["origin"]
        for snippets in snippet_payload["checks"].values()
        for snippet in snippets
    }
    assert first_check_snippets[0]["check_id"]
    assert first_check_snippets[0]["origin"] in {"legacy", "migrated"}
    assert first_check_snippets[0]["definition_language"] in {"python", "dsl", None}
    assert first_check_snippets[0]["path"]
    assert first_check_snippets[0]["start_line"] >= 1
    assert first_check_snippets[0]["end_line"] >= first_check_snippets[0]["start_line"]
    assert first_check_snippets[0]["code"]
    assert "html" not in first_check_snippets[0]
    assert "migrated" in snippet_origins
    assert "Migrated Snippet" in html
    if "legacy" in snippet_origins:
        assert "Legacy Snippet" in html
        assert html.index("Migrated Snippet") < html.index("Legacy Snippet")
    else:
        assert "Legacy Snippet" not in html

    with ZipFile(json_archive_path) as archive:
        assert sorted(archive.namelist()) == sorted(
            [PARITY_ARTIFACT_FILENAME, SNIPPETS_ARTIFACT_FILENAME]
        )
