from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, cast

import examples.google_sheets_demo.workflow as workflow_module
from _pytest.monkeypatch import MonkeyPatch
from examples.google_sheets_demo.models import VALIDATION_HEADERS, TableData
from examples.google_sheets_demo.workflow import (
    OK_ROW_COLOR,
    ROW_COLOR_BY_SEVERITY,
    build_mock_upload_preview,
    prepare_upload_candidates,
    strip_validation_columns,
    validate_table,
)


@dataclass(frozen=True)
class FindingStub:
    check_id: str
    severity: Literal["warning"]


def test_validate_table_prepends_validation_columns(
    monkeypatch: MonkeyPatch,
) -> None:
    table = TableData(
        headers=("code", "product_name"),
        rows=(
            ("0001", ""),
            ("0002", "Valid product"),
        ),
    )

    def fake_run(
        rows: list[dict[str, str]],
        jurisdictions: list[str] | None = None,
    ) -> list[FindingStub]:
        _ = jurisdictions
        row = rows[0]
        if row["code"] == "0001":
            return [FindingStub(check_id="missing-name", severity="warning")]
        return []

    workflow_module_any = cast(Any, workflow_module)
    workflow_checks = workflow_module_any.checks
    monkeypatch.setattr(workflow_checks, "run", fake_run)

    outcome = validate_table(
        table,
        checked_at="2026-04-08T15:00:00+02:00",
    )

    assert outcome.table.headers[: len(VALIDATION_HEADERS)] == VALIDATION_HEADERS
    assert outcome.table.rows[0][:5] == (
        "issue",
        "warning",
        "1",
        "missing-name",
        "2026-04-08T15:00:00+02:00",
    )
    assert outcome.table.rows[1][:5] == (
        "ok",
        "",
        "0",
        "",
        "2026-04-08T15:00:00+02:00",
    )
    assert outcome.row_backgrounds == {
        2: ROW_COLOR_BY_SEVERITY["warning"],
        3: OK_ROW_COLOR,
    }


def test_prepare_upload_candidates_keeps_only_ok_rows() -> None:
    table = TableData(
        headers=(*VALIDATION_HEADERS, "code", "product_name"),
        rows=(
            ("issue", "warning", "1", "missing-name", "now", "0001", ""),
            ("ok", "", "0", "", "now", "0002", "Valid product"),
        ),
    )

    candidates = prepare_upload_candidates(table)

    assert candidates.headers == ("code", "product_name")
    assert candidates.rows == (("0002", "Valid product"),)


def test_strip_validation_columns_removes_dq_headers() -> None:
    table = TableData(
        headers=(*VALIDATION_HEADERS, "code"),
        rows=(("ok", "", "0", "", "now", "0001"),),
    )

    assert strip_validation_columns(table) == TableData(
        headers=("code",),
        rows=(("0001",),),
    )


def test_build_mock_upload_preview_summarizes_rows() -> None:
    table = TableData(
        headers=("code", "product_name", "quantity"),
        rows=(
            ("0001", "Peanut Butter", "350 g"),
            ("0002", "Tea", "45 g"),
        ),
    )

    preview = build_mock_upload_preview(table, sample_rows=1, sample_fields=2)

    assert preview.candidate_count == 2
    assert preview.candidate_codes == ("0001", "0002")
    assert preview.sample_payloads == (
        {"code": "0001", "product_name": "Peanut Butter"},
    )
