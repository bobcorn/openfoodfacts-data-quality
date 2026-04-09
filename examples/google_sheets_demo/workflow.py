from __future__ import annotations

from datetime import UTC, datetime

from examples.google_sheets_demo.models import (
    VALIDATION_HEADER_SET,
    VALIDATION_HEADERS,
    MockUploadPreview,
    Severity,
    TableData,
    ValidationOutcome,
)

from off_data_quality import checks

SEVERITY_ORDER = {
    "info": 0,
    "completeness": 1,
    "warning": 2,
    "error": 3,
    "bug": 4,
}
ROW_COLOR_BY_SEVERITY: dict[Severity, str] = {
    "info": "#FFF7CC",
    "completeness": "#FFF7CC",
    "warning": "#FFE3B3",
    "error": "#F8C1C1",
    "bug": "#F1A5A5",
}
OK_ROW_COLOR = "#E2F3E7"


def clear_validation_output(table: TableData) -> TableData:
    """Remove derived validation columns from one sheet table."""
    return strip_validation_columns(table)


def strip_validation_columns(table: TableData) -> TableData:
    """Return the source table without any `dq_*` columns."""
    headers, keep_indices = _source_headers_and_indices(table)
    rows = tuple(tuple(row[index] for index in keep_indices) for row in table.rows)
    return TableData(headers=headers, rows=rows)


def validate_table(
    table: TableData,
    *,
    checked_at: str | None = None,
) -> ValidationOutcome:
    """Run the public row-based checks and prepend derived validation columns."""
    source_table = strip_validation_columns(table)
    timestamp = checked_at or _checked_at_timestamp()
    output_headers = (*VALIDATION_HEADERS, *source_table.headers)
    output_rows: list[tuple[str, ...]] = []
    row_backgrounds: dict[int, str] = {}
    validated_rows = 0
    issue_rows = 0
    error_rows = 0

    for row_number, row in enumerate(source_table.rows, start=2):
        if _row_is_blank(row):
            output_rows.append(("", "", "", "", timestamp, *row))
            continue

        validated_rows += 1
        row_values = {
            header: value
            for header, value in zip(source_table.headers, row, strict=True)
        }
        try:
            findings = checks.run(
                [row_values],
                jurisdictions=["global"],
            )
        except Exception as error:  # noqa: BLE001
            error_rows += 1
            row_backgrounds[row_number] = ROW_COLOR_BY_SEVERITY["error"]
            output_rows.append(
                (
                    "error",
                    "error",
                    "0",
                    f"Validation error: {error}",
                    timestamp,
                    *row,
                )
            )
            continue

        if not findings:
            row_backgrounds[row_number] = OK_ROW_COLOR
            output_rows.append(("ok", "", "0", "", timestamp, *row))
            continue

        issue_rows += 1
        severity = max(
            findings,
            key=lambda finding: SEVERITY_ORDER[finding.severity],
        ).severity
        finding_ids = tuple(finding.check_id for finding in findings)
        row_backgrounds[row_number] = ROW_COLOR_BY_SEVERITY[severity]
        output_rows.append(
            (
                "issue",
                severity,
                str(len(finding_ids)),
                "\n".join(finding_ids),
                timestamp,
                *row,
            )
        )

    return ValidationOutcome(
        table=TableData(headers=output_headers, rows=tuple(output_rows)),
        row_backgrounds=row_backgrounds,
        validated_rows=validated_rows,
        issue_rows=issue_rows,
        error_rows=error_rows,
    )


def prepare_upload_candidates(table: TableData) -> TableData:
    """Keep only rows that passed validation and drop derived columns."""
    status_index = table.column_index("dq_status")
    if status_index is None:
        raise ValueError("Validate the Data sheet before preparing upload candidates.")

    headers, keep_indices = _source_headers_and_indices(table)
    rows = tuple(
        tuple(row[index] for index in keep_indices)
        for row in table.rows
        if row[status_index] == "ok"
    )
    return TableData(headers=headers, rows=rows)


def build_mock_upload_preview(
    table: TableData,
    *,
    sample_rows: int = 3,
    sample_fields: int = 8,
) -> MockUploadPreview:
    """Return a small payload preview for the mocked OFF upload step."""
    non_blank_rows = [row for row in table.rows if not _row_is_blank(row)]
    code_index = table.column_index("code")
    candidate_codes = tuple(
        row[code_index]
        for row in non_blank_rows
        if code_index is not None and row[code_index]
    )[:10]
    sample_payloads = tuple(
        _sample_payload(table.headers, row, sample_fields=sample_fields)
        for row in non_blank_rows[:sample_rows]
    )
    return MockUploadPreview(
        candidate_count=len(non_blank_rows),
        candidate_codes=candidate_codes,
        sample_payloads=sample_payloads,
    )


def _sample_payload(
    headers: tuple[str, ...],
    row: tuple[str, ...],
    *,
    sample_fields: int,
) -> dict[str, str]:
    payload: dict[str, str] = {}
    for header, value in zip(headers, row, strict=True):
        if not value:
            continue
        payload[header] = value
        if len(payload) >= sample_fields:
            break
    return payload


def _checked_at_timestamp() -> str:
    return datetime.now(UTC).astimezone().isoformat(timespec="seconds")


def _source_headers_and_indices(table: TableData) -> tuple[tuple[str, ...], list[int]]:
    keep_indices = [
        index
        for index, header in enumerate(table.headers)
        if header not in VALIDATION_HEADER_SET
    ]
    headers = tuple(table.headers[index] for index in keep_indices)
    return headers, keep_indices


def _row_is_blank(row: tuple[str, ...]) -> bool:
    return not any(cell.strip() for cell in row)
