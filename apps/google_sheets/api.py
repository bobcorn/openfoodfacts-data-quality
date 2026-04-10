from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

from apps.google_sheets.data_sources import load_csv_table
from apps.google_sheets.models import TableData
from apps.google_sheets.workflow import (
    build_mock_upload_preview,
    clear_validation_output,
    prepare_upload_candidates,
    validate_table,
)
from off_data_quality import checks

GOOGLE_CLIENT_ID_ENV_VAR = "GOOGLE_SHEETS_CLIENT_ID"
GOOGLE_API_KEY_ENV_VAR = "GOOGLE_SHEETS_API_KEY"
GOOGLE_CLOUD_PROJECT_NUMBER_ENV_VAR = "GOOGLE_SHEETS_CLOUD_PROJECT_NUMBER"
DEFAULT_INPUT_SHEET_NAME = "Data"
DEFAULT_OUTPUT_SHEET_NAME = "Ready for OFF upload"
SUPPORTED_COLUMNS = frozenset(checks.COLUMNS)


@dataclass(frozen=True)
class PublicAppConfig:
    google_client_id: str
    google_api_key: str
    google_cloud_project_number: str
    input_sheet_name: str
    output_sheet_name: str

    def to_payload(self) -> dict[str, str]:
        return {
            "googleClientId": self.google_client_id,
            "googleApiKey": self.google_api_key,
            "googleCloudProjectNumber": self.google_cloud_project_number,
            "inputSheetName": self.input_sheet_name,
            "outputSheetName": self.output_sheet_name,
        }


def build_public_app_config(environ: Mapping[str, str]) -> PublicAppConfig:
    """Build the public browser config for one application instance."""
    return PublicAppConfig(
        google_client_id=environ.get(GOOGLE_CLIENT_ID_ENV_VAR, "").strip(),
        google_api_key=environ.get(GOOGLE_API_KEY_ENV_VAR, "").strip(),
        google_cloud_project_number=environ.get(
            GOOGLE_CLOUD_PROJECT_NUMBER_ENV_VAR, ""
        ).strip(),
        input_sheet_name=DEFAULT_INPUT_SHEET_NAME,
        output_sheet_name=DEFAULT_OUTPUT_SHEET_NAME,
    )


def parse_csv_request(payload: Mapping[str, object]) -> dict[str, object]:
    """Parse one CSV upload request into a normalized table payload."""
    file_name = payload.get("fileName")
    csv_text = payload.get("csvText")
    if not isinstance(file_name, str) or not file_name.strip().lower().endswith(".csv"):
        raise ValueError("Upload a .csv file before calling this action.")
    if not isinstance(csv_text, str) or not csv_text:
        raise ValueError("Upload a CSV file before calling this action.")
    table = load_csv_table(csv_text.encode("utf-8"))
    schema = assess_uploaded_csv_schema(table)
    return {
        "table": table_to_payload(table),
        "schema": schema,
    }


def validate_request(payload: Mapping[str, object]) -> dict[str, object]:
    """Run global data-quality checks against one sheet table payload."""
    table = table_from_payload(payload.get("table"))
    outcome = validate_table(table)
    return {
        "table": table_to_payload(outcome.table),
        "rowBackgrounds": {
            str(row_number): color
            for row_number, color in outcome.row_backgrounds.items()
        },
        "validatedRows": outcome.validated_rows,
        "issueRows": outcome.issue_rows,
        "errorRows": outcome.error_rows,
    }


def clear_validation_output_request(payload: Mapping[str, object]) -> dict[str, object]:
    """Strip derived validation columns from one sheet table payload."""
    table = table_from_payload(payload.get("table"))
    return {
        "table": table_to_payload(clear_validation_output(table)),
    }


def prepare_upload_candidates_request(
    payload: Mapping[str, object],
) -> dict[str, object]:
    """Keep only passing rows from one validated table payload."""
    table = table_from_payload(payload.get("table"))
    return {
        "table": table_to_payload(prepare_upload_candidates(table)),
    }


def mock_upload_request(payload: Mapping[str, object]) -> dict[str, object]:
    """Build one small mocked OFF upload preview from a candidate table."""
    table = table_from_payload(payload.get("table"))
    preview = build_mock_upload_preview(table)
    return {
        "candidateCount": preview.candidate_count,
        "candidateCodes": list(preview.candidate_codes),
        "samplePayloads": list(preview.sample_payloads),
    }


def table_to_payload(table: TableData) -> dict[str, object]:
    return {
        "headers": list(table.headers),
        "rows": [list(row) for row in table.rows],
    }


def table_from_payload(payload: object) -> TableData:
    """Parse one JSON table payload into the immutable table model."""
    if not isinstance(payload, Mapping):
        raise ValueError("Expected a table payload.")

    payload_mapping = cast(Mapping[object, object], payload)
    payload_dict: dict[str, object] = {}
    for raw_key, raw_value in payload_mapping.items():
        if isinstance(raw_key, str):
            payload_dict[raw_key] = raw_value
    headers_raw: object = payload_dict.get("headers")
    rows_raw: object = payload_dict.get("rows")
    if not isinstance(headers_raw, Sequence) or isinstance(headers_raw, str | bytes):
        raise ValueError("The table payload must include a header list.")
    if not isinstance(rows_raw, Sequence) or isinstance(rows_raw, str | bytes):
        raise ValueError("The table payload must include a row list.")

    headers_items = cast(Sequence[object], headers_raw)
    rows_items = cast(Sequence[object], rows_raw)
    headers = tuple(
        _string_sequence(headers_items, error_message="Headers must be strings.")
    )
    normalized_row_lists: list[tuple[str, ...]] = []
    for row in rows_items:
        if not isinstance(row, Sequence) or isinstance(row, str | bytes):
            raise ValueError("Each row must be a list of strings.")
        row_values = cast(Sequence[object], row)
        normalized_row_lists.append(
            tuple(
                _string_sequence(
                    row_values,
                    error_message="Rows must contain only strings.",
                )
            )
        )

    width = len(headers)
    normalized_rows = tuple(
        tuple((list(row) + [""] * width)[:width]) for row in normalized_row_lists
    )
    return TableData(headers=headers, rows=normalized_rows)


def _string_sequence(
    values: Sequence[object],
    *,
    error_message: str,
) -> list[str]:
    items: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise ValueError(error_message)
        items.append(value)
    return items


def assess_uploaded_csv_schema(table: TableData) -> dict[str, object]:
    """Describe how well one uploaded table matches the app input contract."""
    recognized_columns = [
        header for header in table.headers if header in SUPPORTED_COLUMNS
    ]
    ignored_columns = [
        header for header in table.headers if header not in SUPPORTED_COLUMNS
    ]
    if "code" not in recognized_columns:
        raise ValueError("This CSV does not include the required 'code' column.")

    return {
        "supportedColumnsCount": len(SUPPORTED_COLUMNS),
        "recognizedColumnsCount": len(recognized_columns),
        "ignoredColumnsCount": len(ignored_columns),
    }
