from __future__ import annotations

import pytest
from apps.google_sheets.api import (
    build_public_app_config,
    parse_csv_request,
    prepare_upload_candidates_request,
    table_from_payload,
)

from off_data_quality import checks

SUPPORTED_COLUMNS_COUNT = len(checks.COLUMNS)


def test_build_public_app_config_reads_client_id_from_env() -> None:
    config = build_public_app_config(
        {
            "GOOGLE_SHEETS_CLIENT_ID": "demo-client-id",
            "GOOGLE_SHEETS_API_KEY": "demo-api-key",
            "GOOGLE_SHEETS_CLOUD_PROJECT_NUMBER": "1234567890",
        }
    )

    assert config.to_payload() == {
        "googleClientId": "demo-client-id",
        "googleApiKey": "demo-api-key",
        "googleCloudProjectNumber": "1234567890",
        "inputSheetName": "Data",
        "outputSheetName": "Ready for OFF upload",
    }


def test_parse_csv_request_returns_table_payload() -> None:
    response = parse_csv_request(
        {
            "fileName": "products.csv",
            "csvText": "code,product_name\n0001,Peanut Butter\n",
        }
    )

    assert response == {
        "table": {
            "headers": ["code", "product_name"],
            "rows": [["0001", "Peanut Butter"]],
        },
        "schema": {
            "supportedColumnsCount": SUPPORTED_COLUMNS_COUNT,
            "recognizedColumnsCount": 2,
            "ignoredColumnsCount": 0,
        },
    }


def test_parse_csv_request_rejects_non_csv_file_names() -> None:
    with pytest.raises(ValueError, match=r"Upload a \.csv file"):
        parse_csv_request(
            {
                "fileName": "products.txt",
                "csvText": "code,product_name\n0001,Peanut Butter\n",
            }
        )


def test_parse_csv_request_rejects_missing_code_column() -> None:
    with pytest.raises(ValueError, match="required 'code' column"):
        parse_csv_request(
            {
                "fileName": "products.csv",
                "csvText": "product_name,quantity\nPeanut Butter,350 g\n",
            }
        )


def test_parse_csv_request_reports_supported_column_counts() -> None:
    response = parse_csv_request(
        {
            "fileName": "products.csv",
            "csvText": "code\n0001\n",
        }
    )

    assert response["schema"] == {
        "supportedColumnsCount": SUPPORTED_COLUMNS_COUNT,
        "recognizedColumnsCount": 1,
        "ignoredColumnsCount": 0,
    }


def test_parse_csv_request_reports_ignored_column_count() -> None:
    response = parse_csv_request(
        {
            "fileName": "products.csv",
            "csvText": "code,item_title\n0001,Peanut Butter\n",
        }
    )

    assert response["schema"] == {
        "supportedColumnsCount": SUPPORTED_COLUMNS_COUNT,
        "recognizedColumnsCount": 1,
        "ignoredColumnsCount": 1,
    }


def test_parse_csv_request_reports_counts_for_mixed_supported_and_extra_columns() -> (
    None
):
    response = parse_csv_request(
        {
            "fileName": "products.csv",
            "csvText": (
                "code,product_name,quantity,categories_tags,item_title\n"
                "0001,Peanut Butter,350 g,en:spreads,item title\n"
            ),
        }
    )

    assert response["schema"] == {
        "supportedColumnsCount": SUPPORTED_COLUMNS_COUNT,
        "recognizedColumnsCount": 4,
        "ignoredColumnsCount": 1,
    }


def test_table_from_payload_rejects_non_string_cells() -> None:
    with pytest.raises(ValueError, match="Rows must contain only strings"):
        table_from_payload(
            {
                "headers": ["code"],
                "rows": [[123]],
            }
        )


def test_prepare_upload_candidates_request_keeps_only_ok_rows() -> None:
    response = prepare_upload_candidates_request(
        {
            "table": {
                "headers": [
                    "dq_status",
                    "dq_severity",
                    "dq_findings_count",
                    "dq_findings",
                    "dq_last_checked_at",
                    "code",
                ],
                "rows": [
                    ["issue", "warning", "1", "missing-name", "now", "0001"],
                    ["ok", "", "0", "", "now", "0002"],
                ],
            }
        }
    )

    assert response == {
        "table": {
            "headers": ["code"],
            "rows": [["0002"]],
        }
    }
