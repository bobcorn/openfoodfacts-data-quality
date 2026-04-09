from __future__ import annotations

import pytest
from examples.google_sheets_demo.api import (
    build_public_demo_config,
    parse_csv_request,
    prepare_upload_candidates_request,
    table_from_payload,
)


def test_build_public_demo_config_reads_client_id_from_env() -> None:
    config = build_public_demo_config(
        {
            "GOOGLE_SHEETS_DEMO_GOOGLE_CLIENT_ID": "demo-client-id",
            "GOOGLE_SHEETS_DEMO_GOOGLE_API_KEY": "demo-api-key",
            "GOOGLE_SHEETS_DEMO_GOOGLE_CLOUD_PROJECT_NUMBER": "1234567890",
        }
    )

    assert config.to_payload() == {
        "googleClientId": "demo-client-id",
        "googleApiKey": "demo-api-key",
        "googleCloudProjectNumber": "1234567890",
        "dataSheetName": "Data",
        "readySheetName": "Ready for OFF upload",
    }


def test_parse_csv_request_returns_table_payload() -> None:
    response = parse_csv_request({"csvText": "code,product_name\n0001,Peanut Butter\n"})

    assert response == {
        "table": {
            "headers": ["code", "product_name"],
            "rows": [["0001", "Peanut Butter"]],
        }
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
