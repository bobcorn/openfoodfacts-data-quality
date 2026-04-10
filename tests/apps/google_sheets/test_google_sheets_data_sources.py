from __future__ import annotations

import pytest
from apps.google_sheets.data_sources import load_csv_table


def test_load_csv_table_detects_tab_delimiter() -> None:
    table = load_csv_table(
        b"code\tproduct_name\tquantity\n0001\tPeanut Butter\t350 g\n"
    )

    assert table.headers == ("code", "product_name", "quantity")
    assert table.rows == (("0001", "Peanut Butter", "350 g"),)


def test_load_csv_table_rejects_empty_payload() -> None:
    with pytest.raises(ValueError, match="empty"):
        load_csv_table(b"")
