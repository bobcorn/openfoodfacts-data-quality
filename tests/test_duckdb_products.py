from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from app.source.duckdb_products import count_source_rows, iter_source_batches

from openfoodfacts_data_quality.contracts.raw import validate_raw_product_row
from openfoodfacts_data_quality.raw_products import RAW_INPUT_COLUMNS


def test_iter_source_batches_reads_only_the_explicit_source_contract(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "products.duckdb"
    connection = duckdb.connect(str(db_path))
    try:
        column_defs = ", ".join(
            f"{_quote_identifier(column)} VARCHAR"
            for column in [*RAW_INPUT_COLUMNS, "extra_field"]
        )
        connection.execute(f"create table products ({column_defs})")
        placeholders = ", ".join("?" for _ in [*RAW_INPUT_COLUMNS, "extra_field"])
        row: dict[str, str | None] = {
            column: None for column in [*RAW_INPUT_COLUMNS, "extra_field"]
        }
        row["code"] = "123"
        row["created_t"] = "123"
        row["product_name"] = "Example"
        row["extra_field"] = "should-not-be-selected"
        connection.execute(
            f"insert into products values ({placeholders})",
            [row[column] for column in [*RAW_INPUT_COLUMNS, "extra_field"]],
        )
        connection.execute("checkpoint")
    finally:
        connection.close()

    batch = next(iter_source_batches(db_path, batch_size=10))

    assert batch == [
        validate_raw_product_row(
            {
                "code": "123",
                "created_t": "123",
                "product_name": "Example",
                **{
                    column: None
                    for column in RAW_INPUT_COLUMNS
                    if column not in {"code", "created_t", "product_name"}
                },
            }
        )
    ]


def test_source_duckdb_validation_rejects_missing_contract_columns(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "products.duckdb"
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute(
            'create table products ("code" VARCHAR, "product_name" VARCHAR)'
        )
        connection.execute("insert into products values ('123', 'Example')")
        connection.execute("checkpoint")
    finally:
        connection.close()

    with pytest.raises(ValueError, match="Missing columns: created_t"):
        count_source_rows(db_path)

    with pytest.raises(ValueError, match="Missing columns: created_t"):
        next(iter_source_batches(db_path, batch_size=10))


def _quote_identifier(value: str) -> str:
    """Quote a DuckDB identifier that may contain dashes."""
    escaped = value.replace('"', '""')
    return f'"{escaped}"'
