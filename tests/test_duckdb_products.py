from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from app.source.duckdb_products import count_source_rows, iter_source_batches

from openfoodfacts_data_quality.contracts.raw import validate_raw_product_row
from openfoodfacts_data_quality.raw_products import RAW_INPUT_COLUMNS


def test_iter_source_batches_reads_one_public_source_snapshot_contract(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "products.duckdb"
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute(
            """
            create table products (
                code VARCHAR,
                created_t BIGINT,
                product_name STRUCT(lang VARCHAR, "text" VARCHAR)[],
                quantity VARCHAR,
                product_quantity VARCHAR,
                serving_size VARCHAR,
                serving_quantity VARCHAR,
                brands VARCHAR,
                categories VARCHAR,
                labels VARCHAR,
                emb_codes VARCHAR,
                ingredients_text STRUCT(lang VARCHAR, "text" VARCHAR)[],
                ingredients_tags VARCHAR[],
                nutriscore_grade VARCHAR,
                nutriscore_score INTEGER,
                categories_tags VARCHAR[],
                labels_tags VARCHAR[],
                countries_tags VARCHAR[],
                no_nutrition_data BOOLEAN,
                nutriments STRUCT("name" VARCHAR, "value" FLOAT, "100g" FLOAT, serving FLOAT, unit VARCHAR, prepared_value FLOAT, prepared_100g FLOAT, prepared_serving FLOAT, prepared_unit VARCHAR)[]
            )
            """
        )
        row = [
            "123",
            123,
            [{"lang": "main", "text": "Example"}],
            "500 g",
            "500",
            "50 g",
            "50",
            "Brand",
            "Supplements",
            "No gluten",
            "FR 01.001",
            [{"lang": "main", "text": "Sugar, salt"}],
            ["en:sugar", "en:salt"],
            "a",
            -2,
            ["en:supplements"],
            ["en:vegan"],
            ["en:france"],
            False,
            [
                {
                    "name": "energy-kcal",
                    "value": None,
                    "100g": 123.0,
                    "serving": None,
                    "unit": "kcal",
                    "prepared_value": None,
                    "prepared_100g": None,
                    "prepared_serving": None,
                    "prepared_unit": None,
                },
                {
                    "name": "fat",
                    "value": None,
                    "100g": 3.5,
                    "serving": None,
                    "unit": "g",
                    "prepared_value": None,
                    "prepared_100g": None,
                    "prepared_serving": None,
                    "prepared_unit": None,
                },
            ],
        ]
        placeholders = ", ".join("?" for _ in row)
        connection.execute(f"insert into products values ({placeholders})", row)
        connection.execute("checkpoint")
    finally:
        connection.close()

    batch = next(iter_source_batches(db_path, batch_size=10))

    assert batch == [
        validate_raw_product_row(
            {
                "code": "123",
                "created_t": 123,
                "product_name": "Example",
                "quantity": "500 g",
                "product_quantity": "500",
                "serving_size": "50 g",
                "serving_quantity": "50",
                "brands": "Brand",
                "categories": "Supplements",
                "labels": "No gluten",
                "emb_codes": "FR 01.001",
                "ingredients_text": "Sugar, salt",
                "ingredients_tags": ["en:sugar", "en:salt"],
                "nutriscore_grade": "a",
                "nutriscore_score": -2,
                "categories_tags": ["en:supplements"],
                "labels_tags": ["en:vegan"],
                "countries_tags": ["en:france"],
                "no_nutrition_data": False,
                "energy-kcal_100g": 123.0,
                "fat_100g": 3.5,
            }
        )
    ]


def test_iter_source_batches_reads_one_public_csv_export_contract(
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


def test_source_duckdb_validation_rejects_unsupported_source_contract(
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

    with pytest.raises(
        ValueError,
        match="Missing public snapshot columns: created_t",
    ):
        count_source_rows(db_path)

    with pytest.raises(
        ValueError,
        match="Missing public CSV export columns: created_t",
    ):
        next(iter_source_batches(db_path, batch_size=10))


def _quote_identifier(value: str) -> str:
    """Quote a DuckDB identifier that may contain dashes."""
    escaped = value.replace('"', '""')
    return f'"{escaped}"'
