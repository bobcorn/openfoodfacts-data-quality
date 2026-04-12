from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
from migration.source.datasets import SourceSelection
from migration.source.models import SourceSnapshotFormat
from migration.source.product_documents import (
    count_source_products,
    iter_source_batches,
    resolve_source_snapshot_format,
    source_batch_record_from_document,
)


def test_duckdb_source_snapshot_yields_product_documents(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "products.duckdb"
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute(
            """
            create table products (
                code VARCHAR,
                product_name STRUCT(lang VARCHAR, "text" VARCHAR)[],
                nutriments STRUCT("name" VARCHAR, "100g" FLOAT)[]
            )
            """
        )
        connection.execute(
            "insert into products values (?, ?, ?)",
            [
                "123",
                [{"lang": "main", "text": "Example"}],
                [{"name": "energy-kcal", "100g": 123.0}],
            ],
        )
        connection.execute("checkpoint")
    finally:
        connection.close()

    batch = next(iter_source_batches(db_path, batch_size=10))

    record = batch[0]
    assert record.product_document.code == "123"
    assert record.product_document.document["product_name"] == [
        {"lang": "main", "text": "Example"}
    ]


def test_jsonl_source_snapshot_yields_product_documents(
    tmp_path: Path,
) -> None:
    jsonl_path = tmp_path / "products.jsonl"
    jsonl_path.write_text(
        "\n".join(
            [
                json.dumps({"code": "002", "product_name": "Second"}),
                json.dumps(
                    {
                        "code": "001",
                        "product_name": "First",
                        "nutriments": {"energy-kcal_100g": 456.0},
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )
    selection = SourceSelection(kind="code_list", codes=("001",))

    batches = list(iter_source_batches(jsonl_path, batch_size=10, selection=selection))

    assert count_source_products(jsonl_path, selection=selection) == 1
    assert len(batches) == 1
    record = batches[0][0]
    assert record.product_document.backend_input_payload()["code"] == "001"


def test_source_snapshot_format_is_inferred_from_jsonl_content_without_suffix(
    tmp_path: Path,
) -> None:
    path = tmp_path / "mounted-source"
    path.write_text(
        json.dumps({"code": "001", "product_name": "First"}), encoding="utf-8"
    )

    assert resolve_source_snapshot_format(path) == SourceSnapshotFormat.JSONL


def test_source_snapshot_format_is_inferred_from_duckdb_content_without_suffix(
    tmp_path: Path,
) -> None:
    path = tmp_path / "mounted-source"
    connection = duckdb.connect(str(path))
    try:
        connection.execute("create table products (code VARCHAR)")
        connection.execute("insert into products values ('001')")
        connection.execute("checkpoint")
    finally:
        connection.close()

    assert resolve_source_snapshot_format(path) == SourceSnapshotFormat.DUCKDB


def test_source_snapshot_format_fails_when_suffix_is_not_supported(
    tmp_path: Path,
) -> None:
    path = tmp_path / "products.csv"
    path.write_text("not a supported source snapshot", encoding="utf-8")

    with pytest.raises(ValueError, match="Cannot infer source snapshot format"):
        resolve_source_snapshot_format(path)


def test_source_batch_record_requires_product_document_code() -> None:
    with pytest.raises(ValueError, match="string 'code' field"):
        source_batch_record_from_document({"product_name": "Missing code"})


def test_source_batch_record_returns_only_the_validated_product_document() -> None:
    document = {
        "code": "123",
        "product_name": [{"lang": "main", "text": "Example"}],
        "nutriments": [{"name": "energy-kcal", "100g": 123.0}],
    }

    record = source_batch_record_from_document(document)
    assert record.product_document.code == "123"
    assert record.product_document.document == document


def test_jsonl_source_snapshot_count_validates_product_documents(
    tmp_path: Path,
) -> None:
    jsonl_path = tmp_path / "products.jsonl"
    jsonl_path.write_text(
        "\n".join(
            [
                json.dumps({"product_name": "Missing code"}),
                json.dumps({"code": "0001", "product_name": "Valid product"}),
            ]
        ),
        encoding="utf-8",
    )

    assert count_source_products(jsonl_path) == 1
    batches = list(iter_source_batches(jsonl_path, batch_size=10))
    assert [[row.product_document.code for row in batch] for batch in batches] == [
        ["0001"]
    ]


def test_duckdb_source_snapshot_skips_rows_without_nonblank_product_codes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "products.duckdb"
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute("create table products (code VARCHAR, product_name VARCHAR)")
        connection.execute("insert into products values (?, ?)", [None, "Missing code"])
        connection.execute(
            "insert into products values (?, ?)", ["0001", "Valid product"]
        )
        connection.execute("checkpoint")
    finally:
        connection.close()

    assert count_source_products(db_path) == 1
    batches = list(iter_source_batches(db_path, batch_size=10))
    assert [[row.product_document.code for row in batch] for batch in batches] == [
        ["0001"]
    ]


def test_jsonl_stable_sample_skips_rows_with_missing_codes(
    tmp_path: Path,
) -> None:
    jsonl_path = tmp_path / "products.jsonl"
    jsonl_path.write_text(
        "\n".join(
            [
                json.dumps({"code": "001", "product_name": "First"}),
                json.dumps({"product_name": "Missing code"}),
                json.dumps({"code": "002", "product_name": "Second"}),
            ]
        ),
        encoding="utf-8",
    )
    selection = SourceSelection(kind="stable_sample", sample_size=1, seed=42)

    assert count_source_products(jsonl_path, selection=selection) == 1
    batches = list(iter_source_batches(jsonl_path, batch_size=10, selection=selection))
    assert sum(len(batch) for batch in batches) == 1
