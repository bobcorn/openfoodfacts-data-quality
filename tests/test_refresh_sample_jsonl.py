from __future__ import annotations

from pathlib import Path

import duckdb

from refresh_sample_jsonl import write_canonical_sample_jsonl


def test_write_canonical_sample_jsonl_skips_codes_missing_from_duckdb(
    tmp_path: Path,
) -> None:
    source_jsonl = tmp_path / "products.jsonl"
    source_jsonl.write_text(
        "\n".join(
            [
                '{"code":"001","name":"First"}',
                '{"code":"002","name":"Missing from DuckDB"}',
                '{"code":"003","name":"Third"}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    source_duckdb = tmp_path / "products.duckdb"
    connection = duckdb.connect(str(source_duckdb))
    try:
        connection.execute("create table products (code varchar)")
        connection.execute("insert into products values ('001'), ('003')")
        connection.execute("checkpoint")
    finally:
        connection.close()
    output_jsonl = tmp_path / "sample.jsonl"

    selected_codes = write_canonical_sample_jsonl(
        source_jsonl=source_jsonl,
        output_jsonl=output_jsonl,
        source_duckdb=source_duckdb,
        sample_size=2,
    )

    assert selected_codes == ("001", "003")
    assert output_jsonl.read_text(encoding="utf-8").splitlines() == [
        '{"code":"001","name":"First"}',
        '{"code":"003","name":"Third"}',
    ]
