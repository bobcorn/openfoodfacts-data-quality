from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from _bootstrap import ROOT, bootstrap_paths

bootstrap_paths()

from off_data_quality.checks import (
    OFF_PRODUCT_EXPORT_COLUMNS,
)
from refresh_sample_jsonl import write_canonical_sample_jsonl

DEFAULT_SOURCE_JSONL = ROOT / "data" / "products.jsonl"
DEFAULT_SOURCE_DUCKDB = ROOT / "data" / "products.duckdb"
DEFAULT_OUTPUT_DIR = ROOT / "examples" / "data"
DEFAULT_SAMPLE_SIZE = 1000


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create synchronized CSV, JSONL, Parquet, and DuckDB demo sources."
    )
    parser.add_argument(
        "--source-jsonl",
        type=Path,
        default=DEFAULT_SOURCE_JSONL,
        help="Path to the canonical Open Food Facts JSONL source snapshot.",
    )
    parser.add_argument(
        "--source-duckdb",
        type=Path,
        default=DEFAULT_SOURCE_DUCKDB,
        help="Path to the full DuckDB source used to derive the shipped export formats.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory that will receive products.csv, products.jsonl, products.parquet, and products.duckdb.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help="Number of products to keep in the demo sources.",
    )
    args = parser.parse_args()

    source_jsonl = args.source_jsonl.resolve()
    source_duckdb = args.source_duckdb.resolve()
    output_dir = args.output_dir.resolve()
    if not source_jsonl.exists():
        raise FileNotFoundError(f"Source JSONL not found: {source_jsonl}")
    if not source_duckdb.exists():
        raise FileNotFoundError(f"Source DuckDB not found: {source_duckdb}")
    if args.sample_size <= 0:
        raise ValueError("--sample-size must be a positive integer.")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / "products.csv"
    output_jsonl = output_dir / "products.jsonl"
    output_parquet = output_dir / "products.parquet"
    output_duckdb = output_dir / "products.duckdb"

    row_count = materialize_example_sample(
        source_jsonl=source_jsonl,
        source_duckdb=source_duckdb,
        output_csv=output_csv,
        output_jsonl=output_jsonl,
        output_parquet=output_parquet,
        output_duckdb=output_duckdb,
        sample_size=args.sample_size,
    )
    print(f"Wrote {row_count} products to {output_csv}")
    print(f"Wrote {row_count} products to {output_jsonl}")
    print(f"Wrote {row_count} products to {output_parquet}")
    print(f"Wrote {row_count} products to {output_duckdb}")
    return 0


def materialize_example_sample(
    *,
    source_jsonl: Path,
    source_duckdb: Path,
    output_csv: Path,
    output_jsonl: Path,
    output_parquet: Path,
    output_duckdb: Path,
    sample_size: int,
) -> int:
    """Create one shipped example sample across the supported example formats."""
    selected_codes = write_canonical_sample_jsonl(
        source_jsonl=source_jsonl,
        output_jsonl=output_jsonl,
        source_duckdb=source_duckdb,
        sample_size=sample_size,
    )
    parquet_row_count = _write_sample_parquet(
        source_duckdb=source_duckdb,
        output_parquet=output_parquet,
        selected_codes=selected_codes,
    )
    csv_row_count = _write_sample_csv(
        source_duckdb=source_duckdb,
        output_csv=output_csv,
        selected_codes=selected_codes,
    )
    if csv_row_count != sample_size:
        raise ValueError(
            f"Source DuckDB {source_duckdb} did not materialize the expected {sample_size} sampled CSV products."
        )
    if parquet_row_count != sample_size:
        raise ValueError(
            f"Source DuckDB {source_duckdb} did not materialize the expected {sample_size} sampled Parquet products."
        )

    _write_sample_duckdb(output_parquet, output_duckdb)
    return sample_size


def _write_sample_parquet(
    *,
    source_duckdb: Path,
    output_parquet: Path,
    selected_codes: tuple[str, ...],
) -> int:
    """Write the sampled export columns to Parquet while preserving nested types."""
    if output_parquet.exists():
        output_parquet.unlink()

    connection = duckdb.connect()
    try:
        _attach_source_duckdb(connection, source_duckdb)
        _create_selected_codes_table(connection, selected_codes)
        select_list = ", ".join(
            f"sample_rows.{_quote_identifier(column)}"
            for column in OFF_PRODUCT_EXPORT_COLUMNS
        )
        placeholders = ", ".join("?" for _ in selected_codes)
        connection.execute(
            f"""
            copy (
                select {select_list}
                from selected_codes
                join (
                    select
                        {", ".join(_quote_identifier(column) for column in OFF_PRODUCT_EXPORT_COLUMNS)},
                        row_number() over (partition by code order by code) as code_rank
                    from source_snapshot.products
                    where code in ({placeholders})
                ) sample_rows using (code)
                where code_rank = 1
                order by sample_index
            ) to '{_sql_literal(output_parquet)}' (format parquet)
            """,
            selected_codes,
        )
        result = connection.execute(
            f"select count(*) from read_parquet('{_sql_literal(output_parquet)}')"
        ).fetchone()
        if result is None:
            raise RuntimeError(
                f"DuckDB did not return a row count for {output_parquet}."
            )
        return int(result[0])
    finally:
        connection.close()


def _write_sample_csv(
    *,
    source_duckdb: Path,
    output_csv: Path,
    selected_codes: tuple[str, ...],
) -> int:
    """Write the sampled export rows in the tab-separated CSV schema."""
    if output_csv.exists():
        output_csv.unlink()

    connection = duckdb.connect()
    try:
        _attach_source_duckdb(connection, source_duckdb)
        _create_selected_codes_table(connection, list(selected_codes))
        placeholders = ", ".join("?" for _ in selected_codes)
        connection.execute(
            f"""
            copy (
                select sample_rows.*
                from selected_codes
                join (
                    select
                        *,
                        row_number() over (partition by code order by code) as code_rank
                    from source_snapshot.products
                    where code in ({placeholders})
                ) sample_rows using (code)
                where code_rank = 1
                order by sample_index
            ) to '{_sql_literal(output_csv)}' (format csv, delimiter '\t', header)
            """,
            selected_codes,
        )
        result = connection.execute(
            f"select count(*) from read_csv('{_sql_literal(output_csv)}', delim='\\t', header=true)"
        ).fetchone()
        if result is None:
            raise RuntimeError(f"DuckDB did not return a row count for {output_csv}.")
        return int(result[0])
    finally:
        connection.close()


def _write_sample_duckdb(output_parquet: Path, output_duckdb: Path) -> None:
    """Write the sampled public Parquet rows to one tiny DuckDB database."""
    if output_duckdb.exists():
        output_duckdb.unlink()

    connection = duckdb.connect(str(output_duckdb))
    try:
        connection.execute(
            "create table products as "
            f"select * from read_parquet('{_sql_literal(output_parquet)}')"
        )
        connection.execute("checkpoint")
    finally:
        connection.close()


def _create_selected_codes_table(
    connection: duckdb.DuckDBPyConnection,
    selected_codes: tuple[str, ...] | list[str],
) -> None:
    """Persist the sampled code order so each output format keeps the same products."""
    connection.execute(
        "create temp table selected_codes (sample_index INTEGER, code VARCHAR)"
    )
    connection.executemany(
        "insert into selected_codes values (?, ?)",
        list(enumerate(selected_codes, start=1)),
    )


def _attach_source_duckdb(
    connection: duckdb.DuckDBPyConnection,
    source_duckdb: Path,
) -> None:
    connection.execute(
        f"attach '{_sql_literal(source_duckdb)}' as source_snapshot (read_only)"
    )


def _quote_identifier(value: str) -> str:
    """Quote one DuckDB identifier that may contain dashes."""
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def _sql_literal(path: Path) -> str:
    """Escape one filesystem path for direct embedding in a simple DuckDB SQL string."""
    return str(path).replace("'", "''")


if __name__ == "__main__":
    raise SystemExit(main())
