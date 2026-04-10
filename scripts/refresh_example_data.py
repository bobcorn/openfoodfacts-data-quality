from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import duckdb

from _bootstrap import ROOT, bootstrap_paths

bootstrap_paths()

from off_data_quality.checks import (
    OFF_PRODUCT_EXPORT_COLUMNS,
)

DEFAULT_SOURCE_PARQUET = ROOT / "data" / "food.parquet"
DEFAULT_SOURCE_CSV = ROOT / "data" / "en.openfoodfacts.org.products.csv"
DEFAULT_OUTPUT_DIR = ROOT / "examples" / "data"
DEFAULT_SAMPLE_SIZE = 1000
DEFAULT_SEED = 42


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create synchronized CSV, Parquet, and DuckDB demo sources from the public Open Food Facts exports."
    )
    parser.add_argument(
        "--source-parquet",
        type=Path,
        default=DEFAULT_SOURCE_PARQUET,
        help="Path to the full Open Food Facts public Parquet snapshot.",
    )
    parser.add_argument(
        "--source-csv",
        type=Path,
        default=DEFAULT_SOURCE_CSV,
        help="Path to the full Open Food Facts public CSV export (tab-separated).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory that will receive products.csv, products.parquet, and products.duckdb.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help="Number of products to keep in the demo sources.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="Seed used for the deterministic code-hash sample ordering.",
    )
    args = parser.parse_args()

    source_parquet = args.source_parquet.resolve()
    source_csv = args.source_csv.resolve()
    output_dir = args.output_dir.resolve()
    if not source_parquet.exists():
        raise FileNotFoundError(f"Source Parquet not found: {source_parquet}")
    if not source_csv.exists():
        raise FileNotFoundError(f"Source CSV not found: {source_csv}")
    if args.sample_size <= 0:
        raise ValueError("--sample-size must be a positive integer.")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / "products.csv"
    output_parquet = output_dir / "products.parquet"
    output_duckdb = output_dir / "products.duckdb"

    row_count = materialize_example_sample(
        source_parquet=source_parquet,
        source_csv=source_csv,
        output_csv=output_csv,
        output_parquet=output_parquet,
        output_duckdb=output_duckdb,
        sample_size=args.sample_size,
        seed=args.seed,
    )
    print(f"Wrote {row_count} products to {output_csv}")
    print(f"Wrote {row_count} products to {output_parquet}")
    print(f"Wrote {row_count} products to {output_duckdb}")
    return 0


def materialize_example_sample(
    *,
    source_parquet: Path,
    source_csv: Path,
    output_csv: Path,
    output_parquet: Path,
    output_duckdb: Path,
    sample_size: int,
    seed: int,
) -> int:
    """Create one deterministic sample across the public CSV, Parquet, and DuckDB examples."""
    selected_codes = _select_sample_codes(
        source_parquet=source_parquet,
        sample_size=sample_size,
        seed=seed,
    )
    if len(selected_codes) != sample_size:
        raise ValueError(
            f"Source Parquet {source_parquet} did not contain {sample_size} unique products with a non-empty code."
        )

    parquet_row_count = _write_sample_parquet(
        source_parquet=source_parquet,
        output_parquet=output_parquet,
        selected_codes=selected_codes,
    )
    csv_row_count = _write_sample_csv(
        source_csv=source_csv,
        output_csv=output_csv,
        selected_codes=selected_codes,
    )
    if parquet_row_count != sample_size:
        raise ValueError(
            f"Source Parquet {source_parquet} did not materialize the expected {sample_size} sampled products."
        )
    if csv_row_count != sample_size:
        raise ValueError(
            f"Source CSV {source_csv} did not materialize the expected {sample_size} sampled products."
        )

    _write_sample_duckdb(output_parquet, output_duckdb)
    return sample_size


def _select_sample_codes(
    *,
    source_parquet: Path,
    sample_size: int,
    seed: int,
) -> list[str]:
    """Return one deterministic sample of unique product codes from the public Parquet snapshot."""
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            f"""
            select code
            from (
                select
                    code,
                    row_number() over (partition by code order by code) as code_rank
                from read_parquet('{_sql_literal(source_parquet)}')
                where code is not null
                  and trim(code) != ''
            ) source_rows
            where code_rank = 1
            order by hash(code || ?), code
            limit ?
            """,
            [f"::{seed}", sample_size],
        ).fetchall()
    finally:
        connection.close()
    return [str(code) for (code,) in rows]


def _write_sample_parquet(
    *,
    source_parquet: Path,
    output_parquet: Path,
    selected_codes: list[str],
) -> int:
    """Write the sampled public snapshot rows to Parquet while preserving nested types."""
    if output_parquet.exists():
        output_parquet.unlink()

    connection = duckdb.connect()
    try:
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
                    from read_parquet('{_sql_literal(source_parquet)}')
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
    source_csv: Path,
    output_csv: Path,
    selected_codes: list[str],
) -> int:
    """Write the sampled public CSV export rows in the original tab-separated schema."""
    _allow_large_csv_fields()
    selected_code_set = set(selected_codes)
    rows_by_code: dict[str, dict[str, str]] = {}

    with source_csv.open("r", encoding="utf-8", newline="") as source_handle:
        reader = csv.DictReader(source_handle, delimiter="\t")
        fieldnames = tuple(reader.fieldnames or ())
        for row in reader:
            code = (row.get("code") or "").strip()
            if code not in selected_code_set or code in rows_by_code:
                continue
            rows_by_code[code] = row
            if len(rows_by_code) == len(selected_codes):
                break

    missing_codes = [code for code in selected_codes if code not in rows_by_code]
    if missing_codes:
        raise ValueError(
            f"Source CSV {source_csv} is missing {len(missing_codes)} sampled product codes."
        )

    with output_csv.open("w", encoding="utf-8", newline="") as output_handle:
        writer = csv.DictWriter(
            output_handle,
            fieldnames=fieldnames,
            delimiter="\t",
        )
        writer.writeheader()
        for code in selected_codes:
            writer.writerow(rows_by_code[code])

    return len(rows_by_code)


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
    selected_codes: list[str],
) -> None:
    """Persist the sampled code order so each output format keeps the same products."""
    connection.execute(
        "create temp table selected_codes (sample_index INTEGER, code VARCHAR)"
    )
    connection.executemany(
        "insert into selected_codes values (?, ?)",
        list(enumerate(selected_codes, start=1)),
    )


def _quote_identifier(value: str) -> str:
    """Quote one DuckDB identifier that may contain dashes."""
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def _sql_literal(path: Path) -> str:
    """Escape one filesystem path for direct embedding in a simple DuckDB SQL string."""
    return str(path).replace("'", "''")


def _allow_large_csv_fields() -> None:
    """Raise the csv module field-size limit high enough for public exports."""
    field_size_limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(field_size_limit)
            return
        except OverflowError:
            field_size_limit //= 10


if __name__ == "__main__":
    raise SystemExit(main())
