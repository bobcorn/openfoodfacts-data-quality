from __future__ import annotations

import argparse
import csv
from pathlib import Path

import duckdb

from _bootstrap import ROOT, bootstrap_paths

bootstrap_paths()

from app.source.duckdb_products import (
    source_snapshot_id_for,
    write_source_snapshot_manifest,
)

from openfoodfacts_data_quality.raw_products import RAW_INPUT_COLUMNS

DEFAULT_SOURCE = ROOT / "data" / "en.openfoodfacts.org.products.csv"
DEFAULT_OUTPUT_DIR = ROOT / "examples" / "data"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create synchronized CSV, Parquet, and DuckDB demo sources for library examples."
    )
    parser.add_argument(
        "--source-csv",
        type=Path,
        default=DEFAULT_SOURCE,
        help="Path to the full OFF public CSV export (tab-separated).",
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
        default=1000,
        help="Number of products to keep in the demo sources.",
    )
    args = parser.parse_args()

    source_csv = args.source_csv.resolve()
    output_dir = args.output_dir.resolve()
    if not source_csv.exists():
        raise FileNotFoundError(f"Source CSV not found: {source_csv}")
    if args.sample_size <= 0:
        raise ValueError("--sample-size must be a positive integer.")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / "products.csv"
    output_parquet = output_dir / "products.parquet"
    output_duckdb = output_dir / "products.duckdb"

    row_count = write_sample_csv(source_csv, output_csv, sample_size=args.sample_size)
    write_sample_parquet(output_csv, output_parquet)
    write_sample_duckdb(output_csv, output_duckdb)
    write_source_snapshot_manifest(
        output_duckdb,
        source_snapshot_id=source_snapshot_id_for(output_duckdb),
    )

    print(f"Wrote {row_count} products to {output_csv}")
    print(f"Wrote {row_count} products to {output_parquet}")
    print(f"Wrote {row_count} products to {output_duckdb}")
    return 0


def write_sample_csv(source_csv: Path, output_csv: Path, *, sample_size: int) -> int:
    """Write one reduced comma separated CSV with only the columns used by the library demo."""
    with source_csv.open("r", encoding="utf-8", newline="") as source_handle:
        reader = csv.DictReader(source_handle, delimiter="\t")
        with output_csv.open("w", encoding="utf-8", newline="") as output_handle:
            writer = csv.DictWriter(output_handle, fieldnames=RAW_INPUT_COLUMNS)
            writer.writeheader()

            written = 0
            seen_codes: set[str] = set()
            for row in reader:
                code = (row.get("code") or "").strip()
                if not code or code in seen_codes:
                    continue
                writer.writerow(
                    {column: row.get(column, "") for column in RAW_INPUT_COLUMNS}
                )
                seen_codes.add(code)
                written += 1
                if written >= sample_size:
                    return written

    raise ValueError(
        f"Source CSV {source_csv} did not contain {sample_size} unique products with a not empty code."
    )


def write_sample_parquet(source_csv: Path, output_parquet: Path) -> None:
    """Materialize the demo rows as Parquet."""
    connection = duckdb.connect()
    try:
        connection.execute(
            "copy (select * from read_csv_auto("
            f"'{_sql_literal(source_csv)}', header=true, sample_size=-1, all_varchar=true"
            f")) to '{_sql_literal(output_parquet)}' (format parquet)"
        )
    finally:
        connection.close()


def write_sample_duckdb(source_csv: Path, output_duckdb: Path) -> None:
    """Materialize the demo rows as a tiny DuckDB database with one products table."""
    if output_duckdb.exists():
        output_duckdb.unlink()

    connection = duckdb.connect(str(output_duckdb))
    try:
        connection.execute(
            "create table products as "
            "select * from read_csv_auto("
            f"'{_sql_literal(source_csv)}', header=true, sample_size=-1, all_varchar=true)"
        )
        connection.execute("checkpoint")
    finally:
        connection.close()


def _sql_literal(path: Path) -> str:
    """Escape one filesystem path for direct embedding in a simple DuckDB SQL string."""
    return str(path).replace("'", "''")


if __name__ == "__main__":
    raise SystemExit(main())
