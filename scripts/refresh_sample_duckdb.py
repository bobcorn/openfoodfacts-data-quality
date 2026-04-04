from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

import duckdb

from _bootstrap import ROOT, bootstrap_paths

bootstrap_paths()

from app.source.duckdb_products import (
    source_snapshot_id_for,
    write_source_snapshot_manifest,
)

from openfoodfacts_data_quality.source_rows import PUBLIC_SOURCE_SNAPSHOT_COLUMNS


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refresh one small application source DuckDB sample from the full Open Food Facts parquet snapshot."
    )
    parser.add_argument(
        "--source-parquet",
        type=Path,
        required=True,
        help="Path to the full Open Food Facts parquet snapshot.",
    )
    parser.add_argument(
        "--output-db",
        type=Path,
        default=ROOT / "data" / "products.duckdb",
        help="Destination DuckDB file for the application source sample.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="Number of products to keep in the source sample.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed used for a stable pseudorandom product selection.",
    )
    args = parser.parse_args()

    source_parquet = args.source_parquet.resolve()
    output_db = args.output_db.resolve()

    if not source_parquet.exists():
        raise FileNotFoundError(f"Source parquet not found: {source_parquet}")
    if args.sample_size <= 0:
        raise ValueError("--sample-size must be a positive integer.")

    output_db.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_db = Path(temp_dir) / output_db.name
        row_count = write_sample_db(
            source_parquet=source_parquet,
            output_db=temp_db,
            sample_size=args.sample_size,
            seed=args.seed,
        )
        temp_db.replace(output_db)
    write_source_snapshot_manifest(
        output_db,
        source_snapshot_id=source_snapshot_id_for(output_db),
    )

    print(
        f"Refreshed {output_db} with {row_count} products from {source_parquet} "
        f"(seed={args.seed})."
    )
    return 0


def write_sample_db(
    *,
    source_parquet: Path,
    output_db: Path,
    sample_size: int,
    seed: int,
) -> int:
    """Write one typed source snapshot sample that matches the public DuckDB shape."""
    select_list = ",\n                ".join(
        quote_identifier(column_name) for column_name in PUBLIC_SOURCE_SNAPSHOT_COLUMNS
    )
    connection = duckdb.connect(str(output_db))
    try:
        connection.execute(
            f"""
            create table products as
            with deduped_products as (
              select
                {select_list},
                row_number() over (
                  partition by code
                  order by coalesce(created_t, 0) desc, hash(code || '::{seed}')
                ) as code_rank
              from read_parquet('{sql_literal(source_parquet)}')
              where code is not null
                and trim(code) != ''
            )
            select
              {select_list}
            from deduped_products
            where code_rank = 1
            order by hash(code || '::{seed}')
            limit {sample_size}
            """
        )
        connection.execute("checkpoint")
        result = connection.execute("select count(*) from products").fetchone()
        if result is None:
            raise RuntimeError("DuckDB did not return the sample row count.")
        return int(result[0])
    finally:
        connection.close()


def quote_identifier(value: str) -> str:
    """Quote a DuckDB identifier that may contain punctuation."""
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def sql_literal(path: Path) -> str:
    """Escape one filesystem path for direct embedding in DuckDB SQL."""
    return str(path).replace("'", "''")


if __name__ == "__main__":
    raise SystemExit(main())
