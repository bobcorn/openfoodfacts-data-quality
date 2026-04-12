from __future__ import annotations

import argparse
import csv
import json
import shutil
from pathlib import Path

import duckdb

from _bootstrap import ROOT, bootstrap_paths

bootstrap_paths()

DEFAULT_SAMPLE_JSONL = ROOT / "examples" / "data" / "products.jsonl"
DEFAULT_OUTPUT_DIR = ROOT / "examples" / "data"
DEFAULT_SAMPLE_SIZE = 1000


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Create synchronized CSV, JSONL, Parquet, and DuckDB demo sources "
            "for one existing 1000-product JSONL sample."
        )
    )
    parser.add_argument(
        "--sample-jsonl",
        type=Path,
        default=DEFAULT_SAMPLE_JSONL,
        help=(
            "Path to the already-selected 1000-product JSONL sample that "
            "defines the shared product codes across all example formats."
        ),
    )
    parser.add_argument(
        "--source-csv",
        type=Path,
        required=True,
        help="Path to the official OFF TSV/CSV export used to derive the shipped CSV sample.",
    )
    parser.add_argument(
        "--source-parquet",
        type=Path,
        required=True,
        help="Path to the official OFF Parquet export used to derive the shipped Parquet sample.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=(
            "Directory that will receive products.csv, products.jsonl, "
            "products.parquet, products-jsonl.duckdb, and products-parquet.duckdb."
        ),
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help="Number of products to keep in the demo sources.",
    )
    args = parser.parse_args()

    sample_jsonl = args.sample_jsonl.resolve()
    source_csv = args.source_csv.resolve()
    source_parquet = args.source_parquet.resolve()
    output_dir = args.output_dir.resolve()
    if not sample_jsonl.exists():
        raise FileNotFoundError(f"Sample JSONL not found: {sample_jsonl}")
    if not source_csv.exists():
        raise FileNotFoundError(f"Source CSV not found: {source_csv}")
    if not source_parquet.exists():
        raise FileNotFoundError(f"Source Parquet not found: {source_parquet}")
    if args.sample_size <= 0:
        raise ValueError("--sample-size must be a positive integer.")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / "products.csv"
    output_jsonl = output_dir / "products.jsonl"
    output_parquet = output_dir / "products.parquet"
    output_jsonl_duckdb = output_dir / "products-jsonl.duckdb"
    output_parquet_duckdb = output_dir / "products-parquet.duckdb"

    row_count = materialize_example_sample(
        sample_jsonl=sample_jsonl,
        source_csv=source_csv,
        source_parquet=source_parquet,
        output_csv=output_csv,
        output_jsonl=output_jsonl,
        output_parquet=output_parquet,
        output_jsonl_duckdb=output_jsonl_duckdb,
        output_parquet_duckdb=output_parquet_duckdb,
        sample_size=args.sample_size,
    )
    print(f"Wrote {row_count} products to {output_csv}")
    print(f"Wrote {row_count} products to {output_jsonl}")
    print(f"Wrote {row_count} products to {output_parquet}")
    print(f"Wrote {row_count} products to {output_jsonl_duckdb}")
    print(f"Wrote {row_count} products to {output_parquet_duckdb}")
    return 0


def materialize_example_sample(
    *,
    sample_jsonl: Path,
    source_csv: Path,
    source_parquet: Path,
    output_csv: Path,
    output_jsonl: Path,
    output_parquet: Path,
    output_jsonl_duckdb: Path,
    output_parquet_duckdb: Path,
    sample_size: int,
) -> int:
    """Create one shipped example sample across the supported example formats."""
    selected_codes = _read_selected_codes(sample_jsonl)
    if len(selected_codes) != sample_size:
        raise ValueError(
            f"Sample JSONL {sample_jsonl} contains {len(selected_codes)} products, "
            f"expected {sample_size}."
        )
    if sample_jsonl != output_jsonl:
        shutil.copyfile(sample_jsonl, output_jsonl)
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
    if csv_row_count != sample_size:
        raise ValueError(
            f"Source CSV {source_csv} did not materialize the expected {sample_size} sampled products."
        )
    if parquet_row_count != sample_size:
        raise ValueError(
            f"Source Parquet {source_parquet} did not materialize the expected {sample_size} sampled products."
        )

    _write_sample_duckdb_from_parquet(output_parquet, output_parquet_duckdb)
    _write_sample_duckdb_from_jsonl(output_jsonl, output_jsonl_duckdb)
    return sample_size


def _write_sample_parquet(
    *,
    source_parquet: Path,
    output_parquet: Path,
    selected_codes: tuple[str, ...],
) -> int:
    """Write the sampled official Parquet rows while preserving nested types."""
    if output_parquet.exists():
        output_parquet.unlink()

    connection = duckdb.connect()
    try:
        _create_selected_codes_table(connection, selected_codes)
        placeholders = ", ".join("?" for _ in selected_codes)
        connection.execute(
            f"""
            copy (
                select sample_rows.* exclude (code_rank)
                from selected_codes
                join (
                    select
                        *,
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
    selected_codes: tuple[str, ...],
) -> int:
    """Write one sampled official OFF TSV export without reserializing row text."""
    if output_csv.exists():
        output_csv.unlink()

    selected_code_order = {code: index for index, code in enumerate(selected_codes)}
    matched_rows: dict[str, str] = {}

    with source_csv.open(encoding="utf-8", newline="") as input_file:
        header_line = input_file.readline()
        if not header_line:
            raise ValueError(f"CSV export {source_csv} is empty.")
        header = next(csv.reader([header_line], delimiter="\t"))
        try:
            code_index = header.index("code")
        except ValueError as exc:
            raise ValueError(
                f"CSV export {source_csv} is missing a 'code' column."
            ) from exc

        for raw_line in input_file:
            row = next(csv.reader([raw_line], delimiter="\t"))
            code = row[code_index]
            if code not in selected_code_order or code in matched_rows:
                continue
            matched_rows[code] = raw_line
            if len(matched_rows) == len(selected_codes):
                break

    with output_csv.open("w", encoding="utf-8", newline="") as output_file:
        output_file.write(header_line)
        for code in selected_codes:
            selected_line = matched_rows.get(code)
            if selected_line is None:
                continue
            output_file.write(selected_line)

    return len(matched_rows)


def _write_sample_duckdb_from_parquet(
    output_parquet: Path, output_duckdb: Path
) -> None:
    """Write one tiny DuckDB database imported from the sampled Parquet rows."""
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


def _write_sample_duckdb_from_jsonl(output_jsonl: Path, output_duckdb: Path) -> None:
    """Write one tiny DuckDB database imported from the sampled JSONL documents."""
    if output_duckdb.exists():
        output_duckdb.unlink()

    connection = duckdb.connect(str(output_duckdb))
    try:
        connection.execute(
            "create table products as "
            "select * from read_json_auto("
            f"'{_sql_literal(output_jsonl)}', "
            "format='newline_delimited'"
            ")"
        )
        connection.execute("checkpoint")
    finally:
        connection.close()


def _read_selected_codes(sample_jsonl: Path) -> tuple[str, ...]:
    selected_codes: list[str] = []
    with sample_jsonl.open(encoding="utf-8") as input_file:
        for line in input_file:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            code = str(row.get("code", "")).strip()
            if not code:
                raise ValueError(
                    f"Sample JSONL {sample_jsonl} contains a row without a code."
                )
            selected_codes.append(code)
    return tuple(selected_codes)


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


def _sql_literal(path: Path) -> str:
    """Escape one filesystem path for direct embedding in a simple DuckDB SQL string."""
    return str(path).replace("'", "''")


if __name__ == "__main__":
    raise SystemExit(main())
