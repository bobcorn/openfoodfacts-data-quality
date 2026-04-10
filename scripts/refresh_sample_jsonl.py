from __future__ import annotations

import argparse
import re
import tempfile
from pathlib import Path

import duckdb

from _bootstrap import ROOT, bootstrap_paths

bootstrap_paths()

DEFAULT_SOURCE_JSONL = ROOT / "data" / "products.jsonl"
DEFAULT_SOURCE_DUCKDB = ROOT / "data" / "products.duckdb"
DEFAULT_OUTPUT_JSONL = ROOT / "examples" / "data" / "products.jsonl"
DEFAULT_SAMPLE_SIZE = 1000
JSONL_CODE_PATTERN = re.compile(r'"code"\s*:\s*"([^"]+)"')


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refresh the example JSONL source snapshot from the canonical full JSONL source."
    )
    parser.add_argument(
        "--source-jsonl",
        type=Path,
        default=DEFAULT_SOURCE_JSONL,
        help="Path to the full Open Food Facts JSONL source snapshot.",
    )
    parser.add_argument(
        "--output-jsonl",
        type=Path,
        default=DEFAULT_OUTPUT_JSONL,
        help="Destination JSONL sample used by the migration demo and local example data.",
    )
    parser.add_argument(
        "--source-duckdb",
        type=Path,
        default=DEFAULT_SOURCE_DUCKDB,
        help="Full DuckDB source used to keep the shipped example formats on the same product set.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help="Number of products to keep in the shipped JSONL sample.",
    )
    args = parser.parse_args()

    source_jsonl = args.source_jsonl.resolve()
    output_jsonl = args.output_jsonl.resolve()
    source_duckdb = args.source_duckdb.resolve()
    if not source_jsonl.exists():
        raise FileNotFoundError(f"Source JSONL not found: {source_jsonl}")
    if not source_duckdb.exists():
        raise FileNotFoundError(f"Source DuckDB not found: {source_duckdb}")
    if args.sample_size <= 0:
        raise ValueError("--sample-size must be a positive integer.")

    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_jsonl = Path(temp_dir) / output_jsonl.name
        selected_codes = write_canonical_sample_jsonl(
            source_jsonl=source_jsonl,
            output_jsonl=temp_jsonl,
            source_duckdb=source_duckdb,
            sample_size=args.sample_size,
        )
        temp_jsonl.replace(output_jsonl)
    print(
        f"Refreshed {output_jsonl} with {len(selected_codes)} products from "
        f"{source_jsonl} that also exist in {source_duckdb}."
    )
    return 0


def write_canonical_sample_jsonl(
    *,
    source_jsonl: Path,
    output_jsonl: Path,
    source_duckdb: Path,
    sample_size: int,
) -> tuple[str, ...]:
    """Write one shipped JSONL sample from the canonical source snapshot."""
    selected_codes: list[str] = []
    seen_codes: set[str] = set()
    connection = duckdb.connect(str(source_duckdb), read_only=True)
    try:
        with source_jsonl.open("r", encoding="utf-8") as source_handle:
            with output_jsonl.open("w", encoding="utf-8") as output_handle:
                for line in source_handle:
                    code = _code_from_jsonl_line(line)
                    if not code or code in seen_codes:
                        continue
                    seen_codes.add(code)
                    if not _source_duckdb_has_code(connection, code):
                        continue
                    output_handle.write(line.rstrip("\n"))
                    output_handle.write("\n")
                    selected_codes.append(code)
                    if len(selected_codes) == sample_size:
                        return tuple(selected_codes)
    finally:
        connection.close()

    raise ValueError(
        f"Sources {source_jsonl} and {source_duckdb} did not expose {sample_size} shared product codes."
    )


def _code_from_jsonl_line(line: str) -> str:
    match = JSONL_CODE_PATTERN.search(line)
    if not match:
        return ""
    return match.group(1).strip()


def _source_duckdb_has_code(
    connection: duckdb.DuckDBPyConnection,
    code: str,
) -> bool:
    row = connection.execute(
        """
        select 1
        from products
        where code = ?
        limit 1
        """,
        [code],
    ).fetchone()
    return row is not None


if __name__ == "__main__":
    raise SystemExit(main())
