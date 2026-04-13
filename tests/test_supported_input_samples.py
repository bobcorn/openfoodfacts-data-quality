from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import cast

import duckdb

from off_data_quality import checks

EXAMPLES_DATA_DIR = Path(__file__).resolve().parents[1] / "examples" / "data"
OFFICIAL_CSV_SAMPLE = EXAMPLES_DATA_DIR / "products.csv"
JSONL_SAMPLE = EXAMPLES_DATA_DIR / "products.jsonl"
PARQUET_SAMPLE = EXAMPLES_DATA_DIR / "products.parquet"
JSONL_DUCKDB_SAMPLE = EXAMPLES_DATA_DIR / "products-jsonl.duckdb"
PARQUET_DUCKDB_SAMPLE = EXAMPLES_DATA_DIR / "products-parquet.duckdb"
SAMPLE_SIZE = 1000
INVARIANT_CHECK_ID = "en:quantity-to-be-completed"
STRUCTURED_EQUIVALENCE_CHECK_ID = "en:serving-quantity-over-product-quantity"


def test_supported_example_samples_prepare_all_rows() -> None:
    prepared_counts = {
        "jsonl": len(checks.prepare(_load_jsonl_rows())),
        "csv": len(checks.prepare(_load_csv_rows())),
        "parquet": len(checks.prepare(_load_parquet_relation())),
        "duckdb_jsonl": _prepare_duckdb_rows(JSONL_DUCKDB_SAMPLE),
        "duckdb_parquet": _prepare_duckdb_rows(PARQUET_DUCKDB_SAMPLE),
    }

    assert prepared_counts == {
        "jsonl": SAMPLE_SIZE,
        "csv": SAMPLE_SIZE,
        "parquet": SAMPLE_SIZE,
        "duckdb_jsonl": SAMPLE_SIZE,
        "duckdb_parquet": SAMPLE_SIZE,
    }


def test_supported_example_samples_run_consistently_on_invariant_check() -> None:
    finding_counts = {
        "jsonl": len(checks.run(_load_jsonl_rows(), check_ids=[INVARIANT_CHECK_ID])),
        "csv": len(checks.run(_load_csv_rows(), check_ids=[INVARIANT_CHECK_ID])),
        "parquet": len(
            checks.run(_load_parquet_relation(), check_ids=[INVARIANT_CHECK_ID])
        ),
        "duckdb_jsonl": _run_duckdb_rows(
            JSONL_DUCKDB_SAMPLE,
            check_ids=[INVARIANT_CHECK_ID],
        ),
        "duckdb_parquet": _run_duckdb_rows(
            PARQUET_DUCKDB_SAMPLE,
            check_ids=[INVARIANT_CHECK_ID],
        ),
    }

    assert len(set(finding_counts.values())) == 1


def test_structured_example_samples_match_on_full_fidelity_check_surface() -> None:
    finding_counts = {
        "jsonl": len(
            checks.run(_load_jsonl_rows(), check_ids=[STRUCTURED_EQUIVALENCE_CHECK_ID])
        ),
        "parquet": len(
            checks.run(
                _load_parquet_relation(),
                check_ids=[STRUCTURED_EQUIVALENCE_CHECK_ID],
            )
        ),
        "duckdb_jsonl": _run_duckdb_rows(
            JSONL_DUCKDB_SAMPLE,
            check_ids=[STRUCTURED_EQUIVALENCE_CHECK_ID],
        ),
        "duckdb_parquet": _run_duckdb_rows(
            PARQUET_DUCKDB_SAMPLE,
            check_ids=[STRUCTURED_EQUIVALENCE_CHECK_ID],
        ),
    }

    assert len(set(finding_counts.values())) == 1


def _load_jsonl_rows() -> list[dict[str, object]]:
    with JSONL_SAMPLE.open(encoding="utf-8") as input_file:
        return [json.loads(line) for line in input_file]


def _load_csv_rows() -> list[dict[str, object]]:
    with OFFICIAL_CSV_SAMPLE.open(newline="", encoding="utf-8") as input_file:
        return cast(
            list[dict[str, object]],
            list(csv.DictReader(input_file, delimiter="\t")),
        )


def _load_parquet_relation() -> duckdb.DuckDBPyRelation:
    return duckdb.read_parquet(str(PARQUET_SAMPLE))


def _prepare_duckdb_rows(path: Path) -> int:
    with duckdb.connect(str(path), read_only=True) as connection:
        return len(checks.prepare(connection.sql("from products")))


def _run_duckdb_rows(path: Path, *, check_ids: list[str]) -> int:
    with duckdb.connect(str(path), read_only=True) as connection:
        return len(checks.run(connection.sql("from products"), check_ids=check_ids))
