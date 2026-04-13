# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     notebook_metadata_filter: kernelspec,jupytext
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Input formats
#
# Show the same `checks.run(...)` call with different loaded row sources.

# %%
import csv
import json
import os

import duckdb

from off_data_quality import checks

DATA_DIR = "examples/data" if os.path.isdir("examples/data") else "../data"
CSV_SAMPLE = f"{DATA_DIR}/products.csv"
JSONL_SAMPLE = f"{DATA_DIR}/products.jsonl"
PARQUET_SAMPLE = f"{DATA_DIR}/products.parquet"
JSONL_DUCKDB_SAMPLE = f"{DATA_DIR}/products-jsonl.duckdb"
PARQUET_DUCKDB_SAMPLE = f"{DATA_DIR}/products-parquet.duckdb"
CHECK_IDS = (
    "en:quantity-to-be-completed",
    "en:serving-quantity-over-product-quantity",
)

# %% [markdown]
# Plain Python row mappings work directly.

# %%
python_findings = checks.run(
    [
        {
            "code": "123",
            "product_name": "Example",
            "quantity": "",
            "product_quantity": "100",
            "serving_size": "150 g",
            "serving_quantity": "150",
        },
        {
            "code": "456",
            "product_name": "Example 2",
            "quantity": "500 g",
            "product_quantity": "100",
            "serving_size": "150 g",
            "serving_quantity": "150",
        },
    ],
    check_ids=CHECK_IDS,
)

# %% [markdown]
# Official OFF JSONL documents work directly once loaded.

# %%
with open(JSONL_SAMPLE, encoding="utf-8") as input_file:
    jsonl_findings = checks.run(
        [json.loads(line) for line in input_file],
        check_ids=CHECK_IDS,
    )

# %% [markdown]
# Official OFF CSV rows also work directly once loaded into memory.

# %%
with open(CSV_SAMPLE, newline="", encoding="utf-8") as input_file:
    csv_findings = checks.run(
        list(csv.DictReader(input_file, delimiter="\t")),
        check_ids=CHECK_IDS,
    )

# %% [markdown]
# Official OFF Parquet rows work directly when loaded as a DuckDB relation.

# %%
parquet_findings = checks.run(
    duckdb.read_parquet(str(PARQUET_SAMPLE)),
    check_ids=CHECK_IDS,
)

# %% [markdown]
# The same is true for DuckDB imports derived from the JSONL and Parquet samples.

# %%
with duckdb.connect(str(JSONL_DUCKDB_SAMPLE), read_only=True) as connection:
    jsonl_duckdb_findings = checks.run(
        connection.sql("from products"),
        check_ids=CHECK_IDS,
    )

with duckdb.connect(str(PARQUET_DUCKDB_SAMPLE), read_only=True) as connection:
    parquet_duckdb_findings = checks.run(
        connection.sql("from products"),
        check_ids=CHECK_IDS,
    )

# %% [markdown]
# Compare the result counts across the supported sample formats.
#
# JSONL, Parquet, and the two DuckDB imports keep the same full-fidelity
# source content, so they line up on the same finding count for this check set.
# The official CSV export is flatter, so some check surfaces can legitimately
# produce fewer findings even though the format is fully supported.

# %%
print("Finding counts by input format")
print(
    json.dumps(
        {
            "python": len(python_findings),
            "jsonl": len(jsonl_findings),
            "csv": len(csv_findings),
            "parquet": len(parquet_findings),
            "duckdb_jsonl": len(jsonl_duckdb_findings),
            "duckdb_parquet": len(parquet_duckdb_findings),
        },
        indent=2,
    )
)
print()

# %% [markdown]
# If your columns use different names, pass a mapping.

# %%
remapped_findings = checks.run(
    [
        {
            "barcode": "123",
            "name": "Example",
            "qty": "500 g",
            "product_qty": "100",
            "serving_label": "150 g",
            "serving_qty": "150",
        }
    ],
    columns={
        "code": "barcode",
        "product_name": "name",
        "quantity": "qty",
        "product_quantity": "product_qty",
        "serving_size": "serving_label",
        "serving_quantity": "serving_qty",
    },
    check_ids=["en:serving-quantity-over-product-quantity"],
)

# %% [markdown]
# Show the findings from the remapped example.

# %%
print("Custom column names")
print(
    json.dumps(
        [
            {
                "product_id": finding.product_id,
                "check_id": finding.check_id,
                "severity": finding.severity,
            }
            for finding in remapped_findings
        ],
        indent=2,
    )
)
