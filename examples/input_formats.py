"""Show the same checks.run(...) call with different input formats."""

import json

import duckdb

from off_data_quality import checks

CSV_SAMPLE = "examples/data/products.csv"
PARQUET_SAMPLE = "examples/data/products.parquet"
DUCKDB_SAMPLE = "examples/data/products.duckdb"
CHECK_IDS = (
    "en:quantity-to-be-completed",
    "en:serving-quantity-over-product-quantity",
)

# CSV data works directly.
csv_findings = checks.run(
    duckdb.read_csv(CSV_SAMPLE, sep="\t", all_varchar=True),
    check_ids=CHECK_IDS,
)

# The same call also works with Parquet data.
parquet_findings = checks.run(
    duckdb.read_parquet(PARQUET_SAMPLE),
    check_ids=CHECK_IDS,
)

# DuckDB data works too.
with duckdb.connect(DUCKDB_SAMPLE, read_only=True) as connection:
    duckdb_findings = checks.run(
        connection.sql("from products"),
        check_ids=CHECK_IDS,
    )

print("Finding counts by input format")
print(
    json.dumps(
        {
            "csv": len(csv_findings),
            "parquet": len(parquet_findings),
            "duckdb": len(duckdb_findings),
        },
        indent=2,
    )
)
print()

# If your columns use different names, pass a mapping.
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
