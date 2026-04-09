"""List the checks, then run them on the sample data."""

import json

import duckdb

from off_data_quality import checks

CSV_SAMPLE = "examples/data/products.csv"
SELECTED_CHECK_IDS = (
    "en:quantity-to-be-completed",
    "en:nutrition-data-per-serving-serving-quantity-is-not-recognized",
    "en:serving-quantity-over-product-quantity",
)

# Load one table of product rows.
rows = duckdb.read_csv(CSV_SAMPLE, sep="\t", all_varchar=True)

# Look at the checks in the library.
all_checks = checks.list()
print("All checks")
print(json.dumps([check.id for check in all_checks], indent=2))

# Run all checks on the loaded rows.
all_findings = checks.run(rows)
print()
print("All findings")
print(json.dumps({"count": len(all_findings)}, indent=2))

# Run a smaller set of checks.
selected_checks = checks.list(check_ids=SELECTED_CHECK_IDS)
selected_findings = checks.run(rows, check_ids=SELECTED_CHECK_IDS)
selected_example = None
for finding in selected_findings:
    selected_example = {
        "product_id": finding.product_id,
        "check_id": finding.check_id,
        "severity": finding.severity,
    }
    break

print()
print("Selected checks")
print(json.dumps([check.id for check in selected_checks], indent=2))

print()
print("Selected findings")
print(
    json.dumps(
        {
            "count": len(selected_findings),
            "example": selected_example,
        },
        indent=2,
    )
)
