"""Minimal raw surface example on the bundled CSV sample."""

import csv
from pprint import pprint

from openfoodfacts_data_quality import raw

# Load the bundled CSV sample with the standard library.
with open("examples/data/products.csv", encoding="utf-8", newline="") as handle:
    rows = list(csv.DictReader(handle))

# Inspect a small, friendly subset of checks before running them.
checks = raw.list_checks(
    check_ids=[
        "en:quantity-to-be-completed",
        "en:nutrition-data-per-serving-serving-quantity-is-not-recognized",
        "en:serving-quantity-over-product-quantity",
    ]
)
print("Selected checks:")
print([check.id for check in checks])

# Run the same checks on the loaded rows.
findings = raw.run_checks(
    rows,
    check_ids=[
        "en:quantity-to-be-completed",
        "en:nutrition-data-per-serving-serving-quantity-is-not-recognized",
        "en:serving-quantity-over-product-quantity",
    ],
)

print(f"\nLoaded {len(rows)} products from examples/data/products.csv.")
print(f"Emitted {len(findings)} findings.")
print("\nFirst five findings:")
pprint(
    [finding.model_dump(mode="json", exclude_none=True) for finding in findings[:5]],
    sort_dicts=False,
)
