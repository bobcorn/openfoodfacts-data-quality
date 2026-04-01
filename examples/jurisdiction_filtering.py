"""Show how jurisdiction filters change the active raw surface checks."""

import csv

from openfoodfacts_data_quality import raw

with open("examples/data/products.csv", encoding="utf-8", newline="") as handle:
    rows = list(csv.DictReader(handle))

# Without a jurisdiction filter, the raw surface exposes every shipped jurisdiction.
all_checks = raw.list_checks(
    check_ids=[
        "en:quantity-to-be-completed",
        "ca:source-of-fibre-claim-but-fibre-below-threshold",
    ]
)
print("Checks returned by default:")
print([check.id for check in all_checks])

# Restrict the catalog to global checks only.
global_checks = raw.list_checks(
    check_ids=["en:quantity-to-be-completed"],
    jurisdictions=["global"],
)
print("\nChecks returned for jurisdictions=['global']:")
print([check.id for check in global_checks])

# Restrict the catalog to Canada-only checks.
canada_checks = raw.list_checks(
    check_ids=["ca:source-of-fibre-claim-but-fibre-below-threshold"],
    jurisdictions=["ca"],
)
print("\nChecks returned for jurisdictions=['ca']:")
print([check.id for check in canada_checks])

# The same filter also applies when you execute the checks.
global_findings = raw.run_checks(
    rows,
    check_ids=["en:quantity-to-be-completed"],
    jurisdictions=["global"],
)
print(f"\nGlobal findings on the bundled sample: {len(global_findings)}")

canada_findings = raw.run_checks(
    rows,
    check_ids=["ca:source-of-fibre-claim-but-fibre-below-threshold"],
    jurisdictions=["ca"],
)
print(f"Canada findings on the bundled sample: {len(canada_findings)}")
