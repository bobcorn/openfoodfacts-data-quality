"""Show how jurisdiction filters change checks.list(...) and checks.run(...)."""

import json

import duckdb

from off_data_quality import checks

CSV_SAMPLE = "examples/data/products.csv"

# Load the sample once.
rows = duckdb.read_csv(CSV_SAMPLE, sep="\t", all_varchar=True)

# Look at the checks with and without a jurisdiction filter.
all_checks = checks.list()
global_checks = checks.list(jurisdictions=["global"])
canada_checks = checks.list(jurisdictions=["ca"])

print("Checks by jurisdiction")
print(
    json.dumps(
        {
            "all": [check.id for check in all_checks],
            "global": [check.id for check in global_checks],
            "ca": [check.id for check in canada_checks],
        },
        indent=2,
    )
)

# Use the same jurisdiction filter when you run the checks.
all_findings = checks.run(rows)
global_findings = checks.run(rows, jurisdictions=["global"])
canada_findings = checks.run(rows, jurisdictions=["ca"])

print()
print("Finding counts by jurisdiction")
print(
    json.dumps(
        {
            "all": len(all_findings),
            "global": len(global_findings),
            "ca": len(canada_findings),
        },
        indent=2,
    )
)
