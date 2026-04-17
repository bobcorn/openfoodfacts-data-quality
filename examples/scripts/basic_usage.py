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
# # Basic usage
#
# List the checks, prepare reusable rows, then run them on the sample data.

# %% [markdown]
# Import the reusable checks API from `off_data_quality`.
#
# `from off_data_quality import checks` gives us the public surface for listing
# shipped checks and running them on loaded rows.

# %%
from off_data_quality import checks

# %% [markdown]
# Import DuckDB as a convenient loader for the bundled sample data.
#
# DuckDB is not required by `off_data_quality`: you can load rows with another
# library and still pass the resulting rows to `checks.run(...)` as long as the
# input matches one supported input shape.

# %%
import duckdb

# %%
import json
import os

DATA_DIR = "examples/data" if os.path.isdir("examples/data") else "../data"
PARQUET_SAMPLE = f"{DATA_DIR}/products.parquet"
SELECTED_CHECK_IDS = (
    "en:quantity-to-be-completed",
    "en:nutrition-data-per-serving-serving-quantity-is-not-recognized",
    "en:serving-quantity-over-product-quantity",
)

# %% [markdown]
# Load the bundled OFF Parquet sample directly.

# %%
rows = duckdb.read_parquet(PARQUET_SAMPLE)

# %% [markdown]
# Look at the checks in the library.

# %%
all_checks = checks.list()
print("All checks")
print(json.dumps([check.id for check in all_checks], indent=2))

# %% [markdown]
# Prepare the loaded rows once if you plan to reuse them across multiple runs.

# %%
prepared_rows = checks.prepare(rows)

# %% [markdown]
# Run all checks on the prepared rows.

# %%
all_findings = checks.run(prepared_rows)
print()
print("All findings")
print(json.dumps({"count": len(all_findings)}, indent=2))

# %% [markdown]
# Run a smaller set of checks.

# %%
selected_checks = checks.list(check_ids=SELECTED_CHECK_IDS)
selected_findings = checks.run(prepared_rows, check_ids=SELECTED_CHECK_IDS)
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
