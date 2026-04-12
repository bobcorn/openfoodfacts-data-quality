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
# ---

# %% [markdown]
# # Jurisdiction filtering
#
# Show how jurisdiction filters change `checks.list(...)` and `checks.run(...)`.

# %%
import json
import os

import duckdb

from off_data_quality import checks

DATA_DIR = "examples/data" if os.path.isdir("examples/data") else "../data"
PARQUET_SAMPLE = f"{DATA_DIR}/products.parquet"

# %% [markdown]
# Load the bundled OFF Parquet sample directly.

# %%
rows = duckdb.read_parquet(PARQUET_SAMPLE)

# %% [markdown]
# Look at the checks with and without a jurisdiction filter.

# %%
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

# %% [markdown]
# Use the same jurisdiction filter when you run the checks.

# %%
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
