"""Show that the raw surface works with public rows loaded from Parquet or DuckDB."""

import duckdb

from openfoodfacts_data_quality import raw

# Load the bundled Parquet sample with DuckDB, then pass the rows to the library.
connection = duckdb.connect()
result = connection.execute(
    "select * from read_parquet('examples/data/products.parquet')"
)
columns = [column[0] for column in result.description]
parquet_rows = [dict(zip(columns, row, strict=False)) for row in result.fetchall()]
connection.close()

parquet_findings = raw.run_checks(
    parquet_rows,
    check_ids=["en:quantity-to-be-completed"],
)

print("Parquet sample:")
print(f"- loaded rows: {len(parquet_rows)}")
print(f"- findings: {len(parquet_findings)}")

# Load the same products from the bundled DuckDB database.
connection = duckdb.connect("examples/data/products.duckdb", read_only=True)
result = connection.execute("select * from products")
columns = [column[0] for column in result.description]
duckdb_rows = [dict(zip(columns, row, strict=False)) for row in result.fetchall()]
connection.close()

duckdb_findings = raw.run_checks(
    duckdb_rows,
    check_ids=["en:quantity-to-be-completed"],
)

print("\nDuckDB sample:")
print(f"- loaded rows: {len(duckdb_rows)}")
print(f"- findings: {len(duckdb_findings)}")
