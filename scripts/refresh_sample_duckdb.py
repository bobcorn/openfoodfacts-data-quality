from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

import duckdb

from _bootstrap import ROOT, bootstrap_paths

bootstrap_paths()

from openfoodfacts_data_quality.raw_products import (
    RAW_NUTRIMENT_COLUMNS,
    RAW_PRODUCT_COLUMNS,
)
from openfoodfacts_data_quality.structured_values import (
    is_string_object_mapping,
    object_list_or_empty,
)

BASE_SELECT = (*RAW_PRODUCT_COLUMNS, "nutriments")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refresh the bundled DuckDB sample from the full OFF parquet snapshot."
    )
    parser.add_argument(
        "--source-parquet",
        type=Path,
        required=True,
        help="Path to the full Open Food Facts parquet snapshot.",
    )
    parser.add_argument(
        "--output-db",
        type=Path,
        default=ROOT / "data" / "products.duckdb",
        help="Destination DuckDB file for the bundled sample.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="Number of products to keep in the bundled sample.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed used for a stable pseudo-random product selection.",
    )
    args = parser.parse_args()

    source_parquet = args.source_parquet.resolve()
    output_db = args.output_db.resolve()

    if not source_parquet.exists():
        raise FileNotFoundError(f"Source parquet not found: {source_parquet}")
    if args.sample_size <= 0:
        raise ValueError("--sample-size must be a positive integer.")

    columns = RAW_PRODUCT_COLUMNS + RAW_NUTRIMENT_COLUMNS
    source_rows = fetch_source_rows(
        source_parquet, sample_size=args.sample_size, seed=args.seed
    )
    records = [transform_row(row, columns) for row in source_rows]
    records.sort(key=lambda record: record[0] or "")

    output_db.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_db = Path(temp_dir) / output_db.name
        write_sample_db(temp_db, columns, records)
        temp_db.replace(output_db)

    print(
        f"Refreshed {output_db} with {len(records)} products from {source_parquet} "
        f"(seed={args.seed})."
    )
    return 0


def fetch_source_rows(
    source_parquet: Path, sample_size: int, seed: int
) -> list[dict[str, object]]:
    """Load a stable pseudo-random slice of the full parquet snapshot."""
    connection = duckdb.connect()
    try:
        select_list = ",\n      ".join(BASE_SELECT)
        query = f"""
            with deduped_products as (
              select
                {select_list},
                row_number() over (
                  partition by code
                  order by coalesce(created_t, 0) desc, hash(code || '::{seed}')
                ) as code_rank
              from read_parquet('{source_parquet.as_posix()}')
              where code is not null
                and trim(code) != ''
            )
            select
              {select_list}
            from deduped_products
            where code_rank = 1
            order by hash(code || '::{seed}')
            limit {sample_size}
        """
        cursor = connection.execute(query)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    finally:
        connection.close()


def transform_row(row: dict[str, object], columns: list[str]) -> list[str | None]:
    """Project one parquet row into the flat DuckDB sample shape used by the prototype."""
    nutrient_values = extract_nutrient_values(row.get("nutriments"))
    flat_row: dict[str, str | None] = {
        "code": stringify(row.get("code")),
        "created_t": stringify(row.get("created_t")),
        "product_name": extract_localized_text(row.get("product_name")),
        "quantity": stringify(row.get("quantity")),
        "product_quantity": stringify(row.get("product_quantity")),
        "serving_size": stringify(row.get("serving_size")),
        "serving_quantity": stringify(row.get("serving_quantity")),
        "brands": stringify(row.get("brands")),
        "categories": stringify(row.get("categories")),
        "labels": stringify(row.get("labels")),
        "emb_codes": stringify(row.get("emb_codes")),
        "ingredients_text": extract_localized_text(row.get("ingredients_text")),
        "ingredients_tags": join_strings(row.get("ingredients_tags")),
        "nutriscore_grade": stringify(row.get("nutriscore_grade")),
        "nutriscore_score": stringify(row.get("nutriscore_score")),
        "categories_tags": join_strings(row.get("categories_tags")),
        "labels_tags": join_strings(row.get("labels_tags")),
        "countries_tags": join_strings(row.get("countries_tags")),
        "no_nutrition_data": "true" if row.get("no_nutrition_data") else None,
    }
    for column_name, value in nutrient_values.items():
        if column_name in columns:
            flat_row[column_name] = value

    return [flat_row.get(column_name) for column_name in columns]


def extract_localized_text(value: object) -> str | None:
    """Pick one readable text value from OFF localized text arrays."""
    items = object_list_or_empty(value)
    if not isinstance(value, list):
        return stringify(value)

    fallback: str | None = None
    for item in items:
        if not is_string_object_mapping(item):
            continue
        text = stringify(item.get("text"))
        if not text:
            continue
        if item.get("lang") == "main":
            return text
        if fallback is None:
            fallback = text
    return fallback


def extract_nutrient_values(value: object) -> dict[str, str]:
    """Flatten OFF nutriment structs into the current *_100g columns."""
    items = object_list_or_empty(value)
    if not isinstance(value, list):
        return {}

    flattened: dict[str, str] = {}
    for item in items:
        if not is_string_object_mapping(item):
            continue
        name = stringify(item.get("name"))
        number = item.get("100g")
        if not name or number is None:
            continue
        flattened[f"{name}_100g"] = stringify(number) or ""
    return {key: value for key, value in flattened.items() if value}


def join_strings(value: object) -> str | None:
    """Serialize an array of strings into the comma-separated format used by the sample."""
    items = object_list_or_empty(value)
    if not isinstance(value, list):
        return stringify(value)

    normalized_items = [stringify(item) for item in items]
    compact = [item for item in normalized_items if item]
    if not compact:
        return None
    return ",".join(compact)


def stringify(value: object) -> str | None:
    """Convert a scalar value into the flat string storage format used by the sample."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def write_sample_db(
    output_db: Path, columns: list[str], records: list[list[str | None]]
) -> None:
    """Write the projected sample rows into a fresh DuckDB database."""
    connection = duckdb.connect(str(output_db))
    try:
        quoted_columns = ", ".join(
            f"{quote_identifier(column)} VARCHAR" for column in columns
        )
        connection.execute(f"create table products ({quoted_columns})")
        placeholders = ", ".join("?" for _ in columns)
        connection.executemany(
            f"insert into products values ({placeholders})",
            records,
        )
        connection.execute("checkpoint")
    finally:
        connection.close()


def quote_identifier(value: str) -> str:
    """Quote a DuckDB identifier that may contain dashes."""
    return '"' + value.replace('"', '""') + '"'


if __name__ == "__main__":
    raise SystemExit(main())
