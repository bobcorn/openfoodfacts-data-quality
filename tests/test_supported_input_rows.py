from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import cast

import duckdb

from off_data_quality import checks
from off_data_quality._scalars import as_number

REPRESENTATIVE_CODE = "0009300001083"
PRODUCT_NAME = "Sweet Gherkins"
QUANTITY = "28 g"
PRODUCT_QUANTITY = "28"
SERVING_SIZE = "2 pickles (28.349 g)"
SERVING_QUANTITY = 28.349
BRANDS = "Mt. Olive"
CATEGORIES = "null"
LABELS = "Kosher, Orthodox Union Kosher, en:no-gluten"
EMB_CODES = "080503190204"
INGREDIENTS_TEXT = (
    "CUCUMBERS, WATER, VINEGAR, SALT, CALCIUM CHLORIDE, 0.1% SODIUM "
    "BENZOATE (PRESERVATIVE), ALUM, SUCRALOSE (SPLENDA BRAND), NATURAL "
    "FLAVORS, XANTHAN GUM, POLYSORBATE 80, AND YELLOW 5."
)
INGREDIENTS_TAGS = [
    "en:cucumber",
    "en:vegetable",
    "en:fruit-vegetable",
    "en:water",
    "en:vinegar",
    "en:salt",
    "en:e509",
    "en:e211",
    "en:alum",
    "en:e955",
    "en:natural-flavouring",
    "en:flavouring",
    "en:e415",
    "en:e433",
    "en:e102",
    "en:preservative",
    "en:splenda-brand",
]
INGREDIENTS_TAGS_CSV = ",".join(INGREDIENTS_TAGS)
LABELS_TAGS = ["en:no-gluten", "en:kosher", "en:orthodox-union-kosher"]
LABELS_TAGS_CSV = ",".join(LABELS_TAGS)
COUNTRIES_TAGS = ["en:united-states"]
COUNTRIES_TAGS_CSV = ",".join(COUNTRIES_TAGS)
CATEGORIES_TAGS = ["en:null"]
CATEGORIES_TAGS_CSV = ",".join(CATEGORIES_TAGS)
NUTRISCORE_GRADE = "c"
NUTRISCORE_SCORE = 6
EXAMPLES_DATA_DIR = Path(__file__).resolve().parents[1] / "examples" / "data"
JSONL_SAMPLE = EXAMPLES_DATA_DIR / "products.jsonl"
CSV_SAMPLE = EXAMPLES_DATA_DIR / "products.csv"
PARQUET_SAMPLE = EXAMPLES_DATA_DIR / "products.parquet"

# These projections are copied from one real product that exists in the bundled
# samples derived from official OFF exports. They intentionally keep only the
# supported source-side columns that the checks facade consumes.
OFFICIAL_JSONL_FULL_DOCUMENT_ROW: dict[str, object] = {
    "code": REPRESENTATIVE_CODE,
    "created_t": 1489053259,
    "product_name": PRODUCT_NAME,
    "quantity": QUANTITY,
    "product_quantity": PRODUCT_QUANTITY,
    "serving_size": SERVING_SIZE,
    "serving_quantity": SERVING_QUANTITY,
    "brands": BRANDS,
    "categories": CATEGORIES,
    "labels": LABELS,
    "emb_codes": EMB_CODES,
    "ingredients_text": INGREDIENTS_TEXT,
    "ingredients_tags": INGREDIENTS_TAGS,
    "nutriscore_grade": NUTRISCORE_GRADE,
    "nutriscore_score": NUTRISCORE_SCORE,
    "categories_tags": CATEGORIES_TAGS,
    "labels_tags": LABELS_TAGS,
    "countries_tags": COUNTRIES_TAGS,
    "no_nutrition_data": "",
    "nutriments": {
        "energy-kcal_100g": 0,
        "salt_100g": 1.5,
        "sodium_100g": 0.6,
        "nutrition-score-fr_100g": 6,
    },
}

OFFICIAL_CSV_ROW: dict[str, object] = {
    "code": REPRESENTATIVE_CODE,
    "created_t": "1489053259",
    "product_name": PRODUCT_NAME,
    "quantity": QUANTITY,
    "product_quantity": PRODUCT_QUANTITY,
    "serving_size": SERVING_SIZE,
    "serving_quantity": str(SERVING_QUANTITY),
    "brands": BRANDS,
    "categories": CATEGORIES,
    "labels": LABELS,
    "emb_codes": EMB_CODES,
    "ingredients_text": INGREDIENTS_TEXT,
    "ingredients_tags": INGREDIENTS_TAGS_CSV,
    "nutriscore_grade": NUTRISCORE_GRADE,
    "nutriscore_score": str(NUTRISCORE_SCORE),
    "categories_tags": CATEGORIES_TAGS_CSV,
    "labels_tags": LABELS_TAGS_CSV,
    "countries_tags": COUNTRIES_TAGS_CSV,
    "no_nutrition_data": "",
}

OFFICIAL_PARQUET_ROW: dict[str, object] = {
    "code": REPRESENTATIVE_CODE,
    "created_t": 1489053259,
    "product_name": [
        {"lang": "main", "text": PRODUCT_NAME},
        {"lang": "en", "text": PRODUCT_NAME},
    ],
    "quantity": QUANTITY,
    "product_quantity": PRODUCT_QUANTITY,
    "serving_size": SERVING_SIZE,
    "serving_quantity": str(SERVING_QUANTITY),
    "brands": BRANDS,
    "categories": CATEGORIES,
    "labels": LABELS,
    "emb_codes": EMB_CODES,
    "ingredients_text": [
        {"lang": "main", "text": INGREDIENTS_TEXT},
        {"lang": "en", "text": INGREDIENTS_TEXT},
    ],
    "ingredients_tags": INGREDIENTS_TAGS,
    "nutriscore_grade": NUTRISCORE_GRADE,
    "nutriscore_score": NUTRISCORE_SCORE,
    "categories_tags": CATEGORIES_TAGS,
    "labels_tags": LABELS_TAGS,
    "countries_tags": COUNTRIES_TAGS,
    "no_nutrition_data": False,
    "nutriments": [
        {
            "name": "energy-kcal",
            "value": 0.0,
            "100g": 0.0,
            "serving": 0.0,
            "unit": "kcal",
        },
        {
            "name": "salt",
            "value": 0.42500001192092896,
            "100g": 1.5,
            "serving": 0.42500001192092896,
            "unit": "g",
        },
        {
            "name": "sodium",
            "value": 0.17000000178813934,
            "100g": 0.6000000238418579,
            "serving": 0.17000000178813934,
            "unit": "g",
        },
    ],
}


def test_frozen_official_rows_prepare_consistently_on_shared_fields() -> None:
    [jsonl_row] = checks.prepare_source_products([OFFICIAL_JSONL_FULL_DOCUMENT_ROW])
    [csv_row] = checks.prepare_source_products([OFFICIAL_CSV_ROW])
    [parquet_row] = checks.prepare_source_products([OFFICIAL_PARQUET_ROW])

    prepared_rows = (jsonl_row, csv_row, parquet_row)
    for row in prepared_rows:
        assert row.code == REPRESENTATIVE_CODE
        assert row.product_name == PRODUCT_NAME
        assert row.quantity == QUANTITY
        assert row.product_quantity == PRODUCT_QUANTITY
        assert row.serving_size == SERVING_SIZE
        _assert_number_equals(row.serving_quantity, SERVING_QUANTITY)
        assert row.brands == BRANDS
        assert row.labels == LABELS
        assert row.emb_codes == EMB_CODES
        assert row.nutriscore_grade == NUTRISCORE_GRADE
        _assert_number_equals(row.nutriscore_score, float(NUTRISCORE_SCORE))
        assert row.ingredients_text is not None
        assert row.ingredients_text.startswith("CUCUMBERS, WATER, VINEGAR")
        assert row.ingredients_tags is not None
        assert "en:splenda-brand" in row.ingredients_tags
        assert _normalized_tags(row.categories_tags) == CATEGORIES_TAGS
        assert _normalized_tags(row.labels_tags) == LABELS_TAGS
        assert _normalized_tags(row.countries_tags) == COUNTRIES_TAGS


def test_frozen_jsonl_row_matches_the_bundled_sample() -> None:
    assert _project_jsonl_row(_load_jsonl_row(REPRESENTATIVE_CODE)) == (
        OFFICIAL_JSONL_FULL_DOCUMENT_ROW
    )


def test_frozen_csv_row_matches_the_bundled_sample() -> None:
    assert _load_csv_row(REPRESENTATIVE_CODE) == OFFICIAL_CSV_ROW


def test_frozen_parquet_row_matches_the_bundled_sample() -> None:
    assert _project_parquet_row(_load_parquet_row(REPRESENTATIVE_CODE)) == (
        OFFICIAL_PARQUET_ROW
    )


def test_frozen_full_fidelity_rows_prepare_consistently_on_nutriments() -> None:
    [jsonl_row] = checks.prepare_source_products([OFFICIAL_JSONL_FULL_DOCUMENT_ROW])
    [parquet_row] = checks.prepare_source_products([OFFICIAL_PARQUET_ROW])

    _assert_number_equals(jsonl_row.energy_kcal_100g, 0.0)
    _assert_number_equals(parquet_row.energy_kcal_100g, 0.0)
    _assert_number_equals(jsonl_row.salt_100g, 1.5)
    _assert_number_equals(parquet_row.salt_100g, 1.5)
    _assert_number_equals(jsonl_row.sodium_100g, 0.6)
    _assert_number_equals(parquet_row.sodium_100g, 0.6)


def test_frozen_official_rows_accept_extra_columns() -> None:
    [prepared_row] = checks.prepare_source_products(
        [{**OFFICIAL_CSV_ROW, "unexpected_extra_column": "kept out of the contract"}]
    )

    assert prepared_row.code == REPRESENTATIVE_CODE
    assert prepared_row.product_name == PRODUCT_NAME


def test_frozen_official_rows_accept_sparse_supported_subsets() -> None:
    sparse_csv_row = {
        "code": OFFICIAL_CSV_ROW["code"],
        "product_name": OFFICIAL_CSV_ROW["product_name"],
        "quantity": OFFICIAL_CSV_ROW["quantity"],
    }
    sparse_jsonl_row = {
        "code": OFFICIAL_JSONL_FULL_DOCUMENT_ROW["code"],
        "product_quantity": OFFICIAL_JSONL_FULL_DOCUMENT_ROW["product_quantity"],
        "serving_quantity": OFFICIAL_JSONL_FULL_DOCUMENT_ROW["serving_quantity"],
    }

    [prepared_csv_row] = checks.prepare_source_products([sparse_csv_row])
    [prepared_jsonl_row] = checks.prepare_source_products([sparse_jsonl_row])

    assert prepared_csv_row.code == REPRESENTATIVE_CODE
    assert prepared_csv_row.product_name == PRODUCT_NAME
    assert prepared_csv_row.quantity == QUANTITY
    assert prepared_jsonl_row.code == REPRESENTATIVE_CODE
    assert prepared_jsonl_row.product_quantity == PRODUCT_QUANTITY
    assert prepared_jsonl_row.serving_quantity == SERVING_QUANTITY


def _assert_number_equals(value: object, expected: float) -> None:
    actual = as_number(value)
    assert actual is not None
    assert abs(actual - expected) < 1e-6


def _normalized_tags(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item for item in value.split(",") if item]
    if isinstance(value, list):
        return [str(item) for item in cast(list[object], value)]
    raise TypeError(f"Unsupported tag value type: {type(value).__name__}")


def _load_jsonl_row(code: str) -> dict[str, object]:
    with JSONL_SAMPLE.open(encoding="utf-8") as input_file:
        for line in input_file:
            row = json.loads(line)
            if row.get("code") == code:
                return cast(dict[str, object], row)
    raise AssertionError(f"Missing JSONL sample row for {code}.")


def _load_csv_row(code: str) -> dict[str, object]:
    with CSV_SAMPLE.open(newline="", encoding="utf-8") as input_file:
        for row in csv.DictReader(input_file, delimiter="\t"):
            if row.get("code") == code:
                return {key: row[key] for key in OFFICIAL_CSV_ROW if key in row}
    raise AssertionError(f"Missing CSV sample row for {code}.")


def _load_parquet_row(code: str) -> dict[str, object]:
    query = duckdb.read_parquet(str(PARQUET_SAMPLE)).filter(f"code = '{code}'")
    rows = query.fetchall()
    columns = list(query.columns)
    if len(rows) != 1:
        raise AssertionError(f"Missing Parquet sample row for {code}.")
    row = dict(zip(columns, rows[0], strict=True))
    return cast(dict[str, object], row)


def _project_jsonl_row(row: dict[str, object]) -> dict[str, object]:
    projected = {
        key: row[key]
        for key in OFFICIAL_JSONL_FULL_DOCUMENT_ROW
        if key != "nutriments" and key in row
    }
    nutriments = cast(dict[str, object], row["nutriments"])
    projected["nutriments"] = {
        key: nutriments[key]
        for key in cast(
            dict[str, object], OFFICIAL_JSONL_FULL_DOCUMENT_ROW["nutriments"]
        )
        if key in nutriments
    }
    return projected


def _project_parquet_row(row: dict[str, object]) -> dict[str, object]:
    projected = {
        key: row[key]
        for key in OFFICIAL_PARQUET_ROW
        if key not in {"product_name", "ingredients_text", "nutriments"} and key in row
    }
    projected["product_name"] = _select_localized_entries(
        cast(list[dict[str, object]], row["product_name"])
    )
    projected["ingredients_text"] = _select_localized_entries(
        cast(list[dict[str, object]], row["ingredients_text"])
    )
    projected["nutriments"] = _select_named_nutriments(
        cast(list[dict[str, object]], row["nutriments"]),
        names={"energy-kcal", "salt", "sodium"},
    )
    return projected


def _select_localized_entries(
    value: list[dict[str, object]],
) -> list[dict[str, object]]:
    selected: list[dict[str, object]] = []
    seen: set[tuple[object, object]] = set()
    for entry in value:
        lang = entry.get("lang")
        if lang not in {"main", "en"}:
            continue
        key = (lang, entry.get("text"))
        if key in seen:
            continue
        seen.add(key)
        selected.append(entry)
    return selected


def _select_named_nutriments(
    value: list[dict[str, object]],
    *,
    names: set[str],
) -> list[dict[str, object]]:
    projected: list[dict[str, object]] = []
    for entry in value:
        name = entry.get("name")
        if name not in names:
            continue
        projected.append(
            {
                key: entry[key]
                for key in ("name", "value", "100g", "serving", "unit")
                if key in entry
            }
        )
    return projected
