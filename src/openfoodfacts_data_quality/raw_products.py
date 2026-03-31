from __future__ import annotations

from typing import TYPE_CHECKING

from openfoodfacts_data_quality.scalars import as_number
from openfoodfacts_data_quality.structured_values import object_list_or_empty

if TYPE_CHECKING:
    from collections.abc import Mapping

RAW_PRODUCT_COLUMNS = [
    "code",
    "created_t",
    "product_name",
    "quantity",
    "product_quantity",
    "serving_size",
    "serving_quantity",
    "brands",
    "categories",
    "labels",
    "emb_codes",
    "ingredients_text",
    "ingredients_tags",
    "nutriscore_grade",
    "nutriscore_score",
    "categories_tags",
    "labels_tags",
    "countries_tags",
    "no_nutrition_data",
]
RAW_NUTRIMENT_COLUMNS = [
    "added-sugars_100g",
    "alcohol_100g",
    "alpha-linolenic-acid_100g",
    "biotin_100g",
    "caffeine_100g",
    "calcium_100g",
    "carbohydrates-total_100g",
    "carbohydrates_100g",
    "cholesterol_100g",
    "choline_100g",
    "chromium_100g",
    "copper_100g",
    "docosahexaenoic-acid_100g",
    "eicosapentaenoic-acid_100g",
    "energy-kcal_100g",
    "energy-kj_100g",
    "energy_100g",
    "fat_100g",
    "fiber_100g",
    "fructose_100g",
    "fruits-vegetables-legumes-estimate-from-ingredients_100g",
    "fruits-vegetables-legumes_100g",
    "fruits-vegetables-nuts-estimate-from-ingredients_100g",
    "fruits-vegetables-nuts_100g",
    "glucose_100g",
    "glycemic-index_100g",
    "iodine_100g",
    "iron_100g",
    "magnesium_100g",
    "maltose_100g",
    "manganese_100g",
    "monounsaturated-fat_100g",
    "nova-group_100g",
    "nutrition-score-fr_100g",
    "omega-3-fat_100g",
    "omega-6-fat_100g",
    "pantothenic-acid_100g",
    "phosphorus_100g",
    "polyols_100g",
    "polyunsaturated-fat_100g",
    "potassium_100g",
    "proteins_100g",
    "salt_100g",
    "saturated-fat_100g",
    "selenium_100g",
    "sodium_100g",
    "starch_100g",
    "sucrose_100g",
    "sugars_100g",
    "taurine_100g",
    "trans-fat_100g",
    "vitamin-a_100g",
    "vitamin-b12_100g",
    "vitamin-b1_100g",
    "vitamin-b2_100g",
    "vitamin-b6_100g",
    "vitamin-b9_100g",
    "vitamin-c_100g",
    "vitamin-d_100g",
    "vitamin-e_100g",
    "vitamin-k_100g",
    "vitamin-pp_100g",
    "zinc_100g",
]
RAW_INPUT_COLUMNS = RAW_PRODUCT_COLUMNS + RAW_NUTRIMENT_COLUMNS


def split_tags(value: object) -> list[str]:
    """Normalize comma-separated raw tag fields into a list."""
    if value is None:
        return []
    list_value = object_list_or_empty(value)
    if isinstance(value, list):
        return [str(item).strip() for item in list_value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [item.strip() for item in text.split(",") if item.strip()]


def ingredient_tags_from_raw_row(row: Mapping[str, object]) -> list[str]:
    """Return the normalized ingredient tags available on one raw product row."""
    return split_tags(row.get("ingredients_tags"))


def build_raw_classifier_fields(row: Mapping[str, object]) -> dict[str, object]:
    """Return the normalized raw classification fields shared by multiple runtime consumers."""
    return {
        "nutriscore_grade": _lowercased_optional_text(row.get("nutriscore_grade")),
        "nutriscore_score": as_number(row.get("nutriscore_score")),
        "categories_tags": split_tags(row.get("categories_tags")),
        "labels_tags": split_tags(row.get("labels_tags")),
        "countries_tags": split_tags(row.get("countries_tags")),
    }


def nutrient_unit_for(nutrient_id: str) -> str:
    """Return the display unit expected for one nutrient id."""
    if nutrient_id == "energy-kcal":
        return "kcal"
    if nutrient_id in {"energy", "energy-kj"}:
        return "kJ"
    return "g"


def build_input_sets(row: Mapping[str, object]) -> list[dict[str, object]]:
    """Build the minimal raw nutrition payload used by runtime consumers."""
    nutrients: dict[str, dict[str, object]] = {}
    for column, value in row.items():
        if not column.endswith("_100g"):
            continue
        number = as_number(value)
        if number is None:
            continue
        nutrient_id = column[:-5]
        nutrients[nutrient_id] = {
            "value": number,
            "unit": nutrient_unit_for(nutrient_id),
        }

    if not nutrients:
        return []

    return [
        {
            "source": "packaging",
            "preparation": "as_sold",
            "per": "100g",
            "nutrients": nutrients,
        }
    ]


def _lowercased_optional_text(value: object) -> str | None:
    """Return one stripped text value normalized to lowercase when present."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.lower()
