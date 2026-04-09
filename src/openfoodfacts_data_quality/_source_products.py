from __future__ import annotations

from openfoodfacts_data_quality.contracts.source_products import (
    SOURCE_PRODUCT_COLUMNS,
    SOURCE_PRODUCT_INPUT_COLUMNS,
    SOURCE_PRODUCT_NUTRIMENT_COLUMNS,
    SourceProduct,
)
from openfoodfacts_data_quality.contracts.structured import (
    NutrientValue,
    NutritionInputSet,
)
from openfoodfacts_data_quality.scalars import as_number
from openfoodfacts_data_quality.structured_values import object_list_or_empty


def split_tags(value: object) -> list[str]:
    """Normalize comma separated tag fields into a list."""
    if value is None:
        return []
    list_value = object_list_or_empty(value)
    if isinstance(value, list):
        return [str(item).strip() for item in list_value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [item.strip() for item in text.split(",") if item.strip()]


def ingredient_tags_from_source_product(
    row: SourceProduct,
) -> list[str]:
    """Return the normalized ingredient tags available on one source product."""
    source_product = _require_source_product(row)
    return split_tags(source_product.ingredients_tags)


def build_source_product_classifier_fields(
    row: SourceProduct,
) -> dict[str, object]:
    """Return the source product classification fields shared by runtime consumers."""
    source_product = _require_source_product(row)
    return {
        "nutriscore_grade": _lowercased_optional_text(source_product.nutriscore_grade),
        "nutriscore_score": as_number(source_product.nutriscore_score),
        "categories_tags": split_tags(source_product.categories_tags),
        "labels_tags": split_tags(source_product.labels_tags),
        "countries_tags": split_tags(source_product.countries_tags),
    }


def nutrient_unit_for(nutrient_id: str) -> str:
    """Return the display unit expected for one nutrient id."""
    if nutrient_id == "energy-kcal":
        return "kcal"
    if nutrient_id in {"energy", "energy-kj"}:
        return "kJ"
    return "g"


def build_input_sets(
    row: SourceProduct,
) -> list[NutritionInputSet]:
    """Build the minimal source product nutrition payload used by runtime consumers."""
    source_product = _require_source_product(row)
    nutrients: dict[str, NutrientValue] = {}
    for column, value in source_product.as_mapping().items():
        if not column.endswith("_100g"):
            continue
        number = as_number(value)
        if number is None:
            continue
        nutrient_id = column[:-5]
        nutrients[nutrient_id] = NutrientValue(
            value=number,
            unit=nutrient_unit_for(nutrient_id),
        )

    if not nutrients:
        return []

    return [
        NutritionInputSet(
            source="packaging",
            preparation="as_sold",
            per="100g",
            nutrients=nutrients,
        )
    ]


def _lowercased_optional_text(value: object) -> str | None:
    """Return one stripped text value normalized to lowercase when present."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.lower()


def _require_source_product(
    row: object,
) -> SourceProduct:
    """Return one canonical source product or fail fast at the runtime boundary."""
    if isinstance(row, SourceProduct):
        return row
    raise TypeError(
        "Source product runtime helpers expect validated SourceProduct values."
    )


__all__ = [
    "SOURCE_PRODUCT_INPUT_COLUMNS",
    "SOURCE_PRODUCT_NUTRIMENT_COLUMNS",
    "SOURCE_PRODUCT_COLUMNS",
    "SourceProduct",
    "build_input_sets",
    "build_source_product_classifier_fields",
    "ingredient_tags_from_source_product",
    "split_tags",
]
