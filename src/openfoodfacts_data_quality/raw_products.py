from __future__ import annotations

from collections.abc import Mapping

from openfoodfacts_data_quality.contracts.raw import (
    RAW_INPUT_COLUMNS,
    RAW_NUTRIMENT_COLUMNS,
    RAW_PRODUCT_COLUMNS,
    RawProductRow,
    validate_raw_product_row,
)
from openfoodfacts_data_quality.contracts.structured import (
    NutrientValue,
    NutritionInputSet,
)
from openfoodfacts_data_quality.scalars import as_number
from openfoodfacts_data_quality.structured_values import object_list_or_empty


def split_tags(value: object) -> list[str]:
    """Normalize comma separated raw tag fields into a list."""
    if value is None:
        return []
    list_value = object_list_or_empty(value)
    if isinstance(value, list):
        return [str(item).strip() for item in list_value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [item.strip() for item in text.split(",") if item.strip()]


def ingredient_tags_from_raw_row(
    row: RawProductRow | Mapping[str, object],
) -> list[str]:
    """Return the normalized ingredient tags available on one raw product row."""
    raw_row = _validated_raw_row(row)
    return split_tags(raw_row.ingredients_tags)


def build_raw_classifier_fields(
    row: RawProductRow | Mapping[str, object],
) -> dict[str, object]:
    """Return the normalized raw classification fields shared by runtime consumers."""
    raw_row = _validated_raw_row(row)
    return {
        "nutriscore_grade": _lowercased_optional_text(raw_row.nutriscore_grade),
        "nutriscore_score": as_number(raw_row.nutriscore_score),
        "categories_tags": split_tags(raw_row.categories_tags),
        "labels_tags": split_tags(raw_row.labels_tags),
        "countries_tags": split_tags(raw_row.countries_tags),
    }


def nutrient_unit_for(nutrient_id: str) -> str:
    """Return the display unit expected for one nutrient id."""
    if nutrient_id == "energy-kcal":
        return "kcal"
    if nutrient_id in {"energy", "energy-kj"}:
        return "kJ"
    return "g"


def build_input_sets(
    row: RawProductRow | Mapping[str, object],
) -> list[NutritionInputSet]:
    """Build the minimal raw nutrition payload used by runtime consumers."""
    raw_row = _validated_raw_row(row)
    nutrients: dict[str, NutrientValue] = {}
    for column, value in raw_row.as_mapping().items():
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


def _validated_raw_row(row: RawProductRow | Mapping[str, object]) -> RawProductRow:
    """Return one validated raw row contract object from any supported caller shape."""
    if isinstance(row, RawProductRow):
        return row
    return validate_raw_product_row(dict(row))


__all__ = [
    "RAW_INPUT_COLUMNS",
    "RAW_NUTRIMENT_COLUMNS",
    "RAW_PRODUCT_COLUMNS",
    "RawProductRow",
    "build_input_sets",
    "build_raw_classifier_fields",
    "ingredient_tags_from_raw_row",
    "split_tags",
    "validate_raw_product_row",
]
