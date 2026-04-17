from __future__ import annotations

from collections.abc import Mapping

from off_data_quality._structured_values import (
    StringObjectMapping,
    is_object_list,
    is_string_object_mapping,
)
from off_data_quality.contracts.context import ContextSectionModel
from off_data_quality.contracts.mapping_view import MappingViewModel
from off_data_quality.contracts.structured import NutritionInputSet


def first_non_estimated_as_sold_nutrients(
    nutrition: Mapping[str, object] | MappingViewModel | ContextSectionModel,
) -> StringObjectMapping:
    """Return the first nutrient set for the product as sold that is not estimated."""
    input_sets = nutrition.get("input_sets")
    if not is_object_list(input_sets):
        return {}
    for input_set in input_sets:
        if isinstance(input_set, NutritionInputSet):
            if input_set.source == "estimate":
                continue
            if input_set.preparation != "as_sold":
                continue
            return {
                nutrient_id: nutrient.as_mapping()
                for nutrient_id, nutrient in input_set.nutrients.items()
            }
        if not is_string_object_mapping(input_set):
            continue
        if input_set.get("source") == "estimate":
            continue
        if input_set.get("preparation") != "as_sold":
            continue
        nutrients = input_set.get("nutrients")
        if is_string_object_mapping(nutrients):
            return nutrients
    return {}
