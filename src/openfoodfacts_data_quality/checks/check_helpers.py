"""Shared support functions for decorated checks."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from openfoodfacts_data_quality.checks.context_dependencies import (
    depends_on_context_paths,
)
from openfoodfacts_data_quality.contracts.checks import CheckEmission, Severity
from openfoodfacts_data_quality.scalars import as_number
from openfoodfacts_data_quality.structured_values import (
    StringObjectMapping,
    is_blank_value,
    is_object_list,
    is_string_object_mapping,
    object_list_or_empty,
)

if TYPE_CHECKING:
    from openfoodfacts_data_quality.contracts.context import NormalizedContext

_NUTRITION_SET_ID_SANITIZE_PATTERN = re.compile(r"[^a-z0-9]+")


def energy_mismatch_for_unit(nutrients: StringObjectMapping, unit: str) -> bool:
    """Return whether one nutrient set has an energy mismatch for one unit."""
    energy = nutrient_value(nutrients, f"energy-{unit}")
    computed_energy = nutrient_value(
        nutrients, f"energy-{unit}", field="value_computed"
    )
    if energy is None or computed_energy is None:
        return False

    minimum = 55 if unit == "kj" else 13
    if energy <= minimum and computed_energy <= minimum:
        return False

    return computed_energy < (energy * 0.7 - 5) or computed_energy > (energy * 1.3 + 5)


def sugars_starch_fiber_for_set(nutrients: StringObjectMapping) -> bool:
    """Return whether sugars, starch, and fiber exceed carbohydrates."""
    total = nutrient_value(nutrients, "carbohydrates-total")
    if total is None:
        return False

    parts = sum(
        value
        for value in [
            nutrient_min_value(nutrients, "sugars"),
            nutrient_min_value(nutrients, "starch"),
            nutrient_min_value(nutrients, "fiber"),
        ]
        if value is not None
    )
    tolerance = 0.1 if total >= 10 else 0.01
    return round(parts, 2) > round(total + tolerance, 2)


def nutrient_value(
    nutrients: StringObjectMapping,
    nutrient_id: str,
    field: str = "value",
) -> float | None:
    """Read one nutrient field as a float."""
    nutrient = nutrients.get(nutrient_id)
    if not is_string_object_mapping(nutrient):
        return None
    return as_number(nutrient.get(field))


def nutrient_min_value(
    nutrients: StringObjectMapping,
    nutrient_id: str,
) -> float | None:
    """Return a nutrient value, defaulting missing values to zero."""
    value = nutrient_value(nutrients, nutrient_id)
    return value if value is not None else 0.0


def missing(value: object) -> bool:
    """Return whether a value should count as blank for Python checks."""
    return is_blank_value(value)


def present(value: object) -> bool:
    """Return whether a value should count as present for Python checks."""
    return not missing(value)


def has_food_group_level(food_groups_tags: object, level: int) -> bool:
    """Return whether one food group level exists in the normalized tag list."""
    return is_object_list(food_groups_tags) and len(food_groups_tags) >= level


def ingredient_claim_has_unknowns(ingredients: object, claim: str) -> bool:
    """Return whether any nested ingredient lacks a decisive claim value."""
    ingredient_list = object_list_or_empty(ingredients)
    if not ingredient_list:
        return False

    for ingredient in ingredient_list:
        if not is_string_object_mapping(ingredient):
            return True

        raw_claim = ingredient.get(claim)
        normalized_claim = (
            str(raw_claim).strip().lower() if raw_claim is not None else ""
        )
        if normalized_claim in {"", "unknown"}:
            return True

        if ingredient_claim_has_unknowns(ingredient.get("ingredients"), claim):
            return True

    return False


@depends_on_context_paths("nutrition.input_sets", "nutrition.aggregated_set")
def iter_nutrient_family_sets(
    context: NormalizedContext,
) -> list[tuple[str, StringObjectMapping, Severity]]:
    """Return the nutrient sets that emit family-tag findings."""
    family_sets: list[tuple[str, StringObjectMapping, Severity]] = []

    input_sets = context.nutrition.input_sets
    if is_object_list(input_sets):
        for input_set in input_sets:
            if not is_string_object_mapping(input_set):
                continue
            if input_set.get("source") == "estimate":
                continue
            nutrients = input_set.get("nutrients")
            if not is_string_object_mapping(nutrients):
                continue
            family_sets.append((_nutrition_set_id(input_set), nutrients, "error"))

    aggregated_set = context.nutrition.aggregated_set
    if is_string_object_mapping(aggregated_set):
        nutrients = aggregated_set.get("nutrients")
        if is_string_object_mapping(nutrients):
            family_sets.append(("nutrition", nutrients, "warning"))

    return family_sets


def _nutrition_set_id(input_set: StringObjectMapping) -> str:
    """Reproduce the backend set-id normalization for nutrition families."""
    source = str(input_set.get("source") or "unknown-source")
    preparation = str(input_set.get("preparation") or "unknown-preparation")
    per = str(input_set.get("per") or "unknown-per")
    raw_set_id = f"nutrition-{source}-{preparation}-{per}".lower()
    normalized = _NUTRITION_SET_ID_SANITIZE_PATTERN.sub("-", raw_set_id)
    return normalized.strip("-")


def single_emission(severity: Severity) -> list[CheckEmission]:
    """Return the common 0/1 emission shape for simple Python checks."""
    return [CheckEmission(severity=severity)]
