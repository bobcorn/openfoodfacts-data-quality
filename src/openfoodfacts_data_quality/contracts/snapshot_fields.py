from __future__ import annotations

from pydantic import ConfigDict, Field

from openfoodfacts_data_quality.contracts.mapping_view import MappingViewModel
from openfoodfacts_data_quality.contracts.structured import (
    IngredientNode,
    NutritionAggregatedSet,
    NutritionInputSet,
    PackagingEntry,
)


def _empty_packaging_entries() -> list[PackagingEntry]:
    """Build an empty packaging list with a concrete static type."""
    return []


def _empty_ingredient_nodes() -> list[IngredientNode]:
    """Build an empty ingredient list with a concrete static type."""
    return []


def _empty_string_list() -> list[str]:
    """Build an empty string list with a concrete static type."""
    return []


def _empty_nutrition_input_sets() -> list[NutritionInputSet]:
    """Build an empty nutrition-input-set list with a concrete static type."""
    return []


class SnapshotSectionModel(MappingViewModel):
    """Immutable snapshot section with a mapping-like view for adapters."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ProductCoreFields(SnapshotSectionModel):
    """Shared core product fields used across stable and internal adapters."""

    code: str | None = None
    created_t: float | None = None
    product_name: str | None = None
    quantity: str | None = None
    product_quantity: float | None = None
    serving_size: str | None = None
    serving_quantity: float | None = None
    brands: str | None = None
    categories: str | None = None
    labels: str | None = None
    emb_codes: str | None = None
    ingredients_text: str | None = None
    ingredients_percent_analysis: float | None = None
    ingredients_with_specified_percent_n: float | None = None
    ingredients_with_unspecified_percent_n: float | None = None
    ingredients_with_specified_percent_sum: float | None = None
    ingredients_with_unspecified_percent_sum: float | None = None
    nutriscore_grade: str | None = None
    nutriscore_grade_producer: str | None = None
    nutriscore_score: float | None = None
    categories_tags: list[str] = Field(default_factory=_empty_string_list)
    labels_tags: list[str] = Field(default_factory=_empty_string_list)
    countries_tags: list[str] = Field(default_factory=_empty_string_list)


class ProductSnapshotFields(ProductCoreFields):
    """Shared enriched-product fields used across stable and internal adapters."""

    lc: str | None = None
    lang: str | None = None
    packagings: list[PackagingEntry] = Field(default_factory=_empty_packaging_entries)
    ingredients: list[IngredientNode] = Field(default_factory=_empty_ingredient_nodes)
    food_groups_tags: list[str] = Field(default_factory=_empty_string_list)


class FlagsSnapshotFields(SnapshotSectionModel):
    """Shared boolean helper flags used across stable and internal adapters."""

    is_european_product: bool | None = None
    has_animal_origin_category: bool | None = None
    ignore_energy_calculated_error: bool | None = None


class CategoryPropsSnapshotFields(SnapshotSectionModel):
    """Shared category property fields used across stable and internal adapters."""

    minimum_number_of_ingredients: float | None = None


class NutritionSnapshotFields(SnapshotSectionModel):
    """Shared nutrition snapshot fields used across stable and internal adapters."""

    input_sets: list[NutritionInputSet] = Field(
        default_factory=_empty_nutrition_input_sets
    )
    aggregated_set: NutritionAggregatedSet | None = None
