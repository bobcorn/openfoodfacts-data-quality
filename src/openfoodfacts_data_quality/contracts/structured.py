from __future__ import annotations

from pydantic import ConfigDict, Field

from openfoodfacts_data_quality.contracts.mapping_view import MappingViewModel


class StructuredModel(MappingViewModel):
    """Frozen structured payload shared by stable runtime contracts."""

    model_config = ConfigDict(extra="forbid", frozen=True)


def _empty_ingredient_nodes() -> list[IngredientNode]:
    """Build an empty ingredient list with a concrete static type."""
    return []


def _empty_nutrient_values() -> dict[str, NutrientValue]:
    """Build an empty nutrient mapping with a concrete static type."""
    return {}


class PackagingEntry(StructuredModel):
    """Stable packaging subset exposed to migrated checks."""

    number: float | int | None = None
    shape: str | None = None
    material: str | None = None


class IngredientNode(StructuredModel):
    """Stable nested ingredient subset exposed to migrated checks."""

    id: str | None = None
    vegan: str | None = None
    vegetarian: str | None = None
    ingredients: list[IngredientNode] = Field(default_factory=_empty_ingredient_nodes)


class NutrientValue(StructuredModel):
    """Stable nutrient payload shared by source-product and enriched-snapshot nutrition sets."""

    value: float | None = None
    unit: str | None = None
    value_computed: float | None = None


class NutritionInputSet(StructuredModel):
    """Stable nutrition input-set payload shared by source-product and enriched-snapshot runs."""

    source: str | None = None
    preparation: str | None = None
    per: str | None = None
    nutrients: dict[str, NutrientValue] = Field(default_factory=_empty_nutrient_values)


class NutritionAggregatedSet(StructuredModel):
    """Stable aggregated nutrition payload exposed to migrated checks."""

    nutrients: dict[str, NutrientValue] = Field(default_factory=_empty_nutrient_values)


IngredientNode.model_rebuild()
