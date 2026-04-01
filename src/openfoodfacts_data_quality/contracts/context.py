from __future__ import annotations

from dataclasses import dataclass
from types import NoneType, UnionType
from typing import Annotated, Any, Literal, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field, model_validator

from openfoodfacts_data_quality.contracts.checks import CheckInputSurface
from openfoodfacts_data_quality.contracts.mapping_view import MappingViewModel
from openfoodfacts_data_quality.contracts.structured import (
    IngredientNode,
    NutritionAggregatedSet,
    NutritionInputSet,
    PackagingEntry,
)

PathType = Literal["string", "number", "boolean", "array", "object"]

CHECK_INPUT_SURFACES: tuple[CheckInputSurface, ...] = (
    "raw_products",
    "enriched_products",
)


@dataclass(frozen=True)
class ContextFieldMetadata:
    """Path metadata attached directly to one normalized context contract field."""

    type: PathType
    dsl_allowed: bool = True
    supported_input_surfaces: tuple[CheckInputSurface, ...] = CHECK_INPUT_SURFACES


@dataclass(frozen=True)
class ContextPathSpec:
    """Materialized metadata for one dotted normalized context path."""

    path: str
    type: PathType
    dsl_allowed: bool
    supported_input_surfaces: tuple[CheckInputSurface, ...]


def context_field(
    path_type: PathType,
    *,
    dsl_allowed: bool = True,
    supported_input_surfaces: tuple[CheckInputSurface, ...] = CHECK_INPUT_SURFACES,
) -> ContextFieldMetadata:
    """Attach normalized context path metadata to one contract field."""
    return ContextFieldMetadata(
        type=path_type,
        dsl_allowed=dsl_allowed,
        supported_input_surfaces=supported_input_surfaces,
    )


def _empty_nutrition_input_sets() -> list[NutritionInputSet]:
    """Build an empty nutrition-input-set list with a concrete static type."""
    return []


class ContextSectionModel(MappingViewModel):
    """Immutable typed section with a mapping view for path based consumers."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ProductContext(ContextSectionModel):
    """Stable normalized product subset consumed by migrated checks."""

    code: Annotated[str, context_field("string")]
    lc: Annotated[
        str | None,
        context_field(
            "string",
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None
    lang: Annotated[
        str | None,
        context_field(
            "string",
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None
    created_t: Annotated[float | None, context_field("number")] = None
    product_name: Annotated[str | None, context_field("string")] = None
    quantity: Annotated[str | None, context_field("string")] = None
    packagings: Annotated[
        list[PackagingEntry] | None,
        context_field(
            "array",
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None
    product_quantity: Annotated[float | None, context_field("number")] = None
    serving_size: Annotated[str | None, context_field("string")] = None
    serving_quantity: Annotated[float | None, context_field("number")] = None
    brands: Annotated[str | None, context_field("string")] = None
    categories: Annotated[str | None, context_field("string")] = None
    labels: Annotated[str | None, context_field("string")] = None
    emb_codes: Annotated[str | None, context_field("string")] = None
    ingredients_text: Annotated[str | None, context_field("string")] = None
    ingredients: Annotated[
        list[IngredientNode] | None,
        context_field(
            "array",
            dsl_allowed=False,
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None
    ingredients_tags: Annotated[list[str], context_field("array")] = Field(
        default_factory=list
    )
    ingredients_percent_analysis: Annotated[
        float | None,
        context_field(
            "number",
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None
    ingredients_with_specified_percent_n: Annotated[
        float | None,
        context_field(
            "number",
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None
    ingredients_with_unspecified_percent_n: Annotated[
        float | None,
        context_field(
            "number",
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None
    ingredients_with_specified_percent_sum: Annotated[
        float | None,
        context_field(
            "number",
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None
    ingredients_with_unspecified_percent_sum: Annotated[
        float | None,
        context_field(
            "number",
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None
    nutriscore_grade: Annotated[
        str | None,
        context_field("string", dsl_allowed=False),
    ] = None
    nutriscore_grade_producer: Annotated[
        str | None,
        context_field(
            "string",
            dsl_allowed=False,
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None
    nutriscore_score: Annotated[float | None, context_field("number")] = None
    categories_tags: Annotated[list[str], context_field("array")] = Field(
        default_factory=list
    )
    labels_tags: Annotated[list[str], context_field("array")] = Field(
        default_factory=list
    )
    countries_tags: Annotated[list[str], context_field("array")] = Field(
        default_factory=list
    )
    food_groups_tags: Annotated[
        list[str] | None,
        context_field(
            "array",
            dsl_allowed=False,
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None


class FlagsContext(ContextSectionModel):
    """Stable normalized flag subset derived from enriched snapshots."""

    is_european_product: Annotated[
        bool | None,
        context_field(
            "boolean",
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None
    has_animal_origin_category: Annotated[
        bool | None,
        context_field(
            "boolean",
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None
    ignore_energy_calculated_error: Annotated[
        bool | None,
        context_field(
            "boolean",
            dsl_allowed=False,
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None


class CategoryPropsContext(ContextSectionModel):
    """Stable normalized category property subset derived from enriched snapshots."""

    minimum_number_of_ingredients: Annotated[
        float | None,
        context_field(
            "number",
            dsl_allowed=False,
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None


class NutritionAsSoldContext(ContextSectionModel):
    """Prepared nutrient claims subset exposed as simple scalar values."""

    energy_kcal: Annotated[float | None, context_field("number")] = None
    fat: Annotated[float | None, context_field("number")] = None
    saturated_fat: Annotated[float | None, context_field("number")] = None
    trans_fat: Annotated[float | None, context_field("number")] = None
    sugars: Annotated[float | None, context_field("number")] = None
    fiber: Annotated[float | None, context_field("number")] = None
    omega_3: Annotated[float | None, context_field("number")] = None


class NutritionContext(ContextSectionModel):
    """Stable normalized nutrition subset shared by raw and enriched runs."""

    input_sets: Annotated[
        list[NutritionInputSet],
        context_field("array", dsl_allowed=False),
    ] = Field(default_factory=_empty_nutrition_input_sets)
    aggregated_set: Annotated[
        NutritionAggregatedSet | None,
        context_field(
            "object",
            dsl_allowed=False,
            supported_input_surfaces=("enriched_products",),
        ),
    ] = None
    as_sold: NutritionAsSoldContext = Field(default_factory=NutritionAsSoldContext)


class NormalizedContext(BaseModel):
    """Stable typed context consumed by quality checks."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: str
    product: ProductContext
    flags: FlagsContext = Field(default_factory=FlagsContext)
    category_props: CategoryPropsContext = Field(default_factory=CategoryPropsContext)
    nutrition: NutritionContext = Field(default_factory=NutritionContext)

    @model_validator(mode="after")
    def validate_product_code(self) -> NormalizedContext:
        """Keep the outer code and product projection aligned."""
        if self.product.code != self.code:
            raise ValueError(
                "Normalized context code must match product.code. "
                f"Got {self.code!r} and {self.product.code!r}."
            )
        return self

    def as_mapping(self) -> dict[str, Any]:
        """Expose the stable nested mapping used by path based evaluation."""
        return {
            "product": self.product.as_mapping(),
            "flags": self.flags.as_mapping(),
            "category_props": self.category_props.as_mapping(),
            "nutrition": self.nutrition.as_mapping(),
        }


def iter_normalized_context_path_specs() -> tuple[ContextPathSpec, ...]:
    """Derive every dotted normalized context path directly from the contract."""
    return tuple(_collect_path_specs(NormalizedContext))


def _collect_path_specs(
    model_type: type[BaseModel], *, prefix: str = ""
) -> list[ContextPathSpec]:
    """Collect path specs recursively from one contract model type."""
    specs: list[ContextPathSpec] = []

    for field_name, model_field in model_type.model_fields.items():
        path = f"{prefix}.{field_name}" if prefix else field_name
        metadata = _context_field_metadata(model_field.metadata)
        nested_model = _context_section_model(model_field.annotation)

        if metadata is not None:
            specs.append(
                ContextPathSpec(
                    path=path,
                    type=metadata.type,
                    dsl_allowed=metadata.dsl_allowed,
                    supported_input_surfaces=metadata.supported_input_surfaces,
                )
            )
            continue

        if nested_model is not None:
            specs.extend(_collect_path_specs(nested_model, prefix=path))

    return specs


def _context_field_metadata(metadata_items: list[Any]) -> ContextFieldMetadata | None:
    """Return the normalized context metadata item attached to one field."""
    for metadata_item in metadata_items:
        if isinstance(metadata_item, ContextFieldMetadata):
            return metadata_item
    return None


def _context_section_model(annotation: Any) -> type[ContextSectionModel] | None:
    """Return the section-model type carried by one annotation, if any."""
    origin = get_origin(annotation)

    if origin is Annotated:
        return _context_section_model(get_args(annotation)[0])

    if origin in (UnionType,):
        candidates = [arg for arg in get_args(annotation) if arg is not NoneType]
        if len(candidates) == 1:
            return _context_section_model(candidates[0])
        return None

    if isinstance(annotation, type) and issubclass(annotation, ContextSectionModel):
        return annotation

    return None
