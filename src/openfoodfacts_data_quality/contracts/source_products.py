from __future__ import annotations

from collections.abc import Mapping

from pydantic import BaseModel, ConfigDict

SourceProductNumericValue = str | float | int | None
SourceProductTagValue = str | list[object] | None
SourceProductFlagValue = str | float | int | bool | None

SOURCE_PRODUCT_BASE_FIELD_TO_COLUMN: dict[str, str] = {
    "code": "code",
    "created_t": "created_t",
    "product_name": "product_name",
    "quantity": "quantity",
    "product_quantity": "product_quantity",
    "serving_size": "serving_size",
    "serving_quantity": "serving_quantity",
    "brands": "brands",
    "categories": "categories",
    "labels": "labels",
    "emb_codes": "emb_codes",
    "ingredients_text": "ingredients_text",
    "ingredients_tags": "ingredients_tags",
    "nutriscore_grade": "nutriscore_grade",
    "nutriscore_score": "nutriscore_score",
    "categories_tags": "categories_tags",
    "labels_tags": "labels_tags",
    "countries_tags": "countries_tags",
    "no_nutrition_data": "no_nutrition_data",
}
SOURCE_PRODUCT_BASE_COLUMN_TO_FIELD: dict[str, str] = {
    column_name: field_name
    for field_name, column_name in SOURCE_PRODUCT_BASE_FIELD_TO_COLUMN.items()
}


class SourceProduct(BaseModel):
    """Explicit source product contract shared by the app and library."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: str
    created_t: SourceProductNumericValue = None
    product_name: str | None = None
    quantity: str | None = None
    product_quantity: SourceProductNumericValue = None
    serving_size: str | None = None
    serving_quantity: SourceProductNumericValue = None
    brands: str | None = None
    categories: str | None = None
    labels: str | None = None
    emb_codes: str | None = None
    ingredients_text: str | None = None
    ingredients_tags: SourceProductTagValue = None
    nutriscore_grade: str | None = None
    nutriscore_score: SourceProductNumericValue = None
    categories_tags: SourceProductTagValue = None
    labels_tags: SourceProductTagValue = None
    countries_tags: SourceProductTagValue = None
    no_nutrition_data: SourceProductFlagValue = None
    added_sugars_100g: SourceProductNumericValue = None
    alcohol_100g: SourceProductNumericValue = None
    alpha_linolenic_acid_100g: SourceProductNumericValue = None
    biotin_100g: SourceProductNumericValue = None
    caffeine_100g: SourceProductNumericValue = None
    calcium_100g: SourceProductNumericValue = None
    carbohydrates_total_100g: SourceProductNumericValue = None
    carbohydrates_100g: SourceProductNumericValue = None
    cholesterol_100g: SourceProductNumericValue = None
    choline_100g: SourceProductNumericValue = None
    chromium_100g: SourceProductNumericValue = None
    copper_100g: SourceProductNumericValue = None
    docosahexaenoic_acid_100g: SourceProductNumericValue = None
    eicosapentaenoic_acid_100g: SourceProductNumericValue = None
    energy_kcal_100g: SourceProductNumericValue = None
    energy_kj_100g: SourceProductNumericValue = None
    energy_100g: SourceProductNumericValue = None
    fat_100g: SourceProductNumericValue = None
    fiber_100g: SourceProductNumericValue = None
    fructose_100g: SourceProductNumericValue = None
    fruits_vegetables_legumes_estimate_from_ingredients_100g: SourceProductNumericValue = None
    fruits_vegetables_legumes_100g: SourceProductNumericValue = None
    fruits_vegetables_nuts_estimate_from_ingredients_100g: SourceProductNumericValue = (
        None
    )
    fruits_vegetables_nuts_100g: SourceProductNumericValue = None
    glucose_100g: SourceProductNumericValue = None
    glycemic_index_100g: SourceProductNumericValue = None
    iodine_100g: SourceProductNumericValue = None
    iron_100g: SourceProductNumericValue = None
    magnesium_100g: SourceProductNumericValue = None
    maltose_100g: SourceProductNumericValue = None
    manganese_100g: SourceProductNumericValue = None
    monounsaturated_fat_100g: SourceProductNumericValue = None
    nova_group_100g: SourceProductNumericValue = None
    nutrition_score_fr_100g: SourceProductNumericValue = None
    omega_3_fat_100g: SourceProductNumericValue = None
    omega_6_fat_100g: SourceProductNumericValue = None
    pantothenic_acid_100g: SourceProductNumericValue = None
    phosphorus_100g: SourceProductNumericValue = None
    polyols_100g: SourceProductNumericValue = None
    polyunsaturated_fat_100g: SourceProductNumericValue = None
    potassium_100g: SourceProductNumericValue = None
    proteins_100g: SourceProductNumericValue = None
    salt_100g: SourceProductNumericValue = None
    saturated_fat_100g: SourceProductNumericValue = None
    selenium_100g: SourceProductNumericValue = None
    sodium_100g: SourceProductNumericValue = None
    starch_100g: SourceProductNumericValue = None
    sucrose_100g: SourceProductNumericValue = None
    sugars_100g: SourceProductNumericValue = None
    taurine_100g: SourceProductNumericValue = None
    trans_fat_100g: SourceProductNumericValue = None
    vitamin_a_100g: SourceProductNumericValue = None
    vitamin_b12_100g: SourceProductNumericValue = None
    vitamin_b1_100g: SourceProductNumericValue = None
    vitamin_b2_100g: SourceProductNumericValue = None
    vitamin_b6_100g: SourceProductNumericValue = None
    vitamin_b9_100g: SourceProductNumericValue = None
    vitamin_c_100g: SourceProductNumericValue = None
    vitamin_d_100g: SourceProductNumericValue = None
    vitamin_e_100g: SourceProductNumericValue = None
    vitamin_k_100g: SourceProductNumericValue = None
    vitamin_pp_100g: SourceProductNumericValue = None
    zinc_100g: SourceProductNumericValue = None

    def as_mapping(self) -> dict[str, object]:
        """Expose the stable source product mapping shape with Open Food Facts column names."""
        values = self.model_dump(mode="python")
        return {
            _source_product_field_to_column(field_name): value
            for field_name, value in values.items()
        }


def _source_product_field_to_column(field_name: str) -> str:
    """Translate one model field name into the canonical source column name."""
    return _translate_source_product_name(
        field_name,
        mapping=SOURCE_PRODUCT_BASE_FIELD_TO_COLUMN,
        source_separator="_",
        target_separator="-",
    )


def _source_product_column_to_field(column_name: str) -> str:
    """Translate one source input column name into the canonical model field name."""
    return _translate_source_product_name(
        column_name,
        mapping=SOURCE_PRODUCT_BASE_COLUMN_TO_FIELD,
        source_separator="-",
        target_separator="_",
    )


def _translate_source_product_name(
    value: str,
    *,
    mapping: Mapping[str, str],
    source_separator: str,
    target_separator: str,
) -> str:
    """Translate one source product field or column name through the canonical rules."""
    mapped_value = mapping.get(value)
    if mapped_value is not None:
        return mapped_value
    normalized_value = value.replace("_100g", "-100g")
    return normalized_value.replace(source_separator, target_separator).replace(
        "-100g", "_100g"
    )


SOURCE_PRODUCT_COLUMNS: tuple[str, ...] = tuple(
    _source_product_field_to_column(field_name)
    for field_name in SourceProduct.model_fields
    if not field_name.endswith("_100g")
)
SOURCE_PRODUCT_NUTRIMENT_COLUMNS: tuple[str, ...] = tuple(
    _source_product_field_to_column(field_name)
    for field_name in SourceProduct.model_fields
    if field_name.endswith("_100g")
)
SOURCE_PRODUCT_INPUT_COLUMNS: tuple[str, ...] = tuple(
    (*SOURCE_PRODUCT_COLUMNS, *SOURCE_PRODUCT_NUTRIMENT_COLUMNS)
)


def validate_source_product(
    value: SourceProduct | Mapping[str, object],
) -> SourceProduct:
    """Return one validated source product contract object."""
    if isinstance(value, SourceProduct):
        return value
    normalized = {
        _source_product_column_to_field(column_name): column_value
        for column_name, column_value in value.items()
    }
    return SourceProduct.model_validate(normalized)
