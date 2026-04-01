from __future__ import annotations

from collections.abc import Mapping

from pydantic import BaseModel, ConfigDict

RawNumericValue = str | float | int | None
RawTagValue = str | list[object] | None
RawFlagValue = str | float | int | bool | None

RAW_ROW_BASE_FIELD_TO_COLUMN: dict[str, str] = {
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
RAW_ROW_BASE_COLUMN_TO_FIELD: dict[str, str] = {
    column_name: field_name
    for field_name, column_name in RAW_ROW_BASE_FIELD_TO_COLUMN.items()
}


class RawProductRow(BaseModel):
    """Explicit raw input contract shared by the app and library surfaces."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: str
    created_t: RawNumericValue = None
    product_name: str | None = None
    quantity: str | None = None
    product_quantity: RawNumericValue = None
    serving_size: str | None = None
    serving_quantity: RawNumericValue = None
    brands: str | None = None
    categories: str | None = None
    labels: str | None = None
    emb_codes: str | None = None
    ingredients_text: str | None = None
    ingredients_tags: RawTagValue = None
    nutriscore_grade: str | None = None
    nutriscore_score: RawNumericValue = None
    categories_tags: RawTagValue = None
    labels_tags: RawTagValue = None
    countries_tags: RawTagValue = None
    no_nutrition_data: RawFlagValue = None
    added_sugars_100g: RawNumericValue = None
    alcohol_100g: RawNumericValue = None
    alpha_linolenic_acid_100g: RawNumericValue = None
    biotin_100g: RawNumericValue = None
    caffeine_100g: RawNumericValue = None
    calcium_100g: RawNumericValue = None
    carbohydrates_total_100g: RawNumericValue = None
    carbohydrates_100g: RawNumericValue = None
    cholesterol_100g: RawNumericValue = None
    choline_100g: RawNumericValue = None
    chromium_100g: RawNumericValue = None
    copper_100g: RawNumericValue = None
    docosahexaenoic_acid_100g: RawNumericValue = None
    eicosapentaenoic_acid_100g: RawNumericValue = None
    energy_kcal_100g: RawNumericValue = None
    energy_kj_100g: RawNumericValue = None
    energy_100g: RawNumericValue = None
    fat_100g: RawNumericValue = None
    fiber_100g: RawNumericValue = None
    fructose_100g: RawNumericValue = None
    fruits_vegetables_legumes_estimate_from_ingredients_100g: RawNumericValue = None
    fruits_vegetables_legumes_100g: RawNumericValue = None
    fruits_vegetables_nuts_estimate_from_ingredients_100g: RawNumericValue = None
    fruits_vegetables_nuts_100g: RawNumericValue = None
    glucose_100g: RawNumericValue = None
    glycemic_index_100g: RawNumericValue = None
    iodine_100g: RawNumericValue = None
    iron_100g: RawNumericValue = None
    magnesium_100g: RawNumericValue = None
    maltose_100g: RawNumericValue = None
    manganese_100g: RawNumericValue = None
    monounsaturated_fat_100g: RawNumericValue = None
    nova_group_100g: RawNumericValue = None
    nutrition_score_fr_100g: RawNumericValue = None
    omega_3_fat_100g: RawNumericValue = None
    omega_6_fat_100g: RawNumericValue = None
    pantothenic_acid_100g: RawNumericValue = None
    phosphorus_100g: RawNumericValue = None
    polyols_100g: RawNumericValue = None
    polyunsaturated_fat_100g: RawNumericValue = None
    potassium_100g: RawNumericValue = None
    proteins_100g: RawNumericValue = None
    salt_100g: RawNumericValue = None
    saturated_fat_100g: RawNumericValue = None
    selenium_100g: RawNumericValue = None
    sodium_100g: RawNumericValue = None
    starch_100g: RawNumericValue = None
    sucrose_100g: RawNumericValue = None
    sugars_100g: RawNumericValue = None
    taurine_100g: RawNumericValue = None
    trans_fat_100g: RawNumericValue = None
    vitamin_a_100g: RawNumericValue = None
    vitamin_b12_100g: RawNumericValue = None
    vitamin_b1_100g: RawNumericValue = None
    vitamin_b2_100g: RawNumericValue = None
    vitamin_b6_100g: RawNumericValue = None
    vitamin_b9_100g: RawNumericValue = None
    vitamin_c_100g: RawNumericValue = None
    vitamin_d_100g: RawNumericValue = None
    vitamin_e_100g: RawNumericValue = None
    vitamin_k_100g: RawNumericValue = None
    vitamin_pp_100g: RawNumericValue = None
    zinc_100g: RawNumericValue = None

    def as_mapping(self) -> dict[str, object]:
        """Expose the stable raw row mapping shape with OFF column names."""
        values = self.model_dump(mode="python")
        return {
            _raw_row_field_to_column(field_name): value
            for field_name, value in values.items()
        }


def _raw_row_field_to_column(field_name: str) -> str:
    """Translate one model field name into the canonical raw column name."""
    return _translate_raw_row_name(
        field_name,
        mapping=RAW_ROW_BASE_FIELD_TO_COLUMN,
        source_separator="_",
        target_separator="-",
    )


def _raw_row_column_to_field(column_name: str) -> str:
    """Translate one raw input column name into the canonical model field name."""
    return _translate_raw_row_name(
        column_name,
        mapping=RAW_ROW_BASE_COLUMN_TO_FIELD,
        source_separator="-",
        target_separator="_",
    )


def _translate_raw_row_name(
    value: str,
    *,
    mapping: Mapping[str, str],
    source_separator: str,
    target_separator: str,
) -> str:
    """Translate one raw-contract field or column name through the canonical rules."""
    mapped_value = mapping.get(value)
    if mapped_value is not None:
        return mapped_value
    normalized_value = value.replace("_100g", "-100g")
    return normalized_value.replace(source_separator, target_separator).replace(
        "-100g", "_100g"
    )


RAW_PRODUCT_COLUMNS: tuple[str, ...] = tuple(
    _raw_row_field_to_column(field_name)
    for field_name in RawProductRow.model_fields
    if not field_name.endswith("_100g")
)
RAW_NUTRIMENT_COLUMNS: tuple[str, ...] = tuple(
    _raw_row_field_to_column(field_name)
    for field_name in RawProductRow.model_fields
    if field_name.endswith("_100g")
)
RAW_INPUT_COLUMNS: tuple[str, ...] = tuple(
    (*RAW_PRODUCT_COLUMNS, *RAW_NUTRIMENT_COLUMNS)
)


def validate_raw_product_row(
    value: RawProductRow | Mapping[str, object],
) -> RawProductRow:
    """Return one validated raw row contract object."""
    if isinstance(value, RawProductRow):
        return value
    normalized = {
        _raw_row_column_to_field(column_name): column_value
        for column_name, column_value in value.items()
    }
    return RawProductRow.model_validate(normalized)
