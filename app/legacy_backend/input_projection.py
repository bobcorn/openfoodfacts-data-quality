from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

from pydantic import BaseModel

from app.legacy_backend.contracts import (
    LegacyBackendInputIngredient,
    LegacyBackendInputNutrition,
    LegacyBackendInputPayload,
)
from openfoodfacts_data_quality.contracts.raw import RawProductRow
from openfoodfacts_data_quality.raw_products import (
    build_input_sets,
    build_raw_classifier_fields,
    ingredient_tags_from_raw_row,
)
from openfoodfacts_data_quality.scalars import as_number
from openfoodfacts_data_quality.source_rows import normalize_raw_input_row


class LegacyBackendInputProduct(BaseModel, frozen=True, extra="forbid"):
    """Serialized backend input prepared for one legacy backend execution."""

    code: str
    projected_input: LegacyBackendInputPayload

    def serialized_input(self) -> dict[str, object]:
        """Return the backend JSON payload while preserving prior omission semantics."""
        return _compact_backend_mapping(
            cast(dict[str, object], self.projected_input.as_mapping())
        )


def build_legacy_backend_input_products(
    rows: Sequence[RawProductRow | Mapping[str, object]],
) -> list[LegacyBackendInputProduct]:
    """Convert raw Open Food Facts rows into backend input payloads."""
    return [_legacy_backend_input_product(raw_row) for raw_row in rows]


def _legacy_backend_input_product(
    row: RawProductRow | Mapping[str, object],
) -> LegacyBackendInputProduct:
    """Build one backend input wrapper from one supported raw row shape."""
    raw_row = _validated_raw_row(row)
    return LegacyBackendInputProduct(
        code=raw_row.code,
        projected_input=project_legacy_backend_input_product(raw_row),
    )


def project_legacy_backend_input_product(
    row: RawProductRow | Mapping[str, object],
) -> LegacyBackendInputPayload:
    """Project a raw Open Food Facts row into the explicit backend input contract."""
    raw_row = _validated_raw_row(row)
    ingredient_tags = ingredient_tags_from_raw_row(raw_row)
    classifier_fields = build_raw_classifier_fields(raw_row)
    ingredients = _ingredient_objects_from_tags(ingredient_tags)
    nutrition = _build_legacy_backend_nutrition(raw_row)
    return LegacyBackendInputPayload(
        code=raw_row.code,
        created_t=as_number(raw_row.created_t),
        product_name=_optional_text(raw_row.product_name),
        quantity=_optional_text(raw_row.quantity),
        product_quantity=as_number(raw_row.product_quantity),
        serving_size=_optional_text(raw_row.serving_size),
        serving_quantity=as_number(raw_row.serving_quantity),
        brands=_optional_text(raw_row.brands),
        categories=_optional_text(raw_row.categories),
        labels=_optional_text(raw_row.labels),
        emb_codes=_optional_text(raw_row.emb_codes),
        ingredients_text=_optional_text(raw_row.ingredients_text),
        ingredients=ingredients,
        nutriscore_grade=_optional_text(raw_row.nutriscore_grade),
        nutriscore_score=as_number(raw_row.nutriscore_score),
        categories_tags=cast(list[str], classifier_fields["categories_tags"]),
        labels_tags=cast(list[str], classifier_fields["labels_tags"]),
        countries_tags=cast(list[str], classifier_fields["countries_tags"]),
        nutrition=nutrition,
    )


def _validated_raw_row(
    row: RawProductRow | Mapping[str, object],
) -> RawProductRow:
    """Return one validated raw row contract object for adapter usage."""
    return normalize_raw_input_row(row)


def _optional_text(value: object) -> str | None:
    """Return a stripped optional text value while keeping blank semantics explicit."""
    if not value:
        return None
    text = str(value).strip()
    return text or None


def _has_truthy_text(value: object) -> bool:
    """Return whether a loose text flag should count as set."""
    if not value:
        return False
    return str(value).strip() != ""


def _ingredient_objects_from_tags(
    ingredient_tags: list[str],
) -> list[LegacyBackendInputIngredient]:
    """Convert ingredient tag ids into the explicit backend ingredient shape."""
    return [
        LegacyBackendInputIngredient(id=ingredient_tag)
        for ingredient_tag in ingredient_tags
        if ingredient_tag
    ]


def _build_legacy_backend_nutrition(
    row: RawProductRow,
) -> LegacyBackendInputNutrition | None:
    """Build the minimal nutrition payload expected by the backend runner."""
    input_sets = build_input_sets(row)
    no_nutrition_data_on_packaging = (
        True if _has_truthy_text(row.no_nutrition_data) else None
    )
    if not input_sets and no_nutrition_data_on_packaging is None:
        return None
    return LegacyBackendInputNutrition(
        input_sets=input_sets,
        no_nutrition_data_on_packaging=no_nutrition_data_on_packaging,
    )


def _compact_backend_mapping(values: Mapping[str, object]) -> dict[str, object]:
    """Drop empty backend input fields while preserving `False` and numeric zero."""
    compacted: dict[str, object] = {}
    for key, value in values.items():
        compacted_value = _compact_backend_value(value)
        if compacted_value in (None, [], {}):
            continue
        compacted[key] = compacted_value
    return compacted


def _compact_backend_value(value: object) -> object:
    """Recursively compact backend payload values."""
    if isinstance(value, dict):
        return _compact_backend_mapping(cast(dict[str, object], value))
    if isinstance(value, list):
        return [_compact_backend_value(item) for item in cast(list[object], value)]
    return value
