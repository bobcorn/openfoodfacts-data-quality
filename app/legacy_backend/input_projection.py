from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel

from openfoodfacts_data_quality.raw_products import (
    build_input_sets,
    build_raw_classifier_fields,
    ingredient_tags_from_raw_row,
)
from openfoodfacts_data_quality.scalars import as_number


class LegacyBackendInputProduct(BaseModel):
    """Serialized backend input prepared for one legacy-backend execution."""

    code: str
    projected_input: dict[str, object]


def build_legacy_backend_input_products(
    rows: list[dict[str, Any]],
) -> list[LegacyBackendInputProduct]:
    """Convert raw OFF rows into backend input payloads."""
    return [
        LegacyBackendInputProduct(
            code=str(row["code"]),
            projected_input=project_legacy_backend_input_product(row),
        )
        for row in rows
    ]


def project_legacy_backend_input_product(
    row: Mapping[str, object],
) -> dict[str, object]:
    """Project a raw OFF row into the explicit backend input contract."""
    ingredient_tags = ingredient_tags_from_raw_row(row)
    classifier_fields = build_raw_classifier_fields(row)
    ingredients = _ingredient_objects_from_tags(ingredient_tags)
    product: dict[str, object] = _compact_mapping(
        {
            "code": str(row["code"]),
            "created_t": as_number(row.get("created_t")),
            "product_name": _optional_text(row.get("product_name")),
            "quantity": _optional_text(row.get("quantity")),
            "product_quantity": as_number(row.get("product_quantity")),
            "serving_size": _optional_text(row.get("serving_size")),
            "serving_quantity": as_number(row.get("serving_quantity")),
            "brands": _optional_text(row.get("brands")),
            "categories": _optional_text(row.get("categories")),
            "labels": _optional_text(row.get("labels")),
            "emb_codes": _optional_text(row.get("emb_codes")),
            "ingredients_text": _optional_text(row.get("ingredients_text")),
            "ingredients": ingredients,
            **classifier_fields,
        }
    )

    nutrition = _build_legacy_backend_nutrition(row)
    if nutrition:
        product["nutrition"] = nutrition

    if _has_truthy_text(row.get("no_nutrition_data")):
        existing_nutrition = product.get("nutrition")
        if not isinstance(existing_nutrition, dict):
            existing_nutrition = {}
            product["nutrition"] = existing_nutrition
        existing_nutrition["no_nutrition_data_on_packaging"] = True

    return product


def _compact_mapping(values: Mapping[str, object]) -> dict[str, object]:
    """Drop empty backend-input fields while preserving `False` and numeric zero."""
    return {key: value for key, value in values.items() if value not in (None, [], {})}


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


def _ingredient_objects_from_tags(ingredient_tags: list[str]) -> list[dict[str, str]]:
    """Convert ingredient tag ids into the explicit backend ingredient shape."""
    return [
        {"id": ingredient_tag} for ingredient_tag in ingredient_tags if ingredient_tag
    ]


def _build_legacy_backend_nutrition(row: Mapping[str, object]) -> dict[str, object]:
    """Build the minimal nutrition payload expected by the backend runner."""
    input_sets = build_input_sets(row)
    if not input_sets:
        return {}
    return {"input_sets": input_sets}
