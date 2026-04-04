from __future__ import annotations

from openfoodfacts_data_quality.source_rows import (
    normalize_public_csv_export_row,
    normalize_public_source_row,
)


def test_normalize_public_source_row_accepts_nested_public_values() -> None:
    row = normalize_public_source_row(
        {
            "code": "123",
            "created_t": 123,
            "product_name": [
                {"lang": "fr", "text": "Exemple"},
                {"lang": "main", "text": "Example"},
            ],
            "ingredients_text": [{"lang": "main", "text": "Sugar, salt"}],
            "ingredients_tags": ["en:sugar", "en:salt"],
            "categories_tags": ["en:supplements"],
            "labels_tags": ["en:vegan"],
            "countries_tags": ["en:france"],
            "nutriments": [
                {"name": "energy-kcal", "100g": 123.0},
                {"name": "fat", "100g": 3.5},
                {"name": "unsupported-nutrient", "100g": 9.0},
            ],
        }
    )

    assert row.code == "123"
    assert row.product_name == "Example"
    assert row.ingredients_text == "Sugar, salt"
    assert row.ingredients_tags == ["en:sugar", "en:salt"]
    assert row.energy_kcal_100g == 123.0
    assert row.fat_100g == 3.5
    assert row.as_mapping().get("unsupported-nutrient_100g") is None


def test_normalize_public_csv_export_row_accepts_flat_public_csv_values() -> None:
    row = normalize_public_csv_export_row(
        {
            "code": "123",
            "url": "https://example.test/products/123",
            "creator": "example-user",
            "product_name": "Example",
            "ingredients_text": "Sugar, salt",
            "ingredients_tags": "en:sugar,en:salt",
            "categories_tags": "en:supplements",
            "labels_tags": "en:vegan",
            "countries_tags": "en:france",
            "energy-kcal_100g": "123.0",
            "fat_100g": "3.5",
        }
    )

    assert row.product_name == "Example"
    assert row.ingredients_text == "Sugar, salt"
    assert row.ingredients_tags == "en:sugar,en:salt"
    assert row.categories_tags == "en:supplements"
    assert row.labels_tags == "en:vegan"
    assert row.countries_tags == "en:france"
    assert row.energy_kcal_100g == "123.0"
    assert row.fat_100g == "3.5"
