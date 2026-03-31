from __future__ import annotations

from openfoodfacts_data_quality.checks.legacy import (
    legacy_code_template_key,
    matches_legacy_check_code,
)


def test_matches_legacy_check_code_matches_exact_ids() -> None:
    assert matches_legacy_check_code(
        "en:quantity-to-be-completed", "en:quantity-to-be-completed"
    )
    assert not matches_legacy_check_code(
        "en:quantity-to-be-completed",
        "en:product-name-to-be-completed",
    )


def test_matches_legacy_check_code_matches_template_ids() -> None:
    check_id = "en:${set_id}-energy-value-in-${unit}-does-not-match-value-computed-from-other-nutrients"
    assert matches_legacy_check_code(
        check_id,
        "en:debug-energy-value-in-kcal-does-not-match-value-computed-from-other-nutrients",
    )
    assert matches_legacy_check_code(
        check_id,
        "en:nutrition-packaging-as-sold-100g-energy-value-in-kj-does-not-match-value-computed-from-other-nutrients",
    )
    assert not matches_legacy_check_code(
        check_id,
        "en:quantity-to-be-completed",
    )
    assert not matches_legacy_check_code(
        check_id,
        "en:debug-energy-value-in-bad:value-does-not-match-value-computed-from-other-nutrients",
    )


def test_legacy_code_template_key_normalizes_template_variable_names() -> None:
    assert legacy_code_template_key(
        "en:ingredients-${lang_code}-photo-selected"
    ) == legacy_code_template_key("en:ingredients-${display_lc}-photo-selected")
