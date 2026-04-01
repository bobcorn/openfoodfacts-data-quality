from __future__ import annotations

from pathlib import Path

import pytest
from jsonschema import ValidationError

from openfoodfacts_data_quality.checks.catalog import get_default_check_catalog
from openfoodfacts_data_quality.checks.dsl.ast import (
    All,
    AnyOf,
    Atom,
    DSLDefinition,
    Not,
)
from openfoodfacts_data_quality.checks.dsl.evaluator import evaluate_expression
from openfoodfacts_data_quality.checks.dsl.parser import load_dsl_definitions
from openfoodfacts_data_quality.checks.dsl.resources import dsl_check_pack_resources
from openfoodfacts_data_quality.checks.dsl.semantic import (
    collect_required_paths,
    validate_dsl_definitions,
)
from openfoodfacts_data_quality.context.paths import (
    MISSING,
    is_blank,
    resolve_path,
    supported_input_surfaces_for,
    validate_input_surface,
)

GLOBAL_DSL_CHECK_IDS = [
    check.id
    for check in get_default_check_catalog().checks
    if check.definition_language == "dsl" and "global" in check.jurisdictions
]
CANADA_DSL_CHECK_IDS = [
    check.id
    for check in get_default_check_catalog().checks
    if check.definition_language == "dsl" and "ca" in check.jurisdictions
]


def _global_dsl_pack_resource() -> Path:
    return next(
        Path(str(resource))
        for resource in dsl_check_pack_resources()
        if resource.name == "global_checks.yaml"
    )


def _canada_dsl_pack_resource() -> Path:
    return next(
        Path(str(resource))
        for resource in dsl_check_pack_resources()
        if resource.name == "canada_checks.yaml"
    )


def test_load_global_dsl_definitions_accepts_current_file() -> None:
    checks = load_dsl_definitions(_global_dsl_pack_resource())

    assert sorted(check.id for check in checks) == GLOBAL_DSL_CHECK_IDS


def test_load_canada_dsl_definitions_accepts_current_file() -> None:
    checks = load_dsl_definitions(_canada_dsl_pack_resource())

    assert sorted(check.id for check in checks) == CANADA_DSL_CHECK_IDS


def test_validate_dsl_definitions_rejects_helper_shaped_path() -> None:
    checks = _replace_when(
        load_dsl_definitions(_global_dsl_pack_resource()),
        "en:quantity-not-recognized",
        Atom(
            field="openfoodfacts_data_quality_helpers.is_european_product", op="is_true"
        ),
    )

    with pytest.raises(ValueError, match="Unknown normalized context field"):
        validate_dsl_definitions(checks)


def test_validate_dsl_definitions_rejects_non_exposed_path() -> None:
    checks = _replace_when(
        load_dsl_definitions(_global_dsl_pack_resource()),
        "en:quantity-not-recognized",
        Atom(field="nutrition.input_sets", op="is_blank"),
    )

    with pytest.raises(ValueError, match="is not exposed to the DSL"):
        validate_dsl_definitions(checks)


def test_validate_dsl_definitions_rejects_contains_on_scalar() -> None:
    checks = _replace_when(
        load_dsl_definitions(_global_dsl_pack_resource()),
        "en:quantity-not-recognized",
        Atom(field="product.quantity", op="contains", value="g"),
    )

    with pytest.raises(ValueError, match="requires an array field"):
        validate_dsl_definitions(checks)


def test_validate_dsl_definitions_rejects_duplicate_ids() -> None:
    duplicate = DSLDefinition(
        id="en:quantity-not-recognized",
        severity="warning",
        when=Atom(field="product.quantity", op="is_blank"),
        parity_baseline="legacy",
        jurisdictions=("global",),
    )

    with pytest.raises(ValueError, match="Duplicate DSL definition ids"):
        validate_dsl_definitions([duplicate, duplicate])


def test_validate_dsl_definitions_rejects_invalid_numeric_value() -> None:
    checks = _replace_when(
        load_dsl_definitions(_global_dsl_pack_resource()),
        "en:quantity-not-recognized",
        Atom(field="product.product_quantity", op="gt", value="not-a-number"),
    )

    with pytest.raises(ValueError, match="requires a numeric value"):
        validate_dsl_definitions(checks)


def test_validate_dsl_definitions_rejects_invalid_contains_and_membership_values() -> (
    None
):
    contains_checks = _replace_when(
        load_dsl_definitions(_global_dsl_pack_resource()),
        "en:quantity-not-recognized",
        Atom(field="product.categories_tags", op="contains", value=["en:dairies"]),
    )
    in_checks = _replace_when(
        load_dsl_definitions(_global_dsl_pack_resource()),
        "en:quantity-not-recognized",
        Atom(field="product.quantity", op="in", value="g"),
    )
    not_in_checks = _replace_when(
        load_dsl_definitions(_global_dsl_pack_resource()),
        "en:quantity-not-recognized",
        Atom(field="product.quantity", op="not_in", value=["g", {"bad": True}]),
    )

    with pytest.raises(ValueError, match="only accepts scalar values"):
        validate_dsl_definitions(contains_checks)
    with pytest.raises(ValueError, match="requires an array value"):
        validate_dsl_definitions(in_checks)
    with pytest.raises(ValueError, match="only accepts scalar array values"):
        validate_dsl_definitions(not_in_checks)


def test_validate_dsl_definitions_rejects_non_scalar_eq_value() -> None:
    checks = _replace_when(
        load_dsl_definitions(_global_dsl_pack_resource()),
        "en:quantity-not-recognized",
        Atom(field="product.quantity", op="eq", value={"bad": True}),
    )

    with pytest.raises(ValueError, match="only accepts scalar values"):
        validate_dsl_definitions(checks)


def test_collect_required_paths_preserves_first_seen_order() -> None:
    expression = All(
        items=(
            Atom(field="product.product_name", op="is_blank"),
            AnyOf(
                items=(
                    Atom(field="product.quantity", op="is_blank"),
                    Not(item=Atom(field="product.product_name", op="is_blank")),
                )
            ),
        )
    )

    assert collect_required_paths(expression) == (
        "product.product_name",
        "product.quantity",
    )


def test_evaluate_expression_uses_missing_boolean_array_and_combinator_semantics() -> (
    None
):
    payload = {
        "product": {
            "product_name": "",
            "quantity": "200 g",
            "product_quantity": 400.0,
            "serving_size": "50 g",
            "serving_quantity": 0.5,
            "categories_tags": ["en:plant-milks", "en:dairies"],
        },
        "flags": {
            "is_european_product": True,
            "has_animal_origin_category": False,
        },
    }

    expression = All(
        items=(
            Atom(field="product.product_name", op="is_blank"),
            Atom(field="product.categories_tags", op="contains", value="en:dairies"),
            AnyOf(
                items=(
                    Atom(field="flags.has_animal_origin_category", op="is_true"),
                    Not(
                        item=Atom(
                            field="flags.has_animal_origin_category", op="is_true"
                        )
                    ),
                )
            ),
            Atom(field="product.product_quantity", op="gt", value=300),
        )
    )

    assert evaluate_expression(expression, payload) is True
    assert (
        evaluate_expression(Atom(field="product.emb_codes", op="is_blank"), payload)
        is True
    )
    assert (
        evaluate_expression(Atom(field="product.emb_codes", op="is_missing"), payload)
        is True
    )
    assert (
        evaluate_expression(
            Atom(field="product.emb_codes", op="eq", value="FR 01"), payload
        )
        is False
    )


def test_evaluate_expression_supports_remaining_boolean_numeric_and_membership_ops() -> (
    None
):
    payload = {
        "product": {
            "quantity": "200 g",
            "product_quantity": 400.0,
            "serving_quantity": 50,
            "categories_tags": ["en:plant-milks", "en:dairies"],
        },
        "flags": {
            "has_animal_origin_category": False,
        },
    }

    assert (
        evaluate_expression(
            Atom(field="flags.has_animal_origin_category", op="is_false"),
            payload,
        )
        is True
    )
    assert (
        evaluate_expression(
            Atom(field="product.product_quantity", op="gte", value=400), payload
        )
        is True
    )
    assert (
        evaluate_expression(
            Atom(field="product.serving_quantity", op="lt", value=100), payload
        )
        is True
    )
    assert (
        evaluate_expression(
            Atom(field="product.serving_quantity", op="lte", value=50), payload
        )
        is True
    )
    assert (
        evaluate_expression(
            Atom(field="product.categories_tags", op="contains", value="en:dairies"),
            payload,
        )
        is True
    )
    assert (
        evaluate_expression(
            Atom(
                field="product.quantity",
                op="in",
                value=["100 g", "200 g"],
            ),
            payload,
        )
        is True
    )
    assert (
        evaluate_expression(
            Atom(
                field="product.quantity",
                op="not_in",
                value=["300 g", "400 g"],
            ),
            payload,
        )
        is True
    )


def test_validate_input_surface_and_path_helpers_cover_error_and_missing_cases() -> (
    None
):
    assert validate_input_surface("raw_products") == "raw_products"
    assert supported_input_surfaces_for(("product.code", "nutrition.input_sets")) == (
        "raw_products",
        "enriched_products",
    )
    assert resolve_path({"product": {}}, "product.quantity") is MISSING
    assert resolve_path({"product": None}, "product.quantity") is MISSING
    assert is_blank(MISSING) is True
    assert is_blank(None) is True
    assert is_blank("   ") is True
    assert is_blank([]) is True
    assert is_blank(123) is False

    with pytest.raises(ValueError, match="Unsupported input surface"):
        validate_input_surface("invalid")


def test_parser_rejects_structurally_invalid_gt_without_value(tmp_path: Path) -> None:
    invalid_yaml = """
metadata:
  parity_baseline: legacy
  jurisdictions: [global]
checks:
  - id: en:quantity-not-recognized
    severity: warning
    when:
      field: product.product_quantity
      op: gt
"""
    path = tmp_path / "invalid.yaml"
    path.write_text(invalid_yaml, encoding="utf-8")

    with pytest.raises(ValidationError):
        load_dsl_definitions(path)


def test_parser_accepts_explicit_legacy_code_template_override(tmp_path: Path) -> None:
    yaml_text = """
metadata:
  parity_baseline: legacy
  jurisdictions: [global]
checks:
  - id: en:quantity-not-recognized
    legacy_code_template: en:legacy-quantity-not-recognized
    severity: warning
    when:
      field: product.quantity
      op: is_blank
"""
    path = tmp_path / "legacy_identity.yaml"
    path.write_text(yaml_text, encoding="utf-8")

    checks = load_dsl_definitions(path)

    assert checks[0].legacy_identity is not None
    assert (
        checks[0].legacy_identity.code_template == "en:legacy-quantity-not-recognized"
    )


def test_parser_rejects_legacy_code_template_for_runtime_only_checks(
    tmp_path: Path,
) -> None:
    yaml_text = """
metadata:
  parity_baseline: none
  jurisdictions: [ca]
checks:
  - id: ca:runtime-only-check
    legacy_code_template: en:legacy-runtime-only-check
    severity: warning
    when:
      field: product.quantity
      op: is_blank
"""
    path = tmp_path / "runtime_only_legacy_identity.yaml"
    path.write_text(yaml_text, encoding="utf-8")

    with pytest.raises(
        ValueError,
        match='cannot declare a legacy identity without parity_baseline="legacy"',
    ):
        load_dsl_definitions(path)


def _replace_when(
    checks: list[DSLDefinition],
    check_id: str,
    when: All | AnyOf | Atom | Not,
) -> list[DSLDefinition]:
    """Return one DSL-definition list with a single definition replaced."""
    replaced: list[DSLDefinition] = []
    for check in checks:
        if check.id == check_id:
            replaced.append(
                DSLDefinition(
                    id=check.id,
                    severity=check.severity,
                    when=when,
                    parity_baseline=check.parity_baseline,
                    jurisdictions=check.jurisdictions,
                )
            )
            continue
        replaced.append(check)
    return replaced
