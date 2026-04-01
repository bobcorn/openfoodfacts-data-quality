from __future__ import annotations

from openfoodfacts_data_quality.checks.dsl.ast import (
    All,
    AnyOf,
    Atom,
    DSLDefinition,
    Expression,
    Not,
)
from openfoodfacts_data_quality.context.paths import ContextPathSpec, path_spec_for
from openfoodfacts_data_quality.structured_values import is_object_list

_BOOLEAN_OPERATORS = frozenset({"is_true", "is_false"})
_NUMERIC_COMPARISON_OPERATORS = frozenset({"gt", "gte", "lt", "lte"})
_MEMBERSHIP_OPERATORS = frozenset({"in", "not_in"})
_SCALAR_COMPARISON_OPERATORS = frozenset({"eq", "ne"})


def validate_dsl_definitions(checks: list[DSLDefinition]) -> None:
    """Run semantic validation after YAML parsing and structural validation."""
    actual_ids = [check.id for check in checks]
    duplicate_ids = sorted(
        {check_id for check_id in actual_ids if actual_ids.count(check_id) > 1}
    )
    if duplicate_ids:
        raise ValueError(f"Duplicate DSL definition ids: {', '.join(duplicate_ids)}")

    for check in checks:
        _validate_expression(check.when, check.id)


def collect_required_paths(expression: Expression) -> tuple[str, ...]:
    """Collect first seen atom fields from one DSL expression."""
    ordered_paths: list[str] = []
    seen: set[str] = set()

    def visit(node: Expression) -> None:
        if isinstance(node, All):
            for item in node.items:
                visit(item)
            return
        if isinstance(node, AnyOf):
            for item in node.items:
                visit(item)
            return
        if isinstance(node, Not):
            visit(node.item)
            return
        if node.field in seen:
            return
        seen.add(node.field)
        ordered_paths.append(node.field)

    visit(expression)
    return tuple(ordered_paths)


def _validate_expression(expression: Expression, check_id: str) -> None:
    """Validate one DSL expression recursively."""
    child_expressions = _child_expressions(expression)
    if child_expressions is not None:
        for item in child_expressions:
            _validate_expression(item, check_id)
        return

    assert isinstance(expression, Atom)
    _validate_atom(expression, check_id)


def _validate_atom(expression: Atom, check_id: str) -> None:
    """Validate one predicate atom against the normalized context contract."""
    spec = _validated_path_spec(expression, check_id)
    if expression.op in _BOOLEAN_OPERATORS:
        _validate_boolean_atom(expression, spec.type)
        return
    if expression.op in _NUMERIC_COMPARISON_OPERATORS:
        _validate_numeric_atom(expression, spec.type, check_id)
        return
    if expression.op == "contains":
        _validate_contains_atom(expression, spec.type, check_id)
        return
    if expression.op in _MEMBERSHIP_OPERATORS:
        _validate_membership_atom(expression, check_id)
        return
    if expression.op in _SCALAR_COMPARISON_OPERATORS:
        _validate_scalar_comparison_atom(expression, check_id)


def _child_expressions(expression: Expression) -> tuple[Expression, ...] | None:
    """Return child nodes for composite expressions, if any."""
    if isinstance(expression, (All, AnyOf)):
        return expression.items
    if isinstance(expression, Not):
        return (expression.item,)
    return None


def _validated_path_spec(expression: Atom, check_id: str) -> ContextPathSpec:
    """Return the path spec after validating the field is DSL-visible."""
    spec = path_spec_for(expression.field)
    if spec is None:
        raise ValueError(
            f"Unknown normalized context field '{expression.field}' in DSL definition '{check_id}'."
        )
    if not spec.dsl_allowed:
        raise ValueError(
            f"Field '{expression.field}' is not exposed to the DSL in definition '{check_id}'."
        )
    if expression.field.startswith("openfoodfacts_data_quality_helpers."):
        raise ValueError(
            f"Helper-shaped field '{expression.field}' is forbidden in DSL definition '{check_id}'."
        )
    return spec


def _validate_boolean_atom(expression: Atom, field_type: str) -> None:
    """Validate boolean-only operators."""
    if field_type != "boolean":
        raise ValueError(
            f"Operator '{expression.op}' requires a boolean field, but '{expression.field}' is {field_type}."
        )


def _validate_numeric_atom(expression: Atom, field_type: str, check_id: str) -> None:
    """Validate numeric comparison operators."""
    if field_type != "number":
        raise ValueError(
            f"Operator '{expression.op}' requires a numeric field, but '{expression.field}' is {field_type}."
        )
    if not isinstance(expression.value, (int, float)):
        raise ValueError(
            f"Operator '{expression.op}' requires a numeric value in DSL definition '{check_id}'."
        )


def _validate_contains_atom(expression: Atom, field_type: str, check_id: str) -> None:
    """Validate array containment operators."""
    if field_type != "array":
        raise ValueError(
            f"Operator '{expression.op}' requires an array field, but '{expression.field}' is {field_type}."
        )
    if isinstance(expression.value, (dict, list)):
        raise ValueError(
            f"Operator '{expression.op}' only accepts scalar values in DSL definition '{check_id}'."
        )


def _validate_membership_atom(expression: Atom, check_id: str) -> None:
    """Validate membership operators that require scalar arrays."""
    if not is_object_list(expression.value):
        raise ValueError(
            f"Operator '{expression.op}' requires an array value in DSL definition '{check_id}'."
        )
    if any(isinstance(item, (dict, list)) for item in expression.value):
        raise ValueError(
            f"Operator '{expression.op}' only accepts scalar array values in DSL definition '{check_id}'."
        )


def _validate_scalar_comparison_atom(expression: Atom, check_id: str) -> None:
    """Validate equality-style operators on scalar values."""
    if isinstance(expression.value, (dict, list)):
        raise ValueError(
            f"Operator '{expression.op}' only accepts scalar values in DSL definition '{check_id}'."
        )
