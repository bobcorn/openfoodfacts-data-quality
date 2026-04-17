from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from operator import eq, ge, gt, le, lt, ne

from off_data_quality._scalars import as_number
from off_data_quality.checks.dsl.ast import (
    All,
    AnyOf,
    Atom,
    DSLDefinition,
    Expression,
    Not,
)
from off_data_quality.context._paths import MISSING, is_blank, resolve_path
from off_data_quality.contracts.checks import CheckEmission
from off_data_quality.contracts.context import CheckContext

CheckEvaluator = Callable[[CheckContext], list[CheckEmission]]
_BOOLEAN_OPERATOR_VALUES = {"is_true": True, "is_false": False}
_SCALAR_COMPARATORS = {"eq": eq, "ne": ne}
_NUMERIC_COMPARATORS = {"gt": gt, "gte": ge, "lt": lt, "lte": le}


def compile_dsl_evaluators(
    checks: Sequence[DSLDefinition],
) -> dict[str, CheckEvaluator]:
    """Compile one sequence of DSL definitions into evaluator callables."""
    return {check.id: _compile_definition(check) for check in checks}


def _compile_definition(check: DSLDefinition) -> CheckEvaluator:
    """Compile one DSL definition into a context evaluator."""

    def evaluator(context: CheckContext) -> list[CheckEmission]:
        if not evaluate_expression(check.when, context.as_mapping()):
            return []
        return [CheckEmission(severity=check.severity)]

    return evaluator


def evaluate_expression(expression: Expression, payload: Mapping[str, object]) -> bool:
    """Evaluate one DSL expression against the check context mapping."""
    if isinstance(expression, All):
        return all(evaluate_expression(item, payload) for item in expression.items)
    if isinstance(expression, AnyOf):
        return any(evaluate_expression(item, payload) for item in expression.items)
    if isinstance(expression, Not):
        return not evaluate_expression(expression.item, payload)
    return _evaluate_atom(expression, payload)


def _evaluate_atom(expression: Atom, payload: Mapping[str, object]) -> bool:
    """Evaluate one predicate atom against the check context mapping."""
    value = resolve_path(payload, expression.field)
    if _matches_missing_or_blank(expression, value):
        return True
    if expression.op in {"is_missing", "is_blank"}:
        return False
    if value is MISSING:
        return False
    boolean_result = _evaluate_boolean_operator(expression, value)
    if boolean_result is not None:
        return boolean_result
    scalar_result = _evaluate_scalar_operator(expression, value)
    if scalar_result is not None:
        return scalar_result
    numeric_result = _evaluate_numeric_operator(expression, value)
    if numeric_result is not None:
        return numeric_result
    if expression.op == "contains":
        return _contains_value(value, expression.value)
    if expression.op == "in":
        return _value_is_in_collection(value, expression.value)
    if expression.op == "not_in":
        return not _value_is_in_collection(value, expression.value)
    raise ValueError(f"Unsupported DSL operator '{expression.op}'.")


def _matches_missing_or_blank(expression: Atom, value: object) -> bool:
    """Evaluate null related operators before any other narrowing."""
    if expression.op == "is_missing":
        return value is MISSING
    if expression.op == "is_blank":
        return is_blank(value)
    return False


def _evaluate_boolean_operator(expression: Atom, value: object) -> bool | None:
    """Evaluate boolean identity operators when present."""
    expected = _BOOLEAN_OPERATOR_VALUES.get(expression.op)
    if expected is None:
        return None
    return value is expected


def _evaluate_scalar_operator(expression: Atom, value: object) -> bool | None:
    """Evaluate equality operators for scalar values."""
    comparator = _SCALAR_COMPARATORS.get(expression.op)
    if comparator is None:
        return None
    return bool(comparator(value, expression.value))


def _evaluate_numeric_operator(expression: Atom, value: object) -> bool | None:
    """Evaluate numeric comparison operators after numeric coercion."""
    comparator = _NUMERIC_COMPARATORS.get(expression.op)
    if comparator is None:
        return None
    return _compare_numeric(value, expression.value, comparator)


def _compare_numeric(
    value: object,
    other: object,
    comparator: Callable[[float, float], bool],
) -> bool:
    """Compare two values only when both can be interpreted as numbers."""
    value_number = as_number(value)
    other_number = as_number(other)
    if value_number is None or other_number is None:
        return False
    return comparator(value_number, other_number)


def _contains_value(value: object, expected: object) -> bool:
    """Return whether one collection contains the expected item."""
    if not isinstance(value, list):
        return False
    return expected in value


def _value_is_in_collection(value: object, candidates: object) -> bool:
    """Return whether one value belongs to a candidate collection."""
    if not isinstance(candidates, list):
        return False
    return value in candidates
