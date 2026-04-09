from __future__ import annotations

import json
from dataclasses import replace
from typing import TYPE_CHECKING, Protocol

import yaml
from jsonschema import Draft202012Validator

from openfoodfacts_data_quality.checks.dsl.ast import (
    All,
    AnyOf,
    Atom,
    DSLDefinition,
    Expression,
    Not,
    Operator,
)
from openfoodfacts_data_quality.checks.dsl.resources import dsl_schema_resource
from openfoodfacts_data_quality.checks.dsl.semantic import (
    collect_required_paths,
    validate_dsl_definitions,
)
from openfoodfacts_data_quality.contracts.checks import (
    CheckPackMetadata,
    normalize_check_jurisdictions,
    normalize_check_parity_baselines,
)
from openfoodfacts_data_quality.structured_values import (
    StringObjectMapping,
    is_string_object_mapping,
    object_list_or_empty,
)

if TYPE_CHECKING:
    from importlib.resources.abc import Traversable

    from openfoodfacts_data_quality.contracts.checks import (
        CheckJurisdiction,
        CheckParityBaseline,
        Severity,
    )

_SEVERITIES_BY_NAME: dict[str, Severity] = {
    "bug": "bug",
    "info": "info",
    "completeness": "completeness",
    "warning": "warning",
    "error": "error",
}

_OPERATORS_BY_NAME: dict[str, Operator] = {
    "is_missing": "is_missing",
    "is_blank": "is_blank",
    "is_true": "is_true",
    "is_false": "is_false",
    "eq": "eq",
    "ne": "ne",
    "gt": "gt",
    "gte": "gte",
    "lt": "lt",
    "lte": "lte",
    "contains": "contains",
    "in": "in",
    "not_in": "not_in",
}


def load_dsl_definitions(path: Traversable) -> list[DSLDefinition]:
    """Load DSL definitions from YAML, run structural validation, then build the AST."""
    payload = _require_mapping(
        yaml.safe_load(path.read_text(encoding="utf-8")) or {},
        label="DSL definitions payload",
    )
    schema = _require_mapping(
        json.loads(dsl_schema_resource().read_text(encoding="utf-8")),
        label="DSL schema",
    )
    _schema_validator(schema).validate(dict(payload))
    pack_metadata = _parse_pack_metadata(payload.get("metadata"))

    checks = [
        _parse_check(item, pack_metadata=pack_metadata)
        for item in _mapping_items(payload.get("checks"))
    ]
    validate_dsl_definitions(checks)
    return [
        replace(
            check,
            required_context_paths=collect_required_paths(check.when),
        )
        for check in checks
    ]


class _SchemaValidator(Protocol):
    def validate(self, _instance: object) -> None: ...


def _parse_check(
    payload: StringObjectMapping,
    *,
    pack_metadata: CheckPackMetadata,
) -> DSLDefinition:
    """Parse one validated DSL definition payload into the AST."""
    check_id = _required_string(payload, "id")
    return DSLDefinition(
        id=check_id,
        severity=_parse_severity(payload.get("severity")),
        when=_parse_expression(_require_mapping(payload.get("when"), label="DSL when")),
        parity_baseline=pack_metadata.parity_baseline,
        jurisdictions=pack_metadata.jurisdictions,
    )


def _parse_expression(payload: StringObjectMapping) -> Expression:
    """Parse one validated DSL expression payload into the AST."""
    if "all" in payload:
        return All(
            items=tuple(
                _parse_expression(item) for item in _mapping_items(payload["all"])
            )
        )
    if "any" in payload:
        return AnyOf(
            items=tuple(
                _parse_expression(item) for item in _mapping_items(payload["any"])
            )
        )
    if "not" in payload:
        return Not(
            item=_parse_expression(
                _require_mapping(payload["not"], label="DSL negated expression")
            )
        )
    return Atom(
        field=_required_string(payload, "field"),
        op=_parse_operator(payload.get("op")),
        value=payload.get("value"),
    )


def _schema_validator(schema: StringObjectMapping) -> _SchemaValidator:
    """Build a validator behind a small typed boundary."""
    return Draft202012Validator(dict(schema))


def _require_mapping(value: object, *, label: str) -> StringObjectMapping:
    """Return a string keyed mapping or raise a descriptive parse error."""
    if not is_string_object_mapping(value):
        raise ValueError(f"{label} must be a string keyed mapping.")
    return value


def _mapping_items(value: object) -> list[StringObjectMapping]:
    """Return a list of string keyed mappings from a payload slot."""
    items_source = object_list_or_empty(value)
    if not items_source:
        return []
    items: list[StringObjectMapping] = []
    for item in items_source:
        items.append(_require_mapping(item, label="DSL array item"))
    return items


def _parse_pack_metadata(value: object) -> CheckPackMetadata:
    """Return the required pack-level metadata block for one DSL file."""
    payload = _require_mapping(value, label="DSL metadata")
    return CheckPackMetadata(
        parity_baseline=_parse_parity_baseline(payload.get("parity_baseline")),
        jurisdictions=_parse_jurisdictions(payload.get("jurisdictions")),
    )


def _required_string(payload: StringObjectMapping, key: str) -> str:
    """Return a required not empty string field from one validated payload."""
    value = payload.get(key)
    if isinstance(value, str) and value:
        return value
    raise ValueError(f"DSL field {key!r} must be a not empty string.")


def _parse_severity(value: object) -> Severity:
    """Return a validated severity literal from one DSL payload."""
    return _parse_choice(
        value,
        choices=_SEVERITIES_BY_NAME,
        label="severity",
    )


def _parse_operator(value: object) -> Operator:
    """Return a validated operator literal from one DSL payload."""
    return _parse_choice(
        value,
        choices=_OPERATORS_BY_NAME,
        label="operator",
    )


def _parse_choice[LiteralValueT](
    value: object,
    *,
    choices: dict[str, LiteralValueT],
    label: str,
) -> LiteralValueT:
    """Return one validated literal from a string keyed choice table."""
    if isinstance(value, str):
        choice = choices.get(value)
        if choice is not None:
            return choice
    raise ValueError(f"Unsupported DSL {label} {value!r}.")


def _parse_parity_baseline(
    value: object,
) -> CheckParityBaseline:
    """Return the validated parity baseline for one DSL pack."""
    if not isinstance(value, str):
        raise ValueError("DSL field 'parity_baseline' must be a string.")
    normalized = normalize_check_parity_baselines([value])
    if normalized is None:
        raise ValueError("DSL field 'parity_baseline' must contain one value.")
    return normalized[0]


def _parse_jurisdictions(
    value: object,
) -> tuple[CheckJurisdiction, ...]:
    """Return validated jurisdiction metadata for one DSL pack."""
    raw_items = object_list_or_empty(value)
    if not raw_items:
        raise ValueError("DSL field 'jurisdictions' must be a not empty array.")
    raw_jurisdictions: list[str] = []
    for item in raw_items:
        if not isinstance(item, str) or not item.strip():
            raise ValueError("DSL field 'jurisdictions' must contain strings.")
        raw_jurisdictions.append(item)
    normalized = normalize_check_jurisdictions(raw_jurisdictions)
    if normalized is None:
        raise ValueError("DSL field 'jurisdictions' must contain one value.")
    return normalized
