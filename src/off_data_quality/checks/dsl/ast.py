from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from off_data_quality.contracts.checks import (
        CheckJurisdiction,
        CheckParityBaseline,
        Severity,
    )

Operator = Literal[
    "is_missing",
    "is_blank",
    "is_true",
    "is_false",
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "contains",
    "in",
    "not_in",
]


@dataclass(frozen=True)
class Atom:
    field: str
    op: Operator
    value: object = None


@dataclass(frozen=True)
class All:
    items: tuple[Expression, ...]


@dataclass(frozen=True)
class AnyOf:
    items: tuple[Expression, ...]


@dataclass(frozen=True)
class Not:
    item: Expression


Expression = Atom | All | AnyOf | Not


@dataclass(frozen=True)
class DSLDefinition:
    id: str
    severity: Severity
    when: Expression
    parity_baseline: CheckParityBaseline
    jurisdictions: tuple[CheckJurisdiction, ...]
    required_context_paths: tuple[str, ...] = ()
