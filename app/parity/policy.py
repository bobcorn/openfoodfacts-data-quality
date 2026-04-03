from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

from openfoodfacts_data_quality.contracts.checks import SEVERITY_ORDER, Severity
from openfoodfacts_data_quality.contracts.observations import ObservedFinding
from openfoodfacts_data_quality.structured_values import (
    StringObjectMapping,
    is_object_list,
    is_string_object_mapping,
)

ExpectedDifferenceMismatchKind = Literal["missing", "extra"]
EXPECTED_DIFFERENCES_SCHEMA_VERSION = 1
_MISMATCH_KINDS_BY_NAME: dict[str, ExpectedDifferenceMismatchKind] = {
    "missing": "missing",
    "extra": "extra",
}
_SEVERITIES_BY_NAME: dict[str, Severity] = {
    name: cast(Severity, name) for name in SEVERITY_ORDER
}


@dataclass(frozen=True, slots=True)
class ExpectedDifferenceRule:
    """One governance rule that marks a mismatch as intentional."""

    id: str
    justification: str
    check_ids: tuple[str, ...]
    mismatch_kinds: tuple[ExpectedDifferenceMismatchKind, ...]
    observed_codes: tuple[str, ...] | None = None
    severities: tuple[Severity, ...] | None = None
    product_ids: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        """Reject structurally empty rules."""
        if not self.id:
            raise ValueError("Expected-difference rule id must not be empty.")
        if not self.justification:
            raise ValueError(
                f"Expected-difference rule {self.id!r} must declare a justification."
            )
        if not self.check_ids:
            raise ValueError(
                f"Expected-difference rule {self.id!r} must target at least one check."
            )
        if not self.mismatch_kinds:
            raise ValueError(
                f"Expected-difference rule {self.id!r} must target at least one mismatch kind."
            )

    def matches(
        self,
        *,
        kind: ExpectedDifferenceMismatchKind,
        finding: ObservedFinding,
    ) -> bool:
        """Return whether this rule covers one concrete mismatch."""
        if finding.check_id not in self.check_ids:
            return False
        if kind not in self.mismatch_kinds:
            return False
        if self.observed_codes is not None and finding.observed_code not in (
            self.observed_codes
        ):
            return False
        if self.severities is not None and finding.severity not in self.severities:
            return False
        if self.product_ids is not None and finding.product_id not in self.product_ids:
            return False
        return True


@dataclass(frozen=True, slots=True)
class ExpectedDifferencesRegistry:
    """Loaded governance rules applied to strict parity mismatches."""

    rules: tuple[ExpectedDifferenceRule, ...] = ()
    source_path: Path | None = None

    def classify(
        self,
        *,
        kind: ExpectedDifferenceMismatchKind,
        finding: ObservedFinding,
    ) -> ExpectedDifferenceRule | None:
        """Return the unique rule covering one mismatch, if any."""
        matches = [
            rule for rule in self.rules if rule.matches(kind=kind, finding=finding)
        ]
        if not matches:
            return None
        if len(matches) == 1:
            return matches[0]
        matched_ids = ", ".join(rule.id for rule in matches)
        source = str(self.source_path) if self.source_path is not None else "<inline>"
        raise ValueError(
            "Expected-difference registry contains overlapping rules for "
            f"{kind} mismatch {finding.check_id!r}/{finding.observed_code!r}/"
            f"{finding.product_id!r}/{finding.severity!r} in {source}: {matched_ids}"
        )

    @property
    def rule_count(self) -> int:
        """Return the number of active governance rules."""
        return len(self.rules)


def load_expected_differences_registry(
    path: Path | None,
) -> ExpectedDifferencesRegistry:
    """Load the optional expected-differences registry."""
    if path is None:
        return ExpectedDifferencesRegistry()
    if not path.exists():
        raise FileNotFoundError(f"Expected-differences registry not found: {path}")

    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    raw_schema_version = raw.get("schema_version", EXPECTED_DIFFERENCES_SCHEMA_VERSION)
    if not isinstance(raw_schema_version, int):
        raise ValueError(
            f"Invalid expected-differences registry {path}: schema_version must be an integer."
        )
    if raw_schema_version != EXPECTED_DIFFERENCES_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported expected-differences registry schema_version "
            f"{raw_schema_version!r} in {path}; expected "
            f"{EXPECTED_DIFFERENCES_SCHEMA_VERSION}."
        )

    raw_rules = raw.get("rules", [])
    if not is_object_list(raw_rules):
        raise ValueError(
            f"Invalid expected-differences registry {path}: rules must be an array of tables."
        )

    rules = tuple(
        _parse_expected_difference_rule(path, raw_rule, index)
        for index, raw_rule in enumerate(raw_rules, start=1)
    )
    _validate_unique_rule_ids(path, rules)
    return ExpectedDifferencesRegistry(
        rules=rules,
        source_path=path.resolve(),
    )


def _validate_unique_rule_ids(
    path: Path,
    rules: tuple[ExpectedDifferenceRule, ...],
) -> None:
    """Reject duplicate rule ids before store-backed run setup begins."""
    seen_rule_ids: set[str] = set()
    duplicate_rule_ids: list[str] = []
    for rule in rules:
        if rule.id in seen_rule_ids:
            duplicate_rule_ids.append(rule.id)
            continue
        seen_rule_ids.add(rule.id)
    if not duplicate_rule_ids:
        return
    duplicate_csv = ", ".join(sorted(set(duplicate_rule_ids)))
    raise ValueError(
        f"Invalid expected-differences registry {path}: duplicate rule ids: {duplicate_csv}."
    )


def _parse_expected_difference_rule(
    path: Path,
    raw_rule: object,
    index: int,
) -> ExpectedDifferenceRule:
    """Parse one expected-difference rule from TOML."""
    if not is_string_object_mapping(raw_rule):
        raise ValueError(
            f"Invalid expected-differences registry {path}: rules[{index}] must be a table."
        )

    rule_id = _required_string(raw_rule, path=path, index=index, field="id")
    justification = _required_string(
        raw_rule,
        path=path,
        index=index,
        field="justification",
    )
    return ExpectedDifferenceRule(
        id=rule_id,
        justification=justification,
        check_ids=_required_string_tuple(
            raw_rule,
            path=path,
            index=index,
            singular_field="check_id",
            plural_field="check_ids",
        ),
        mismatch_kinds=_mismatch_kinds(
            raw_rule,
            path=path,
            index=index,
        ),
        observed_codes=_optional_string_tuple(
            raw_rule,
            path=path,
            index=index,
            singular_field="observed_code",
            plural_field="observed_codes",
        ),
        severities=_optional_severity_tuple(
            raw_rule,
            path=path,
            index=index,
        ),
        product_ids=_optional_string_tuple(
            raw_rule,
            path=path,
            index=index,
            singular_field="product_id",
            plural_field="product_ids",
        ),
    )


def _required_string(
    raw_rule: StringObjectMapping,
    *,
    path: Path,
    index: int,
    field: str,
) -> str:
    """Return one required non-empty string field."""
    raw_value = raw_rule.get(field)
    if isinstance(raw_value, str) and raw_value.strip():
        return raw_value.strip()
    raise ValueError(
        f"Invalid expected-differences registry {path}: rules[{index}].{field} "
        "must be a non-empty string."
    )


def _required_string_tuple(
    raw_rule: StringObjectMapping,
    *,
    path: Path,
    index: int,
    singular_field: str,
    plural_field: str,
) -> tuple[str, ...]:
    """Return one required string-or-string-list field as a tuple."""
    values = _string_tuple(
        raw_rule,
        path=path,
        index=index,
        singular_field=singular_field,
        plural_field=plural_field,
    )
    if values is not None:
        return values
    raise ValueError(
        f"Invalid expected-differences registry {path}: rules[{index}] must declare "
        f"{singular_field} or {plural_field}."
    )


def _optional_string_tuple(
    raw_rule: StringObjectMapping,
    *,
    path: Path,
    index: int,
    singular_field: str,
    plural_field: str,
) -> tuple[str, ...] | None:
    """Return one optional string-or-string-list field as a tuple."""
    return _string_tuple(
        raw_rule,
        path=path,
        index=index,
        singular_field=singular_field,
        plural_field=plural_field,
    )


def _string_tuple(
    raw_rule: StringObjectMapping,
    *,
    path: Path,
    index: int,
    singular_field: str,
    plural_field: str,
) -> tuple[str, ...] | None:
    """Normalize one scalar-or-list string field preserving first seen order."""
    singular_value = raw_rule.get(singular_field)
    plural_value = raw_rule.get(plural_field)
    if singular_value is not None and plural_value is not None:
        raise ValueError(
            f"Invalid expected-differences registry {path}: rules[{index}] cannot "
            f"declare both {singular_field} and {plural_field}."
        )

    raw_values: list[str]
    if singular_value is not None:
        if not isinstance(singular_value, str) or not singular_value.strip():
            raise ValueError(
                f"Invalid expected-differences registry {path}: rules[{index}]."
                f"{singular_field} must be a non-empty string."
            )
        raw_values = [singular_value]
    elif plural_value is not None:
        if not is_object_list(plural_value) or not plural_value:
            raise ValueError(
                f"Invalid expected-differences registry {path}: rules[{index}]."
                f"{plural_field} must be a non-empty array of strings."
            )
        raw_values = []
        for raw_value in plural_value:
            if not isinstance(raw_value, str) or not raw_value.strip():
                raise ValueError(
                    f"Invalid expected-differences registry {path}: rules[{index}]."
                    f"{plural_field} must contain only non-empty strings."
                )
            raw_values.append(raw_value)
    else:
        return None

    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        value = raw_value.strip()
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return tuple(normalized)


def _mismatch_kinds(
    raw_rule: StringObjectMapping,
    *,
    path: Path,
    index: int,
) -> tuple[ExpectedDifferenceMismatchKind, ...]:
    """Return the optional mismatch-kind selection for one rule."""
    raw_values = _string_tuple(
        raw_rule,
        path=path,
        index=index,
        singular_field="mismatch_kind",
        plural_field="mismatch_kinds",
    )
    if raw_values is None:
        return tuple(_MISMATCH_KINDS_BY_NAME.values())

    normalized: list[ExpectedDifferenceMismatchKind] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        kind = _MISMATCH_KINDS_BY_NAME.get(raw_value)
        if kind is None:
            raise ValueError(
                f"Invalid expected-differences registry {path}: rules[{index}] "
                f"declares unsupported mismatch kind {raw_value!r}."
            )
        if kind in seen:
            continue
        seen.add(kind)
        normalized.append(kind)
    return tuple(normalized)


def _optional_severity_tuple(
    raw_rule: StringObjectMapping,
    *,
    path: Path,
    index: int,
) -> tuple[Severity, ...] | None:
    """Return the optional severity filter for one rule."""
    raw_values = _string_tuple(
        raw_rule,
        path=path,
        index=index,
        singular_field="severity",
        plural_field="severities",
    )
    if raw_values is None:
        return None

    normalized: list[Severity] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        severity = _SEVERITIES_BY_NAME.get(raw_value)
        if severity is None:
            raise ValueError(
                f"Invalid expected-differences registry {path}: rules[{index}] "
                f"declares unsupported severity {raw_value!r}."
            )
        if severity in seen:
            continue
        seen.add(severity)
        normalized.append(severity)
    return tuple(normalized)
