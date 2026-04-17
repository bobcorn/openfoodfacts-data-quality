from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from off_data_quality.contracts.checks import CheckDefinition

_TEMPLATE_KEY_TOKEN = "${*}"


@dataclass(frozen=True, slots=True)
class _ParsedLegacyCodeTemplate:
    literals: tuple[str, ...]
    placeholders: tuple[str, ...]

    @property
    def wildcard_count(self) -> int:
        return len(self.placeholders)


def has_legacy_code_template_tokens(code_template: str) -> bool:
    """Return whether one legacy code template contains wildcard tokens."""
    return _parsed_legacy_code_template(code_template).wildcard_count > 0


def legacy_code_template_placeholders(code_template: str) -> tuple[str, ...]:
    """Return the placeholder names carried by one legacy code template."""
    return _parsed_legacy_code_template(code_template).placeholders


@cache
def legacy_code_template_key(code_template: str) -> str:
    """Return the canonical template key used to compare legacy identities."""
    parsed_template = _parsed_legacy_code_template(code_template)
    if parsed_template.wildcard_count == 0:
        return code_template

    parts = [parsed_template.literals[0]]
    for literal in parsed_template.literals[1:]:
        parts.append(_TEMPLATE_KEY_TOKEN)
        parts.append(literal)
    return "".join(parts)


def matches_legacy_check_code(code_template: str, observed_code: str) -> bool:
    """Return whether one observed legacy code belongs to a legacy template."""
    parsed_template = _parsed_legacy_code_template(code_template)
    if parsed_template.wildcard_count == 0:
        return observed_code == code_template
    return _matches_parsed_template(parsed_template, observed_code)


@dataclass(frozen=True, slots=True)
class LegacyCheckIndex:
    """Index checks with a legacy baseline by their observed code matching strategy."""

    exact_checks_by_code: Mapping[str, tuple[CheckDefinition, ...]]
    templated_checks: tuple[CheckDefinition, ...]

    @classmethod
    def build(cls, checks: Iterable[CheckDefinition]) -> LegacyCheckIndex:
        """Build a reusable observed code matcher for one check collection."""
        exact_checks_by_code: dict[str, list[CheckDefinition]] = {}
        templated_checks: list[CheckDefinition] = []
        for check in checks:
            legacy_identity = check.legacy_identity
            if legacy_identity is None:
                continue
            code_template = legacy_identity.code_template
            if has_legacy_code_template_tokens(code_template):
                templated_checks.append(check)
                continue
            exact_checks_by_code.setdefault(code_template, []).append(check)
        return cls(
            exact_checks_by_code=MappingProxyType(
                {
                    code: tuple(matching_checks)
                    for code, matching_checks in exact_checks_by_code.items()
                }
            ),
            templated_checks=tuple(templated_checks),
        )

    def match_observed_code(self, observed_code: str) -> tuple[CheckDefinition, ...]:
        """Return the checks whose legacy identity matches one observed code."""
        matches = list(self.exact_checks_by_code.get(observed_code, ()))
        for check in self.templated_checks:
            legacy_identity = check.legacy_identity
            if legacy_identity is None:
                continue
            if matches_legacy_check_code(legacy_identity.code_template, observed_code):
                matches.append(check)
        return tuple(matches)


@cache
def _parsed_legacy_code_template(code_template: str) -> _ParsedLegacyCodeTemplate:
    """Parse one legacy code template into literal segments around placeholders."""
    literals: list[str] = []
    placeholders: list[str] = []
    cursor = 0
    literal_start = 0

    while cursor < len(code_template):
        if not code_template.startswith("${", cursor):
            cursor += 1
            continue

        end = code_template.find("}", cursor + 2)
        if end == -1:
            cursor += 2
            continue

        placeholder_name = code_template[cursor + 2 : end]
        if not _is_template_placeholder_name(placeholder_name):
            cursor += 2
            continue

        literals.append(code_template[literal_start:cursor])
        placeholders.append(placeholder_name)
        literal_start = end + 1
        cursor = end + 1

    literals.append(code_template[literal_start:])
    return _ParsedLegacyCodeTemplate(
        literals=tuple(literals),
        placeholders=tuple(placeholders),
    )


def _is_template_placeholder_name(value: str) -> bool:
    """Return whether one placeholder name is a supported template identifier."""
    return bool(value) and value.isidentifier()


def _matches_parsed_template(
    parsed_template: _ParsedLegacyCodeTemplate,
    observed_code: str,
) -> bool:
    """Return whether one observed code matches a parsed legacy template."""

    @cache
    def match_literal(literal_index: int, position: int) -> bool:
        literal = parsed_template.literals[literal_index]
        if not observed_code.startswith(literal, position):
            return False

        next_position = position + len(literal)
        if literal_index == parsed_template.wildcard_count:
            return next_position == len(observed_code)
        return match_wildcard(literal_index, next_position)

    @cache
    def match_wildcard(literal_index: int, position: int) -> bool:
        next_literal = parsed_template.literals[literal_index + 1]
        if next_literal:
            search_position = position + 1
            while True:
                next_literal_position = observed_code.find(
                    next_literal, search_position
                )
                if next_literal_position == -1:
                    return False
                wildcard_value = observed_code[position:next_literal_position]
                if _is_valid_legacy_wildcard_value(wildcard_value) and match_literal(
                    literal_index + 1, next_literal_position
                ):
                    return True
                search_position = next_literal_position + 1

        for split_position in range(position + 1, len(observed_code) + 1):
            wildcard_value = observed_code[position:split_position]
            if _is_valid_legacy_wildcard_value(wildcard_value) and match_literal(
                literal_index + 1,
                split_position,
            ):
                return True
        return False

    return match_literal(0, 0)


def _is_valid_legacy_wildcard_value(value: str) -> bool:
    """Return whether one interpolated legacy slot value is acceptable."""
    return bool(value) and all(
        character != ":" and not character.isspace() for character in value
    )
