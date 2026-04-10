from __future__ import annotations

from typing import TypeGuard

type StringObjectMapping = dict[str, object]


def is_object_list(value: object) -> TypeGuard[list[object]]:
    """Return whether a runtime value is a list of generic Python objects."""
    return isinstance(value, list)


def object_list_or_empty(value: object) -> list[object]:
    """Return a list value when present, or a shared empty-list shape otherwise."""
    if is_object_list(value):
        return value
    return []


def is_string_object_mapping(value: object) -> TypeGuard[StringObjectMapping]:
    """Return whether a runtime value is a mapping keyed by strings."""
    return isinstance(value, dict)


def is_blank_value(value: object, *, missing_sentinel: object | None = None) -> bool:
    """Return whether a dynamic value should count as blank in normalized checks."""
    if missing_sentinel is not None and value is missing_sentinel:
        return True
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if is_object_list(value):
        return not value
    if isinstance(value, tuple):
        return not value
    if isinstance(value, set):
        return not value
    if is_string_object_mapping(value):
        return not value
    return False
