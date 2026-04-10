"""Helpers for validating loose TOML and JSON payload shapes in the app layer."""

from __future__ import annotations

from typing import TypeGuard

type StringObjectMapping = dict[str, object]


def is_object_list(value: object) -> TypeGuard[list[object]]:
    """Return whether a runtime value is a list of generic Python objects."""
    return isinstance(value, list)


def is_string_object_mapping(value: object) -> TypeGuard[StringObjectMapping]:
    """Return whether a runtime value is a mapping keyed by strings."""
    return isinstance(value, dict)


__all__ = [
    "StringObjectMapping",
    "is_object_list",
    "is_string_object_mapping",
]
