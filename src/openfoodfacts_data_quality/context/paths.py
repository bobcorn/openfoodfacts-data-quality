from __future__ import annotations

from collections.abc import Mapping

from openfoodfacts_data_quality.contracts.context import (
    ContextPathSpec,
    iter_check_context_path_specs,
)
from openfoodfacts_data_quality.structured_values import (
    is_blank_value,
    is_string_object_mapping,
)

__all__ = [
    "ContextPathSpec",
    "MISSING",
    "PATH_SPECS",
    "is_blank",
    "path_spec_for",
    "resolve_path",
]


class _Missing:
    def __repr__(self) -> str:
        return "MISSING"


MISSING = _Missing()

PATH_SPECS = iter_check_context_path_specs()
PATHS_BY_NAME = {spec.path: spec for spec in PATH_SPECS}


def path_spec_for(path: str) -> ContextPathSpec | None:
    """Return metadata for one check context path."""
    return PATHS_BY_NAME.get(path)


def resolve_path(payload: Mapping[str, object], path: str) -> object:
    """Traverse a dotted path inside the check context mapping."""
    current: object = payload
    for segment in path.split("."):
        if not is_string_object_mapping(current):
            return MISSING
        if segment not in current:
            return MISSING
        current = current[segment]
    return current


def is_blank(value: object) -> bool:
    """Return whether a resolved value should count as blank."""
    return is_blank_value(value, missing_sentinel=MISSING)
