from __future__ import annotations

from collections.abc import Mapping

from openfoodfacts_data_quality.contracts.checks import CheckInputSurface
from openfoodfacts_data_quality.contracts.context import (
    CHECK_INPUT_SURFACES,
    ContextPathSpec,
    iter_normalized_context_path_specs,
)
from openfoodfacts_data_quality.structured_values import (
    is_blank_value,
    is_string_object_mapping,
)

__all__ = [
    "CHECK_INPUT_SURFACES",
    "ContextPathSpec",
    "MISSING",
    "PATH_SPECS",
    "is_blank",
    "path_spec_for",
    "resolve_path",
    "supported_input_surfaces_for",
    "supports_input_surface",
    "validate_input_surface",
]


class _Missing:
    def __repr__(self) -> str:
        return "MISSING"


MISSING = _Missing()

PATH_SPECS = iter_normalized_context_path_specs()
PATHS_BY_NAME = {spec.path: spec for spec in PATH_SPECS}


def path_spec_for(path: str) -> ContextPathSpec | None:
    """Return metadata for one normalized context path."""
    return PATHS_BY_NAME.get(path)


def validate_input_surface(surface: str) -> CheckInputSurface:
    """Normalize and validate one configured check-input surface name."""
    if surface == "raw_products":
        return "raw_products"
    if surface == "enriched_products":
        return "enriched_products"
    raise ValueError(
        f"Unsupported input surface {surface!r}. Expected one of: "
        f"{', '.join(CHECK_INPUT_SURFACES)}."
    )


def supports_input_surface(
    required_context_paths: tuple[str, ...],
    surface: CheckInputSurface,
) -> bool:
    """Return whether all required paths are available on one input surface."""
    for path in required_context_paths:
        spec = path_spec_for(path)
        if spec is None or surface not in spec.supported_input_surfaces:
            return False
    return True


def supported_input_surfaces_for(
    required_context_paths: tuple[str, ...],
) -> tuple[CheckInputSurface, ...]:
    """Return every input surface that can satisfy one check dependency set."""
    return tuple(
        surface
        for surface in CHECK_INPUT_SURFACES
        if supports_input_surface(required_context_paths, surface)
    )


def resolve_path(payload: Mapping[str, object], path: str) -> object:
    """Traverse a dotted path inside the normalized context mapping."""
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
