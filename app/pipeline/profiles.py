from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from openfoodfacts_data_quality.checks.catalog import (
    CheckCatalog,
    get_default_check_catalog,
)
from openfoodfacts_data_quality.context.paths import validate_input_surface
from openfoodfacts_data_quality.contracts.checks import (
    LEGACY_PARITY_BASELINES,
    CheckJurisdiction,
    CheckParityBaseline,
    CheckSelection,
    normalize_check_jurisdictions,
    normalize_check_parity_baselines,
)
from openfoodfacts_data_quality.structured_values import (
    StringObjectMapping,
    is_object_list,
    is_string_object_mapping,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from openfoodfacts_data_quality.contracts.checks import (
        CheckDefinition,
        CheckInputSurface,
    )


@dataclass(frozen=True)
class ActiveCheckProfile:
    """Validated named subset of checks to execute for one pipeline run."""

    name: str
    description: str
    check_input_surface: CheckInputSurface
    parity_baselines: tuple[CheckParityBaseline, ...]
    jurisdictions: tuple[CheckJurisdiction, ...] | None
    check_ids: tuple[str, ...]
    checks: tuple[CheckDefinition, ...]

    @property
    def selection(self) -> CheckSelection:
        """Return the selection filters that produced this active check set."""
        return CheckSelection(
            input_surface=self.check_input_surface,
            parity_baselines=self.parity_baselines,
            jurisdictions=self.jurisdictions,
        )


_ProfileMode = Literal["all", "include"]


@dataclass(frozen=True)
class _RequestedCheckProfile:
    """Normalized profile request parsed from TOML before catalog selection."""

    name: str
    description: str
    check_input_surface: CheckInputSurface
    parity_baselines: tuple[CheckParityBaseline, ...]
    jurisdictions: tuple[CheckJurisdiction, ...] | None
    mode: _ProfileMode
    requested_check_ids: tuple[str, ...]

    @property
    def selection(self) -> CheckSelection:
        """Return the catalog selection implied by this requested profile."""
        return CheckSelection(
            input_surface=self.check_input_surface,
            parity_baselines=self.parity_baselines,
            jurisdictions=self.jurisdictions,
        )


def configured_check_profile_name() -> str | None:
    """Return the selected check profile name from the environment, if any."""
    configured = os.environ.get("CHECK_PROFILE")
    if configured is None:
        return None
    normalized = configured.strip()
    return normalized or None


def load_check_profile(
    config_path: Path,
    profile_name: str | None = None,
    *,
    catalog: CheckCatalog | None = None,
) -> ActiveCheckProfile:
    """Load and validate one named check profile from TOML config."""
    selected_catalog = catalog or get_default_check_catalog()
    requested_profile = _load_requested_profile(config_path, profile_name)
    active_checks = _select_active_checks(selected_catalog, requested_profile)

    return ActiveCheckProfile(
        name=requested_profile.name,
        description=requested_profile.description,
        check_input_surface=requested_profile.check_input_surface,
        parity_baselines=requested_profile.parity_baselines,
        jurisdictions=requested_profile.jurisdictions,
        check_ids=tuple(check.id for check in active_checks),
        checks=active_checks,
    )


def _load_requested_profile(
    config_path: Path,
    profile_name: str | None,
) -> _RequestedCheckProfile:
    """Load, select, and normalize one profile definition from TOML."""
    raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    profiles = _profiles_mapping(raw, config_path)
    selected_name = _selected_profile_name(raw, config_path, profile_name)
    selected_profile = _selected_profile_mapping(profiles, selected_name)
    description = _profile_description(selected_profile, selected_name)
    check_input_surface = _profile_check_input_surface(selected_profile, selected_name)
    parity_baselines = _profile_parity_baselines(selected_profile, selected_name)
    jurisdictions = _profile_jurisdictions(selected_profile, selected_name)
    mode = _profile_mode(selected_profile, selected_name)
    return _RequestedCheckProfile(
        name=selected_name,
        description=description,
        check_input_surface=check_input_surface,
        parity_baselines=parity_baselines,
        jurisdictions=jurisdictions,
        mode=mode,
        requested_check_ids=_profile_check_ids(selected_profile, selected_name, mode),
    )


def _profiles_mapping(
    raw: Mapping[str, object],
    config_path: Path,
) -> dict[str, StringObjectMapping]:
    """Return the non-empty profiles mapping from the loaded config."""
    raw_profiles = raw.get("profiles")
    if not is_string_object_mapping(raw_profiles) or not raw_profiles:
        raise ValueError(
            f"Invalid check profile config: {config_path} defines no profiles."
        )
    profiles: dict[str, StringObjectMapping] = {}
    for profile_name, profile in raw_profiles.items():
        if not is_string_object_mapping(profile):
            raise ValueError(
                f"Invalid check profile config: profile {profile_name} must be a table."
            )
        profiles[profile_name] = profile
    if profiles:
        return profiles
    raise ValueError(
        f"Invalid check profile config: {config_path} defines no profiles."
    )


def _selected_profile_name(
    raw: Mapping[str, object],
    config_path: Path,
    profile_name: str | None,
) -> str:
    """Resolve the configured or default profile name."""
    selected_name = profile_name or raw.get("default_profile")
    if isinstance(selected_name, str) and selected_name.strip():
        return selected_name
    raise ValueError(
        f"Invalid check profile config: {config_path} is missing a default profile."
    )


def _selected_profile_mapping(
    profiles: Mapping[str, StringObjectMapping],
    selected_name: str,
) -> StringObjectMapping:
    """Return the selected profile mapping."""
    selected_profile = profiles.get(selected_name)
    if is_string_object_mapping(selected_profile):
        return selected_profile
    raise ValueError(f"Unknown check profile: {selected_name}")


def _profile_description(
    selected_profile: StringObjectMapping,
    selected_name: str,
) -> str:
    """Return the normalized human-readable profile description."""
    description = selected_profile.get("description")
    if isinstance(description, str) and description.strip():
        return description.strip()
    raise ValueError(
        f"Invalid check profile config: profile {selected_name} is missing a description."
    )


def _profile_check_input_surface(
    selected_profile: StringObjectMapping,
    selected_name: str,
) -> CheckInputSurface:
    """Return the normalized check input surface for one profile."""
    raw_check_input_surface = selected_profile.get(
        "check_input_surface", "enriched_products"
    )
    if (
        not isinstance(raw_check_input_surface, str)
        or not raw_check_input_surface.strip()
    ):
        raise ValueError(
            f"Invalid check profile config: profile {selected_name} is missing a valid check_input_surface."
        )
    return validate_input_surface(raw_check_input_surface.strip())


def _profile_parity_baselines(
    selected_profile: StringObjectMapping,
    selected_name: str,
) -> tuple[CheckParityBaseline, ...]:
    """Return the parity baselines selected for one profile."""
    raw_baselines = selected_profile.get("parity_baselines")
    if raw_baselines is None:
        return LEGACY_PARITY_BASELINES
    values = _required_string_list(
        raw_baselines,
        field="parity_baselines",
        profile_name=selected_name,
    )
    normalized = normalize_check_parity_baselines(values)
    if normalized is None:
        return LEGACY_PARITY_BASELINES
    return normalized


def _profile_jurisdictions(
    selected_profile: StringObjectMapping,
    selected_name: str,
) -> tuple[CheckJurisdiction, ...] | None:
    """Return the optional jurisdiction filter for one profile."""
    raw_jurisdictions = selected_profile.get("jurisdictions")
    if raw_jurisdictions is None:
        return None
    values = _required_string_list(
        raw_jurisdictions,
        field="jurisdictions",
        profile_name=selected_name,
    )
    return normalize_check_jurisdictions(values)


def _profile_mode(
    selected_profile: StringObjectMapping,
    selected_name: str,
) -> _ProfileMode:
    """Return the supported selection mode for one profile."""
    mode = selected_profile.get("mode")
    if mode == "all":
        return "all"
    if mode == "include":
        return "include"
    raise ValueError(
        f"Invalid check profile config: profile {selected_name} has unsupported mode {mode!r}."
    )


def _profile_check_ids(
    selected_profile: StringObjectMapping,
    selected_name: str,
    mode: _ProfileMode,
) -> tuple[str, ...]:
    """Return normalized requested ids for include-mode profiles."""
    if mode == "all":
        return ()
    raw_check_ids = selected_profile.get("check_ids")
    return _normalize_check_ids(
        _required_string_list(
            raw_check_ids,
            field="check_ids",
            profile_name=selected_name,
        ),
        selected_name,
    )


def _required_string_list(
    raw_values: object,
    *,
    field: str,
    profile_name: str,
) -> list[str]:
    """Return a non-empty string list from one profile field."""
    if not is_object_list(raw_values) or not raw_values:
        raise ValueError(
            f"Invalid check profile config: profile {profile_name} must define {field}."
        )
    values: list[str] = []
    for raw_value in raw_values:
        if not isinstance(raw_value, str) or not raw_value.strip():
            raise ValueError(
                f"Invalid check profile config: profile {profile_name} contains an invalid {field} value."
            )
        values.append(raw_value.strip())
    return values


def _select_active_checks(
    catalog: CheckCatalog,
    requested_profile: _RequestedCheckProfile,
) -> tuple[CheckDefinition, ...]:
    """Resolve the concrete active checks for one normalized profile request."""
    if requested_profile.mode == "all":
        return catalog.select_checks(selection=requested_profile.selection)
    return _select_included_checks(catalog, requested_profile)


def _select_included_checks(
    catalog: CheckCatalog,
    requested_profile: _RequestedCheckProfile,
) -> tuple[CheckDefinition, ...]:
    """Resolve include-mode checks with profile-specific error translation."""
    try:
        return catalog.select_checks(
            requested_profile.requested_check_ids,
            selection=requested_profile.selection,
        )
    except ValueError as exc:
        raise _translated_profile_selection_error(requested_profile, exc) from exc


def _translated_profile_selection_error(
    requested_profile: _RequestedCheckProfile,
    exc: ValueError,
) -> ValueError:
    """Translate catalog selection failures into config-centered error messages."""
    message = str(exc)
    if message.startswith("Unknown active check ids:"):
        return ValueError(
            f"Invalid check profile config: profile {requested_profile.name} references unknown checks: "
            f"{message.removeprefix('Unknown active check ids: ')}."
        )
    if message.startswith("Checks not available for "):
        return ValueError(
            f"Invalid check profile config: profile {requested_profile.name} references checks outside "
            f"{message.removeprefix('Checks not available for ')}."
        )
    return exc


def _normalize_check_ids(
    raw_check_ids: list[str], profile_name: str
) -> tuple[str, ...]:
    """Return unique check ids with stable first-seen ordering."""
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_check_id in raw_check_ids:
        check_id = raw_check_id.strip()
        if check_id in seen:
            continue
        seen.add(check_id)
        normalized.append(check_id)
    if not normalized:
        raise ValueError(
            f"Invalid check profile config: profile {profile_name} contains no usable check ids."
        )
    return tuple(normalized)
