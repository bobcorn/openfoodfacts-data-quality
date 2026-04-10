from __future__ import annotations

import hashlib
import json
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from migration._value_shapes import (
    StringObjectMapping,
    is_object_list,
    is_string_object_mapping,
)

DatasetSelectionKind = Literal["all_products", "stable_sample", "code_list"]

_SELECTION_KINDS = frozenset({"all_products", "stable_sample", "code_list"})


@dataclass(frozen=True, slots=True)
class SourceSelection:
    """Deterministic source selection resolved for one dataset profile."""

    kind: DatasetSelectionKind
    sample_size: int | None = None
    seed: int | None = None
    codes: tuple[str, ...] = ()
    codes_path: Path | None = None

    def __post_init__(self) -> None:
        """Reject structurally invalid selection combinations."""
        if self.kind == "all_products":
            if self.sample_size is not None or self.seed is not None or self.codes:
                raise ValueError(
                    "all_products selections must not define sample or code members."
                )
            return
        if self.kind == "stable_sample":
            if self.sample_size is None or self.sample_size <= 0:
                raise ValueError(
                    "stable_sample selections must define a positive sample_size."
                )
            if self.seed is None:
                raise ValueError("stable_sample selections must define a seed.")
            if self.codes:
                raise ValueError(
                    "stable_sample selections must not define explicit codes."
                )
            return
        if not self.codes:
            raise ValueError("code_list selections must define at least one code.")
        if self.sample_size is not None or self.seed is not None:
            raise ValueError(
                "code_list selections must not define sample_size or seed."
            )

    @property
    def fingerprint(self) -> str:
        """Return a stable fingerprint for this explicit selection."""
        payload = json.dumps(self.as_payload(), ensure_ascii=False, sort_keys=True)
        return f"sha256:{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]}"

    def as_payload(self) -> dict[str, Any]:
        """Return the JSON-serializable payload for persistence and review."""
        payload: dict[str, Any] = {"kind": self.kind}
        if self.kind == "stable_sample":
            payload["sample_size"] = self.sample_size
            payload["seed"] = self.seed
            return payload
        if self.kind == "code_list":
            payload["codes"] = list(self.codes)
            if self.codes_path is not None:
                payload["codes_path"] = str(self.codes_path)
        return payload


@dataclass(frozen=True, slots=True)
class ActiveDatasetProfile:
    """Validated named source dataset profile for one migration run."""

    name: str
    description: str
    selection: SourceSelection


def default_dataset_profile() -> ActiveDatasetProfile:
    """Return the implicit full dataset profile."""
    return ActiveDatasetProfile(
        name="full",
        description="Runs the entire configured source snapshot.",
        selection=SourceSelection(kind="all_products"),
    )


def load_dataset_profile(
    config_path: Path,
    profile_name: str | None = None,
) -> ActiveDatasetProfile:
    """Load and validate one named dataset profile from TOML config."""
    raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    profiles = _profiles_mapping(raw, config_path)
    selected_name = _selected_profile_name(raw, config_path, profile_name)
    selected_profile = _selected_profile_mapping(profiles, selected_name)
    return ActiveDatasetProfile(
        name=selected_name,
        description=_profile_description(selected_profile, selected_name),
        selection=_profile_selection(
            config_path=config_path,
            selected_profile=selected_profile,
            selected_name=selected_name,
        ),
    )


def _profiles_mapping(
    raw: dict[str, object],
    config_path: Path,
) -> dict[str, StringObjectMapping]:
    """Return the validated dataset profile mapping."""
    raw_profiles = raw.get("profiles")
    if not is_string_object_mapping(raw_profiles) or not raw_profiles:
        raise ValueError(
            f"Invalid dataset profile config: {config_path} defines no profiles."
        )
    profiles: dict[str, StringObjectMapping] = {}
    for profile_name, profile in raw_profiles.items():
        if not is_string_object_mapping(profile):
            raise ValueError(
                f"Invalid dataset profile config: profile {profile_name} must be a table."
            )
        profiles[profile_name] = profile
    return profiles


def _selected_profile_name(
    raw: dict[str, object],
    config_path: Path,
    profile_name: str | None,
) -> str:
    """Resolve the configured or default dataset profile name."""
    selected_name = profile_name or raw.get("default_profile")
    if isinstance(selected_name, str) and selected_name.strip():
        return selected_name.strip()
    raise ValueError(
        f"Invalid dataset profile config: {config_path} is missing a default profile."
    )


def _selected_profile_mapping(
    profiles: dict[str, StringObjectMapping],
    selected_name: str,
) -> StringObjectMapping:
    """Return the selected dataset profile mapping."""
    selected_profile = profiles.get(selected_name)
    if selected_profile is None:
        raise ValueError(f"Unknown dataset profile: {selected_name}")
    return selected_profile


def _profile_description(
    selected_profile: StringObjectMapping,
    selected_name: str,
) -> str:
    """Return the human description for one dataset profile."""
    description = selected_profile.get("description")
    if isinstance(description, str) and description.strip():
        return description.strip()
    raise ValueError(
        f"Invalid dataset profile config: profile {selected_name} is missing a description."
    )


def _profile_selection(
    *,
    config_path: Path,
    selected_profile: StringObjectMapping,
    selected_name: str,
) -> SourceSelection:
    """Return the resolved source selection for one dataset profile."""
    raw_kind = selected_profile.get("kind", "all_products")
    if not isinstance(raw_kind, str) or raw_kind.strip() not in _SELECTION_KINDS:
        raise ValueError(
            f"Invalid dataset profile config: profile {selected_name} has unsupported kind {raw_kind!r}."
        )
    kind = raw_kind.strip()
    if kind == "all_products":
        return SourceSelection(kind="all_products")
    if kind == "stable_sample":
        return SourceSelection(
            kind="stable_sample",
            sample_size=_required_positive_int(
                selected_profile,
                profile_name=selected_name,
                field="sample_size",
            ),
            seed=_optional_int(selected_profile, field="seed") or 42,
        )
    return SourceSelection(
        kind="code_list",
        codes=_profile_codes(
            config_path=config_path,
            selected_profile=selected_profile,
            selected_name=selected_name,
        ),
        codes_path=_profile_codes_path(
            config_path=config_path,
            selected_profile=selected_profile,
        ),
    )


def _profile_codes(
    *,
    config_path: Path,
    selected_profile: StringObjectMapping,
    selected_name: str,
) -> tuple[str, ...]:
    """Return the normalized explicit code set for one code-list profile."""
    raw_codes = selected_profile.get("codes")
    raw_codes_path = selected_profile.get("codes_path")
    if raw_codes is not None and raw_codes_path is not None:
        raise ValueError(
            f"Invalid dataset profile config: profile {selected_name} must define either codes or codes_path, not both."
        )
    if raw_codes is not None:
        if not is_object_list(raw_codes) or not raw_codes:
            raise ValueError(
                f"Invalid dataset profile config: profile {selected_name} must define a not empty codes array."
            )
        return _normalize_codes(raw_codes, context=f"profile {selected_name} codes")
    if raw_codes_path is None:
        raise ValueError(
            f"Invalid dataset profile config: profile {selected_name} must define codes or codes_path."
        )
    resolved_path = _resolve_relative_path(
        config_path=config_path,
        raw_path=raw_codes_path,
        profile_name=selected_name,
        field="codes_path",
    )
    return _load_codes_from_path(resolved_path, context=f"profile {selected_name}")


def _profile_codes_path(
    *,
    config_path: Path,
    selected_profile: StringObjectMapping,
) -> Path | None:
    """Return the resolved code file path when the profile uses one."""
    raw_codes_path = selected_profile.get("codes_path")
    if raw_codes_path is None:
        return None
    return _resolve_relative_path(
        config_path=config_path,
        raw_path=raw_codes_path,
        profile_name="<resolved>",
        field="codes_path",
    )


def _resolve_relative_path(
    *,
    config_path: Path,
    raw_path: object,
    profile_name: str,
    field: str,
) -> Path:
    """Resolve one relative path field against the dataset config directory."""
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ValueError(
            f"Invalid dataset profile config: profile {profile_name} must define a non-empty {field}."
        )
    candidate = Path(raw_path.strip())
    if candidate.is_absolute():
        return candidate.resolve()
    return (config_path.parent / candidate).resolve()


def _load_codes_from_path(path: Path, *, context: str) -> tuple[str, ...]:
    """Load one newline-delimited code list."""
    if not path.exists():
        raise FileNotFoundError(f"Dataset code list not found for {context}: {path}")
    return _normalize_codes(
        path.read_text(encoding="utf-8").splitlines(),
        context=f"{context} from {path}",
    )


def _normalize_codes(raw_codes: Sequence[object], *, context: str) -> tuple[str, ...]:
    """Return unique product codes with stable first-seen ordering."""
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_code in raw_codes:
        if not isinstance(raw_code, str) or not raw_code.strip():
            raise ValueError(f"Invalid dataset codes in {context}.")
        code = raw_code.strip()
        if code.startswith("#"):
            continue
        if code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    if not normalized:
        raise ValueError(f"No usable product codes found in {context}.")
    return tuple(normalized)


def _required_positive_int(
    selected_profile: StringObjectMapping,
    *,
    profile_name: str,
    field: str,
) -> int:
    """Return one required positive integer field."""
    raw_value = selected_profile.get(field)
    if isinstance(raw_value, int) and raw_value > 0:
        return raw_value
    raise ValueError(
        f"Invalid dataset profile config: profile {profile_name} must define a positive integer {field}."
    )


def _optional_int(
    selected_profile: StringObjectMapping,
    *,
    field: str,
) -> int | None:
    """Return one optional integer field."""
    raw_value = selected_profile.get(field)
    if raw_value is None:
        return None
    if isinstance(raw_value, int):
        return raw_value
    raise ValueError(
        f"Invalid dataset profile config: {field} must be an integer when set."
    )
