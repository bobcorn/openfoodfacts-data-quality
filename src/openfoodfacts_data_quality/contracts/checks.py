from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

Severity = Literal["bug", "info", "completeness", "warning", "error"]
CheckDefinitionLanguage = Literal["python", "dsl"]
CheckInputSurface = Literal["raw_products", "enriched_products"]
CheckParityBaseline = Literal["legacy", "none"]
CheckJurisdiction = Literal["global", "ca"]
SEVERITY_ORDER: dict[str, int] = {
    "info": 0,
    "completeness": 1,
    "warning": 2,
    "error": 3,
    "bug": 4,
}
LEGACY_PARITY_BASELINES: tuple[CheckParityBaseline, ...] = ("legacy",)
_PARITY_BASELINES_BY_NAME: dict[str, CheckParityBaseline] = {
    "legacy": "legacy",
    "none": "none",
}
_JURISDICTIONS_BY_NAME: dict[str, CheckJurisdiction] = {
    "global": "global",
    "ca": "ca",
}


@dataclass(frozen=True, slots=True)
class CheckEmission:
    """One concrete finding emission produced by a check evaluator."""

    severity: Severity
    raw_code: str | None = None


@dataclass(frozen=True, slots=True)
class LegacyCheckIdentity:
    """Explicit legacy-side identity used for parity-backed check mapping."""

    code_template: str

    def __post_init__(self) -> None:
        """Reject empty legacy-code templates."""
        if not self.code_template:
            raise ValueError("Legacy check identity code_template must be non-empty.")


@dataclass(frozen=True, slots=True)
class CheckSelection:
    """Optional metadata filters applied when selecting checks from the catalog."""

    input_surface: CheckInputSurface | None = None
    parity_baselines: tuple[CheckParityBaseline, ...] | None = None
    jurisdictions: tuple[CheckJurisdiction, ...] | None = None


@dataclass(frozen=True, slots=True)
class CheckPackMetadata:
    """Static metadata applied uniformly to every check defined in one pack."""

    parity_baseline: CheckParityBaseline
    jurisdictions: tuple[CheckJurisdiction, ...]


@dataclass(frozen=True, slots=True)
class CheckDefinition:
    """Static identity and capability metadata for one quality check."""

    id: str
    definition_language: CheckDefinitionLanguage
    parity_baseline: CheckParityBaseline
    jurisdictions: tuple[CheckJurisdiction, ...]
    legacy_identity: LegacyCheckIdentity | None = None
    required_context_paths: tuple[str, ...] = ()
    supported_input_surfaces: tuple[CheckInputSurface, ...] = ()

    def __post_init__(self) -> None:
        """Resolve the explicit legacy identity implied by the parity baseline."""
        object.__setattr__(
            self,
            "legacy_identity",
            resolve_legacy_check_identity(
                check_id=self.id,
                parity_baseline=self.parity_baseline,
                legacy_identity=self.legacy_identity,
            ),
        )

    def supports_input_surface(self, surface: CheckInputSurface) -> bool:
        """Return whether this check can run on the requested normalized surface."""
        return surface in self.supported_input_surfaces

    def matches_selection(self, selection: CheckSelection) -> bool:
        """Return whether this check satisfies one catalog selection filter."""
        if selection.input_surface is not None and not self.supports_input_surface(
            selection.input_surface
        ):
            return False
        if (
            selection.parity_baselines is not None
            and self.parity_baseline not in selection.parity_baselines
        ):
            return False
        if selection.jurisdictions is None:
            return True
        return any(
            jurisdiction in self.jurisdictions
            for jurisdiction in selection.jurisdictions
        )


def validate_check_parity_baseline(value: str) -> CheckParityBaseline:
    """Return one validated parity-baseline literal."""
    baseline = _PARITY_BASELINES_BY_NAME.get(value)
    if baseline is not None:
        return baseline
    raise ValueError(f"Unsupported check parity baseline {value!r}.")


def validate_check_jurisdiction(value: str) -> CheckJurisdiction:
    """Return one validated jurisdiction literal."""
    jurisdiction = _JURISDICTIONS_BY_NAME.get(value)
    if jurisdiction is not None:
        return jurisdiction
    raise ValueError(f"Unsupported check jurisdiction {value!r}.")


def normalize_check_parity_baselines(
    values: Iterable[str] | None,
) -> tuple[CheckParityBaseline, ...] | None:
    """Return unique validated parity baselines preserving first-seen order."""
    if values is None:
        return None
    normalized: list[CheckParityBaseline] = []
    seen: set[str] = set()
    for raw_value in values:
        value = raw_value.strip()
        baseline = validate_check_parity_baseline(value)
        if baseline in seen:
            continue
        seen.add(baseline)
        normalized.append(baseline)
    if not normalized:
        raise ValueError("Check parity baselines must contain at least one value.")
    return tuple(normalized)


def resolve_legacy_check_identity(
    *,
    check_id: str,
    parity_baseline: CheckParityBaseline,
    legacy_identity: LegacyCheckIdentity | None,
) -> LegacyCheckIdentity | None:
    """Return the explicit legacy identity for one check."""
    if parity_baseline == "legacy":
        return legacy_identity or LegacyCheckIdentity(code_template=check_id)
    if legacy_identity is not None:
        raise ValueError(
            f"Check {check_id} cannot declare a legacy identity without "
            'parity_baseline="legacy".'
        )
    return None


def normalize_check_jurisdictions(
    values: Iterable[str] | None,
) -> tuple[CheckJurisdiction, ...] | None:
    """Return unique validated jurisdictions preserving first-seen order."""
    if values is None:
        return None
    normalized: list[CheckJurisdiction] = []
    seen: set[str] = set()
    for raw_value in values:
        value = raw_value.strip()
        jurisdiction = validate_check_jurisdiction(value)
        if jurisdiction in seen:
            continue
        seen.add(jurisdiction)
        normalized.append(jurisdiction)
    if not normalized:
        raise ValueError("Check jurisdictions must contain at least one value.")
    return tuple(normalized)
