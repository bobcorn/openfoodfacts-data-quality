from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from off_data_quality.contracts.checks import CheckDefinition


@dataclass(frozen=True, slots=True)
class ContextAvailability:
    """Context paths a provider can expose to checks."""

    available_context_paths: frozenset[str]


@dataclass(frozen=True, slots=True)
class CheckCapability:
    """Availability outcome for one check against one context provider."""

    check_id: str
    required_context_paths: tuple[str, ...]
    missing_context_paths: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CheckCapabilityReport:
    """Capability outcomes for an ordered check set."""

    runnable_capabilities: tuple[CheckCapability, ...]
    unsupported_capabilities: tuple[CheckCapability, ...]


def missing_context_paths(
    required_context_paths: tuple[str, ...],
    availability: ContextAvailability,
) -> tuple[str, ...]:
    """Return required check context paths the provider cannot expose."""
    return tuple(
        context_path
        for context_path in required_context_paths
        if context_path not in availability.available_context_paths
    )


def supports_context_paths(
    required_context_paths: tuple[str, ...],
    availability: ContextAvailability,
) -> bool:
    """Return whether the provider can expose all required context paths."""
    return not missing_context_paths(required_context_paths, availability)


def resolve_check_capabilities(
    checks: Iterable[CheckDefinition],
    availability: ContextAvailability,
) -> CheckCapabilityReport:
    """Return provider capability outcomes for the given checks."""
    runnable_capabilities: list[CheckCapability] = []
    unsupported_capabilities: list[CheckCapability] = []
    for check in checks:
        missing_paths = missing_context_paths(
            check.required_context_paths,
            availability,
        )
        capability = CheckCapability(
            check_id=check.id,
            required_context_paths=check.required_context_paths,
            missing_context_paths=missing_paths,
        )
        if missing_paths:
            unsupported_capabilities.append(capability)
        else:
            runnable_capabilities.append(capability)

    return CheckCapabilityReport(
        runnable_capabilities=tuple(runnable_capabilities),
        unsupported_capabilities=tuple(unsupported_capabilities),
    )


__all__ = [
    "CheckCapability",
    "CheckCapabilityReport",
    "ContextAvailability",
    "missing_context_paths",
    "resolve_check_capabilities",
    "supports_context_paths",
]
