from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Literal

from off_data_quality.context._builder import (
    build_enriched_snapshot_contexts,
    build_source_product_contexts,
)
from off_data_quality.context._paths import PATH_SPECS
from off_data_quality.context._projection import SOURCE_PRODUCT_CONTEXT_PATHS
from off_data_quality.contracts.capabilities import ContextAvailability
from off_data_quality.contracts.context import CheckContext
from off_data_quality.contracts.enrichment import EnrichedSnapshotRecord
from off_data_quality.contracts.source_products import SourceProduct

ContextProviderId = Literal["source_products", "enriched_snapshots"]

type ContextBuilder[ProviderInput] = Callable[
    [Iterable[ProviderInput]],
    list[CheckContext],
]


@dataclass(frozen=True, slots=True)
class ContextProvider[ProviderInput]:
    """Runtime provider contract used by library APIs and capability checks."""

    name: ContextProviderId
    availability: ContextAvailability
    build_contexts: ContextBuilder[ProviderInput]


_ALL_CONTEXT_PATHS: frozenset[str] = frozenset(spec.path for spec in PATH_SPECS)
_UNKNOWN_SOURCE_PRODUCT_CONTEXT_PATHS = (
    SOURCE_PRODUCT_CONTEXT_PATHS - _ALL_CONTEXT_PATHS
)
if _UNKNOWN_SOURCE_PRODUCT_CONTEXT_PATHS:
    unknown_paths = ", ".join(sorted(_UNKNOWN_SOURCE_PRODUCT_CONTEXT_PATHS))
    raise RuntimeError(f"Unknown source product context paths: {unknown_paths}")


SOURCE_PRODUCTS_PROVIDER = ContextProvider[SourceProduct](
    name="source_products",
    availability=ContextAvailability(
        available_context_paths=SOURCE_PRODUCT_CONTEXT_PATHS,
    ),
    build_contexts=build_source_product_contexts,
)
ENRICHED_SNAPSHOTS_PROVIDER = ContextProvider[EnrichedSnapshotRecord](
    name="enriched_snapshots",
    availability=ContextAvailability(
        available_context_paths=_ALL_CONTEXT_PATHS,
    ),
    build_contexts=build_enriched_snapshot_contexts,
)
CHECK_CONTEXT_PROVIDERS: tuple[ContextProviderId, ...] = (
    SOURCE_PRODUCTS_PROVIDER.name,
    ENRICHED_SNAPSHOTS_PROVIDER.name,
)


def validate_context_provider(provider: str) -> ContextProviderId:
    """Normalize and validate one configured check context provider name."""
    if provider == SOURCE_PRODUCTS_PROVIDER.name:
        return SOURCE_PRODUCTS_PROVIDER.name
    if provider == ENRICHED_SNAPSHOTS_PROVIDER.name:
        return ENRICHED_SNAPSHOTS_PROVIDER.name
    raise ValueError(
        f"Unsupported context provider {provider!r}. Expected one of: "
        f"{', '.join(CHECK_CONTEXT_PROVIDERS)}."
    )


def context_availability_for_provider(
    provider: ContextProviderId,
) -> ContextAvailability:
    """Return the context paths exposed by one provider."""
    if provider == SOURCE_PRODUCTS_PROVIDER.name:
        return SOURCE_PRODUCTS_PROVIDER.availability
    if provider == ENRICHED_SNAPSHOTS_PROVIDER.name:
        return ENRICHED_SNAPSHOTS_PROVIDER.availability
    raise ValueError(f"Unsupported check context provider: {provider!r}")


__all__ = [
    "ENRICHED_SNAPSHOTS_PROVIDER",
    "CHECK_CONTEXT_PROVIDERS",
    "ContextProviderId",
    "ContextProvider",
    "SOURCE_PRODUCTS_PROVIDER",
    "context_availability_for_provider",
    "validate_context_provider",
]
