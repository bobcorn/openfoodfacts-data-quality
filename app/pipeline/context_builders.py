from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from openfoodfacts_data_quality.context.builder import (
    build_enriched_contexts,
    build_raw_contexts,
)

if TYPE_CHECKING:
    from openfoodfacts_data_quality.contracts.checks import CheckInputSurface
    from openfoodfacts_data_quality.contracts.context import NormalizedContext
    from openfoodfacts_data_quality.contracts.enrichment import EnrichedSnapshotResult


class SupportsCheckContextBuilder(Protocol):
    """Context-building strategy selected for one migrated check runtime surface."""

    @property
    def input_surface(self) -> CheckInputSurface: ...

    @property
    def requires_enriched_snapshots(self) -> bool: ...

    def build_contexts(
        self,
        *,
        rows: list[dict[str, Any]],
        enriched_snapshots: Sequence[EnrichedSnapshotResult],
    ) -> list[NormalizedContext]: ...


@dataclass(frozen=True, slots=True)
class RawProductsContextBuilder:
    """Build migrated check contexts directly from raw public-product rows."""

    input_surface: CheckInputSurface = "raw_products"
    requires_enriched_snapshots: bool = False

    def build_contexts(
        self,
        *,
        rows: list[dict[str, Any]],
        enriched_snapshots: Sequence[EnrichedSnapshotResult],
    ) -> list[NormalizedContext]:
        """Build runtime contexts from the raw source rows."""
        del enriched_snapshots
        return build_raw_contexts(rows)


@dataclass(frozen=True, slots=True)
class EnrichedProductsContextBuilder:
    """Build migrated check contexts from explicit backend-enriched snapshots."""

    input_surface: CheckInputSurface = "enriched_products"
    requires_enriched_snapshots: bool = True

    def build_contexts(
        self,
        *,
        rows: list[dict[str, Any]],
        enriched_snapshots: Sequence[EnrichedSnapshotResult],
    ) -> list[NormalizedContext]:
        """Build runtime contexts from the enriched backend snapshot."""
        del rows
        return build_enriched_contexts(enriched_snapshots)


RAW_PRODUCTS_CONTEXT_BUILDER = RawProductsContextBuilder()
ENRICHED_PRODUCTS_CONTEXT_BUILDER = EnrichedProductsContextBuilder()


def check_context_builder_for(
    input_surface: CheckInputSurface,
) -> SupportsCheckContextBuilder:
    """Return the migrated-runtime context builder for one input surface."""
    if input_surface == "raw_products":
        return RAW_PRODUCTS_CONTEXT_BUILDER
    if input_surface == "enriched_products":
        return ENRICHED_PRODUCTS_CONTEXT_BUILDER
    raise ValueError(f"Unsupported check input surface: {input_surface!r}")
