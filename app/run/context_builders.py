from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from openfoodfacts_data_quality.context.builder import (
    build_source_product_contexts,
    iter_source_product_contexts,
)
from openfoodfacts_data_quality.context.providers import (
    ENRICHED_SNAPSHOTS_PROVIDER,
    SOURCE_PRODUCTS_PROVIDER,
)

if TYPE_CHECKING:
    from openfoodfacts_data_quality.context.providers import ContextProviderId
    from openfoodfacts_data_quality.contracts.context import CheckContext
    from openfoodfacts_data_quality.contracts.source_products import SourceProduct


class SupportsCheckContextBuilder(Protocol):
    """Context-building strategy selected for one migrated check runtime provider."""

    @property
    def context_provider(self) -> ContextProviderId: ...

    @property
    def requires_reference_check_contexts(self) -> bool: ...

    def build_contexts(
        self,
        *,
        rows: list[SourceProduct],
        reference_check_contexts: Sequence[CheckContext],
    ) -> list[CheckContext]: ...

    def iter_contexts(
        self,
        *,
        rows: list[SourceProduct],
        reference_check_contexts: Sequence[CheckContext],
    ) -> Iterable[CheckContext]: ...


@dataclass(frozen=True, slots=True)
class SourceProductContextBuilder:
    """Build migrated check contexts directly from source products."""

    context_provider: ContextProviderId = SOURCE_PRODUCTS_PROVIDER.name
    requires_reference_check_contexts: bool = False

    def build_contexts(
        self,
        *,
        rows: list[SourceProduct],
        reference_check_contexts: Sequence[CheckContext],
    ) -> list[CheckContext]:
        """Build runtime contexts from the source products."""
        del reference_check_contexts
        return build_source_product_contexts(rows)

    def iter_contexts(
        self,
        *,
        rows: list[SourceProduct],
        reference_check_contexts: Sequence[CheckContext],
    ) -> Iterable[CheckContext]:
        """Yield runtime contexts from the source products."""
        del reference_check_contexts
        return iter_source_product_contexts(rows)


@dataclass(frozen=True, slots=True)
class EnrichedSnapshotContextBuilder:
    """Build migrated check contexts from reference-side enriched contexts."""

    context_provider: ContextProviderId = ENRICHED_SNAPSHOTS_PROVIDER.name
    requires_reference_check_contexts: bool = True

    def build_contexts(
        self,
        *,
        rows: list[SourceProduct],
        reference_check_contexts: Sequence[CheckContext],
    ) -> list[CheckContext]:
        """Build runtime contexts from reference-side enriched contexts."""
        del rows
        return list(reference_check_contexts)

    def iter_contexts(
        self,
        *,
        rows: list[SourceProduct],
        reference_check_contexts: Sequence[CheckContext],
    ) -> Iterable[CheckContext]:
        """Yield runtime contexts from reference-side enriched contexts."""
        del rows
        return (context for context in reference_check_contexts)


SOURCE_PRODUCT_CONTEXT_BUILDER = SourceProductContextBuilder()
ENRICHED_SNAPSHOT_CONTEXT_BUILDER = EnrichedSnapshotContextBuilder()


def check_context_builder_for(
    context_provider: ContextProviderId,
) -> SupportsCheckContextBuilder:
    """Return the migrated-runtime context builder for one context provider."""
    if context_provider == SOURCE_PRODUCTS_PROVIDER.name:
        return SOURCE_PRODUCT_CONTEXT_BUILDER
    if context_provider == ENRICHED_SNAPSHOTS_PROVIDER.name:
        return ENRICHED_SNAPSHOT_CONTEXT_BUILDER
    raise ValueError(f"Unsupported check context provider: {context_provider!r}")
