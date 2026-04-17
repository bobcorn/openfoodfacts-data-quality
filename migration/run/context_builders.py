from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from off_data_quality.context import ENRICHED_SNAPSHOTS_PROVIDER

if TYPE_CHECKING:
    from off_data_quality.context import ContextProviderId
    from off_data_quality.contracts.context import CheckContext


class SupportsCheckContextBuilder(Protocol):
    """Context builder selected for one strict parity migration run."""

    @property
    def context_provider(self) -> ContextProviderId: ...

    @property
    def requires_reference_check_contexts(self) -> bool: ...

    def build_contexts(
        self,
        *,
        reference_check_contexts: Sequence[CheckContext],
    ) -> list[CheckContext]: ...

    def iter_contexts(
        self,
        *,
        reference_check_contexts: Sequence[CheckContext],
    ) -> Iterable[CheckContext]: ...


@dataclass(frozen=True, slots=True)
class EnrichedSnapshotContextBuilder:
    """Build migrated check contexts from enriched reference contexts."""

    context_provider: ContextProviderId = ENRICHED_SNAPSHOTS_PROVIDER.name
    requires_reference_check_contexts: bool = True

    def build_contexts(
        self,
        *,
        reference_check_contexts: Sequence[CheckContext],
    ) -> list[CheckContext]:
        """Build runtime contexts from enriched reference contexts."""
        return list(reference_check_contexts)

    def iter_contexts(
        self,
        *,
        reference_check_contexts: Sequence[CheckContext],
    ) -> Iterable[CheckContext]:
        """Yield runtime contexts from enriched reference contexts."""
        return (context for context in reference_check_contexts)


ENRICHED_SNAPSHOT_CONTEXT_BUILDER = EnrichedSnapshotContextBuilder()


def check_context_builder_for(
    context_provider: ContextProviderId,
) -> SupportsCheckContextBuilder:
    """Return the migration runtime context builder for the strict parity provider."""
    if context_provider == ENRICHED_SNAPSHOTS_PROVIDER.name:
        return ENRICHED_SNAPSHOT_CONTEXT_BUILDER
    raise ValueError(
        "Migration strict parity only supports the "
        f"{ENRICHED_SNAPSHOTS_PROVIDER.name!r} context provider, got "
        f"{context_provider!r}."
    )
