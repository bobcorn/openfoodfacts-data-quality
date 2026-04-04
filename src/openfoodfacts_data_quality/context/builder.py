from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping

from openfoodfacts_data_quality.context.projection import (
    build_enriched_category_props_projection,
    build_enriched_flags_projection,
    build_enriched_nutrition_projection,
    build_enriched_product_projection,
    build_raw_nutrition_projection,
    build_raw_product_projection,
)
from openfoodfacts_data_quality.contracts.context import NormalizedContext
from openfoodfacts_data_quality.contracts.enrichment import EnrichedSnapshotResult
from openfoodfacts_data_quality.contracts.raw import RawProductRow
from openfoodfacts_data_quality.source_rows import normalize_raw_input_row


def build_raw_contexts(
    rows: Iterable[RawProductRow | Mapping[str, object]],
) -> list[NormalizedContext]:
    """Build normalized contexts from public Open Food Facts product rows."""
    return list(iter_raw_contexts(rows))


def iter_raw_contexts(
    rows: Iterable[RawProductRow | Mapping[str, object]],
) -> Iterator[NormalizedContext]:
    """Yield normalized contexts from public Open Food Facts product rows."""
    for row in rows:
        yield _build_raw_context(normalize_raw_input_row(row))


def _build_raw_context(row: RawProductRow) -> NormalizedContext:
    """Assemble the normalized context from one normalized raw row."""
    return NormalizedContext(
        code=row.code,
        product=build_raw_product_projection(row),
        nutrition=build_raw_nutrition_projection(row),
    )


def build_enriched_contexts(
    enriched_snapshots: Iterable[EnrichedSnapshotResult],
) -> list[NormalizedContext]:
    """Build normalized contexts from explicit enriched snapshots."""
    return list(iter_enriched_contexts(enriched_snapshots))


def iter_enriched_contexts(
    enriched_snapshots: Iterable[EnrichedSnapshotResult],
) -> Iterator[NormalizedContext]:
    """Yield normalized contexts from explicit enriched snapshots."""
    for enriched_snapshot in enriched_snapshots:
        yield _build_enriched_context(enriched_snapshot)


def _build_enriched_context(
    enriched_snapshot: EnrichedSnapshotResult,
) -> NormalizedContext:
    """Assemble the stable check context from one enriched snapshot."""
    snapshot = enriched_snapshot.enriched_snapshot

    return NormalizedContext(
        code=enriched_snapshot.code,
        product=build_enriched_product_projection(
            code=enriched_snapshot.code,
            product_snapshot=snapshot.product,
        ),
        flags=build_enriched_flags_projection(snapshot.flags),
        category_props=build_enriched_category_props_projection(
            snapshot.category_props
        ),
        nutrition=build_enriched_nutrition_projection(snapshot.nutrition),
    )
