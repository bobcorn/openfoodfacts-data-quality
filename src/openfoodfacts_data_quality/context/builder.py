from __future__ import annotations

from collections.abc import Iterable, Iterator

from openfoodfacts_data_quality.context.projection import (
    build_enriched_category_props_projection,
    build_enriched_flags_projection,
    build_enriched_nutrition_projection,
    build_enriched_product_projection,
    build_source_nutrition_projection,
    build_source_product_projection,
)
from openfoodfacts_data_quality.contracts.context import CheckContext
from openfoodfacts_data_quality.contracts.enrichment import (
    EnrichedSnapshot,
    EnrichedSnapshotRecord,
)
from openfoodfacts_data_quality.contracts.source_products import SourceProduct


def build_source_product_contexts(
    rows: Iterable[SourceProduct],
) -> list[CheckContext]:
    """Build check contexts from Open Food Facts source product rows."""
    return list(iter_source_product_contexts(rows))


def iter_source_product_contexts(
    rows: Iterable[SourceProduct],
) -> Iterator[CheckContext]:
    """Yield check contexts from Open Food Facts source product rows."""
    for row in rows:
        yield _build_source_product_context(_require_source_product(row))


def _build_source_product_context(row: SourceProduct) -> CheckContext:
    """Assemble the check context from one source product."""
    return CheckContext(
        code=row.code,
        product=build_source_product_projection(row),
        nutrition=build_source_nutrition_projection(row),
    )


def _require_source_product(row: object) -> SourceProduct:
    """Return one canonical source product or fail fast at the runtime boundary."""
    if isinstance(row, SourceProduct):
        return row
    raise TypeError(
        "Source product context building expects validated SourceProduct values."
    )


def build_enriched_snapshot_context(
    *,
    code: str,
    enriched_snapshot: EnrichedSnapshot,
) -> CheckContext:
    """Build one check context from one enriched snapshot payload."""
    return CheckContext(
        code=code,
        product=build_enriched_product_projection(
            code=code,
            product_snapshot=enriched_snapshot.product,
        ),
        flags=build_enriched_flags_projection(enriched_snapshot.flags),
        category_props=build_enriched_category_props_projection(
            enriched_snapshot.category_props
        ),
        nutrition=build_enriched_nutrition_projection(enriched_snapshot.nutrition),
    )


def build_enriched_snapshot_record_context(
    enriched_snapshot_result: EnrichedSnapshotRecord,
) -> CheckContext:
    """Build one check context from one enriched snapshot record."""
    return build_enriched_snapshot_context(
        code=enriched_snapshot_result.code,
        enriched_snapshot=enriched_snapshot_result.enriched_snapshot,
    )


def build_enriched_snapshot_contexts(
    enriched_snapshots: Iterable[EnrichedSnapshotRecord],
) -> list[CheckContext]:
    """Build check contexts from explicit backend-enriched snapshots."""
    return list(iter_enriched_snapshot_contexts(enriched_snapshots))


def iter_enriched_snapshot_contexts(
    enriched_snapshots: Iterable[EnrichedSnapshotRecord],
) -> Iterator[CheckContext]:
    """Yield check contexts from explicit backend-enriched snapshots."""
    for enriched_snapshot in enriched_snapshots:
        yield build_enriched_snapshot_record_context(enriched_snapshot)
