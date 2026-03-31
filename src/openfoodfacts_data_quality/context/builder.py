from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

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


def build_raw_contexts(
    rows: Iterable[Mapping[str, Any]],
) -> list[NormalizedContext]:
    """Build normalized contexts directly from raw OFF product rows."""
    return [_build_raw_context(dict(row)) for row in rows]


def _build_raw_context(row: dict[str, Any]) -> NormalizedContext:
    """Assemble the normalized context from one raw OFF row."""
    return NormalizedContext(
        code=str(row["code"]),
        product=build_raw_product_projection(row),
        nutrition=build_raw_nutrition_projection(row),
    )


def build_enriched_contexts(
    enriched_snapshots: Iterable[EnrichedSnapshotResult],
) -> list[NormalizedContext]:
    """Build normalized contexts from explicit enriched snapshots."""
    return [
        _build_enriched_context(enriched_snapshot)
        for enriched_snapshot in enriched_snapshots
    ]


def _build_enriched_context(
    enriched_snapshot: EnrichedSnapshotResult,
) -> NormalizedContext:
    """Assemble the stable check context from one enriched snapshot."""
    snapshot = enriched_snapshot.enriched_snapshot
    product_snapshot = snapshot.product
    snapshot_code = product_snapshot.get("code")
    if snapshot_code is not None and snapshot_code != enriched_snapshot.code:
        raise ValueError(
            f"Enriched snapshot code mismatch for snapshot {enriched_snapshot.code}."
        )

    return NormalizedContext(
        code=enriched_snapshot.code,
        product=build_enriched_product_projection(
            code=enriched_snapshot.code,
            product_snapshot=product_snapshot,
        ),
        flags=build_enriched_flags_projection(snapshot.flags),
        category_props=build_enriched_category_props_projection(
            snapshot.category_props
        ),
        nutrition=build_enriched_nutrition_projection(snapshot.nutrition),
    )
