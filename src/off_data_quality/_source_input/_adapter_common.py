from __future__ import annotations

from collections.abc import Callable, Mapping

from off_data_quality.contracts.source_products import (
    SOURCE_PRODUCT_INPUT_COLUMNS,
    SOURCE_PRODUCT_NUTRIMENT_COLUMNS,
)

_CANONICAL_SOURCE_COLUMNS = frozenset(SOURCE_PRODUCT_INPUT_COLUMNS)
_CANONICAL_NUTRIMENT_COLUMNS = frozenset(SOURCE_PRODUCT_NUTRIMENT_COLUMNS)


def select_canonical_source_fields(row: Mapping[str, object]) -> dict[str, object]:
    """Keep only canonical source columns from one row."""
    return {
        column: value
        for column, value in row.items()
        if column in _CANONICAL_SOURCE_COLUMNS
    }


def overlapping_canonical_nutriment_columns(
    row: Mapping[str, object],
) -> list[str]:
    """Return canonical nutriment columns already present in one row."""
    return sorted(column for column in _CANONICAL_NUTRIMENT_COLUMNS if column in row)


def combine_canonical_nutriment_values(
    row: Mapping[str, object],
    *,
    decoder: Callable[[object], dict[str, object]],
) -> dict[str, object]:
    """Merge explicit canonical nutriments with structured nutriments when present."""
    projected = {
        column: row[column] for column in _CANONICAL_NUTRIMENT_COLUMNS if column in row
    }
    if "nutriments" not in row:
        return projected

    projected.update(decoder(row.get("nutriments")))
    return projected
