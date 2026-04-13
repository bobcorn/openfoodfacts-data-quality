from __future__ import annotations

import hashlib

from migration.source.datasets import SourceSelection
from migration.source.models import SkippedSourceRow

SKIPPED_SOURCE_ROW_EXAMPLE_LIMIT = 20
MISSING_OR_BLANK_CODE_REASON = "missing or blank code"


def stable_sample_hash(code: str, seed: int) -> int:
    """Return a deterministic hash used for stable source sampling."""
    digest = hashlib.sha256(f"{code}::{seed}".encode()).hexdigest()
    return int(digest, 16)


def required_selection_sample_size(selection: SourceSelection) -> int:
    """Return the required sample size for one stable-sample selection."""
    if selection.sample_size is None:
        raise ValueError("stable_sample selections must define sample_size.")
    return selection.sample_size


def required_selection_seed(selection: SourceSelection) -> int:
    """Return the required sampling seed for one stable-sample selection."""
    if selection.seed is None:
        raise ValueError("stable_sample selections must define seed.")
    return selection.seed


def record_skipped_source_row(
    skipped_row: SkippedSourceRow,
    *,
    skipped_row_count: int,
    skipped_row_examples: list[SkippedSourceRow],
) -> int:
    """Accumulate one skipped-row diagnostic and return the new count."""
    skipped_row_count += 1
    if len(skipped_row_examples) < SKIPPED_SOURCE_ROW_EXAMPLE_LIMIT:
        skipped_row_examples.append(skipped_row)
    return skipped_row_count


def skip_reason_from_product_document_error(error: ValueError) -> str | None:
    """Map product-document validation errors to skip reasons when possible."""
    if str(error) not in {
        "ProductDocument requires a string 'code' field.",
        "ProductDocument requires a non-empty 'code' field.",
    }:
        return None
    return MISSING_OR_BLANK_CODE_REASON
