from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from off_data_quality.contracts.source_products import SourceProduct


class SourceSnapshotFormat(StrEnum):
    """Supported full-document source snapshot formats for migration runs."""

    DUCKDB = "duckdb"
    JSONL = "jsonl"


@dataclass(frozen=True, slots=True)
class ProductDocument:
    """Migration-owned full product document used by the reference path."""

    code: str
    document: dict[str, object]

    def backend_input_payload(self) -> dict[str, object]:
        """Return the full product document payload sent to the legacy backend."""
        return dict(self.document)


@dataclass(frozen=True, slots=True)
class SourceBatchRecord:
    """One migration source record with migrated and reference-side views."""

    source_product: SourceProduct
    product_document: ProductDocument


@dataclass(frozen=True, slots=True)
class SkippedSourceRow:
    """One source row excluded before execution because it failed boundary validation."""

    location: str
    reason: str

    def as_payload(self) -> dict[str, str]:
        """Return a stable JSON-friendly payload."""
        return {
            "location": self.location,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class SourceInputSummary:
    """Summary of valid source rows together with any skipped-row diagnostics."""

    processed_product_count: int = 0
    skipped_row_count: int = 0
    skipped_row_examples: tuple[SkippedSourceRow, ...] = ()

    def as_payload(self) -> dict[str, object]:
        """Return a stable JSON-friendly payload."""
        return {
            "processed_product_count": self.processed_product_count,
            "skipped_row_count": self.skipped_row_count,
            "skipped_row_examples": [
                row.as_payload() for row in self.skipped_row_examples
            ],
        }


__all__ = [
    "ProductDocument",
    "SkippedSourceRow",
    "SourceBatchRecord",
    "SourceInputSummary",
    "SourceSnapshotFormat",
]
