from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from openfoodfacts_data_quality.contracts.source_products import SourceProduct


class SourceSnapshotFormat(StrEnum):
    """Supported full-document source snapshot formats for application runs."""

    DUCKDB = "duckdb"
    JSONL = "jsonl"


@dataclass(frozen=True, slots=True)
class ProductDocument:
    """Application-owned full product document used by the reference path."""

    code: str
    document: dict[str, object]

    def backend_input_payload(self) -> dict[str, object]:
        """Return the full product document payload sent to the legacy backend."""
        return dict(self.document)


@dataclass(frozen=True, slots=True)
class SourceBatchRecord:
    """One application source record with migrated and reference-side views."""

    source_product: SourceProduct
    product_document: ProductDocument


__all__ = [
    "ProductDocument",
    "SourceBatchRecord",
    "SourceSnapshotFormat",
]
