from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Literal

from off_data_quality.contracts.source_products import SourceProduct

type SourceInputRow = Mapping[str, object]
type SourceInputContractId = Literal[
    "canonical_compatible",
    "off_full_document",
    "off_product_export",
]
type SourceInputProbeDecision = Literal["matched", "no_match", "invalid"]
type SourceInputProbe = Callable[[SourceInputRow, int], "SourceInputProbeResult"]
type SourceInputPreparer = Callable[[SourceInputRow, int], SourceProduct]

CANONICAL_COMPATIBLE_CONTRACT_NAME = "canonical-compatible source row"
OFF_PRODUCT_EXPORT_CONTRACT_NAME = "Open Food Facts product export"
OFF_FULL_DOCUMENT_CONTRACT_NAME = "Open Food Facts full document"
OFF_FULL_DOCUMENT_LOCALIZED_COLUMNS = frozenset({"product_name", "ingredients_text"})
OFF_FULL_DOCUMENT_ONLY_COLUMNS = frozenset(
    {
        "_id",
        "_keywords",
        "id",
        "categories_hierarchy",
        "brands_hierarchy",
        "countries_hierarchy",
    }
)
OFF_TAG_COLUMNS = frozenset(
    {"ingredients_tags", "categories_tags", "labels_tags", "countries_tags"}
)
OFF_PRODUCT_EXPORT_COLUMNS: tuple[str, ...] = (
    "code",
    "created_t",
    "product_name",
    "quantity",
    "product_quantity",
    "serving_size",
    "serving_quantity",
    "brands",
    "categories",
    "labels",
    "emb_codes",
    "ingredients_text",
    "ingredients_tags",
    "nutriscore_grade",
    "nutriscore_score",
    "categories_tags",
    "labels_tags",
    "countries_tags",
    "no_nutrition_data",
    "nutriments",
)


@dataclass(frozen=True, slots=True)
class SourceInputProbeResult:
    """Result of checking one input row against one supported contract."""

    decision: SourceInputProbeDecision
    reason: str | None = None

    @classmethod
    def matched(cls) -> SourceInputProbeResult:
        """Return a matching result."""
        return cls(decision="matched")

    @classmethod
    def no_match(cls) -> SourceInputProbeResult:
        """Return a non-matching result."""
        return cls(decision="no_match")

    @classmethod
    def invalid(cls, reason: str) -> SourceInputProbeResult:
        """Return an invalid result with a reason."""
        return cls(decision="invalid", reason=reason)


@dataclass(frozen=True, slots=True)
class SourceInputContractSpec:
    """One supported input contract at the `checks` facade boundary."""

    id: SourceInputContractId
    name: str
    probe: SourceInputProbe
    prepare: SourceInputPreparer


__all__ = [
    "CANONICAL_COMPATIBLE_CONTRACT_NAME",
    "OFF_FULL_DOCUMENT_CONTRACT_NAME",
    "OFF_FULL_DOCUMENT_LOCALIZED_COLUMNS",
    "OFF_FULL_DOCUMENT_ONLY_COLUMNS",
    "OFF_PRODUCT_EXPORT_COLUMNS",
    "OFF_PRODUCT_EXPORT_CONTRACT_NAME",
    "OFF_TAG_COLUMNS",
    "SourceInputContractId",
    "SourceInputContractSpec",
    "SourceInputProbe",
    "SourceInputProbeDecision",
    "SourceInputProbeResult",
    "SourceInputPreparer",
    "SourceInputRow",
]
