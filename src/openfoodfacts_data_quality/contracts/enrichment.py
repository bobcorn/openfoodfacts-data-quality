from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class EnrichedSnapshot(BaseModel):
    """Framework-facing enriched product snapshot consumed by enriched-quality checks."""

    product: dict[str, Any] = Field(default_factory=dict)
    flags: dict[str, Any] = Field(default_factory=dict)
    category_props: dict[str, Any] = Field(default_factory=dict)
    nutrition: dict[str, Any] = Field(default_factory=dict)


class EnrichedSnapshotResult(BaseModel):
    """Framework-facing enriched snapshot identified by product code."""

    code: str
    enriched_snapshot: EnrichedSnapshot
