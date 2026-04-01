from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class MappingViewModel(BaseModel):
    """Base model with a stable mapping view for adapter-friendly contracts."""

    def as_mapping(self) -> dict[str, Any]:
        """Serialize one section while dropping absent optional fields."""
        return self.model_dump(mode="python", exclude_none=True)

    def __getitem__(self, key: str) -> Any:
        return self.as_mapping()[key]

    def __contains__(self, key: object) -> bool:
        return key in self.as_mapping()

    def get(self, key: str, default: Any = None) -> Any:
        """Read one field from the mapping view while preserving missing semantics."""
        return self.as_mapping().get(key, default)
