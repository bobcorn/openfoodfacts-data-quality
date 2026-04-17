from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from off_data_quality.contracts.checks import Severity


class Finding(BaseModel):
    """Library level finding emitted by the quality check runtime."""

    product_id: str
    check_id: str
    severity: Severity
    emitted_code: str | None = None


def _finding_types_namespace() -> dict[str, object]:
    """Return runtime types needed by Pydantic to resolve deferred annotations."""
    from off_data_quality.contracts.checks import Severity

    return {"Severity": Severity}


Finding.model_rebuild(_types_namespace=_finding_types_namespace())
