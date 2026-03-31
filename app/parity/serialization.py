from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from app.parity.models import ParityResult

PARITY_ARTIFACT_FILENAME = "parity.json"


def build_parity_artifact(parity_result: ParityResult) -> dict[str, Any]:
    """Serialize the canonical machine-readable parity artifact."""
    return parity_result.model_dump(mode="json")


def write_parity_artifact(parity_result: ParityResult, output_dir: Path) -> Path:
    """Write the canonical machine-readable parity artifact to disk."""
    output_path = output_dir / PARITY_ARTIFACT_FILENAME
    output_path.write_text(
        json.dumps(
            build_parity_artifact(parity_result),
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return output_path
