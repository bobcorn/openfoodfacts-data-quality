from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from openfoodfacts_data_quality.contracts.run import RunResult

RUN_ARTIFACT_KIND = "openfoodfacts_data_quality.run_artifact"
RUN_ARTIFACT_SCHEMA_VERSION = 1
RUN_ARTIFACT_FILENAME = "run.json"


def build_run_artifact(run_result: RunResult) -> dict[str, Any]:
    """Serialize the canonical run artifact."""
    return {
        "kind": RUN_ARTIFACT_KIND,
        "schema_version": RUN_ARTIFACT_SCHEMA_VERSION,
        **run_result.model_dump(mode="json"),
    }


def write_run_artifact(run_result: RunResult, output_dir: Path) -> Path:
    """Write the canonical run artifact to disk."""
    output_path = output_dir / RUN_ARTIFACT_FILENAME
    output_path.write_text(
        json.dumps(
            build_run_artifact(run_result),
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return output_path
