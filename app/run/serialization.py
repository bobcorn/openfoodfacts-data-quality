from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping
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


def parse_run_artifact(run_artifact: Mapping[str, Any]) -> RunResult:
    """Validate one run artifact payload and return the embedded run result."""
    from openfoodfacts_data_quality.contracts.run import RunResult

    kind = run_artifact.get("kind")
    if kind != RUN_ARTIFACT_KIND:
        raise ValueError(f"Unsupported run artifact kind {kind!r}.")
    schema_version = run_artifact.get("schema_version")
    if schema_version != RUN_ARTIFACT_SCHEMA_VERSION:
        raise ValueError(
            "Unsupported run artifact schema_version "
            f"{schema_version!r}; expected {RUN_ARTIFACT_SCHEMA_VERSION}."
        )
    payload = {
        key: value
        for key, value in run_artifact.items()
        if key not in {"kind", "schema_version"}
    }
    return RunResult.model_validate(payload)


def write_run_artifact_payload(
    run_artifact: Mapping[str, Any], output_dir: Path
) -> Path:
    """Write one prebuilt run artifact payload to disk."""
    output_path = output_dir / RUN_ARTIFACT_FILENAME
    output_path.write_text(
        json.dumps(
            dict(run_artifact),
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return output_path
