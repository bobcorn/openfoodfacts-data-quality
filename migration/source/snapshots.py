from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

SOURCE_SNAPSHOT_ID_ENV_VAR = "SOURCE_SNAPSHOT_ID"


def source_snapshot_manifest_path_for(path: Path) -> Path:
    """Return the sidecar manifest path for one source snapshot."""
    return path.with_suffix(f"{path.suffix}.snapshot.json")


def write_source_snapshot_manifest(
    path: Path,
    *,
    source_snapshot_id: str,
) -> Path:
    """Persist one explicit sidecar manifest for a source snapshot."""
    normalized_snapshot_id = source_snapshot_id.strip()
    if not normalized_snapshot_id:
        raise ValueError("source_snapshot_id must not be blank.")
    manifest_path = source_snapshot_manifest_path_for(path)
    manifest_path.write_text(
        json.dumps({"source_snapshot_id": normalized_snapshot_id}, indent=2),
        encoding="utf-8",
    )
    return manifest_path


def source_snapshot_id_for(path: Path) -> str:
    """Return the configured or derived source snapshot identifier."""
    configured_snapshot_id = os.environ.get(SOURCE_SNAPSHOT_ID_ENV_VAR)
    if configured_snapshot_id is not None:
        normalized_snapshot_id = configured_snapshot_id.strip()
        if not normalized_snapshot_id:
            raise ValueError(
                f"{SOURCE_SNAPSHOT_ID_ENV_VAR} must not be blank when set."
            )
        return normalized_snapshot_id

    manifest_path = source_snapshot_manifest_path_for(path)
    if manifest_path.exists():
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        snapshot_id = str(payload.get("source_snapshot_id") or "").strip()
        if not snapshot_id:
            raise ValueError(
                f"Source snapshot manifest {manifest_path} must define "
                "'source_snapshot_id'."
            )
        return snapshot_id

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    source_snapshot_id = digest.hexdigest()[:12]
    write_source_snapshot_manifest(path, source_snapshot_id=source_snapshot_id)
    return source_snapshot_id


__all__ = [
    "SOURCE_SNAPSHOT_ID_ENV_VAR",
    "source_snapshot_id_for",
    "source_snapshot_manifest_path_for",
    "write_source_snapshot_manifest",
]
