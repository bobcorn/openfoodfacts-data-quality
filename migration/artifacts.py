from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ArtifactWorkspace:
    """Resolved artifact paths for one local migration run."""

    artifacts_dir: Path
    site_dir: Path
    legacy_backend_stderr_path: Path


def prepare_artifact_workspace(project_root: Path) -> ArtifactWorkspace:
    """Reset and recreate the latest-artifacts directory tree."""
    artifacts_dir = project_root / "artifacts" / "latest"
    site_dir = artifacts_dir / "site"
    if artifacts_dir.exists():
        shutil.rmtree(artifacts_dir)
    site_dir.mkdir(parents=True, exist_ok=True)
    return ArtifactWorkspace(
        artifacts_dir=artifacts_dir,
        site_dir=site_dir,
        legacy_backend_stderr_path=artifacts_dir / "legacy-backend-stderr.log",
    )


def display_path(path: Path, project_root: Path) -> str:
    """Return a stable, user-friendly display path."""
    try:
        return str(path.relative_to(project_root))
    except ValueError:
        return str(path)
