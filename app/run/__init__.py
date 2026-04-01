"""Run orchestration for the OFF quality application."""

from __future__ import annotations

from pathlib import Path

__all__ = [
    "build_site",
    "configured_batch_size",
    "configured_batch_workers",
    "configured_database_path",
    "configured_legacy_backend_workers",
    "configured_mismatch_examples_limit",
]


def build_site(
    project_root: Path,
    *,
    db_path: Path | None = None,
    batch_size: int | None = None,
    mismatch_examples_limit: int | None = None,
    batch_workers: int | None = None,
    legacy_backend_workers: int | None = None,
) -> Path:
    """Build artifacts and render the report for the current source snapshot."""
    from app.run.orchestrator import (
        DEFAULT_BATCH_SIZE,
        DEFAULT_BATCH_WORKERS,
        DEFAULT_LEGACY_BACKEND_WORKERS,
        DEFAULT_MISMATCH_EXAMPLES_LIMIT,
    )
    from app.run.orchestrator import (
        build_site as orchestrator_build_site,
    )

    return orchestrator_build_site(
        project_root,
        db_path=db_path,
        batch_size=(DEFAULT_BATCH_SIZE if batch_size is None else batch_size),
        mismatch_examples_limit=(
            DEFAULT_MISMATCH_EXAMPLES_LIMIT
            if mismatch_examples_limit is None
            else mismatch_examples_limit
        ),
        batch_workers=(
            DEFAULT_BATCH_WORKERS if batch_workers is None else batch_workers
        ),
        legacy_backend_workers=(
            DEFAULT_LEGACY_BACKEND_WORKERS
            if legacy_backend_workers is None
            else legacy_backend_workers
        ),
    )


def configured_batch_size() -> int:
    """Return the configured batch size for the current run."""
    from app.run.orchestrator import configured_batch_size as get_configured_batch_size

    return get_configured_batch_size()


def configured_batch_workers() -> int:
    """Return the configured batch worker count for the current run."""
    from app.run.orchestrator import (
        configured_batch_workers as get_configured_batch_workers,
    )

    return get_configured_batch_workers()


def configured_database_path(project_root: Path) -> Path:
    """Return the configured source database path."""
    from app.run.orchestrator import (
        configured_database_path as get_configured_database_path,
    )

    return get_configured_database_path(project_root)


def configured_legacy_backend_workers() -> int:
    """Return the configured number of legacy backend workers."""
    from app.run.orchestrator import (
        configured_legacy_backend_workers as get_configured_legacy_backend_workers,
    )

    return get_configured_legacy_backend_workers()


def configured_mismatch_examples_limit() -> int:
    """Return the configured mismatch example cap."""
    from app.run.orchestrator import (
        configured_mismatch_examples_limit as get_configured_mismatch_examples_limit,
    )

    return get_configured_mismatch_examples_limit()
