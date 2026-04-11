from __future__ import annotations

import os
from os import cpu_count
from pathlib import Path

from migration.reference.cache import (
    configured_reference_result_cache_dir,
    configured_reference_result_cache_salt,
)
from migration.run.models import PreviewSettings, RunSpec

SOURCE_SNAPSHOT_PATH_ENV_VAR = "SOURCE_SNAPSHOT_PATH"
BATCH_SIZE_ENV_VAR = "BATCH_SIZE"
MISMATCH_EXAMPLES_LIMIT_ENV_VAR = "MISMATCH_EXAMPLES_LIMIT"
BATCH_WORKERS_ENV_VAR = "BATCH_WORKERS"
LEGACY_BACKEND_WORKERS_ENV_VAR = "LEGACY_BACKEND_WORKERS"
CHECK_PROFILE_ENV_VAR = "CHECK_PROFILE"
SOURCE_DATASET_PROFILE_ENV_VAR = "SOURCE_DATASET_PROFILE"
PORT_ENV_VAR = "PORT"
PARITY_STORE_PATH_ENV_VAR = "PARITY_STORE_PATH"

DEFAULT_BATCH_SIZE = 5_000
DEFAULT_MISMATCH_EXAMPLES_LIMIT = 20
DEFAULT_BATCH_WORKERS = max(1, min(8, cpu_count() or 2))
DEFAULT_LEGACY_BACKEND_WORKERS = max(1, min(4, (cpu_count() or 2) // 2 or 1))
DEFAULT_PORT = 8000


def configured_run_spec(
    project_root: Path,
    *,
    db_path: Path | None = None,
    batch_size: int | None = None,
    mismatch_examples_limit: int | None = None,
    batch_workers: int | None = None,
    legacy_backend_workers: int | None = None,
    check_profile_name: str | None = None,
    parity_store_path: Path | None = None,
    dataset_profile_name: str | None = None,
) -> RunSpec:
    """Return the explicit run spec for one local migration execution."""
    return RunSpec(
        project_root=project_root.resolve(),
        db_path=(
            db_path.expanduser().resolve()
            if db_path is not None
            else configured_source_snapshot_path(project_root)
        ),
        batch_size=(configured_batch_size() if batch_size is None else int(batch_size)),
        mismatch_examples_limit=(
            configured_mismatch_examples_limit()
            if mismatch_examples_limit is None
            else int(mismatch_examples_limit)
        ),
        batch_workers=(
            configured_batch_workers() if batch_workers is None else int(batch_workers)
        ),
        legacy_backend_workers=(
            configured_legacy_backend_workers()
            if legacy_backend_workers is None
            else int(legacy_backend_workers)
        ),
        reference_result_cache_dir=configured_reference_result_cache_dir(project_root),
        reference_result_cache_salt=configured_reference_result_cache_salt(),
        check_profile_name=(
            configured_check_profile_name()
            if check_profile_name is None
            else check_profile_name
        ),
        dataset_profile_name=(
            configured_source_dataset_profile_name()
            if dataset_profile_name is None
            else dataset_profile_name
        ),
        parity_store_path=(
            configured_parity_store_path(project_root)
            if parity_store_path is None
            else parity_store_path.expanduser().resolve()
        ),
    )


def configured_preview_settings() -> PreviewSettings:
    """Return the configured local preview settings."""
    return PreviewSettings(port=_configured_int(PORT_ENV_VAR, DEFAULT_PORT))


def configured_check_profile_name() -> str | None:
    """Return the selected check profile, if explicitly configured."""
    return _configured_optional_name(CHECK_PROFILE_ENV_VAR)


def configured_source_dataset_profile_name() -> str | None:
    """Return the selected source dataset profile, if explicitly configured."""
    return _configured_optional_name(SOURCE_DATASET_PROFILE_ENV_VAR)


def _configured_optional_name(env_var: str) -> str | None:
    """Return one optional name-like environment setting."""
    configured = os.environ.get(env_var)
    if configured is None:
        return None
    normalized = configured.strip()
    return normalized or None


def configured_source_snapshot_path(project_root: Path) -> Path:
    """Return the configured source snapshot path."""
    del project_root
    configured = os.environ.get(SOURCE_SNAPSHOT_PATH_ENV_VAR)
    if configured is None or not configured.strip():
        raise ValueError(
            "SOURCE_SNAPSHOT_PATH must be set for local runtime runs. Use the migration demo image for the bundled example snapshot."
        )
    return Path(configured).expanduser().resolve()


def configured_parity_store_path(project_root: Path) -> Path:
    """Return the configured migration-owned run store path."""
    configured = os.environ.get(PARITY_STORE_PATH_ENV_VAR)
    if configured is not None and configured.strip():
        return Path(configured).expanduser().resolve()
    return (project_root / "data" / "parity_store" / "parity.duckdb").resolve()


def configured_batch_size() -> int:
    """Return the configured execution batch size."""
    return _configured_int(BATCH_SIZE_ENV_VAR, DEFAULT_BATCH_SIZE)


def configured_mismatch_examples_limit() -> int:
    """Return the configured mismatch example retention budget for each check."""
    return _configured_int(
        MISMATCH_EXAMPLES_LIMIT_ENV_VAR,
        DEFAULT_MISMATCH_EXAMPLES_LIMIT,
    )


def configured_batch_workers() -> int:
    """Return the configured number of concurrent batch workers."""
    return _configured_int(BATCH_WORKERS_ENV_VAR, DEFAULT_BATCH_WORKERS)


def configured_legacy_backend_workers() -> int:
    """Return the configured number of persistent backend workers."""
    return _configured_int(
        LEGACY_BACKEND_WORKERS_ENV_VAR,
        DEFAULT_LEGACY_BACKEND_WORKERS,
    )


def _configured_int(env_var: str, default: int) -> int:
    """Return one integer environment setting with a fallback default."""
    configured = os.environ.get(env_var)
    if configured is None or not configured.strip():
        return default
    return int(configured)
