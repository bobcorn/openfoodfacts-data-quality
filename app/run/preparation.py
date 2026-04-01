from __future__ import annotations

import logging
import shutil
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter

from app.reference.observers import reference_observer_for
from app.run.context_builders import check_context_builder_for
from app.run.models import PreparedRun
from app.run.profiles import (
    configured_check_profile_name,
    load_check_profile,
)
from app.source.duckdb_products import count_source_rows, source_snapshot_id_for
from openfoodfacts_data_quality.checks.catalog import (
    get_default_check_catalog,
)


def prepare_artifacts_dir(project_root: Path) -> tuple[Path, Path]:
    """Reset and recreate the latest-artifacts directory tree."""
    artifacts_dir = project_root / "artifacts" / "latest"
    site_dir = artifacts_dir / "site"
    if artifacts_dir.exists():
        shutil.rmtree(artifacts_dir)
    site_dir.mkdir(parents=True, exist_ok=True)
    return artifacts_dir, site_dir


def display_path(path: Path, project_root: Path) -> str:
    """Return a stable, user-friendly display path."""
    try:
        return str(path.relative_to(project_root))
    except ValueError:
        return str(path)


def prepare_run(
    project_root: Path,
    db_path: Path,
    *,
    logger: logging.Logger,
) -> PreparedRun:
    """Resolve source metadata, active checks, and evaluator counts."""
    input_started = perf_counter()
    logger.info("[Input] Loading products from %s", display_path(db_path, project_root))
    source_snapshot_id = source_snapshot_id_for(db_path)
    run_id = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    product_count = count_source_rows(db_path)
    logger.info(
        "[Input] Found %d products in source snapshot %s in %.1fs.",
        product_count,
        source_snapshot_id,
        perf_counter() - input_started,
    )
    check_catalog = get_default_check_catalog()
    active_check_profile = load_check_profile(
        project_root / "config" / "check-profiles.toml",
        configured_check_profile_name(),
        catalog=check_catalog,
    )
    evaluators = check_catalog.select_evaluators(active_check_profile.check_ids)
    python_count = sum(
        1
        for check in active_check_profile.checks
        if check.definition_language == "python"
    )
    return PreparedRun(
        source_snapshot_id=source_snapshot_id,
        run_id=run_id,
        product_count=product_count,
        active_check_profile=active_check_profile,
        check_context_builder=check_context_builder_for(
            active_check_profile.check_input_surface
        ),
        reference_observer=reference_observer_for(active_check_profile.checks),
        evaluators=evaluators,
        reference_result_cache_key=None,
        reference_result_cache_path=None,
        python_count=python_count,
        dsl_count=len(active_check_profile.checks) - python_count,
        legacy_parity_count=sum(
            1
            for check in active_check_profile.checks
            if check.parity_baseline == "legacy"
        ),
        runtime_only_count=sum(
            1
            for check in active_check_profile.checks
            if check.parity_baseline == "none"
        ),
    )


def log_run_configuration(
    run: PreparedRun,
    project_root: Path,
    *,
    logger: logging.Logger,
) -> None:
    """Log the active profile, loaded definitions, and reference cache path."""
    logger.info(
        "[Checks] Active profile: %s (%d checks, check input surface %s).",
        run.active_check_profile.name,
        len(run.active_check_profile.checks),
        run.active_check_profile.check_input_surface,
    )
    logger.info(
        "[Checks] Loaded %d Python checks and %d DSL checks for the active profile.",
        run.python_count,
        run.dsl_count,
    )
    logger.info(
        "[Checks] Legacy parity backed checks: %d. Runtime-only checks: %d.",
        run.legacy_parity_count,
        run.runtime_only_count,
    )
    logger.info(
        "[Reference Path] Result cache: %s",
        (
            display_path(run.reference_result_cache_path, project_root)
            if run.requires_reference_results
            and run.reference_result_cache_path is not None
            else "disabled for this run"
        ),
    )
