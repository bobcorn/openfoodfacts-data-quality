from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter

from app.artifacts import display_path
from app.migration import load_migration_catalog
from app.reference.observers import reference_observer_for
from app.run.context_builders import check_context_builder_for
from app.run.models import PreparedRun, RunPreparationTimings, RunSpec
from app.run.profiles import load_check_profile
from app.source.datasets import load_dataset_profile
from app.source.product_documents import count_source_products, source_snapshot_id_for
from openfoodfacts_data_quality.checks.catalog import (
    get_default_check_catalog,
)


def prepare_run(
    run_spec: RunSpec,
    *,
    logger: logging.Logger,
) -> PreparedRun:
    """Resolve source metadata, active checks, and evaluator counts."""
    prepare_started = perf_counter()
    logger.info(
        "[Input] Loading products from %s",
        display_path(run_spec.db_path, run_spec.project_root),
    )
    source_snapshot_started = perf_counter()
    source_snapshot_id = source_snapshot_id_for(run_spec.db_path)
    source_snapshot_id_seconds = perf_counter() - source_snapshot_started
    dataset_profile_started = perf_counter()
    active_dataset_profile = load_dataset_profile(
        run_spec.dataset_profile_config_path,
        run_spec.dataset_profile_name,
    )
    dataset_profile_load_seconds = perf_counter() - dataset_profile_started
    run_id = datetime.now(UTC).strftime("%Y%m%d-%H%M%S-%f")
    source_row_count_started = perf_counter()
    product_count = count_source_products(
        run_spec.db_path,
        selection=active_dataset_profile.selection,
    )
    source_row_count_seconds = perf_counter() - source_row_count_started
    prepare_run_seconds = perf_counter() - prepare_started
    logger.info(
        "[Input] Found %d products in source snapshot %s in %.1fs.",
        product_count,
        source_snapshot_id,
        prepare_run_seconds,
    )
    check_catalog = get_default_check_catalog()
    migration_catalog = load_migration_catalog(
        artifact_path=run_spec.legacy_inventory_artifact_path,
        estimation_sheet_path=run_spec.legacy_estimation_sheet_path,
    )
    active_check_profile = load_check_profile(
        run_spec.profile_config_path,
        run_spec.check_profile_name,
        catalog=check_catalog,
        migration_catalog=migration_catalog,
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
            active_check_profile.check_context_provider
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
        preparation_timings=RunPreparationTimings(
            prepare_run_seconds=prepare_run_seconds,
            source_snapshot_id_seconds=source_snapshot_id_seconds,
            dataset_profile_load_seconds=dataset_profile_load_seconds,
            source_row_count_seconds=source_row_count_seconds,
        ),
        active_dataset_profile=active_dataset_profile,
        active_migration_plan=migration_catalog.active_plan_for_check_ids(
            active_check_profile.check_ids
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
        "[Input] Active dataset profile: %s (%s, selection %s).",
        run.active_dataset_profile.name,
        run.active_dataset_profile.description,
        run.active_dataset_profile.selection.kind,
    )
    logger.info(
        "[Checks] Active profile: %s (%d checks, check context provider %s).",
        run.active_check_profile.name,
        len(run.active_check_profile.checks),
        run.active_check_profile.check_context_provider,
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
        "[Migration] Active families: %d matched, %d assessed, %d unmatched checks.",
        run.active_migration_plan.family_count,
        run.active_migration_plan.assessed_family_count,
        len(run.active_migration_plan.missing_check_ids),
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
