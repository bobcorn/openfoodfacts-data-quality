from __future__ import annotations

import logging
import os
from contextlib import ExitStack
from os import cpu_count
from pathlib import Path
from time import perf_counter
from typing import cast

from app.legacy_backend.runner import LegacyBackendSessionPool
from app.parity.accumulator import ParityAccumulator
from app.parity.models import ParityRunMetadata
from app.pipeline.batch_inputs import (
    NoOpBatchInputResolver,
    ReferenceBatchInputResolver,
)
from app.pipeline.execution import run_batches
from app.pipeline.models import BatchExecutionContext, BatchRunPlan
from app.pipeline.preparation import (
    display_path,
    log_run_configuration,
    prepare_artifacts_dir,
    prepare_run,
)
from app.pipeline.progress import (
    ExecutionProgressConfig,
    ExecutionProgressReporter,
    build_execution_plan,
)
from app.reference.cache import (
    ReferenceResultCache,
    configured_reference_result_cache_dir,
    configured_reference_result_cache_salt,
    reference_result_cache_key,
    reference_result_cache_path,
)
from app.reference.loader import ReferenceResultLoader
from app.report.renderer import render_report

LOGGER = logging.getLogger(__name__)
DEFAULT_BATCH_SIZE = 5_000
DEFAULT_MISMATCH_EXAMPLES_LIMIT = 20
DEFAULT_LEGACY_BACKEND_WORKERS = max(1, min(4, (cpu_count() or 2) // 2 or 1))


def build_site(
    project_root: Path,
    *,
    db_path: Path | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    mismatch_examples_limit: int = DEFAULT_MISMATCH_EXAMPLES_LIMIT,
    legacy_backend_workers: int = DEFAULT_LEGACY_BACKEND_WORKERS,
) -> Path:
    """Build artifacts and render the static site for the current source snapshot."""
    db_path = (db_path or project_root / "data" / "products.duckdb").resolve()
    if not db_path.exists():
        raise FileNotFoundError(
            "Source DuckDB not found. Mount or provide a DuckDB file and set DATABASE_PATH."
        )
    artifacts_dir, site_dir = prepare_artifacts_dir(project_root)
    run = prepare_run(project_root, db_path, logger=LOGGER)
    cache_dir = configured_reference_result_cache_dir(project_root)
    result_cache_key = reference_result_cache_key(
        project_root,
        extra_salt=configured_reference_result_cache_salt(),
    )
    result_cache_path = reference_result_cache_path(
        cache_dir=cache_dir,
        source_snapshot_id=run.source_snapshot_id,
        cache_key=result_cache_key,
    )
    run = run.with_reference_result_cache(
        result_cache_key=result_cache_key,
        result_cache_path=result_cache_path,
    )
    log_run_configuration(run, project_root, logger=LOGGER)

    execution_progress = ExecutionProgressReporter(
        config=ExecutionProgressConfig(
            plan=build_execution_plan(
                product_count=run.product_count,
                batch_size=batch_size,
                configured_workers=legacy_backend_workers,
            ),
            mismatch_examples_limit=mismatch_examples_limit,
        ),
        logger=LOGGER,
    )
    batch_accumulator = ParityAccumulator(
        max_examples_per_side=mismatch_examples_limit,
        checks=run.active_check_profile.checks,
    )
    backend_stderr_path = artifacts_dir / "legacy-backend-stderr.log"
    execution_progress.log_plan()
    with ExitStack() as stack:
        batch_input_resolver: NoOpBatchInputResolver | ReferenceBatchInputResolver = (
            NoOpBatchInputResolver()
        )
        if run.requires_reference_results:
            reference_result_cache = stack.enter_context(
                ReferenceResultCache(
                    path=run.reference_result_cache_path,
                    source_snapshot_id=run.source_snapshot_id,
                    cache_key=run.reference_result_cache_key,
                )
            )
            legacy_backend = cast(
                LegacyBackendSessionPool,
                stack.enter_context(
                    LegacyBackendSessionPool(
                        worker_count=legacy_backend_workers,
                        stderr_path=backend_stderr_path,
                    )
                ),
            )
            batch_input_resolver = ReferenceBatchInputResolver(
                reference_result_loader=ReferenceResultLoader(
                    legacy_backend_runner=legacy_backend,
                    reference_result_cache=reference_result_cache,
                ),
                reference_observer=run.reference_observer,
                include_enriched_snapshots=run.requires_enriched_snapshots,
            )
        run_batches(
            plan=BatchRunPlan(
                db_path=db_path,
                batch_size=batch_size,
                legacy_backend_workers=legacy_backend_workers,
            ),
            execution=BatchExecutionContext(
                batch_input_resolver=batch_input_resolver,
                evaluators=run.evaluators,
                active_checks=run.active_check_profile.checks,
                check_context_builder=run.check_context_builder,
                run_id=run.run_id,
                source_snapshot_id=run.source_snapshot_id,
            ),
            execution_progress=execution_progress,
            accumulator=batch_accumulator,
        )

    parity_started = perf_counter()
    parity_result = batch_accumulator.build_result(
        run=ParityRunMetadata(
            run_id=run.run_id,
            source_snapshot_id=run.source_snapshot_id,
            product_count=run.product_count,
        ),
    )
    matching_checks = sum(1 for check in parity_result.checks if check.passed is True)
    mismatching_checks = sum(
        1 for check in parity_result.checks if check.passed is False
    )
    LOGGER.info(
        "[Parity] Finalized %d checks in %.1fs (%d matching, %d mismatching, %d not compared).",
        len(parity_result.checks),
        perf_counter() - parity_started,
        matching_checks,
        mismatching_checks,
        parity_result.not_compared_check_count,
    )

    presentation_started = perf_counter()
    LOGGER.info("[Presentation] Rendering static report artifacts...")
    render_report(parity_result, site_dir)
    LOGGER.info(
        "[Presentation] Report artifacts written to %s in %.1fs.",
        display_path(site_dir, project_root),
        perf_counter() - presentation_started,
    )
    return site_dir


def configured_database_path(project_root: Path) -> Path:
    """Return the configured DuckDB source path."""
    configured = os.environ.get("DATABASE_PATH")
    if not configured:
        return project_root / "data" / "products.duckdb"
    return Path(configured).expanduser().resolve()


def configured_batch_size() -> int:
    """Return the configured execution batch size."""
    configured = os.environ.get("BATCH_SIZE")
    if not configured:
        return DEFAULT_BATCH_SIZE
    return int(configured)


def configured_mismatch_examples_limit() -> int:
    """Return the configured per-check mismatch example retention budget."""
    configured = os.environ.get("MISMATCH_EXAMPLES_LIMIT")
    if not configured:
        return DEFAULT_MISMATCH_EXAMPLES_LIMIT
    return int(configured)


def configured_legacy_backend_workers() -> int:
    """Return the configured number of persistent backend workers."""
    configured = os.environ.get("LEGACY_BACKEND_WORKERS")
    if not configured:
        return DEFAULT_LEGACY_BACKEND_WORKERS
    return int(configured)
