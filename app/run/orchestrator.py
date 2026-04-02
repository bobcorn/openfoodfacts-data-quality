from __future__ import annotations

import logging
import os
from contextlib import ExitStack
from os import cpu_count
from pathlib import Path
from time import perf_counter

from app.legacy_backend.runner import LazyLegacyBackendRunner
from app.reference.cache import (
    ReferenceResultCache,
    ReferenceResultCacheMetadata,
    configured_reference_result_cache_dir,
    configured_reference_result_cache_salt,
    reference_result_cache_identity,
    reference_result_cache_path,
)
from app.reference.loader import ReferenceResultLoader
from app.reference.materializers import (
    EnrichedSnapshotMaterializer,
    ReferenceFindingMaterializer,
)
from app.report.renderer import render_report
from app.run.accumulator import RunResultAccumulator
from app.run.execution import run_batches
from app.run.models import BatchExecutionContext, BatchRunPlan
from app.run.preparation import (
    display_path,
    log_run_configuration,
    prepare_artifacts_dir,
    prepare_run,
)
from app.run.progress import (
    ExecutionProgressConfig,
    ExecutionProgressReporter,
    build_execution_plan,
)
from openfoodfacts_data_quality.contracts.run import RunMetadata

LOGGER = logging.getLogger(__name__)
DEFAULT_BATCH_SIZE = 5_000
DEFAULT_MISMATCH_EXAMPLES_LIMIT = 20
DEFAULT_BATCH_WORKERS = max(1, min(8, cpu_count() or 2))
DEFAULT_LEGACY_BACKEND_WORKERS = max(1, min(4, (cpu_count() or 2) // 2 or 1))


def build_site(
    project_root: Path,
    *,
    db_path: Path | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    mismatch_examples_limit: int = DEFAULT_MISMATCH_EXAMPLES_LIMIT,
    batch_workers: int = DEFAULT_BATCH_WORKERS,
    legacy_backend_workers: int = DEFAULT_LEGACY_BACKEND_WORKERS,
) -> Path:
    """Build artifacts and render the static site for the current source snapshot."""
    db_path = (db_path or configured_database_path(project_root)).resolve()
    if not db_path.exists():
        raise FileNotFoundError(
            "Source DuckDB not found. Mount or provide a DuckDB file and set DATABASE_PATH."
        )
    artifacts_dir, site_dir = prepare_artifacts_dir(project_root)
    run = prepare_run(project_root, db_path, logger=LOGGER)
    warn_if_legacy_backend_workers_exceed_batch_workers(
        requires_reference_results=run.requires_reference_results,
        batch_workers=batch_workers,
        legacy_backend_workers=legacy_backend_workers,
        logger=LOGGER,
    )
    cache_identity = None
    if run.requires_reference_results:
        cache_dir = configured_reference_result_cache_dir(project_root)
        cache_identity = reference_result_cache_identity(
            project_root,
            extra_salt=configured_reference_result_cache_salt(),
        )
        result_cache_key = cache_identity.cache_key
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
                configured_workers=batch_workers,
            ),
            mismatch_examples_limit=mismatch_examples_limit,
        ),
        logger=LOGGER,
    )
    batch_accumulator = RunResultAccumulator(
        max_examples_per_side=mismatch_examples_limit,
        checks=run.active_check_profile.checks,
    )
    backend_stderr_path = artifacts_dir / "legacy-backend-stderr.log"
    execution_progress.log_plan()
    with ExitStack() as stack:
        reference_result_loader: ReferenceResultLoader | None = None
        enriched_snapshot_materializer: EnrichedSnapshotMaterializer | None = None
        reference_finding_materializer: ReferenceFindingMaterializer | None = None
        if run.requires_reference_results:
            if (
                run.reference_result_cache_path is None
                or run.reference_result_cache_key is None
                or cache_identity is None
            ):
                raise RuntimeError(
                    "Reference-result cache metadata must be configured when the run "
                    "requires reference results."
                )
            reference_result_cache = stack.enter_context(
                ReferenceResultCache(
                    path=run.reference_result_cache_path,
                    metadata=ReferenceResultCacheMetadata.expected(
                        source_snapshot_id=run.source_snapshot_id,
                        identity=cache_identity,
                    ),
                )
            )
            legacy_backend = stack.enter_context(
                LazyLegacyBackendRunner(
                    worker_count=legacy_backend_workers,
                    stderr_path=backend_stderr_path,
                )
            )
            reference_result_loader = ReferenceResultLoader(
                legacy_backend_runner=legacy_backend,
                reference_result_cache=reference_result_cache,
            )
            enriched_snapshot_materializer = (
                EnrichedSnapshotMaterializer()
                if run.requires_enriched_snapshots
                else None
            )
            reference_finding_materializer = (
                ReferenceFindingMaterializer(run.reference_observer)
                if run.requires_reference_findings
                else None
            )
        run_batches(
            plan=BatchRunPlan(
                db_path=db_path,
                batch_size=batch_size,
                batch_workers=batch_workers,
                legacy_backend_workers=legacy_backend_workers,
            ),
            execution=BatchExecutionContext(
                reference_result_loader=reference_result_loader,
                enriched_snapshot_materializer=enriched_snapshot_materializer,
                reference_finding_materializer=reference_finding_materializer,
                evaluators=run.evaluators,
                active_checks=run.active_check_profile.checks,
                check_context_builder=run.check_context_builder,
                run_id=run.run_id,
                source_snapshot_id=run.source_snapshot_id,
            ),
            execution_progress=execution_progress,
            accumulator=batch_accumulator,
        )

    run_result_started = perf_counter()
    run_result = batch_accumulator.build_result(
        run=RunMetadata(
            run_id=run.run_id,
            source_snapshot_id=run.source_snapshot_id,
            product_count=run.product_count,
        ),
    )
    matching_checks = sum(1 for check in run_result.checks if check.passed is True)
    mismatching_checks = sum(1 for check in run_result.checks if check.passed is False)
    runtime_only_checks = sum(
        1 for check in run_result.checks if check.comparison_status == "runtime_only"
    )
    LOGGER.info(
        "[Run] Finalized %d checks in %.1fs (%d matching, %d mismatching, %d runtime only).",
        len(run_result.checks),
        perf_counter() - run_result_started,
        matching_checks,
        mismatching_checks,
        runtime_only_checks,
    )

    presentation_started = perf_counter()
    LOGGER.info("[Presentation] Rendering static report artifacts...")
    render_report(run_result, site_dir)
    LOGGER.info(
        "[Presentation] Report artifacts written to %s in %.1fs.",
        display_path(site_dir, project_root),
        perf_counter() - presentation_started,
    )
    return site_dir


def warn_if_legacy_backend_workers_exceed_batch_workers(
    *,
    requires_reference_results: bool,
    batch_workers: int,
    legacy_backend_workers: int,
    logger: logging.Logger,
) -> None:
    """Log when the backend pool is configured larger than batch concurrency."""
    if not requires_reference_results or legacy_backend_workers <= batch_workers:
        return
    logger.warning(
        "[Config] LEGACY_BACKEND_WORKERS=%d exceeds BATCH_WORKERS=%d; at most %d backend worker(s) can be used concurrently in this run.",
        legacy_backend_workers,
        batch_workers,
        batch_workers,
    )


def configured_database_path(project_root: Path) -> Path:
    """Return the configured DuckDB source path."""
    configured = os.environ.get("DATABASE_PATH")
    if configured is None or not configured.strip():
        raise ValueError(
            "DATABASE_PATH must be set for local runtime runs. Use the demo image for the bundled example snapshot."
        )
    return Path(configured).expanduser().resolve()


def configured_batch_size() -> int:
    """Return the configured execution batch size."""
    configured = os.environ.get("BATCH_SIZE")
    if not configured:
        return DEFAULT_BATCH_SIZE
    return int(configured)


def configured_mismatch_examples_limit() -> int:
    """Return the configured mismatch example retention budget for each check."""
    configured = os.environ.get("MISMATCH_EXAMPLES_LIMIT")
    if not configured:
        return DEFAULT_MISMATCH_EXAMPLES_LIMIT
    return int(configured)


def configured_batch_workers() -> int:
    """Return the configured number of concurrent batch workers."""
    configured = os.environ.get("BATCH_WORKERS")
    if not configured:
        return DEFAULT_BATCH_WORKERS
    return int(configured)


def configured_legacy_backend_workers() -> int:
    """Return the configured number of persistent backend workers."""
    configured = os.environ.get("LEGACY_BACKEND_WORKERS")
    if not configured:
        return DEFAULT_LEGACY_BACKEND_WORKERS
    return int(configured)
