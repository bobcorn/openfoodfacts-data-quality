from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING

from app.parity.comparator import evaluate_parity
from app.run.models import (
    BatchExecutionContext,
    BatchExecutionResult,
    BatchRunPlan,
    ScheduledBatch,
)
from app.run.scheduler import BatchScheduler
from app.source.duckdb_products import iter_source_batches
from openfoodfacts_data_quality.checks.engine import (
    CheckRunOptions,
    iter_check_findings_with_evaluators,
)
from openfoodfacts_data_quality.contracts.observations import (
    observed_migrated_finding,
)
from openfoodfacts_data_quality.contracts.run import RunMetadata

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from contextlib import AbstractContextManager

    from app.run.models import (
        SupportsBatchExecutor,
        SupportsExecutionProgress,
        SupportsRunAccumulator,
    )


def run_batches(
    *,
    plan: BatchRunPlan,
    execution: BatchExecutionContext,
    execution_progress: SupportsExecutionProgress,
    accumulator: SupportsRunAccumulator,
) -> None:
    """Run the end to end batch loop for one prepared application run."""
    run_scheduled_batches(
        batch_iterator=_iter_scheduled_batches(
            plan.db_path,
            batch_size=plan.batch_size,
        ),
        process_batch=lambda batch: execute_batch(batch, execution),
        worker_limit=plan.batch_workers,
        execution_progress=execution_progress,
        accumulator=accumulator,
        executor_factory=lambda max_workers: ThreadPoolExecutor(
            max_workers=max_workers
        ),
    )


def run_scheduled_batches(
    *,
    batch_iterator: Iterator[ScheduledBatch],
    process_batch: Callable[[ScheduledBatch], BatchExecutionResult],
    worker_limit: int,
    execution_progress: SupportsExecutionProgress,
    accumulator: SupportsRunAccumulator,
    executor_factory: Callable[[int], AbstractContextManager[SupportsBatchExecutor]],
) -> None:
    """Run, merge, and log one scheduled batch stream until it is exhausted."""
    processed_products = 0
    with executor_factory(worker_limit) as executor:
        scheduler = BatchScheduler(
            batch_iterator=batch_iterator,
            executor=executor,
            process_batch=process_batch,
            worker_limit=worker_limit,
        )
        while scheduler.has_pending_work():
            scheduler.submit_ready_batches()
            if scheduler.exhausted_batches and not scheduler.in_flight:
                break
            completed_futures = scheduler.wait_for_completed_batches(
                execution_progress.heartbeat_interval_seconds
            )
            if not completed_futures:
                _log_batch_heartbeat(execution_progress, processed_products, scheduler)
                continue
            scheduler.record_completed_batches(completed_futures)
            processed_products = _merge_completed_batches(
                accumulator=accumulator,
                execution_progress=execution_progress,
                processed_products=processed_products,
                scheduler=scheduler,
            )


def _iter_scheduled_batches(
    db_path: Path,
    *,
    batch_size: int,
) -> Iterator[ScheduledBatch]:
    """Wrap raw source batches with monotonically increasing batch indices."""
    for batch_index, rows in enumerate(
        iter_source_batches(db_path, batch_size=batch_size),
        start=1,
    ):
        yield ScheduledBatch(batch_index=batch_index, rows=rows)


def execute_batch(
    batch: ScheduledBatch,
    execution: BatchExecutionContext,
) -> BatchExecutionResult:
    """Run one batch end to end through the current run model."""
    started = perf_counter()
    resolved_reference_results = (
        execution.reference_result_loader.load_many(batch.rows)
        if execution.reference_result_loader is not None
        else None
    )
    reference_results = (
        resolved_reference_results.reference_results
        if resolved_reference_results is not None
        else []
    )
    enriched_snapshots = (
        execution.enriched_snapshot_materializer.materialize(reference_results)
        if execution.enriched_snapshot_materializer is not None
        else []
    )
    reference_findings = (
        execution.reference_finding_materializer.materialize(reference_results)
        if execution.reference_finding_materializer is not None
        else ()
    )
    batch_run_result = evaluate_parity(
        reference_findings=reference_findings,
        migrated_findings=(
            observed_migrated_finding(finding)
            for finding in iter_check_findings_with_evaluators(
                execution.check_context_builder.iter_contexts(
                    rows=batch.rows,
                    enriched_snapshots=enriched_snapshots,
                ),
                execution.evaluators,
                options=CheckRunOptions(
                    log_loaded=False,
                    log_progress=False,
                ),
            )
        ),
        run=RunMetadata(
            run_id=execution.run_id,
            source_snapshot_id=execution.source_snapshot_id,
            product_count=len(batch.rows),
        ),
        checks=execution.active_checks,
    )
    return BatchExecutionResult(
        batch_index=batch.batch_index,
        row_count=len(batch.rows),
        cache_hit_count=(
            resolved_reference_results.cache_hit_count
            if resolved_reference_results is not None
            else 0
        ),
        backend_run_count=(
            resolved_reference_results.backend_run_count
            if resolved_reference_results is not None
            else 0
        ),
        reference_finding_count=batch_run_result.reference_total,
        migrated_finding_count=(
            batch_run_result.compared_migrated_total
            + batch_run_result.runtime_only_migrated_total
        ),
        run_result=batch_run_result,
        elapsed_seconds=perf_counter() - started,
    )


def _log_batch_heartbeat(
    execution_progress: SupportsExecutionProgress,
    processed_products: int,
    scheduler: BatchScheduler,
) -> None:
    """Log one heartbeat using the current scheduler state."""
    execution_progress.log_heartbeat(
        processed_products=processed_products,
        buffered_results=scheduler.buffered_results(),
        merged_batch_count=scheduler.merged_batch_count(),
        in_flight_count=len(scheduler.in_flight),
    )


def _merge_completed_batches(
    *,
    accumulator: SupportsRunAccumulator,
    execution_progress: SupportsExecutionProgress,
    processed_products: int,
    scheduler: BatchScheduler,
) -> int:
    """Merge contiguous completed batches and return the new processed count."""
    for batch_result in scheduler.merge_ready_batches():
        accumulator.add_batch(batch_result.run_result)
        processed_products += batch_result.row_count
        execution_progress.log_batch_completed(
            batch_result,
            processed_products=processed_products,
        )
    return processed_products
