from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING

from migration.run.models import (
    BatchExecutionContext,
    BatchExecutionResult,
    BatchRunPlan,
    BatchStageTimings,
    ScheduledBatch,
)
from migration.run.scheduler import BatchScheduler
from migration.source.datasets import SourceSelection
from migration.source.product_documents import iter_source_batches

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from contextlib import AbstractContextManager

    from migration.run.models import (
        SupportsBatchExecutor,
        SupportsExecutionProgress,
        SupportsRunAccumulator,
        SupportsRunRecorder,
    )


def run_batches(
    *,
    plan: BatchRunPlan,
    execution: BatchExecutionContext,
    execution_progress: SupportsExecutionProgress,
    accumulator: SupportsRunAccumulator,
    run_recorder: SupportsRunRecorder | None = None,
) -> None:
    """Run the end to end batch loop for one prepared migration run."""
    run_scheduled_batches(
        batch_iterator=_iter_scheduled_batches(
            plan.db_path,
            batch_size=plan.batch_size,
            selection=plan.source_selection,
        ),
        process_batch=lambda batch: execute_batch(batch, execution),
        worker_limit=plan.batch_workers,
        execution_progress=execution_progress,
        accumulator=accumulator,
        run_recorder=run_recorder,
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
    run_recorder: SupportsRunRecorder | None,
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
                run_recorder=run_recorder,
                scheduler=scheduler,
            )


def _iter_scheduled_batches(
    db_path: Path,
    *,
    batch_size: int,
    selection: SourceSelection,
) -> Iterator[ScheduledBatch]:
    """Wrap source product batches with monotonically increasing batch indices."""
    source_batches = iter_source_batches(
        db_path,
        batch_size=batch_size,
        selection=selection,
    )
    batch_index = 1
    while True:
        started = perf_counter()
        try:
            rows = next(source_batches)
        except StopIteration:
            break
        yield ScheduledBatch(
            batch_index=batch_index,
            records=rows,
            source_read_seconds=perf_counter() - started,
        )
        batch_index += 1


def execute_batch(
    batch: ScheduledBatch,
    execution: BatchExecutionContext,
) -> BatchExecutionResult:
    """Run one batch end to end through the current run model."""
    started = perf_counter()
    resolved_reference_batch = execution.reference_runner.resolve(
        batch.product_documents
    )
    migrated_started = perf_counter()
    migrated_findings = tuple(
        execution.migrated_runner.observe_findings(
            rows=batch.source_products,
            reference_check_contexts=(
                resolved_reference_batch.reference_check_contexts
            ),
        )
    )
    migrated_findings_seconds = perf_counter() - migrated_started
    parity_started = perf_counter()
    batch_run_result = execution.parity_runner.compare(
        product_count=len(batch.records),
        reference_findings=resolved_reference_batch.reference_findings,
        migrated_findings=migrated_findings,
    )
    parity_compare_seconds = perf_counter() - parity_started
    return BatchExecutionResult(
        batch_index=batch.batch_index,
        row_count=len(batch.records),
        cache_hit_count=resolved_reference_batch.cache_hit_count,
        backend_run_count=resolved_reference_batch.backend_run_count,
        reference_finding_count=batch_run_result.reference_total,
        migrated_finding_count=(
            batch_run_result.compared_migrated_total
            + batch_run_result.runtime_only_migrated_total
        ),
        run_result=batch_run_result,
        elapsed_seconds=perf_counter() - started,
        stage_timings=BatchStageTimings(
            source_read_seconds=batch.source_read_seconds,
            reference_load_seconds=resolved_reference_batch.load_seconds,
            reference_check_context_materialization_seconds=(
                resolved_reference_batch.reference_check_context_materialization_seconds
            ),
            reference_finding_materialization_seconds=(
                resolved_reference_batch.reference_finding_materialization_seconds
            ),
            migrated_findings_seconds=migrated_findings_seconds,
            parity_compare_seconds=parity_compare_seconds,
        ),
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
    run_recorder: SupportsRunRecorder | None,
    scheduler: BatchScheduler,
) -> int:
    """Merge contiguous completed batches and return the new processed count."""
    for batch_result in scheduler.merge_ready_batches():
        if run_recorder is not None:
            run_recorder.record_batch(batch_result)
        accumulator.add_batch(batch_result.run_result)
        processed_products += batch_result.row_count
        execution_progress.log_batch_completed(
            batch_result,
            processed_products=processed_products,
        )
    return processed_products
