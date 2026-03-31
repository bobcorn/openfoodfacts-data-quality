from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING

from app.parity.comparator import evaluate_parity
from app.parity.models import ParityRunMetadata, observed_migrated_finding
from app.pipeline.models import (
    BatchExecutionContext,
    BatchExecutionResult,
    BatchRunPlan,
    ScheduledBatch,
)
from app.pipeline.scheduler import BatchScheduler
from app.sources.duckdb_products import iter_source_batches
from openfoodfacts_data_quality.checks.engine import (
    CheckRunOptions,
    run_checks_with_evaluators,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from contextlib import AbstractContextManager

    from app.pipeline.models import (
        SupportsBatchExecutor,
        SupportsExecutionProgress,
        SupportsParityAccumulator,
    )


def run_batches(
    *,
    plan: BatchRunPlan,
    execution: BatchExecutionContext,
    execution_progress: SupportsExecutionProgress,
    accumulator: SupportsParityAccumulator,
) -> None:
    """Run the end-to-end batch loop for one prepared pipeline execution."""
    run_scheduled_batches(
        batch_iterator=_iter_scheduled_batches(
            plan.db_path,
            batch_size=plan.batch_size,
        ),
        process_batch=lambda batch: execute_batch(batch, execution),
        worker_limit=plan.legacy_backend_workers,
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
    accumulator: SupportsParityAccumulator,
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
    """Run one batch end-to-end through the strict parity pipeline."""
    started = perf_counter()
    batch_inputs = execution.batch_input_resolver.resolve(batch.rows)
    contexts = execution.check_context_builder.build_contexts(
        rows=batch.rows,
        enriched_snapshots=batch_inputs.enriched_snapshots,
    )
    library_findings = run_checks_with_evaluators(
        contexts,
        execution.evaluators,
        options=CheckRunOptions(
            log_loaded=False,
            log_progress=False,
        ),
    )
    migrated_findings = [
        observed_migrated_finding(finding) for finding in library_findings
    ]
    batch_parity = evaluate_parity(
        reference_findings=batch_inputs.reference_findings,
        migrated_findings=migrated_findings,
        run=ParityRunMetadata(
            run_id=execution.run_id,
            source_snapshot_id=execution.source_snapshot_id,
            product_count=len(batch.rows),
        ),
        checks=execution.active_checks,
    )
    return BatchExecutionResult(
        batch_index=batch.batch_index,
        row_count=len(batch.rows),
        cache_hit_count=batch_inputs.cache_hit_count,
        backend_run_count=batch_inputs.backend_run_count,
        reference_finding_count=len(batch_inputs.reference_findings),
        migrated_finding_count=len(migrated_findings),
        parity_result=batch_parity,
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
    accumulator: SupportsParityAccumulator,
    execution_progress: SupportsExecutionProgress,
    processed_products: int,
    scheduler: BatchScheduler,
) -> int:
    """Merge contiguous completed batches and return the new processed count."""
    for batch_result in scheduler.merge_ready_batches():
        accumulator.add_batch(batch_result.parity_result)
        processed_products += batch_result.row_count
        execution_progress.log_batch_completed(
            batch_result,
            processed_products=processed_products,
        )
    return processed_products
