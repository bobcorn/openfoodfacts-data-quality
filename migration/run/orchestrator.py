from __future__ import annotations

import logging
from contextlib import ExitStack
from pathlib import Path
from time import perf_counter

from migration.artifacts import display_path, prepare_artifact_workspace
from migration.legacy_backend.runner import LazyLegacyBackendRunner
from migration.reference.cache import (
    ReferenceResultCache,
    ReferenceResultCacheIdentity,
    ReferenceResultCacheMetadata,
    reference_result_cache_identity,
    reference_result_cache_path,
)
from migration.reference.loader import ReferenceResultLoader
from migration.reference.materializers import (
    ReferenceCheckContextMaterializer,
    ReferenceFindingMaterializer,
)
from migration.run.accumulator import RunResultAccumulator
from migration.run.execution import run_batches
from migration.run.models import (
    BatchExecutionContext,
    BatchRunPlan,
    ExecutedMigrationRun,
    PreparedRun,
    RunSpec,
)
from migration.run.preparation import (
    log_run_configuration,
    prepare_run,
)
from migration.run.progress import (
    ExecutionProgressConfig,
    ExecutionProgressReporter,
    build_execution_plan,
)
from migration.run.runners import (
    LegacyReferenceRunner,
    MigratedRunner,
    NoReferenceRunner,
    ParityRunner,
)
from migration.storage import DuckDBRunRecorder, NoopRunRecorder
from off_data_quality.contracts.run import RunMetadata

LOGGER = logging.getLogger(__name__)


class MigrationRunner:
    """Explicit migration service for one configured local run."""

    def __init__(
        self,
        run_spec: RunSpec,
        *,
        logger: logging.Logger = LOGGER,
    ) -> None:
        self.run_spec = run_spec
        self.logger = logger

    def execute(self) -> ExecutedMigrationRun:
        """Run the migration pipeline and return the completed run state."""
        self._require_source_db()
        artifact_workspace = prepare_artifact_workspace(self.run_spec.project_root)
        run, cache_identity = self._prepare_run_with_reference_cache()
        warn_if_legacy_backend_workers_exceed_batch_workers(
            requires_reference_results=run.requires_reference_results,
            batch_workers=self.run_spec.batch_workers,
            legacy_backend_workers=self.run_spec.legacy_backend_workers,
            logger=self.logger,
        )
        log_run_configuration(run, self.run_spec.project_root, logger=self.logger)
        self.logger.info(
            "[Storage] Parity store: %s",
            (
                display_path(
                    self.run_spec.parity_store_path, self.run_spec.project_root
                )
                if self.run_spec.parity_store_path is not None
                else "disabled for this run"
            ),
        )

        execution_progress = ExecutionProgressReporter(
            config=ExecutionProgressConfig(
                plan=build_execution_plan(
                    product_count=run.product_count,
                    batch_size=self.run_spec.batch_size,
                    configured_workers=self.run_spec.batch_workers,
                ),
                mismatch_examples_limit=self.run_spec.mismatch_examples_limit,
            ),
            logger=self.logger,
        )
        batch_accumulator = RunResultAccumulator(
            max_examples_per_side=self.run_spec.mismatch_examples_limit,
            checks=run.active_check_profile.checks,
        )
        execution_progress.log_plan()

        with ExitStack() as stack:
            run_recorder = self._run_recorder_for_run(
                stack=stack,
                run=run,
            )
            run_batches(
                plan=BatchRunPlan(
                    db_path=self.run_spec.db_path,
                    batch_size=self.run_spec.batch_size,
                    batch_workers=self.run_spec.batch_workers,
                    legacy_backend_workers=self.run_spec.legacy_backend_workers,
                    source_selection=run.active_dataset_profile.selection,
                ),
                execution=BatchExecutionContext(
                    reference_runner=self._reference_runner_for_run(
                        stack=stack,
                        run=run,
                        backend_stderr_path=artifact_workspace.legacy_backend_stderr_path,
                        cache_identity=cache_identity,
                    ),
                    migrated_runner=MigratedRunner(
                        check_context_builder=run.check_context_builder,
                        evaluators=run.evaluators,
                    ),
                    parity_runner=ParityRunner(
                        run_id=run.run_id,
                        source_snapshot_id=run.source_snapshot_id,
                        active_checks=run.active_check_profile.checks,
                    ),
                ),
                execution_progress=execution_progress,
                accumulator=batch_accumulator,
                run_recorder=run_recorder,
            )

            run_result_started = perf_counter()
            run_result = batch_accumulator.build_result(
                run=RunMetadata(
                    run_id=run.run_id,
                    source_snapshot_id=run.source_snapshot_id,
                    product_count=run.product_count,
                ),
            )
            run_recorder.record_final_result(run_result)
        matching_checks = sum(1 for check in run_result.checks if check.passed is True)
        mismatching_checks = sum(
            1 for check in run_result.checks if check.passed is False
        )
        runtime_only_checks = sum(
            1
            for check in run_result.checks
            if check.comparison_status == "runtime_only"
        )
        self.logger.info(
            "[Run] Finalized %d checks in %.1fs (%d matching, %d mismatching, %d runtime only).",
            len(run_result.checks),
            perf_counter() - run_result_started,
            matching_checks,
            mismatching_checks,
            runtime_only_checks,
        )
        return ExecutedMigrationRun(
            run_result=run_result,
            artifact_workspace=artifact_workspace,
            source_input_summary=run.source_input_summary,
        )

    def _require_source_db(self) -> None:
        """Raise when the configured source snapshot path does not exist."""
        if self.run_spec.db_path.exists():
            return
        raise FileNotFoundError(
            "Source snapshot not found. Mount or provide a JSONL or DuckDB source "
            "snapshot and set SOURCE_SNAPSHOT_PATH."
        )

    def _prepare_run_with_reference_cache(
        self,
    ) -> tuple[PreparedRun, ReferenceResultCacheIdentity | None]:
        """Prepare the run and attach the reference-result cache namespace if needed."""
        run = prepare_run(self.run_spec, logger=self.logger)
        if not run.requires_reference_results:
            return run, None

        cache_identity = reference_result_cache_identity(
            self.run_spec.project_root,
            extra_salt=self.run_spec.reference_result_cache_salt,
        )
        return (
            run.with_reference_result_cache(
                result_cache_key=cache_identity.cache_key,
                result_cache_path=reference_result_cache_path(
                    cache_dir=self.run_spec.reference_result_cache_dir,
                    source_snapshot_id=run.source_snapshot_id,
                    cache_key=cache_identity.cache_key,
                ),
            ),
            cache_identity,
        )

    def _reference_runner_for_run(
        self,
        *,
        stack: ExitStack,
        run: PreparedRun,
        backend_stderr_path: Path,
        cache_identity: ReferenceResultCacheIdentity | None,
    ) -> LegacyReferenceRunner | NoReferenceRunner:
        """Build the reference-side runner selected for the prepared run."""
        if not run.requires_reference_results:
            return NoReferenceRunner()

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
                worker_count=self.run_spec.legacy_backend_workers,
                stderr_path=backend_stderr_path,
            )
        )
        return LegacyReferenceRunner(
            reference_result_loader=ReferenceResultLoader(
                legacy_backend_runner=legacy_backend,
                reference_result_cache=reference_result_cache,
            ),
            reference_check_context_materializer=(
                ReferenceCheckContextMaterializer()
                if run.requires_reference_check_contexts
                else None
            ),
            reference_finding_materializer=(
                ReferenceFindingMaterializer(run.reference_observer)
                if run.requires_reference_findings
                else None
            ),
        )

    def _run_recorder_for_run(
        self,
        *,
        stack: ExitStack,
        run: PreparedRun,
    ) -> DuckDBRunRecorder | NoopRunRecorder:
        """Build the run recorder selected for the current migration run."""
        if self.run_spec.parity_store_path is None:
            return NoopRunRecorder()
        return stack.enter_context(
            DuckDBRunRecorder(
                path=self.run_spec.parity_store_path,
                run_spec=self.run_spec,
                prepared_run=run,
            )
        )


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
