from __future__ import annotations

import logging
from dataclasses import dataclass

import pytest
from app.pipeline.progress import (
    ExecutionProgressConfig,
    ExecutionProgressReporter,
    build_execution_plan,
    finished_execution_progress,
)


@dataclass(frozen=True)
class _BufferedBatch:
    row_count: int


def _progress_reporter() -> ExecutionProgressReporter:
    """Build a reporter with the shared test execution shape."""
    return ExecutionProgressReporter(
        config=ExecutionProgressConfig(
            plan=build_execution_plan(
                product_count=1_000,
                batch_size=250,
                configured_workers=4,
            ),
            mismatch_examples_limit=20,
        ),
        logger=logging.getLogger("tests.execution_progress"),
        started_at=0.0,
    )


def test_build_execution_plan_uses_all_workers_when_batches_allow() -> None:
    plan = build_execution_plan(
        product_count=1_000,
        batch_size=250,
        configured_workers=4,
    )

    assert plan.batch_count == 4
    assert plan.effective_workers == 4


def test_build_execution_plan_caps_effective_workers_to_batch_count() -> None:
    plan = build_execution_plan(
        product_count=1_000,
        batch_size=5_000,
        configured_workers=4,
    )

    assert plan.batch_count == 1
    assert plan.effective_workers == 1


def test_finished_execution_progress_counts_buffered_out_of_order_batches() -> None:
    finished_products, finished_batches = finished_execution_progress(
        processed_products=250,
        buffered_results=[_BufferedBatch(row_count=250)],
        merged_batch_count=1,
    )

    assert finished_products == 500
    assert finished_batches == 2


def test_execution_progress_reporter_logs_plan(
    caplog: pytest.LogCaptureFixture,
) -> None:
    reporter = _progress_reporter()
    caplog.set_level(logging.INFO)

    reporter.log_plan()

    assert caplog.messages == [
        "[Execution] Planned 1000 products across 4 batch(es); effective workers: 4/4.",
        "[Execution] Running strict parity in batches of 250 products (keeping up to 20 mismatch examples per side and check).",
    ]


def test_execution_progress_reporter_logs_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    reporter = _progress_reporter()
    monkeypatch.setattr("app.pipeline.progress.perf_counter", lambda: 15.0)
    caplog.set_level(logging.INFO)

    reporter.log_heartbeat(
        processed_products=250,
        buffered_results=[_BufferedBatch(row_count=250)],
        merged_batch_count=1,
        in_flight_count=2,
    )

    assert caplog.messages == [
        "[Execution] Still running after 15.0s: 500 / 1000 products finished, 2 / 4 batches finished, 2 in flight."
    ]
