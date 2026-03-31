from __future__ import annotations

import json
import logging
import queue
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Protocol

import pytest
from app.legacy_backend.input_projection import LegacyBackendInputProduct
from app.legacy_backend.runner import (
    LegacyBackendSession,
    LegacyBackendSessionPool,
)
from app.reference.findings import emit_reference_findings
from app.reference.models import ReferenceResult

from openfoodfacts_data_quality.checks.catalog import get_default_check_catalog

if TYPE_CHECKING:
    from pathlib import Path

_EOF = object()


class _SupportsFakeStdin(Protocol):
    closed: bool

    def write(self, text: str) -> int: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


LegacyBackendInputProductFactory = Callable[..., LegacyBackendInputProduct]
ReferenceResultFactory = Callable[..., ReferenceResult]


class _FakeLineStream:
    def __init__(self, initial_lines: list[str] | None = None) -> None:
        self._queue: queue.Queue[object] = queue.Queue()
        for line in initial_lines or []:
            self._queue.put(line)
        self.closed = False

    def readline(self) -> str:
        item = self._queue.get()
        if item is _EOF:
            return ""
        return str(item)

    def push(self, line: str) -> None:
        self._queue.put(line)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._queue.put(_EOF)


class _FakeProcess:
    def __init__(self, *, stderr_lines: list[str] | None = None) -> None:
        self.stdout = _FakeLineStream()
        self.stderr = _FakeLineStream(stderr_lines or [])
        self.returncode: int | None = None
        self.stdin: _SupportsFakeStdin = _FakeStdin(self)

    def poll(self) -> int | None:
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        if self.returncode is None:
            self.returncode = 0
        self.stdout.close()
        self.stderr.close()
        return self.returncode

    def terminate(self) -> None:
        self.returncode = 0
        self.stdout.close()
        self.stderr.close()

    def kill(self) -> None:
        self.returncode = -9
        self.stdout.close()
        self.stderr.close()


class _FakeStdin:
    def __init__(self, process: _FakeProcess) -> None:
        self._process = process
        self._buffer = ""
        self.closed = False

    def write(self, text: str) -> int:
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if not line.strip():
                continue
            incoming = json.loads(line)
            payload = {
                "code": incoming["code"],
                "enriched_snapshot": {
                    "product": {
                        "code": incoming["code"],
                        "product_name": "Prepared",
                    },
                    "flags": {},
                    "category_props": {},
                    "nutrition": {},
                },
                "legacy_check_tags": {},
            }
            self._process.stdout.push(json.dumps(payload) + "\n")
        return len(text)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True


class _ScriptedQueue(queue.Queue[object]):
    def __init__(self, items: list[object]) -> None:
        super().__init__()
        self._items = iter(items)

    def get(self, block: bool = True, timeout: float | None = None) -> object:
        item = next(self._items)
        if isinstance(item, BaseException):
            raise item
        return item


class _FailingProcess(_FakeProcess):
    def __init__(self) -> None:
        super().__init__(
            stderr_lines=["Can't locate ProductOpener/DataQuality.pm in @INC\n"]
        )
        self.stdin = _FailingStdin(self)


class _FailingStdin:
    def __init__(self, process: _FailingProcess) -> None:
        self._process = process
        self.closed = False

    def write(self, text: str) -> int:
        return len(text)

    def flush(self) -> None:
        self._process.returncode = 1
        self._process.stdout.close()
        self._process.stderr.close()

    def close(self) -> None:
        self.closed = True


class _BrokenPipeProcess(_FakeProcess):
    def __init__(self) -> None:
        super().__init__(
            stderr_lines=["Can't locate ProductOpener/DataQuality.pm in @INC\n"]
        )
        self.stdin = _BrokenPipeStdin(self)


class _BrokenPipeStdin:
    def __init__(self, process: _BrokenPipeProcess) -> None:
        self._process = process
        self.closed = False

    def write(self, text: str) -> int:
        self._process.returncode = 1
        self._process.stdout.close()
        self._process.stderr.close()
        raise BrokenPipeError(32, "Broken pipe")

    def flush(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True
        raise BrokenPipeError(32, "Broken pipe")


def _legacy_backend_batch(
    legacy_backend_input_product_factory: LegacyBackendInputProductFactory,
    *codes: str,
) -> list[LegacyBackendInputProduct]:
    return [
        legacy_backend_input_product_factory(code=code, projected_input={"code": code})
        for code in codes
    ]


def _patch_fake_popen(
    monkeypatch: pytest.MonkeyPatch, *processes: _FakeProcess
) -> list[_FakeProcess]:
    created_processes: list[_FakeProcess] = []
    queued_processes = list(processes)

    def fake_popen(*args: object, **kwargs: object) -> _FakeProcess:
        process = queued_processes.pop(0) if queued_processes else _FakeProcess()
        created_processes.append(process)
        return process

    monkeypatch.setattr("app.legacy_backend.runner.subprocess.Popen", fake_popen)
    return created_processes


def test_emit_reference_findings_maps_codes_and_keeps_highest_severity(
    reference_result_factory: ReferenceResultFactory,
) -> None:
    active_checks = get_default_check_catalog().checks
    reference_result = reference_result_factory(
        code="123",
        legacy_check_tags={
            "bug": [
                "en:created-missing",
            ],
            "info": [
                "en:food-groups-1-known",
                "en:food-groups-2-known",
            ],
            "completeness": [
                "en:quantity-to-be-completed",
            ],
            "warning": [
                "en:quantity-to-be-completed",
            ],
            "error": [
                "en:debug-energy-value-in-kcal-does-not-match-value-computed-from-other-nutrients",
            ],
        },
    )

    findings = emit_reference_findings(
        [reference_result],
        active_checks=active_checks,
    )
    findings_by_key = {
        (finding.check_id, finding.observed_code): finding for finding in findings
    }

    assert (
        findings_by_key[("en:created-missing", "en:created-missing")].severity == "bug"
    )
    assert (
        findings_by_key[
            ("en:quantity-to-be-completed", "en:quantity-to-be-completed")
        ].severity
        == "warning"
    )
    assert (
        findings_by_key[
            ("en:food-groups-${level}-known", "en:food-groups-1-known")
        ].severity
        == "info"
    )
    assert (
        findings_by_key[
            ("en:food-groups-${level}-known", "en:food-groups-2-known")
        ].severity
        == "info"
    )
    assert (
        findings_by_key[
            (
                "en:${set_id}-energy-value-in-${unit}-does-not-match-value-computed-from-other-nutrients",
                "en:debug-energy-value-in-kcal-does-not-match-value-computed-from-other-nutrients",
            )
        ].severity
        == "error"
    )


def test_emit_reference_findings_filters_to_active_checks(
    reference_result_factory: ReferenceResultFactory,
) -> None:
    checks_by_id = get_default_check_catalog().checks_by_id
    reference_result = reference_result_factory(
        code="123",
        legacy_check_tags={
            "warning": [
                "en:quantity-to-be-completed",
                "en:serving-quantity-over-product-quantity",
            ],
        },
    )

    findings = emit_reference_findings(
        [reference_result],
        active_checks=[checks_by_id["en:serving-quantity-over-product-quantity"]],
    )

    assert [finding.check_id for finding in findings] == [
        "en:serving-quantity-over-product-quantity"
    ]


def test_legacy_backend_session_runs_one_batch_through_streaming_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    legacy_backend_input_product_factory: LegacyBackendInputProductFactory,
) -> None:
    stderr_path = tmp_path / "logs" / "reference.log"

    def fake_popen(*args: object, **kwargs: object) -> _FakeProcess:
        return _FakeProcess(stderr_lines=["backend warning\n"])

    monkeypatch.setattr("app.legacy_backend.runner.subprocess.Popen", fake_popen)

    with LegacyBackendSession(stderr_path=stderr_path) as session:
        backend_results = session.run(
            [
                legacy_backend_input_product_factory(
                    code="123", projected_input={"code": "123"}
                ),
                legacy_backend_input_product_factory(
                    code="456", projected_input={"code": "456"}
                ),
            ]
        )

    assert [result.code for result in backend_results] == ["123", "456"]
    assert backend_results[0].enriched_snapshot.product["product_name"] == "Prepared"
    assert stderr_path.read_text(encoding="utf-8") == "backend warning\n"


@pytest.mark.parametrize(
    "process_factory",
    [_FailingProcess, _BrokenPipeProcess],
    ids=["startup-hint", "broken-pipe"],
)
def test_legacy_backend_session_surfaces_backend_startup_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
    legacy_backend_input_product_factory: LegacyBackendInputProductFactory,
    process_factory: type[_FakeProcess],
) -> None:
    _patch_fake_popen(monkeypatch, process_factory())

    with LegacyBackendSession() as session:
        with pytest.raises(
            RuntimeError, match="Run the demo through the provided Docker image"
        ):
            session.run(
                _legacy_backend_batch(legacy_backend_input_product_factory, "123")
            )


def test_legacy_backend_session_logs_slow_first_output_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    session = LegacyBackendSession(
        stdout_queue=_ScriptedQueue(
            [
                queue.Empty(),
                queue.Empty(),
                json.dumps(
                    {"code": "123", "enriched_snapshot": {}, "legacy_check_tags": {}}
                )
                + "\n",
            ]
        )
    )
    fake_process = _FakeProcess()
    monkeypatch.setattr(session, "_require_process", lambda: fake_process)

    timings = iter([0.0, 15.0, 31.0])
    monkeypatch.setattr("app.legacy_backend.runner.perf_counter", lambda: next(timings))
    caplog.set_level(logging.WARNING)

    line = session.next_stdout_line(batch_size=250, received_count=0)

    assert json.loads(line)["code"] == "123"
    assert caplog.messages == [
        "[Legacy Backend] Worker still waiting for backend output after 31.0s (batch size: 250)."
    ]


def test_legacy_backend_session_pool_runs_batches_through_multiple_workers(
    monkeypatch: pytest.MonkeyPatch,
    legacy_backend_input_product_factory: LegacyBackendInputProductFactory,
) -> None:
    popen_calls = _patch_fake_popen(monkeypatch)

    with LegacyBackendSessionPool(worker_count=2) as pool:
        with ThreadPoolExecutor(max_workers=2) as executor:
            first_future = executor.submit(
                pool.run,
                _legacy_backend_batch(legacy_backend_input_product_factory, "123"),
            )
            second_future = executor.submit(
                pool.run,
                _legacy_backend_batch(legacy_backend_input_product_factory, "456"),
            )

        first_result = first_future.result()
        second_result = second_future.result()

    assert len(popen_calls) == 2
    assert [product.code for product in first_result] == ["123"]
    assert [product.code for product in second_result] == ["456"]


def test_legacy_backend_session_pool_reuses_worker_after_success(
    monkeypatch: pytest.MonkeyPatch,
    legacy_backend_input_product_factory: LegacyBackendInputProductFactory,
) -> None:
    popen_calls = _patch_fake_popen(monkeypatch)

    with LegacyBackendSessionPool(worker_count=1) as pool:
        first_result = pool.run(
            _legacy_backend_batch(legacy_backend_input_product_factory, "123")
        )
        second_result = pool.run(
            _legacy_backend_batch(legacy_backend_input_product_factory, "456")
        )

    assert len(popen_calls) == 1
    assert [product.code for product in first_result] == ["123"]
    assert [product.code for product in second_result] == ["456"]


def test_legacy_backend_session_pool_replaces_failed_worker_before_re_raise(
    monkeypatch: pytest.MonkeyPatch,
    legacy_backend_input_product_factory: LegacyBackendInputProductFactory,
) -> None:
    popen_calls = _patch_fake_popen(monkeypatch, _FailingProcess(), _FakeProcess())

    with LegacyBackendSessionPool(worker_count=1) as pool:
        with pytest.raises(
            RuntimeError, match="Run the demo through the provided Docker image"
        ):
            pool.run(_legacy_backend_batch(legacy_backend_input_product_factory, "123"))

        result = pool.run(
            _legacy_backend_batch(legacy_backend_input_product_factory, "456")
        )

    assert len(popen_calls) == 2
    assert [product.code for product in result] == ["456"]
