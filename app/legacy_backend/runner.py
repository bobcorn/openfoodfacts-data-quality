from __future__ import annotations

import json
import logging
import queue
import subprocess
import threading
from collections import deque
from contextlib import AbstractContextManager
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, TextIO, TypeVar

from app.reference.models import ReferenceResult

if TYPE_CHECKING:
    from types import TracebackType

    from app.legacy_backend.input_projection import LegacyBackendInputProduct

LOGGER = logging.getLogger(__name__)
_EOF = object()
STDOUT_POLL_SECONDS = 1.0
FIRST_OUTPUT_WARNING_SECONDS = 30.0

_StartedContextManagerT = TypeVar(
    "_StartedContextManagerT", bound="_StartedContextManager"
)


class _StartedContextManager(AbstractContextManager[object]):
    """Context-manager mixin for services that expose explicit start/close hooks."""

    def __enter__(self: _StartedContextManagerT) -> _StartedContextManagerT:
        self.start()
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def start(self) -> None:
        """Start the managed resource."""
        raise NotImplementedError

    def close(self) -> None:
        """Release the managed resource."""
        raise NotImplementedError


class LegacyBackendSession(_StartedContextManager):
    """Persistent streaming session around the OFF legacy backend wrapper."""

    def __init__(
        self,
        *,
        stderr_path: Path | None = None,
        stdout_queue: queue.Queue[object] | None = None,
    ) -> None:
        self._stderr_path = stderr_path
        self._process: subprocess.Popen[str] | None = None
        self._stdout_queue: queue.Queue[object] = stdout_queue or queue.Queue()
        self._stderr_lines: deque[str] = deque(maxlen=200)
        self._stdout_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._stderr_handle: TextIO | None = None

    def start(self) -> None:
        """Start the persistent backend worker if it is not running yet."""
        if self._process is not None:
            return

        if self._stderr_path is not None:
            self._stderr_path.parent.mkdir(parents=True, exist_ok=True)
            self._stderr_handle = self._stderr_path.open("a", encoding="utf-8")

        self._process = subprocess.Popen(
            [
                "perl",
                str(Path(__file__).with_name("off_runtime.pl")),
            ],
            cwd=_project_root(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        if (
            self._process.stdin is None
            or self._process.stdout is None
            or self._process.stderr is None
        ):
            raise RuntimeError("Legacy backend session could not allocate stdio pipes.")

        self._stdout_thread = threading.Thread(
            target=self._pump_stdout,
            name="legacy-backend-stdout",
            daemon=True,
        )
        self._stderr_thread = threading.Thread(
            target=self._pump_stderr,
            name="legacy-backend-stderr",
            daemon=True,
        )
        self._stdout_thread.start()
        self._stderr_thread.start()

    def run(
        self,
        backend_input_products: list[LegacyBackendInputProduct],
    ) -> list[ReferenceResult]:
        """Send one batch through the persistent backend worker."""
        if not backend_input_products:
            return []

        self.start()
        process = self._require_process()
        stdin = process.stdin
        if stdin is None:
            raise RuntimeError("Legacy backend session lost its stdin pipe.")

        for product in backend_input_products:
            self._ensure_running()
            try:
                stdin.write(json.dumps(product.projected_input, ensure_ascii=False))
                stdin.write("\n")
            except BrokenPipeError as exc:
                self._raise_pipe_failure(exc)
        try:
            stdin.flush()
        except BrokenPipeError as exc:
            self._raise_pipe_failure(exc)

        backend_results: list[ReferenceResult] = []
        for _ in backend_input_products:
            payload = json.loads(
                self.next_stdout_line(
                    batch_size=len(backend_input_products),
                    received_count=len(backend_results),
                )
            )
            backend_results.append(
                ReferenceResult(
                    code=payload["code"],
                    enriched_snapshot=payload["enriched_snapshot"],
                    legacy_check_tags=payload.get("legacy_check_tags", {}),
                )
            )

        return backend_results

    def close(self) -> None:
        """Terminate the persistent backend worker and close any open resources."""
        process = self._process
        if process is not None:
            if process.stdin is not None and not process.stdin.closed:
                try:
                    process.stdin.close()
                except BrokenPipeError:
                    pass
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

        if self._stdout_thread is not None:
            self._stdout_thread.join(timeout=1)
        if self._stderr_thread is not None:
            self._stderr_thread.join(timeout=1)
        if self._stderr_handle is not None:
            self._stderr_handle.close()

        self._process = None
        self._stdout_thread = None
        self._stderr_thread = None
        self._stderr_handle = None

    def _pump_stdout(self) -> None:
        """Read backend stdout continuously so the worker never blocks on a full pipe."""
        process = self._require_process()
        stdout = process.stdout
        if stdout is None:
            self._stdout_queue.put(
                RuntimeError("Legacy backend stdout pipe is unavailable.")
            )
            self._stdout_queue.put(_EOF)
            return

        try:
            for line in iter(stdout.readline, ""):
                self._stdout_queue.put(line)
        except Exception as exc:  # pragma: no cover - defensive path
            self._stdout_queue.put(exc)
        finally:
            self._stdout_queue.put(_EOF)

    def _pump_stderr(self) -> None:
        """Drain backend stderr into memory and the optional artifact log."""
        process = self._require_process()
        stderr = process.stderr
        if stderr is None:
            return

        for line in iter(stderr.readline, ""):
            self._stderr_lines.append(line.rstrip("\n"))
            if self._stderr_handle is not None:
                self._stderr_handle.write(line)
                self._stderr_handle.flush()

    def next_stdout_line(self, *, batch_size: int, received_count: int) -> str:
        """Return the next backend output line or raise a helpful runtime error."""
        waiting_started = perf_counter()
        warned_about_slow_first_output = False
        while True:
            self._ensure_running(allow_clean_exit=True)
            try:
                item = self._stdout_queue.get(timeout=STDOUT_POLL_SECONDS)
            except queue.Empty:
                warned_about_slow_first_output = (
                    self._maybe_warn_about_slow_first_output(
                        started_at=waiting_started,
                        batch_size=batch_size,
                        received_count=received_count,
                        warned_already=warned_about_slow_first_output,
                    )
                )
                continue

            if item is _EOF:
                raise RuntimeError(self._build_unexpected_exit_message())
            if isinstance(item, Exception):  # pragma: no cover - defensive path
                raise RuntimeError("Legacy backend stdout reader failed.") from item
            return str(item)

    def _maybe_warn_about_slow_first_output(
        self,
        *,
        started_at: float,
        batch_size: int,
        received_count: int,
        warned_already: bool,
    ) -> bool:
        """Log once when the backend is unusually slow to produce the first result."""
        if received_count != 0 or warned_already:
            return warned_already

        elapsed_seconds = perf_counter() - started_at
        if elapsed_seconds < FIRST_OUTPUT_WARNING_SECONDS:
            return False

        LOGGER.warning(
            "[Legacy Backend] Worker still waiting for backend output after %.1fs (batch size: %d).",
            elapsed_seconds,
            batch_size,
        )
        return True

    def _ensure_running(self, *, allow_clean_exit: bool = False) -> None:
        """Raise if the backend worker has exited unexpectedly."""
        process = self._require_process()
        returncode = process.poll()
        if returncode is None:
            return
        if allow_clean_exit and returncode == 0:
            return
        raise RuntimeError(self._build_unexpected_exit_message(returncode))

    def _build_unexpected_exit_message(self, returncode: int | None = None) -> str:
        """Build a helpful runtime error for worker crashes or premature exits."""
        self._wait_for_stderr_drain()
        actual_returncode = (
            self._require_process().poll() if returncode is None else returncode
        )
        stderr_text = "\n".join(self._stderr_lines).strip()
        hint = ""
        if "Can't locate ProductOpener" in stderr_text:
            hint = (
                "\n\nThe OFF backend Perl modules are unavailable in the current environment. "
                "Run the demo through the provided Docker image or from an OFF backend image."
            )
        return (
            "Legacy backend session failed.\n"
            f"returncode: {actual_returncode}\n\n"
            f"stderr:\n{stderr_text}{hint}"
        )

    def _raise_pipe_failure(self, exc: BrokenPipeError) -> None:
        """Raise a diagnostic runtime error when the backend stdin pipe breaks."""
        process = self._require_process()
        returncode = process.poll()
        if returncode is not None:
            raise RuntimeError(self._build_unexpected_exit_message(returncode)) from exc
        raise RuntimeError(
            "Legacy backend session lost its stdin pipe while the worker was still running."
        ) from exc

    def _wait_for_stderr_drain(self) -> None:
        """Give the stderr reader a brief chance to flush the final backend diagnostics."""
        process = self._process
        if process is None or process.poll() is None or self._stderr_thread is None:
            return
        self._stderr_thread.join(timeout=0.2)

    def _require_process(self) -> subprocess.Popen[str]:
        """Return the active backend process or raise if the session is closed."""
        if self._process is None:
            raise RuntimeError("Legacy backend session is not started.")
        return self._process


class LegacyBackendSessionPool(_StartedContextManager):
    """Pool of persistent legacy backend sessions for concurrent batch execution."""

    def __init__(
        self,
        *,
        worker_count: int,
        stderr_path: Path | None = None,
    ) -> None:
        if worker_count <= 0:
            raise ValueError("worker_count must be a positive integer.")
        self._worker_count = worker_count
        self._stderr_path = stderr_path
        self._sessions: list[LegacyBackendSession] = []
        self._available_sessions: queue.Queue[LegacyBackendSession] = queue.Queue()
        self._state_lock = threading.Lock()
        self._session_serial = 0

    def start(self) -> None:
        """Start all persistent backend workers if they are not running yet."""
        with self._state_lock:
            if self._sessions:
                return

            for _ in range(self._worker_count):
                self._start_session_locked()

    def run(
        self,
        backend_input_products: list[LegacyBackendInputProduct],
    ) -> list[ReferenceResult]:
        """Run one batch through the next available persistent backend worker."""
        if not backend_input_products:
            return []

        self.start()
        session = self._available_sessions.get()
        try:
            backend_results = session.run(backend_input_products)
        except Exception as exc:
            self._retire_failed_session(session, exc)
            raise
        self._available_sessions.put(session)
        return backend_results

    def close(self) -> None:
        """Terminate all backend workers and release any pool state."""
        while not self._available_sessions.empty():
            try:
                self._available_sessions.get_nowait()
            except queue.Empty:  # pragma: no cover - defensive path
                break

        with self._state_lock:
            sessions = list(reversed(self._sessions))
            self._sessions = []

        for session in sessions:
            session.close()

    def _start_session_locked(self) -> LegacyBackendSession:
        """Start one session and register it while holding the pool state lock."""
        self._session_serial += 1
        session = LegacyBackendSession(
            stderr_path=self._worker_stderr_path(self._session_serial)
        )
        session.start()
        self._sessions.append(session)
        self._available_sessions.put(session)
        return session

    def _retire_failed_session(
        self,
        session: LegacyBackendSession,
        cause: Exception,
    ) -> None:
        """Remove one failed session and eagerly restore pool capacity."""
        with self._state_lock:
            if session in self._sessions:
                self._sessions.remove(session)

        session.close()

        replacement_error: Exception | None = None
        try:
            with self._state_lock:
                if len(self._sessions) < self._worker_count:
                    self._start_session_locked()
        except Exception as exc:  # pragma: no cover - defensive path
            replacement_error = exc

        if replacement_error is not None:
            cause.add_note(
                "The failed backend worker could not be replaced automatically."
            )
            cause.add_note(f"Replacement startup error: {replacement_error}")

    def _worker_stderr_path(self, worker_index: int) -> Path | None:
        """Return the stderr artifact path for one started worker session."""
        if self._stderr_path is None:
            return None
        if self._worker_count == 1:
            return self._stderr_path
        stem = self._stderr_path.stem
        suffix = self._stderr_path.suffix
        return self._stderr_path.with_name(f"{stem}-worker-{worker_index:02d}{suffix}")


def _project_root() -> Path:
    """Return the repository root."""
    return Path(__file__).resolve().parents[2]
