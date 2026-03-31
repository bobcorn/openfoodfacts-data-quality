from __future__ import annotations

import hashlib
import os
import threading
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

from app.reference.models import REFERENCE_RESULT_SCHEMA_VERSION, ReferenceResult

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

LEGACY_BACKEND_FINGERPRINT_ENV_VAR = "LEGACY_BACKEND_FINGERPRINT"
REFERENCE_RESULT_CACHE_DIR_ENV_VAR = "REFERENCE_RESULT_CACHE_DIR"
REFERENCE_RESULT_CACHE_SALT_ENV_VAR = "REFERENCE_RESULT_CACHE_SALT"
LEGACY_SOURCE_ROOT_ENV_VAR = "LEGACY_SOURCE_ROOT"
REFERENCE_RESULT_EXECUTION_FINGERPRINT_PATHS = (
    Path("app") / "reference" / "models.py",
    Path("app") / "legacy_backend" / "input_projection.py",
    Path("app") / "legacy_backend" / "off_runtime.pl",
    Path("src") / "openfoodfacts_data_quality" / "raw_products.py",
    Path("src") / "openfoodfacts_data_quality" / "scalars.py",
)
LEGACY_BACKEND_FINGERPRINT_PATHS = (
    Path("lib") / "ProductOpener" / "DataQuality.pm",
    Path("lib") / "ProductOpener" / "DataQualityCommon.pm",
    Path("lib") / "ProductOpener" / "DataQualityFood.pm",
    Path("lib") / "ProductOpener" / "DataQualityDimensions.pm",
    Path("lib") / "ProductOpener" / "Nutrition.pm",
    Path("lib") / "ProductOpener" / "Tags.pm",
    Path("taxonomies") / "categories.txt",
    Path("taxonomies") / "countries.txt",
    Path("taxonomies") / "data_quality.txt",
)


def configured_reference_result_cache_dir(project_root: Path) -> Path:
    """Return the configured directory for persisted reference results."""
    configured = os.environ.get(REFERENCE_RESULT_CACHE_DIR_ENV_VAR)
    if configured:
        return Path(configured).expanduser().resolve()
    return (project_root / "data" / "reference_result_cache").resolve()


def configured_reference_result_cache_salt() -> str:
    """Return an optional cache salt for manual invalidation."""
    configured = os.environ.get(REFERENCE_RESULT_CACHE_SALT_ENV_VAR)
    if configured is not None:
        return configured
    return ""


def reference_result_cache_key(
    project_root: Path,
    *,
    extra_salt: str = "",
) -> str:
    """Derive a stable cache key from the full reference-result execution contract."""
    digest = hashlib.sha256()
    digest.update(f"schema:{REFERENCE_RESULT_SCHEMA_VERSION}".encode())
    digest.update(_legacy_backend_fingerprint(project_root).encode("utf-8"))
    for relative_path in REFERENCE_RESULT_EXECUTION_FINGERPRINT_PATHS:
        digest.update((project_root / relative_path).read_bytes())
    if extra_salt:
        digest.update(extra_salt.encode("utf-8"))
    return digest.hexdigest()[:12]


def _legacy_backend_fingerprint(project_root: Path) -> str:
    """Return a stable fingerprint for the actual backend runtime behind reference execution."""
    configured = os.environ.get(LEGACY_BACKEND_FINGERPRINT_ENV_VAR)
    if configured is not None:
        normalized = configured.strip()
        if not normalized:
            raise ValueError(
                f"{LEGACY_BACKEND_FINGERPRINT_ENV_VAR} must not be blank when set."
            )
        return f"env:{normalized}"

    backend_root = _resolve_legacy_backend_root(project_root)
    if backend_root is None:
        raise RuntimeError(
            "Legacy backend fingerprint could not be determined. "
            f"Set {LEGACY_BACKEND_FINGERPRINT_ENV_VAR} or make the legacy backend source "
            f"available via {LEGACY_SOURCE_ROOT_ENV_VAR}, ../openfoodfacts-server, or /opt/product-opener."
        )

    missing_paths = [
        relative_path
        for relative_path in LEGACY_BACKEND_FINGERPRINT_PATHS
        if not (backend_root / relative_path).exists()
    ]
    if missing_paths:
        missing_list = ", ".join(str(path) for path in missing_paths)
        raise RuntimeError(
            f"Legacy backend root {backend_root} is missing required fingerprint inputs: {missing_list}"
        )

    digest = hashlib.sha256()
    for relative_path in LEGACY_BACKEND_FINGERPRINT_PATHS:
        digest.update(str(relative_path).encode("utf-8"))
        digest.update((backend_root / relative_path).read_bytes())
    return f"files:{digest.hexdigest()}"


def _resolve_legacy_backend_root(project_root: Path) -> Path | None:
    """Resolve the legacy backend source root used for local reference execution."""
    configured = os.environ.get(LEGACY_SOURCE_ROOT_ENV_VAR)
    candidates = [
        Path(configured).expanduser().resolve() if configured else None,
        (project_root.parent / "openfoodfacts-server").resolve(),
        Path("/opt/product-opener"),
    ]
    for candidate in candidates:
        if candidate is None:
            continue
        if (candidate / LEGACY_BACKEND_FINGERPRINT_PATHS[0]).exists():
            return candidate
    return None


def reference_result_cache_path(
    *,
    cache_dir: Path,
    source_snapshot_id: str,
    cache_key: str,
) -> Path:
    """Return the on-disk cache database path for one reference-result set."""
    return cache_dir / f"reference-result-{source_snapshot_id}-{cache_key}.duckdb"


class ReferenceResultCache(AbstractContextManager["ReferenceResultCache"]):
    """Persist reference results in a local DuckDB artifact."""

    def __init__(
        self,
        *,
        path: Path,
        source_snapshot_id: str,
        cache_key: str,
    ) -> None:
        self.path = path
        self.source_snapshot_id = source_snapshot_id
        self.cache_key = cache_key
        self._lock = threading.Lock()
        self._started = False

    def __enter__(self) -> ReferenceResultCache:
        self.start()
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        return None

    def start(self) -> None:
        """Create the cache DB and metadata tables if needed."""
        if self._started:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._locked_connection() as connection:
            connection.execute(
                """
                create table if not exists reference_results (
                    code varchar primary key,
                    payload_json text not null
                )
                """
            )
            connection.execute(
                """
                create table if not exists cache_meta (
                    source_snapshot_id varchar not null,
                    cache_key varchar not null,
                    schema_version integer not null
                )
                """
            )
            row = connection.execute("select count(*) from cache_meta").fetchone()
            if row is None:
                raise RuntimeError(
                    "Reference result cache metadata query returned no row."
                )
            existing = row[0]
            if existing == 0:
                connection.execute(
                    "insert into cache_meta values (?, ?, ?)",
                    [
                        self.source_snapshot_id,
                        self.cache_key,
                        REFERENCE_RESULT_SCHEMA_VERSION,
                    ],
                )
        self._started = True

    def load_many(self, codes: list[str]) -> dict[str, ReferenceResult]:
        """Load cached reference results for the requested product codes."""
        if not codes:
            return {}
        self.start()
        placeholders = ",".join(["?"] * len(codes))
        with self._locked_connection() as connection:
            rows = connection.execute(
                f"select code, payload_json from reference_results where code in ({placeholders})",
                codes,
            ).fetchall()
        return {
            str(code): ReferenceResult.model_validate_json(payload_json)
            for code, payload_json in rows
        }

    def store_many(self, reference_results: list[ReferenceResult]) -> None:
        """Persist reference results for future reuse."""
        if not reference_results:
            return
        self.start()
        codes = [(result.code,) for result in reference_results]
        rows = [(result.code, result.model_dump_json()) for result in reference_results]
        with self._locked_connection() as connection:
            connection.executemany(
                "delete from reference_results where code = ?", codes
            )
            connection.executemany(
                "insert into reference_results (code, payload_json) values (?, ?)",
                rows,
            )

    @contextmanager
    def _locked_connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Yield a DuckDB connection under the cache mutex."""
        with self._lock:
            connection = duckdb.connect(str(self.path))
            try:
                yield connection
            finally:
                connection.close()
