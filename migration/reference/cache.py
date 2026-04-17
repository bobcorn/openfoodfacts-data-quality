from __future__ import annotations

import hashlib
import json
import os
import threading
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Self

import duckdb

from migration.reference.models import REFERENCE_RESULT_SCHEMA_VERSION, ReferenceResult
from off_data_quality.metadata import packaged_runtime_fingerprint

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

LEGACY_BACKEND_FINGERPRINT_ENV_VAR = "LEGACY_BACKEND_FINGERPRINT"
REFERENCE_RESULT_CACHE_DIR_ENV_VAR = "REFERENCE_RESULT_CACHE_DIR"
REFERENCE_RESULT_CACHE_SALT_ENV_VAR = "REFERENCE_RESULT_CACHE_SALT"
LEGACY_SOURCE_ROOT_ENV_VAR = "LEGACY_SOURCE_ROOT"
REFERENCE_RESULT_APPLICATION_FINGERPRINT_PATHS = (
    Path("migration") / "reference" / "cache.py",
    Path("migration") / "reference" / "models.py",
    Path("migration") / "legacy_backend" / "contracts.py",
    Path("migration") / "legacy_backend" / "input_payloads.py",
    Path("migration") / "legacy_backend" / "off_runtime.pl",
    Path("migration") / "source" / "product_documents.py",
    Path("migration") / "source" / "snapshots.py",
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


@dataclass(frozen=True, slots=True)
class ReferenceResultCacheIdentity:
    """Stable execution identity for one reference result cache namespace."""

    cache_key: str
    execution_contract_fingerprint: str
    legacy_backend_fingerprint: str


@dataclass(frozen=True, slots=True)
class ReferenceResultCacheMetadata:
    """Stored cache metadata used to validate cache reuse."""

    source_snapshot_id: str
    cache_key: str
    schema_version: int
    execution_contract_fingerprint: str
    legacy_backend_fingerprint: str
    created_at_utc: str | None = None

    @classmethod
    def expected(
        cls,
        *,
        source_snapshot_id: str,
        identity: ReferenceResultCacheIdentity,
    ) -> Self:
        """Return the expected metadata for the current cache file."""
        return cls(
            source_snapshot_id=source_snapshot_id,
            cache_key=identity.cache_key,
            schema_version=REFERENCE_RESULT_SCHEMA_VERSION,
            execution_contract_fingerprint=identity.execution_contract_fingerprint,
            legacy_backend_fingerprint=identity.legacy_backend_fingerprint,
        )

    def with_created_at(self, created_at_utc: str) -> ReferenceResultCacheMetadata:
        """Return this metadata with a persisted creation timestamp."""
        return ReferenceResultCacheMetadata(
            source_snapshot_id=self.source_snapshot_id,
            cache_key=self.cache_key,
            schema_version=self.schema_version,
            execution_contract_fingerprint=self.execution_contract_fingerprint,
            legacy_backend_fingerprint=self.legacy_backend_fingerprint,
            created_at_utc=created_at_utc,
        )

    def matches_expected(self, expected: ReferenceResultCacheMetadata) -> bool:
        """Return whether the stored metadata matches the current runtime contract."""
        return (
            self.source_snapshot_id == expected.source_snapshot_id
            and self.cache_key == expected.cache_key
            and self.schema_version == expected.schema_version
            and self.execution_contract_fingerprint
            == expected.execution_contract_fingerprint
            and self.legacy_backend_fingerprint == expected.legacy_backend_fingerprint
        )

    def mismatch_details(
        self,
        expected: ReferenceResultCacheMetadata,
    ) -> dict[str, tuple[str | int | None, str | int | None]]:
        """Return the stored and expected fields that block cache reuse."""
        mismatches: dict[str, tuple[str | int | None, str | int | None]] = {}
        for field_name in (
            "source_snapshot_id",
            "cache_key",
            "schema_version",
            "execution_contract_fingerprint",
            "legacy_backend_fingerprint",
        ):
            actual_value = getattr(self, field_name)
            expected_value = getattr(expected, field_name)
            if actual_value != expected_value:
                mismatches[field_name] = (actual_value, expected_value)
        return mismatches


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
    """Derive a stable cache key from the full reference result execution contract."""
    return reference_result_cache_identity(
        project_root,
        extra_salt=extra_salt,
    ).cache_key


def reference_result_cache_identity(
    project_root: Path,
    *,
    extra_salt: str = "",
) -> ReferenceResultCacheIdentity:
    """Return the execution identity for one persisted reference result cache."""
    legacy_backend_fingerprint = _legacy_backend_fingerprint(project_root)
    execution_contract_fingerprint = _execution_contract_fingerprint(
        project_root,
        legacy_backend_fingerprint=legacy_backend_fingerprint,
        extra_salt=extra_salt,
    )
    return ReferenceResultCacheIdentity(
        cache_key=execution_contract_fingerprint[:12],
        execution_contract_fingerprint=execution_contract_fingerprint,
        legacy_backend_fingerprint=legacy_backend_fingerprint,
    )


def _execution_contract_fingerprint(
    project_root: Path,
    *,
    legacy_backend_fingerprint: str,
    extra_salt: str,
) -> str:
    """Hash the full execution contract that shapes cached reference results."""
    digest = hashlib.sha256()
    digest.update(f"schema:{REFERENCE_RESULT_SCHEMA_VERSION}".encode())
    digest.update(legacy_backend_fingerprint.encode("utf-8"))
    digest.update(packaged_runtime_fingerprint().encode("utf-8"))
    for relative_path in REFERENCE_RESULT_APPLICATION_FINGERPRINT_PATHS:
        digest.update(str(relative_path).encode("utf-8"))
        digest.update((project_root / relative_path).read_bytes())
    if extra_salt:
        digest.update(extra_salt.encode("utf-8"))
    return digest.hexdigest()


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
    """Return the on disk cache database path for one reference result set."""
    return cache_dir / f"reference-result-{source_snapshot_id}-{cache_key}.duckdb"


def reference_result_cache_manifest_path(cache_path: Path) -> Path:
    """Return the readable sidecar manifest path for one cache DB."""
    return cache_path.with_suffix(f"{cache_path.suffix}.meta.json")


class ReferenceResultCache(AbstractContextManager["ReferenceResultCache"]):
    """Persist reference results in a local DuckDB artifact."""

    def __init__(
        self,
        *,
        path: Path,
        metadata: ReferenceResultCacheMetadata,
    ) -> None:
        self.path = path
        self.metadata = metadata
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
        persisted_metadata: ReferenceResultCacheMetadata
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
                    schema_version integer not null,
                    execution_contract_fingerprint varchar not null,
                    legacy_backend_fingerprint varchar not null,
                    created_at_utc varchar not null
                )
                """
            )
            existing_metadata = self._load_existing_metadata(connection)
            if existing_metadata is None:
                stored_metadata = self.metadata.with_created_at(
                    datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                )
                connection.execute(
                    "insert into cache_meta values (?, ?, ?, ?, ?, ?)",
                    [
                        stored_metadata.source_snapshot_id,
                        stored_metadata.cache_key,
                        stored_metadata.schema_version,
                        stored_metadata.execution_contract_fingerprint,
                        stored_metadata.legacy_backend_fingerprint,
                        stored_metadata.created_at_utc,
                    ],
                )
                persisted_metadata = stored_metadata
            elif not existing_metadata.matches_expected(self.metadata):
                mismatch_summary = ", ".join(
                    f"{field}={actual!r} (expected {expected!r})"
                    for field, (actual, expected) in existing_metadata.mismatch_details(
                        self.metadata
                    ).items()
                )
                raise RuntimeError(
                    "Reference result cache metadata does not match the current "
                    "runtime contract. Remove or relocate the cache file and rerun. "
                    f"Mismatched fields: {mismatch_summary}."
                )
            else:
                persisted_metadata = existing_metadata
        self._write_manifest(persisted_metadata)
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

    def _load_existing_metadata(
        self,
        connection: duckdb.DuckDBPyConnection,
    ) -> ReferenceResultCacheMetadata | None:
        """Return stored cache metadata when present and structurally valid."""
        try:
            rows = connection.execute(
                """
                select
                    source_snapshot_id,
                    cache_key,
                    schema_version,
                    execution_contract_fingerprint,
                    legacy_backend_fingerprint,
                    created_at_utc
                from cache_meta
                """
            ).fetchall()
        except duckdb.Error as exc:
            raise RuntimeError(
                "Reference result cache metadata schema is incompatible with the "
                "current runtime. Remove or relocate the cache file and rerun."
            ) from exc

        if not rows:
            return None
        if len(rows) != 1:
            raise RuntimeError(
                "Reference result cache metadata must contain exactly one row."
            )

        (
            source_snapshot_id,
            cache_key,
            schema_version,
            execution_contract_fingerprint,
            legacy_backend_fingerprint,
            created_at_utc,
        ) = rows[0]
        return ReferenceResultCacheMetadata(
            source_snapshot_id=str(source_snapshot_id),
            cache_key=str(cache_key),
            schema_version=int(schema_version),
            execution_contract_fingerprint=str(execution_contract_fingerprint),
            legacy_backend_fingerprint=str(legacy_backend_fingerprint),
            created_at_utc=str(created_at_utc),
        )

    def _write_manifest(self, metadata: ReferenceResultCacheMetadata) -> None:
        """Write a readable manifest sidecar for quick cache inspection."""
        manifest_payload = {
            "path": str(self.path),
            "source_snapshot_id": metadata.source_snapshot_id,
            "cache_key": metadata.cache_key,
            "schema_version": metadata.schema_version,
            "execution_contract_fingerprint": (metadata.execution_contract_fingerprint),
            "legacy_backend_fingerprint": metadata.legacy_backend_fingerprint,
            "created_at_utc": metadata.created_at_utc,
        }
        reference_result_cache_manifest_path(self.path).write_text(
            json.dumps(manifest_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
