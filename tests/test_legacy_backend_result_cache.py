from __future__ import annotations

import hashlib
from collections.abc import Callable
from pathlib import Path

import pytest
from app.reference.cache import (
    LEGACY_BACKEND_FINGERPRINT_ENV_VAR,
    LEGACY_BACKEND_FINGERPRINT_PATHS,
    LEGACY_SOURCE_ROOT_ENV_VAR,
    REFERENCE_RESULT_CACHE_DIR_ENV_VAR,
    REFERENCE_RESULT_CACHE_SALT_ENV_VAR,
    REFERENCE_RESULT_EXECUTION_FINGERPRINT_PATHS,
    ReferenceResultCache,
    configured_reference_result_cache_dir,
    configured_reference_result_cache_salt,
    reference_result_cache_key,
)
from app.reference.models import ReferenceResult

ReferenceResultFactory = Callable[..., ReferenceResult]


def _write_fingerprint_inputs(root: Path, paths: tuple[Path, ...]) -> None:
    """Create one set of fingerprinted files under the requested root."""
    for relative_path in paths:
        target = root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"{relative_path}\n", encoding="utf-8")


def _prepare_local_backend_result_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path]:
    """Create the local project/backend inputs needed by cache-key tests."""
    project_root = tmp_path / "project"
    _write_fingerprint_inputs(
        project_root,
        REFERENCE_RESULT_EXECUTION_FINGERPRINT_PATHS,
    )

    backend_root = tmp_path / "openfoodfacts-server"
    _write_fingerprint_inputs(backend_root, LEGACY_BACKEND_FINGERPRINT_PATHS)

    monkeypatch.delenv(LEGACY_BACKEND_FINGERPRINT_ENV_VAR, raising=False)
    monkeypatch.setenv(LEGACY_SOURCE_ROOT_ENV_VAR, str(backend_root))
    return project_root, backend_root


def test_reference_result_cache_roundtrips_reference_results(
    tmp_path: Path,
    reference_result_factory: ReferenceResultFactory,
) -> None:
    cache = ReferenceResultCache(
        path=tmp_path / "reference-result-cache.duckdb",
        source_snapshot_id="source-snapshot-123",
        cache_key="cache-key",
    )
    reference_result = reference_result_factory(
        code="123",
        enriched_snapshot={
            "product": {
                "code": "123",
                "product_name": "Prepared name",
                "serving_quantity": "28.000000000000004",
            },
            "flags": {"is_european_product": True},
            "category_props": {"minimum_number_of_ingredients": "4"},
            "nutrition": {"input_sets": []},
        },
        legacy_check_tags={"warning": ["en:quantity-to-be-completed"]},
    )

    with cache:
        cache.store_many([reference_result])
        loaded = cache.load_many(["123", "999"])

    assert set(loaded) == {"123"}
    loaded_product = loaded["123"]
    assert (
        loaded_product.enriched_snapshot.product["serving_quantity"]
        == "28.000000000000004"
    )
    assert loaded_product.legacy_check_tags.warning == ["en:quantity-to-be-completed"]


def test_reference_result_cache_key_prefers_explicit_legacy_backend_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = Path(__file__).resolve().parents[1]
    monkeypatch.setenv(LEGACY_BACKEND_FINGERPRINT_ENV_VAR, "backend-image:demo")

    cache_key = reference_result_cache_key(project_root)

    digest = hashlib.sha256()
    digest.update(b"schema:2")
    digest.update(b"env:backend-image:demo")
    for relative_path in REFERENCE_RESULT_EXECUTION_FINGERPRINT_PATHS:
        digest.update((project_root / relative_path).read_bytes())
    assert cache_key == digest.hexdigest()[:12]


def test_reference_result_cache_key_hashes_local_backend_files_when_no_override_is_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root, backend_root = _prepare_local_backend_result_execution(
        tmp_path,
        monkeypatch,
    )
    first_key = reference_result_cache_key(project_root)
    (backend_root / LEGACY_BACKEND_FINGERPRINT_PATHS[0]).write_text(
        "changed\n", encoding="utf-8"
    )
    second_key = reference_result_cache_key(project_root)

    assert first_key != second_key


def test_reference_result_cache_key_changes_when_backend_input_projection_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root, _ = _prepare_local_backend_result_execution(
        tmp_path,
        monkeypatch,
    )
    first_key = reference_result_cache_key(project_root)
    (project_root / "app/legacy_backend/input_projection.py").write_text(
        "projection = 'changed'\n",
        encoding="utf-8",
    )
    second_key = reference_result_cache_key(project_root)

    assert first_key != second_key


def test_reference_result_cache_uses_configured_env_vars(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    monkeypatch.setenv(
        REFERENCE_RESULT_CACHE_DIR_ENV_VAR,
        str(tmp_path / "cache"),
    )
    monkeypatch.setenv(REFERENCE_RESULT_CACHE_SALT_ENV_VAR, "salted")

    assert configured_reference_result_cache_dir(project_root) == (tmp_path / "cache")
    assert configured_reference_result_cache_salt() == "salted"
