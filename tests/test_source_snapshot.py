from __future__ import annotations

import json
from pathlib import Path

import pytest
from migration.source.snapshots import (
    SOURCE_SNAPSHOT_ID_ENV_VAR,
    source_snapshot_id_for,
    source_snapshot_manifest_path_for,
    write_source_snapshot_manifest,
)


def test_source_snapshot_id_uses_sidecar_manifest_when_present(tmp_path: Path) -> None:
    db_path = tmp_path / "products.duckdb"
    db_path.write_bytes(b"duckdb")
    source_snapshot_manifest_path_for(db_path).write_text(
        json.dumps({"source_snapshot_id": "snapshot-from-manifest"}),
        encoding="utf-8",
    )

    assert source_snapshot_id_for(db_path) == "snapshot-from-manifest"


def test_source_snapshot_id_prefers_explicit_env_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "products.duckdb"
    db_path.write_bytes(b"duckdb")
    source_snapshot_manifest_path_for(db_path).write_text(
        json.dumps({"source_snapshot_id": "snapshot-from-manifest"}),
        encoding="utf-8",
    )
    monkeypatch.setenv(SOURCE_SNAPSHOT_ID_ENV_VAR, "snapshot-from-env")

    assert source_snapshot_id_for(db_path) == "snapshot-from-env"


def test_source_snapshot_id_rejects_blank_env_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "products.duckdb"
    db_path.write_bytes(b"duckdb")
    monkeypatch.setenv(SOURCE_SNAPSHOT_ID_ENV_VAR, "   ")

    with pytest.raises(
        ValueError,
        match=f"{SOURCE_SNAPSHOT_ID_ENV_VAR} must not be blank",
    ):
        source_snapshot_id_for(db_path)


def test_write_source_snapshot_manifest_persists_explicit_snapshot_id(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "products.duckdb"
    db_path.write_bytes(b"duckdb")

    manifest_path = write_source_snapshot_manifest(
        db_path,
        source_snapshot_id="snapshot-written",
    )

    assert manifest_path == source_snapshot_manifest_path_for(db_path)
    assert json.loads(manifest_path.read_text(encoding="utf-8")) == {
        "source_snapshot_id": "snapshot-written"
    }


def test_source_snapshot_id_writes_sidecar_manifest_after_hash_fallback(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "products.duckdb"
    db_path.write_bytes(b"duckdb-bytes")

    snapshot_id = source_snapshot_id_for(db_path)

    assert snapshot_id
    assert json.loads(
        source_snapshot_manifest_path_for(db_path).read_text(encoding="utf-8")
    ) == {"source_snapshot_id": snapshot_id}
