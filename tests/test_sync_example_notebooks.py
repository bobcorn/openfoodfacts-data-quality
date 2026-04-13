from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any, cast

import nbformat
import pytest
from nbformat import NotebookNode

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_MODULE_PATH = SCRIPTS_DIR / "sync_example_notebooks.py"
_SPEC = importlib.util.spec_from_file_location("sync_example_notebooks", _MODULE_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"Unable to load {_MODULE_PATH}.")
sync_example_notebooks = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(sync_example_notebooks)

_COVERAGE_ENV_VARS = (
    "COVERAGE_PROCESS_START",
    "COV_CORE_CONFIG",
    "COV_CORE_DATAFILE",
    "COV_CORE_SOURCE",
)


def test_selected_example_stems_default_to_all_examples() -> None:
    assert sync_example_notebooks._selected_example_stems(()) == (
        "basic_usage",
        "input_formats",
        "jurisdiction_filtering",
    )


def test_selected_example_stems_deduplicate_script_and_notebook_paths() -> None:
    assert sync_example_notebooks._selected_example_stems(
        (
            "examples/scripts/input_formats.py",
            "examples/notebooks/input_formats.ipynb",
            "README.md",
        )
    ) == ("input_formats",)


def test_execute_notebook_writes_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_coverage_env(monkeypatch)
    notebook_path = tmp_path / "example.ipynb"
    notebook: dict[str, object] = {
        "cells": [
            {
                "cell_type": "code",
                "execution_count": None,
                "id": "cell-1",
                "metadata": {},
                "outputs": [],
                "source": "print('hello from notebook')",
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            }
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    notebook_path.write_text(json.dumps(notebook), encoding="utf-8")

    sync_example_notebooks._execute_notebook(notebook_path, working_directory=tmp_path)

    executed = _read_notebook(notebook_path)
    cells = cast(list[dict[str, Any]], executed["cells"])
    outputs = cast(list[dict[str, Any]], cells[0]["outputs"])
    assert outputs
    assert outputs[0]["output_type"] == "stream"
    assert outputs[0]["text"] == "hello from notebook\n"


@pytest.mark.parametrize("stem", sync_example_notebooks.EXAMPLE_STEMS)
def test_bundled_example_notebooks_execute(
    stem: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_coverage_env(monkeypatch)
    source_notebook = ROOT / "examples" / "notebooks" / f"{stem}.ipynb"
    notebook_copy = tmp_path / source_notebook.name
    shutil.copy2(source_notebook, notebook_copy)

    sync_example_notebooks._execute_notebook(
        notebook_copy,
        working_directory=ROOT,
    )

    executed = _read_notebook(notebook_copy)
    cells = cast(list[dict[str, Any]], executed["cells"])
    assert any(cell["cell_type"] == "code" and cell.get("outputs") for cell in cells)


def _clear_coverage_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _COVERAGE_ENV_VARS:
        if name in os.environ:
            monkeypatch.delenv(name)


def _read_notebook(path: Path) -> NotebookNode:
    reader = cast(Any, nbformat).read
    return cast(NotebookNode, reader(path, as_version=4))
