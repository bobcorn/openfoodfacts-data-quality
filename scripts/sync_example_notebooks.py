from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any, cast

import nbformat
from nbclient import NotebookClient
from nbformat import NotebookNode

from _bootstrap import ROOT, bootstrap_paths

bootstrap_paths()

EXAMPLE_STEMS: tuple[str, ...] = (
    "basic_usage",
    "input_formats",
    "jurisdiction_filtering",
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Sync paired example scripts and notebooks, then execute notebooks in place."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="Optional changed example paths from pre-commit. Defaults to all paired examples.",
    )
    args = parser.parse_args(argv)

    selected_stems = _selected_example_stems(args.paths)
    if not selected_stems:
        return 0

    _sync_pairs(selected_stems)
    for stem in selected_stems:
        _execute_notebook(_notebook_path_for(stem), working_directory=ROOT)
    return 0


def _selected_example_stems(paths: Iterable[str]) -> tuple[str, ...]:
    normalized_paths = tuple(Path(path) for path in paths)
    if not normalized_paths:
        return EXAMPLE_STEMS

    selected_stems = {
        path.stem
        for path in normalized_paths
        if path.stem in EXAMPLE_STEMS and path.suffix in {".py", ".ipynb"}
    }
    return tuple(stem for stem in EXAMPLE_STEMS if stem in selected_stems)


def _sync_pairs(selected_stems: Sequence[str]) -> None:
    sync_targets = [
        str(path)
        for stem in selected_stems
        for path in (_script_path_for(stem), _notebook_path_for(stem))
    ]
    subprocess.run(
        [sys.executable, "-m", "jupytext", "--sync", *sync_targets],
        check=True,
        cwd=ROOT,
    )


def _execute_notebook(
    notebook_path: Path,
    *,
    working_directory: Path,
) -> None:
    notebook = _read_notebook(notebook_path)
    NotebookClient(
        notebook,
        kernel_name=_kernel_name(notebook),
        resources={"metadata": {"path": str(working_directory)}},
    ).execute()
    _write_notebook(notebook, notebook_path)


def _read_notebook(path: Path) -> NotebookNode:
    reader = cast(Any, nbformat).read
    return cast(NotebookNode, reader(path, as_version=4))


def _write_notebook(notebook: NotebookNode, path: Path) -> None:
    writer = cast(Any, nbformat).write
    writer(notebook, path)


def _kernel_name(notebook: NotebookNode) -> str:
    metadata = cast(dict[str, object], notebook.get("metadata", {}))
    kernelspec = metadata.get("kernelspec", {})
    if isinstance(kernelspec, dict):
        kernelspec_mapping = cast(dict[str, object], kernelspec)
        kernel_name_value = kernelspec_mapping.get("name")
        if isinstance(kernel_name_value, str) and kernel_name_value.strip():
            return kernel_name_value
    return "python3"


def _script_path_for(stem: str) -> Path:
    return ROOT / "examples" / "scripts" / f"{stem}.py"


def _notebook_path_for(stem: str) -> Path:
    return ROOT / "examples" / "notebooks" / f"{stem}.ipynb"


if __name__ == "__main__":
    raise SystemExit(main())
