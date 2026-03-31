from __future__ import annotations

import argparse
import time
from datetime import datetime
from pathlib import Path

from _bootstrap import SRC_ROOT

PACKAGE_ROOT = SRC_ROOT / "openfoodfacts_data_quality"

from openfoodfacts_data_quality.checks.dsl.parser import load_dsl_definitions


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate OFF Quality DSL definitions with structural and semantic validation."
    )
    parser.add_argument(
        "--path",
        type=Path,
        default=PACKAGE_ROOT / "checks" / "packs" / "dsl",
        help="Path to one DSL definitions YAML file or a directory of DSL packs.",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Watch the definitions YAML and schema files for changes and re-run validation.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="Polling interval in seconds for watch mode.",
    )
    args = parser.parse_args()

    path = args.path.resolve()
    if not args.watch:
        return validate_once(path)

    return watch(path, interval=args.interval)


def validate_once(path: Path) -> int:
    """Run structural and semantic validation once."""
    try:
        definition_paths = resolve_definition_paths(path)
        loaded_count = 0
        for definition_path in definition_paths:
            checks = load_dsl_definitions(definition_path)
            loaded_count += len(checks)
            print(
                f"[OK] {definition_path}: loaded {len(checks)} DSL definitions.",
                flush=True,
            )
    except Exception as exc:  # pragma: no cover - exercised through CLI behavior
        print(f"[ERROR] {path}:1:1: {exc}", flush=True)
        return 1

    if len(definition_paths) > 1:
        print(
            f"[OK] {path}: loaded {loaded_count} DSL definitions across {len(definition_paths)} files.",
            flush=True,
        )
    return 0


def watch(path: Path, interval: float) -> int:
    """Poll the relevant files and re-run validation when they change."""
    watch_paths = [
        *resolve_watch_paths(path),
        PACKAGE_ROOT / "checks" / "dsl" / "schema" / "definitions.schema.json",
        PACKAGE_ROOT / "checks" / "dsl" / "parser.py",
        PACKAGE_ROOT / "checks" / "dsl" / "semantic.py",
        PACKAGE_ROOT / "context" / "paths.py",
    ]
    previous_state = _snapshot(watch_paths)

    print(f"Watching DSL definitions: {path}", flush=True)
    _print_timestamp()
    validate_once(path)
    _print_cycle_end()

    try:
        while True:
            time.sleep(interval)
            current_state = _snapshot(watch_paths)
            if current_state == previous_state:
                continue
            previous_state = current_state
            _print_timestamp()
            validate_once(path)
            _print_cycle_end()
    except KeyboardInterrupt:
        print("\nStopped DSL validation watcher.", flush=True)
        return 0


def _snapshot(paths: list[Path]) -> dict[Path, int]:
    """Return a cheap filesystem state snapshot for polling."""
    state: dict[Path, int] = {}
    for candidate in paths:
        try:
            state[candidate] = candidate.stat().st_mtime_ns
        except FileNotFoundError:
            state[candidate] = -1
    return state


def _print_timestamp() -> None:
    """Print a short timestamp before each validation run in watch mode."""
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Re-validating...", flush=True)


def _print_cycle_end() -> None:
    """Print a marker that tells the VS Code background matcher one cycle is complete."""
    print("[VALIDATION-END]", flush=True)


def resolve_definition_paths(path: Path) -> list[Path]:
    """Resolve one file or one directory of YAML packs into ordered definition files."""
    if path.is_file():
        return [path]
    if not path.exists():
        raise FileNotFoundError(path)
    if not path.is_dir():
        raise ValueError(f"DSL path must be a file or directory, got {path}.")

    definition_paths = sorted(candidate for candidate in path.glob("*.yaml"))
    if not definition_paths:
        raise FileNotFoundError(f"No DSL definition files found under {path}.")
    return definition_paths


def resolve_watch_paths(path: Path) -> list[Path]:
    """Return the filesystem paths whose changes should trigger re-validation."""
    return [path, *resolve_definition_paths(path)]


if __name__ == "__main__":
    raise SystemExit(main())
