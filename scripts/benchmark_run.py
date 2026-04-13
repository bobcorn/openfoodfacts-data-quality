from __future__ import annotations

import argparse
import json
import logging
import shutil
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

from _bootstrap import ROOT, bootstrap_paths

bootstrap_paths()

from migration.run.orchestrator import MigrationRunner
from migration.run.settings import configured_run_spec
from migration.storage import load_recorded_run_benchmark_summary

from runtime_support.logging_config import configure_cli_logging

LOGGER = logging.getLogger(__name__)
BENCHMARK_ARTIFACT_KIND = "openfoodfacts_data_quality.run_benchmark"
BENCHMARK_ARTIFACT_SCHEMA_VERSION = 1


def main() -> int:
    configure_cli_logging()
    args = parse_args()
    benchmark_root = (ROOT / "artifacts" / "benchmarks").resolve()
    reference_cache_dir = (
        args.reference_cache_dir.expanduser().resolve()
        if args.reference_cache_dir is not None
        else (benchmark_root / "reference-cache").resolve()
    )
    parity_store_path = (
        args.parity_store_path.expanduser().resolve()
        if args.parity_store_path is not None
        else (benchmark_root / "benchmark-parity.duckdb").resolve()
    )
    output_path = (
        args.output.expanduser().resolve()
        if args.output is not None
        else (
            benchmark_root
            / f"benchmark-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}.json"
        ).resolve()
    )
    benchmark_root.mkdir(parents=True, exist_ok=True)

    if args.clear_reference_cache and reference_cache_dir.exists():
        shutil.rmtree(reference_cache_dir)

    results: list[dict[str, Any]] = []
    for iteration in range(1, args.repeat + 1):
        run_spec = replace(
            configured_run_spec(
                ROOT,
                check_profile_name=args.check_profile,
                dataset_profile_name=args.dataset_profile,
                parity_store_path=parity_store_path,
            ),
            reference_result_cache_dir=reference_cache_dir,
        )
        LOGGER.info(
            "[Benchmark] Iteration %d/%d using dataset profile %s.",
            iteration,
            args.repeat,
            run_spec.dataset_profile_name or "default",
        )
        started = perf_counter()
        executed = MigrationRunner(run_spec, logger=LOGGER).execute()
        wall_seconds = perf_counter() - started
        if run_spec.parity_store_path is None:
            raise RuntimeError("Benchmark runs require a parity store path.")
        summary = load_recorded_run_benchmark_summary(
            run_spec.parity_store_path,
            run_id=executed.run_result.run_id,
        )
        results.append(
            {
                "iteration": iteration,
                "wall_seconds": wall_seconds,
                "summary": summary.as_payload(),
            }
        )
        LOGGER.info(
            "[Benchmark] Run %s finished in %.1fs wall time across %d batch(es).",
            executed.run_result.run_id,
            wall_seconds,
            summary.batch_count,
        )

    payload = {
        "kind": BENCHMARK_ARTIFACT_KIND,
        "schema_version": BENCHMARK_ARTIFACT_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataset_profile_name": args.dataset_profile,
        "check_profile_name": args.check_profile,
        "repeat": args.repeat,
        "reference_cache_dir": str(reference_cache_dir),
        "parity_store_path": str(parity_store_path),
        "results": results,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    LOGGER.info("[Benchmark] Wrote benchmark summary to %s.", output_path)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the application pipeline and write a machine-readable benchmark summary.",
    )
    parser.add_argument(
        "--dataset-profile",
        default="benchmark_10k",
        help="Named dataset profile from config/dataset-profiles.toml.",
    )
    parser.add_argument(
        "--check-profile",
        default=None,
        help="Optional named check profile from config/check-profiles.toml.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="How many consecutive runs to execute with the same cache and store paths.",
    )
    parser.add_argument(
        "--reference-cache-dir",
        type=Path,
        default=None,
        help="Directory used for benchmark reference cache files.",
    )
    parser.add_argument(
        "--parity-store-path",
        type=Path,
        default=None,
        help="DuckDB parity store used to persist benchmarked runs.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Path to the benchmark JSON output file.",
    )
    parser.add_argument(
        "--clear-reference-cache",
        action="store_true",
        help="Delete the benchmark reference cache directory before the first run.",
    )
    args = parser.parse_args()
    if args.repeat <= 0:
        parser.error("--repeat must be a positive integer.")
    return args


if __name__ == "__main__":
    raise SystemExit(main())
