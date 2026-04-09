from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import cast

from _bootstrap import ROOT, bootstrap_paths

bootstrap_paths()

from app.source.snapshots import source_snapshot_id_for

DEFAULT_SOURCE_JSONL = ROOT / "data" / "products.jsonl"
DEFAULT_OUTPUT_JSONL = ROOT / "examples" / "data" / "products.jsonl"
DEFAULT_SAMPLE_SIZE = 100


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refresh the application JSONL source snapshot sample."
    )
    parser.add_argument(
        "--source-jsonl",
        type=Path,
        default=DEFAULT_SOURCE_JSONL,
        help="Path to the full Open Food Facts JSONL source snapshot.",
    )
    parser.add_argument(
        "--output-jsonl",
        type=Path,
        default=DEFAULT_OUTPUT_JSONL,
        help="Destination JSONL sample used by the application Docker flow.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help="Number of products to keep in the source snapshot sample.",
    )
    args = parser.parse_args()

    source_jsonl = args.source_jsonl.resolve()
    output_jsonl = args.output_jsonl.resolve()
    if not source_jsonl.exists():
        raise FileNotFoundError(f"Source JSONL not found: {source_jsonl}")
    if args.sample_size <= 0:
        raise ValueError("--sample-size must be a positive integer.")

    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_jsonl = Path(temp_dir) / output_jsonl.name
        product_count = write_sample_jsonl(
            source_jsonl=source_jsonl,
            output_jsonl=temp_jsonl,
            sample_size=args.sample_size,
        )
        temp_jsonl.replace(output_jsonl)
    source_snapshot_id = source_snapshot_id_for(output_jsonl)

    print(
        f"Refreshed {output_jsonl} with {product_count} products from "
        f"{source_jsonl} (source snapshot {source_snapshot_id})."
    )
    return 0


def write_sample_jsonl(
    *,
    source_jsonl: Path,
    output_jsonl: Path,
    sample_size: int,
) -> int:
    """Write the first valid full product documents from a JSONL source snapshot."""
    product_count = 0
    with source_jsonl.open("r", encoding="utf-8") as source_handle:
        with output_jsonl.open("w", encoding="utf-8") as output_handle:
            for line in source_handle:
                if not line.strip():
                    continue
                document = json.loads(line)
                if not _has_valid_code(document):
                    continue
                output_handle.write(
                    json.dumps(
                        document,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                )
                output_handle.write("\n")
                product_count += 1
                if product_count == sample_size:
                    return product_count

    raise ValueError(
        f"Source JSONL {source_jsonl} did not contain {sample_size} products "
        "with a non-empty code."
    )


def _has_valid_code(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    code = cast(dict[str, object], value).get("code")
    return isinstance(code, str) and bool(code.strip())


if __name__ == "__main__":
    raise SystemExit(main())
