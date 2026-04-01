from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / "src"


def bootstrap_paths() -> None:
    """Prepend the repo root and src tree to sys.path for direct script execution."""
    for candidate in (ROOT, SRC_ROOT):
        candidate_str = str(candidate)
        if candidate_str not in sys.path:
            sys.path.insert(0, candidate_str)
