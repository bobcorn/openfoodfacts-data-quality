from __future__ import annotations

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
APPS_ROOT = PROJECT_ROOT / "apps"
MIGRATION_ROOT = PROJECT_ROOT / "migration"
UI_ROOT = PROJECT_ROOT / "ui"
FORBIDDEN_SOURCE_TREE_PATTERNS = (
    re.compile(r"src/off_data_quality"),
    re.compile(r'["\']src["\']\s*\)?\s*/\s*["\']off_data_quality["\']'),
)


def test_runtime_layers_do_not_reference_library_source_tree_paths() -> None:
    violating_locations: list[str] = []
    for root in (APPS_ROOT, MIGRATION_ROOT, UI_ROOT):
        for file_path in _python_files(root):
            source_text = file_path.read_text(encoding="utf-8")
            for pattern in FORBIDDEN_SOURCE_TREE_PATTERNS:
                if pattern.search(source_text):
                    relative_path = file_path.relative_to(PROJECT_ROOT)
                    violating_locations.append(
                        f"{relative_path}: matched forbidden source-tree pattern {pattern.pattern!r}"
                    )
                    break
    assert not violating_locations, "\n".join(violating_locations)


def _python_files(root: Path) -> list[Path]:
    return sorted(
        file_path
        for file_path in root.rglob("*.py")
        if "__pycache__" not in file_path.parts
    )
