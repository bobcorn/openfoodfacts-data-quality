from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

UI_ROOT = Path(__file__).resolve().parent
UI_TEMPLATE_ROOT = UI_ROOT / "templates"
UI_STATIC_ROOT = UI_ROOT / "static"


def create_template_environment(*template_roots: Path) -> Environment:
    """Build one Jinja environment with app templates and shared UI assets."""

    return Environment(
        loader=FileSystemLoader(_template_search_paths(template_roots)),
        autoescape=select_autoescape(["html", "xml"]),
    )


def _template_search_paths(template_roots: Iterable[Path]) -> list[str]:
    search_paths: list[str] = []
    for root in (*template_roots, UI_TEMPLATE_ROOT, UI_STATIC_ROOT):
        resolved_root = str(root.resolve())
        if resolved_root not in search_paths:
            search_paths.append(resolved_root)
    return search_paths
