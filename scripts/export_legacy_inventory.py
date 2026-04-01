from __future__ import annotations

import argparse
import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import Any, Literal

from _bootstrap import ROOT, bootstrap_paths

bootstrap_paths()

from app.legacy_source import (
    LegacySubroutineRecord,
    collect_legacy_subroutine_records,
    resolve_legacy_module_paths,
    resolve_legacy_source_root,
)

from openfoodfacts_data_quality.checks.legacy import (
    legacy_code_template_key,
    legacy_code_template_placeholders,
)

LEGACY_FAMILIES_FILENAME = "legacy_families.json"
ESTIMATION_SHEET_FILENAME = "estimation_sheet.csv"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "legacy_inventory"
_PERL_LANGUAGE: Literal["perl"] = "perl"
_LOOP_NODE_TYPES = frozenset(
    {"cstyle_for_statement", "foreach_statement", "while_statement", "until_statement"}
)
_BRANCHING_NODE_TYPES = frozenset(
    {"conditional_statement", "unless_statement", "given_statement", "when_statement"}
)
_ARITHMETIC_OPERATORS = frozenset({"+", "-", "*", "/", "%", "**"})
_INCREMENT_NODE_TYPES = frozenset(
    {
        "postinc_expression",
        "postdec_expression",
        "preinc_expression",
        "predec_expression",
    }
)
_EMISSION_FUNCTION_NAMES = frozenset({"add_tag", "push"})
_CSV_FIELDNAMES = (
    "check_id",
    "source_file",
    "line_start",
    "line_end",
    "cluster_id",
    "target_impl",
    "size",
    "risk",
    "estimated_hours",
    "rationale",
)


@dataclass(frozen=True, slots=True)
class _EstimationSheetRow:
    check_id: str
    source_file: str
    line_start: str
    line_end: str
    cluster_id: str
    target_impl: str
    size: str
    risk: str
    estimated_hours: str
    rationale: str

    def as_csv_row(self) -> tuple[str, ...]:
        """Return the flat CSV row in fieldname order."""
        return (
            self.check_id,
            self.source_file,
            self.line_start,
            self.line_end,
            self.cluster_id,
            self.target_impl,
            self.size,
            self.risk,
            self.estimated_hours,
            self.rationale,
        )


@dataclass(frozen=True, slots=True)
class _AnalyzedSubroutineFeatures:
    has_loop: bool
    has_branching: bool
    has_arithmetic: bool
    helper_calls: tuple[str, ...]
    statement_count: int


def main() -> int:
    """Export legacy inventory artifacts for migration planning."""
    parser = argparse.ArgumentParser(
        description=(
            "Export legacy data-quality family inventory artifacts and a planning CSV."
        )
    )
    parser.add_argument(
        "--legacy-source-root",
        type=Path,
        default=None,
        help=(
            "Path to the openfoodfacts-server checkout or extracted legacy source tree. "
            "If omitted, the tool tries LEGACY_SOURCE_ROOT, ../openfoodfacts-server, "
            "then /opt/product-opener."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where legacy_families.json and estimation_sheet.csv are written.",
    )
    args = parser.parse_args()

    legacy_source_root = args.legacy_source_root or resolve_legacy_source_root()
    if legacy_source_root is None:
        parser.error(
            "Could not resolve the legacy source tree. Pass --legacy-source-root or set "
            "LEGACY_SOURCE_ROOT."
        )

    artifact_path, csv_path = export_legacy_inventory(
        legacy_source_root=legacy_source_root,
        output_dir=args.output_dir,
    )
    print(f"[OK] legacy source root: {legacy_source_root}", flush=True)
    print(f"[OK] wrote {artifact_path}", flush=True)
    print(f"[OK] wrote {csv_path}", flush=True)
    return 0


def export_legacy_inventory(
    *,
    legacy_source_root: Path,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Write the legacy family artifact and estimation sheet for one source tree."""
    module_paths = resolve_legacy_module_paths(legacy_source_root)
    subroutine_records = collect_legacy_subroutine_records(module_paths)
    if not subroutine_records:
        raise RuntimeError(
            f"No legacy data-quality emissions were found under {legacy_source_root}."
        )

    artifact = build_legacy_families_artifact(
        legacy_source_root=legacy_source_root,
        module_paths=module_paths,
        subroutine_records=subroutine_records,
    )
    artifact_path = write_legacy_families_artifact(artifact, output_dir)
    csv_path = write_estimation_sheet(
        build_estimation_sheet_rows(artifact),
        output_dir,
    )
    return artifact_path, csv_path


def build_legacy_families_artifact(
    *,
    legacy_source_root: Path,
    module_paths: tuple[Path, ...] | None = None,
    subroutine_records: tuple[LegacySubroutineRecord, ...] | None = None,
) -> dict[str, Any]:
    """Build the canonical legacy-family inventory artifact."""
    resolved_module_paths = module_paths or resolve_legacy_module_paths(
        legacy_source_root
    )
    records = subroutine_records or collect_legacy_subroutine_records(
        resolved_module_paths
    )
    grouped_families = _group_family_records(
        records,
        legacy_source_root=legacy_source_root,
    )

    return {
        "version": 2,
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_root": str(legacy_source_root),
        "module_paths": [
            _relative_path_from_root(path, root=legacy_source_root)
            for path in resolved_module_paths
        ],
        "source_fingerprint": _source_fingerprint(
            legacy_source_root=legacy_source_root,
            module_paths=resolved_module_paths,
        ),
        "families": grouped_families,
    }


def write_legacy_families_artifact(
    artifact: dict[str, Any],
    output_dir: Path,
) -> Path:
    """Write the canonical legacy family artifact."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / LEGACY_FAMILIES_FILENAME
    output_path.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def build_estimation_sheet_rows(
    artifact: dict[str, Any],
) -> list[_EstimationSheetRow]:
    """Project the machine artifact into a flat planning-oriented CSV scaffold."""
    rows: list[_EstimationSheetRow] = []
    for family in artifact.get("families", []):
        sources = family.get("sources", [])
        rows.append(
            _EstimationSheetRow(
                check_id=str(family["check_id"]),
                source_file="; ".join(str(source["source_file"]) for source in sources),
                line_start="; ".join(str(source["line_start"]) for source in sources),
                line_end="; ".join(str(source["line_end"]) for source in sources),
                cluster_id=_cluster_id_from_sources(sources),
                target_impl="",
                size="",
                risk="",
                estimated_hours="",
                rationale="",
            )
        )
    return rows


def write_estimation_sheet(
    rows: list[_EstimationSheetRow],
    output_dir: Path,
) -> Path:
    """Write the flat estimation CSV scaffold."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / ESTIMATION_SHEET_FILENAME
    with output_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(_CSV_FIELDNAMES)
        for row in rows:
            writer.writerow(row.as_csv_row())
    return output_path


def _cluster_id_from_sources(sources: list[dict[str, Any]]) -> str:
    """Return one deterministic cluster identifier derived from source spans."""
    return "; ".join(
        f"{source['source_file']}:{source['line_start']}-{source['line_end']}"
        for source in sources
    )


def _group_family_records(
    subroutine_records: tuple[LegacySubroutineRecord, ...],
    *,
    legacy_source_root: Path,
) -> list[dict[str, Any]]:
    families_by_key: dict[str, _MutableFamilyState] = {}

    for record in subroutine_records:
        features = _analyze_subroutine_code(record.code)
        relative_path = _relative_path_from_root(record.path, root=legacy_source_root)
        for code_template in record.emitted_templates:
            template_key = legacy_code_template_key(code_template)
            matching_templates = tuple(
                sorted(
                    candidate
                    for candidate in record.emitted_templates
                    if legacy_code_template_key(candidate) == template_key
                )
            )
            state = families_by_key.setdefault(template_key, _MutableFamilyState())
            state.code_templates.update(matching_templates)
            state.helper_calls.update(features.helper_calls)
            state.source_files.add(relative_path)
            state.sources_by_identity.setdefault(
                (
                    relative_path,
                    record.subroutine_name,
                    record.start_line,
                    record.end_line,
                ),
                {
                    "source_file": relative_path,
                    "source_subroutine": record.subroutine_name,
                    "line_start": record.start_line,
                    "line_end": record.end_line,
                    "unsupported_data_quality_emission_count": (
                        record.unsupported_data_quality_emission_count
                    ),
                    "code_templates": list(matching_templates),
                    "code": record.code,
                },
            )
            state.has_loop = state.has_loop or features.has_loop
            state.has_branching = state.has_branching or features.has_branching
            state.has_arithmetic = state.has_arithmetic or features.has_arithmetic
            state.unsupported_data_quality_emission_count_total += (
                record.unsupported_data_quality_emission_count
            )
            state.statement_count_max = max(
                state.statement_count_max,
                features.statement_count,
            )
            state.line_span_max = max(state.line_span_max, record.line_span)

    families: list[dict[str, Any]] = []
    for template_key, state in sorted(families_by_key.items()):
        code_templates = sorted(state.code_templates)
        representative_template = code_templates[0]
        placeholder_names = legacy_code_template_placeholders(representative_template)
        families.append(
            {
                "check_id": representative_template,
                "template_key": template_key,
                "code_templates": code_templates,
                "placeholder_names": list(placeholder_names),
                "placeholder_count": len(placeholder_names),
                "features": {
                    "has_loop": state.has_loop,
                    "has_branching": state.has_branching,
                    "has_arithmetic": state.has_arithmetic,
                    "helper_calls": sorted(state.helper_calls),
                    "source_files_count": len(state.source_files),
                    "source_subroutines_count": len(state.sources_by_identity),
                    "unsupported_data_quality_emission_count_total": (
                        state.unsupported_data_quality_emission_count_total
                    ),
                    "line_span_max": state.line_span_max,
                    "statement_count_max": state.statement_count_max,
                },
                "sources": [
                    state.sources_by_identity[source_identity]
                    for source_identity in sorted(state.sources_by_identity)
                ],
            }
        )
    return families


def _source_fingerprint(
    *,
    legacy_source_root: Path,
    module_paths: tuple[Path, ...],
) -> str:
    """Return a deterministic fingerprint for the legacy module inputs."""
    digest = hashlib.sha256()
    for path in module_paths:
        digest.update(
            _relative_path_from_root(path, root=legacy_source_root).encode("utf-8")
        )
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


def _relative_path_from_root(path: Path, *, root: Path) -> str:
    """Return one path relative to the analyzed legacy root."""
    return str(path.relative_to(root)).replace("\\", "/")


@cache
def _get_perl_parser() -> Any:
    """Return a cached Tree-sitter parser configured for Perl."""
    try:
        from tree_sitter_language_pack import get_parser
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Legacy inventory export requires tree-sitter-language-pack. "
            'Install the app dependencies with `pip install -e ".[app,dev]"`.'
        ) from exc

    try:
        return get_parser(_PERL_LANGUAGE)
    except Exception as exc:  # pragma: no cover - native package failure surface
        raise RuntimeError(
            "Could not create the Perl Tree-sitter parser required for legacy "
            "inventory export."
        ) from exc


def _analyze_subroutine_code(code: str) -> _AnalyzedSubroutineFeatures:
    """Return planning-oriented structural signals for one subroutine snippet."""
    source = code.encode("utf-8")
    root_node = _get_perl_parser().parse(source).root_node
    return _AnalyzedSubroutineFeatures(
        has_loop=any(
            getattr(node, "type", None) in _LOOP_NODE_TYPES
            for node in _iter_nodes(root_node)
        ),
        has_branching=any(
            getattr(node, "type", None) in _BRANCHING_NODE_TYPES
            for node in _iter_nodes(root_node)
        ),
        has_arithmetic=_subtree_has_arithmetic(root_node, source),
        helper_calls=_subtree_helper_calls(root_node, source),
        statement_count=sum(
            1
            for node in _iter_nodes(root_node)
            if getattr(node, "type", None) == "expression_statement"
        ),
    )


def _subtree_has_arithmetic(node: Any, source: bytes) -> bool:
    """Return whether one subtree contains arithmetic operators or increments."""
    for descendant in _iter_nodes(node):
        node_type = getattr(descendant, "type", None)
        if node_type in _INCREMENT_NODE_TYPES:
            return True
        if node_type != "binary_expression":
            continue
        operator = _binary_operator(descendant, source)
        if operator in _ARITHMETIC_OPERATORS:
            return True
    return False


def _subtree_helper_calls(node: Any, source: bytes) -> tuple[str, ...]:
    """Return non-emission helper calls used within one subtree."""
    helper_calls = {
        function_name
        for descendant in _iter_nodes(node)
        if getattr(descendant, "type", None)
        in {"function_call_expression", "ambiguous_function_call_expression"}
        for function_name in [_function_name(descendant, source)]
        if function_name is not None and function_name not in _EMISSION_FUNCTION_NAMES
    }
    return tuple(sorted(helper_calls))


def _iter_nodes(root_node: Any) -> tuple[Any, ...]:
    """Return a depth-first traversal over one Tree-sitter node subtree."""
    nodes: list[Any] = []
    stack = [root_node]
    while stack:
        node = stack.pop()
        nodes.append(node)
        stack.extend(reversed(tuple(getattr(node, "children", ()))))
    return tuple(nodes)


def _function_name(expression: Any, source: bytes) -> str | None:
    """Return the invoked function name for one call expression."""
    for child in getattr(expression, "children", ()):
        if getattr(child, "type", None) == "function":
            return source[child.start_byte : child.end_byte].decode("utf-8")
    return None


def _binary_operator(node: Any, source: bytes) -> str | None:
    """Return the infix operator carried by one binary expression node."""
    named_children: tuple[Any, ...] = tuple(getattr(node, "named_children", ()))
    if len(named_children) != 2:
        return None
    left, right = named_children
    return source[left.end_byte : right.start_byte].decode("utf-8").strip()


class _MutableFamilyState:
    def __init__(self) -> None:
        self.code_templates: set[str] = set()
        self.helper_calls: set[str] = set()
        self.source_files: set[str] = set()
        self.sources_by_identity: dict[
            tuple[str, str, int, int],
            dict[str, object],
        ] = {}
        self.has_loop = False
        self.has_branching = False
        self.has_arithmetic = False
        self.unsupported_data_quality_emission_count_total = 0
        self.statement_count_max = 0
        self.line_span_max = 0


if __name__ == "__main__":
    raise SystemExit(main())
