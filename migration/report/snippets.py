from __future__ import annotations

import inspect
import json
import textwrap
from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

import yaml
from pygments.lexer import Lexer
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode

from migration.legacy_source import (
    LegacySourceIndex,
    resolve_legacy_module_paths,
    resolve_legacy_source_root,
)
from off_data_quality.catalog import CheckCatalog, get_default_check_catalog
from off_data_quality.metadata import (
    packaged_dsl_check_pack_resource_path,
    packaged_dsl_check_pack_resources,
    packaged_module_path,
)

if TYPE_CHECKING:
    from collections.abc import Collection
    from importlib.resources.abc import Traversable

    from off_data_quality.contracts.checks import CheckDefinitionLanguage

SnippetOrigin = Literal["implementation", "legacy"]
LegacySnippetStatus = Literal["available", "not_applicable", "unavailable"]
HighlightFunction = Callable[[str, Lexer, object], str]
LexerFactory = Callable[[], Lexer]
FormatterFactory = Callable[..., object]

LEGACY_SNIPPET_TITLE = "Legacy Source"
IMPLEMENTATION_SNIPPET_TITLE = "Current Implementation"
SNIPPETS_ARTIFACT_KIND = "openfoodfacts_data_quality.snippets_artifact"
SNIPPETS_ARTIFACT_SCHEMA_VERSION = 1
SNIPPETS_ARTIFACT_FILENAME = "snippets.json"


type CodeSnippetValue = str | int | None
type CodeSnippetPayload = dict[str, CodeSnippetValue]
type SnippetCheckEntryPayload = dict[
    str,
    LegacySnippetStatus | list[CodeSnippetPayload],
]
type SnippetChecks = dict[str, SnippetCheckEntryPayload]
type SnippetIssuePayload = dict[str, str | list[str]]
type SnippetArtifact = dict[str, str | int | SnippetChecks | list[SnippetIssuePayload]]


@dataclass(frozen=True)
class CodeSnippet:
    """Structured snippet used by report and review tooling."""

    check_id: str
    origin: SnippetOrigin
    definition_language: CheckDefinitionLanguage | None
    path: str
    start_line: int
    end_line: int
    code: str

    def as_payload(self) -> CodeSnippetPayload:
        """Serialize one structured snippet for artifacts."""
        return {
            "check_id": self.check_id,
            "origin": self.origin,
            "definition_language": self.definition_language,
            "path": self.path,
            "start_line": self.start_line,
            "end_line": self.end_line,
            "code": self.code,
        }


@dataclass(frozen=True)
class CodeSnippetPanel:
    """One syntax highlighted code snippet shown in the report."""

    title: str
    html: str

    def as_payload(self) -> dict[str, str]:
        """Serialize the panel for the report payload."""
        return {"title": self.title, "html": self.html}


@dataclass(frozen=True)
class _TextSpan:
    text: str
    start_line: int
    end_line: int


@dataclass(frozen=True)
class SnippetIssue:
    """One warning emitted while collecting optional snippet provenance."""

    severity: Literal["warning"]
    message: str
    check_ids: tuple[str, ...] = ()

    def as_payload(self) -> SnippetIssuePayload:
        """Serialize one issue for the snippet artifact."""
        return {
            "severity": self.severity,
            "message": self.message,
            "check_ids": list(self.check_ids),
        }


@dataclass(frozen=True)
class SnippetCollection:
    """Collected snippets together with any optional provenance warnings."""

    snippets_by_check: dict[str, list[CodeSnippet]]
    legacy_snippet_status_by_check: dict[str, LegacySnippetStatus]
    issues: tuple[SnippetIssue, ...] = ()


def build_snippet_artifact(
    check_ids: Collection[str],
    *,
    catalog: CheckCatalog | None = None,
    legacy_source_root: Path | None = None,
) -> SnippetArtifact:
    """Collect structured snippets keyed by canonical check id."""
    snippet_collection = collect_code_snippets(
        check_ids,
        catalog=catalog,
        legacy_source_root=legacy_source_root,
    )
    return {
        "kind": SNIPPETS_ARTIFACT_KIND,
        "schema_version": SNIPPETS_ARTIFACT_SCHEMA_VERSION,
        "issues": [issue.as_payload() for issue in snippet_collection.issues],
        "checks": {
            check_id: {
                "legacy_snippet_status": (
                    snippet_collection.legacy_snippet_status_by_check[check_id]
                ),
                "snippets": [snippet.as_payload() for snippet in snippets],
            }
            for check_id, snippets in sorted(
                snippet_collection.snippets_by_check.items()
            )
        },
    }


def write_snippet_artifact(
    snippet_artifact: SnippetArtifact,
    output_dir: Path,
) -> Path:
    """Write the canonical snippet artifact to disk."""
    output_path = output_dir / SNIPPETS_ARTIFACT_FILENAME
    output_path.write_text(
        json.dumps(snippet_artifact, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def collect_code_snippets(
    check_ids: Collection[str],
    *,
    catalog: CheckCatalog | None = None,
    legacy_source_root: Path | None = None,
) -> SnippetCollection:
    """Collect structured implementation and legacy snippets for each check."""
    selected_catalog = catalog or get_default_check_catalog()
    selected_check_ids = set(check_ids)
    legacy_snippet_status_by_check: dict[str, LegacySnippetStatus] = {
        check_id: _legacy_snippet_status_for(
            check_id,
            catalog=selected_catalog,
        )
        for check_id in selected_check_ids
        if selected_catalog.checks_by_id.get(check_id) is not None
    }
    implementation_snippets = _collect_implementation_snippets(
        selected_check_ids,
        selected_catalog,
    )
    legacy_snippet_collection = _collect_legacy_snippets(
        selected_check_ids,
        catalog=selected_catalog,
        legacy_source_root=legacy_source_root,
    )

    snippets_by_check: dict[str, list[CodeSnippet]] = {}
    for check_id in sorted(
        set(implementation_snippets) | set(legacy_snippet_collection.snippets_by_check)
    ):
        snippets: list[CodeSnippet] = []
        snippets.extend(implementation_snippets.get(check_id, ()))
        snippets.extend(legacy_snippet_collection.snippets_by_check.get(check_id, ()))
        if legacy_snippet_collection.snippets_by_check.get(check_id):
            legacy_snippet_status_by_check[check_id] = "available"
        if snippets:
            snippets_by_check[check_id] = snippets
    return SnippetCollection(
        snippets_by_check=snippets_by_check,
        legacy_snippet_status_by_check=legacy_snippet_status_by_check,
        issues=legacy_snippet_collection.issues,
    )


def build_code_snippet_panels(
    snippet_artifact: SnippetArtifact,
) -> dict[str, list[dict[str, str]]]:
    """Build report-ready HTML panels from the structured snippet artifact."""
    panels_by_check: dict[str, list[dict[str, str]]] = {}
    checks_by_id = cast(SnippetChecks, snippet_artifact.get("checks", {}))
    for check_id, check_payload in checks_by_id.items():
        snippets = cast(list[CodeSnippetPayload], check_payload.get("snippets", []))
        panels = [
            CodeSnippetPanel(
                title=_panel_title_for(str(snippet["origin"])),
                html=_highlight_snippet(
                    str(snippet["code"]),
                    lexer=_lexer_for_snippet(snippet),
                ),
            ).as_payload()
            for snippet in snippets
        ]
        if panels:
            panels_by_check[str(check_id)] = panels
    return panels_by_check


def legacy_snippet_status_by_check(
    snippet_artifact: SnippetArtifact,
) -> dict[str, LegacySnippetStatus]:
    """Return the legacy snippet status for each check from one snippet artifact."""
    checks_by_id = cast(SnippetChecks, snippet_artifact.get("checks", {}))
    return {
        str(check_id): cast(
            LegacySnippetStatus,
            check_payload.get("legacy_snippet_status", "unavailable"),
        )
        for check_id, check_payload in checks_by_id.items()
    }


def _collect_implementation_snippets(
    check_ids: set[str],
    catalog: CheckCatalog,
) -> dict[str, tuple[CodeSnippet, ...]]:
    """Collect implementation snippets for Python and DSL checks."""
    snippets = _collect_python_snippets(check_ids, catalog)
    _merge_snippets(
        snippets,
        _collect_dsl_snippets_from_resources(
            packaged_dsl_check_pack_resources(),
            check_ids,
        ),
    )
    return snippets


def _legacy_snippet_status_for(
    check_id: str,
    *,
    catalog: CheckCatalog,
) -> LegacySnippetStatus:
    """Return whether legacy snippet provenance applies to one selected check."""
    check_definition = catalog.checks_by_id.get(check_id)
    if check_definition is None or check_definition.legacy_identity is None:
        return "not_applicable"
    return "unavailable"


def _collect_python_snippets(
    check_ids: set[str],
    catalog: CheckCatalog,
) -> dict[str, tuple[CodeSnippet, ...]]:
    """Return source excerpts for Python-defined checks."""
    snippets: dict[str, tuple[CodeSnippet, ...]] = {}
    for check in catalog.checks:
        if check.definition_language != "python" or check.id not in check_ids:
            continue
        evaluator = catalog.evaluators_by_id[check.id]
        source_lines, start_line = inspect.getsourcelines(evaluator)
        source_text = textwrap.dedent("".join(source_lines)).strip()
        source_file = inspect.getsourcefile(evaluator)
        if source_file is None:
            raise RuntimeError(f"Could not resolve source file for check {check.id}.")
        end_line = start_line + len(source_lines) - 1
        snippets[check.id] = (
            CodeSnippet(
                check_id=check.id,
                origin="implementation",
                definition_language="python",
                path=packaged_module_path(evaluator.__module__),
                start_line=start_line,
                end_line=end_line,
                code=source_text,
            ),
        )
    return snippets


def _collect_dsl_snippets(
    path: Traversable,
    check_ids: set[str],
) -> dict[str, tuple[CodeSnippet, ...]]:
    """Return YAML excerpts for DSL definitions keyed by check id."""
    return {
        check_id: (
            CodeSnippet(
                check_id=check_id,
                origin="implementation",
                definition_language="dsl",
                path=packaged_dsl_check_pack_resource_path(path),
                start_line=span.start_line,
                end_line=span.end_line,
                code=span.text,
            ),
        )
        for check_id, span in _extract_dsl_blocks(
            path.read_text(encoding="utf-8")
        ).items()
        if check_id in check_ids
    }


def _collect_dsl_snippets_from_resources(
    resources: tuple[Traversable, ...],
    check_ids: set[str],
) -> dict[str, tuple[CodeSnippet, ...]]:
    """Collect YAML excerpts for all shipped DSL definition files."""
    snippets: dict[str, tuple[CodeSnippet, ...]] = {}
    for resource in resources:
        _merge_snippets(snippets, _collect_dsl_snippets(resource, check_ids))
    return snippets


def _collect_legacy_snippets(
    check_ids: set[str],
    *,
    catalog: CheckCatalog,
    legacy_source_root: Path | None = None,
) -> SnippetCollection:
    """Return Perl excerpts for legacy checks from the legacy source tree."""
    legacy_check_ids = tuple(
        sorted(
            check_id
            for check_id in check_ids
            if (check_definition := catalog.checks_by_id.get(check_id)) is not None
            and check_definition.legacy_identity is not None
        )
    )
    if not legacy_check_ids:
        return SnippetCollection(
            snippets_by_check={},
            legacy_snippet_status_by_check={},
        )

    resolved_root = legacy_source_root or resolve_legacy_source_root()
    if resolved_root is None:
        return SnippetCollection(
            snippets_by_check={},
            legacy_snippet_status_by_check={},
            issues=(
                SnippetIssue(
                    severity="warning",
                    message=(
                        "Legacy snippets unavailable because the legacy source tree "
                        "could not be resolved."
                    ),
                    check_ids=legacy_check_ids,
                ),
            ),
        )

    try:
        module_paths = resolve_legacy_module_paths(resolved_root)
    except RuntimeError:
        return SnippetCollection(
            snippets_by_check={},
            legacy_snippet_status_by_check={},
            issues=(
                SnippetIssue(
                    severity="warning",
                    message=(
                        "Legacy snippets unavailable because the legacy source tree "
                        "does not contain the required Perl data-quality modules."
                    ),
                    check_ids=legacy_check_ids,
                ),
            ),
        )

    legacy_source_index = LegacySourceIndex.build(module_paths)
    snippets: dict[str, tuple[CodeSnippet, ...]] = {}
    unresolved_check_ids: list[str] = []

    for check_id in legacy_check_ids:
        legacy_identity = catalog.check_by_id(check_id).legacy_identity
        if legacy_identity is None:
            continue
        matches = legacy_source_index.matches_for_identity(legacy_identity)
        if not matches:
            unresolved_check_ids.append(check_id)
            continue
        snippets[check_id] = tuple(
            CodeSnippet(
                check_id=check_id,
                origin="legacy",
                definition_language=None,
                path=_relative_path_from_root(match.path, root=resolved_root),
                start_line=match.start_line,
                end_line=match.end_line,
                code=match.code,
            )
            for match in matches
        )

    issues: tuple[SnippetIssue, ...] = ()
    if unresolved_check_ids:
        issues = (
            SnippetIssue(
                severity="warning",
                message=(
                    "Legacy snippets unavailable for some checks with a legacy baseline "
                    "because no matching source span was found."
                ),
                check_ids=tuple(unresolved_check_ids),
            ),
        )
    return SnippetCollection(
        snippets_by_check={
            check_id: list(values) for check_id, values in snippets.items()
        },
        legacy_snippet_status_by_check={},
        issues=issues,
    )


def _merge_snippets(
    destination: dict[str, tuple[CodeSnippet, ...]],
    source: dict[str, tuple[CodeSnippet, ...]],
) -> None:
    """Merge snippet maps while rejecting duplicate check ids."""
    duplicate_ids = sorted(set(destination) & set(source))
    if duplicate_ids:
        raise ValueError(
            f"Duplicate snippet definitions for checks: {', '.join(duplicate_ids)}"
        )
    destination.update(source)


def _extract_dsl_blocks(source_text: str) -> dict[str, _TextSpan]:
    """Extract YAML check blocks using parser spans instead of line scanning."""
    lines = source_text.splitlines()
    document = cast(Node | None, cast(Any, yaml).compose(source_text))
    if document is None:
        return {}
    check_sequence = _check_sequence_node(document)

    blocks: dict[str, _TextSpan] = {}
    for item in check_sequence.value:
        if not isinstance(item, yaml.MappingNode):
            continue
        check_id = _mapping_node_scalar_value(item, "id")
        if check_id is None:
            continue
        start_line = item.start_mark.line + 1
        end_line = item.end_mark.line
        blocks[check_id] = _TextSpan(
            text=textwrap.dedent("\n".join(lines[start_line - 1 : end_line])).strip(),
            start_line=start_line,
            end_line=end_line,
        )

    return blocks


def _check_sequence_node(node: Node) -> SequenceNode:
    """Return the YAML sequence node that contains check definitions."""
    if isinstance(node, SequenceNode):
        return node
    if isinstance(node, MappingNode):
        checks_node = _mapping_node_value_node(node, "checks")
        if isinstance(checks_node, SequenceNode):
            return checks_node
    raise RuntimeError(
        "DSL check definitions must expose a sequence at the root or a "
        "'checks' sequence at the root."
    )


def _mapping_node_scalar_value(
    mapping_node: MappingNode,
    key: str,
) -> str | None:
    """Return the scalar value for one YAML mapping key when present."""
    value_node = _mapping_node_value_node(mapping_node, key)
    if value_node is None:
        return None
    if not isinstance(value_node, ScalarNode):
        return None
    return str(value_node.value)


def _mapping_node_value_node(
    mapping_node: MappingNode,
    key: str,
) -> Node | None:
    """Return the raw YAML value node for one mapping key when present."""
    for key_node, value_node in mapping_node.value:
        if not isinstance(key_node, ScalarNode):
            continue
        if key_node.value != key:
            continue
        return cast(Node, value_node)
    return None


def _relative_path_from_root(path: Path, *, root: Path) -> str:
    """Return a stable relative path for one source file under a known root."""
    resolved_path = path.resolve()
    resolved_root = root.resolve()
    try:
        return str(resolved_path.relative_to(resolved_root))
    except ValueError as exc:
        raise RuntimeError(
            f"Path {resolved_path} is not inside expected root {resolved_root}."
        ) from exc


def _panel_title_for(origin: str) -> str:
    """Return the report-facing panel title for one snippet origin."""
    if origin == "legacy":
        return LEGACY_SNIPPET_TITLE
    if origin == "implementation":
        return IMPLEMENTATION_SNIPPET_TITLE
    raise ValueError(f"Unsupported snippet origin {origin!r}.")


def _lexer_for_snippet(snippet: CodeSnippetPayload) -> Lexer:
    """Return the syntax highlighter lexer for one structured snippet."""
    origin = str(snippet["origin"])
    if origin == "legacy":
        return PERL_LEXER()

    definition_language = snippet.get("definition_language")
    if definition_language == "python":
        return PYTHON_LEXER()
    if definition_language == "dsl":
        return YAML_LEXER()
    raise ValueError(
        "Unsupported implementation snippet definition language "
        f"{definition_language!r} for origin {origin!r}."
    )


def _highlight_snippet(text: str, lexer: Lexer) -> str:
    """Return syntax highlighted HTML for one source excerpt."""
    formatter = PYGMENTS_FORMATTER(nowrap=True, noclasses=True)
    return PYGMENTS_HIGHLIGHT(text, lexer, formatter).strip()


def _load_highlight_function() -> HighlightFunction:
    """Load the Pygments highlighter behind a typed boundary."""
    module = import_module("pygments")
    highlight_function = getattr(module, "highlight", None)
    if not callable(highlight_function):
        raise RuntimeError("Pygments highlight() is unavailable.")

    def render_highlight(text: str, lexer: Lexer, formatter: object) -> str:
        rendered: object = highlight_function(text, lexer, formatter)
        if not isinstance(rendered, str):
            raise RuntimeError("Pygments highlight() did not return HTML text.")
        return rendered

    return render_highlight


def _load_lexer_factory(name: str) -> LexerFactory:
    """Load one Pygments lexer constructor behind a typed boundary."""
    module = import_module("pygments.lexers")
    lexer_factory = getattr(module, name, None)
    if not callable(lexer_factory):
        raise RuntimeError(f"Pygments lexer {name} is unavailable.")

    def build_lexer() -> Lexer:
        lexer: object = lexer_factory()
        if not isinstance(lexer, Lexer):
            raise RuntimeError(f"Pygments lexer {name} did not build a lexer instance.")
        return lexer

    return build_lexer


def _load_formatter_factory(name: str) -> FormatterFactory:
    """Load one Pygments formatter constructor behind a typed boundary."""
    module = import_module("pygments.formatters")
    formatter_factory = getattr(module, name, None)
    if not callable(formatter_factory):
        raise RuntimeError(f"Pygments formatter {name} is unavailable.")

    def build_formatter(*args: object, **kwargs: object) -> object:
        formatter: object = formatter_factory(*args, **kwargs)
        return formatter

    return build_formatter


PYGMENTS_HIGHLIGHT = _load_highlight_function()
PYGMENTS_FORMATTER = _load_formatter_factory("HtmlFormatter")
PERL_LEXER = _load_lexer_factory("PerlLexer")
PYTHON_LEXER = _load_lexer_factory("PythonLexer")
YAML_LEXER = _load_lexer_factory("YamlLexer")
