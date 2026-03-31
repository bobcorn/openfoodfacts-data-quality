from __future__ import annotations

import inspect
import json
import re
import textwrap
from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pygments.lexer import Lexer

from app.report.legacy_source import (
    LegacySourceIndex,
    resolve_legacy_module_paths,
    resolve_legacy_source_root,
)
from openfoodfacts_data_quality.checks.catalog import (
    CheckCatalog,
    get_default_check_catalog,
)
from openfoodfacts_data_quality.checks.sources import default_dsl_check_pack_resources

if TYPE_CHECKING:
    from collections.abc import Collection
    from importlib.resources.abc import Traversable

    from openfoodfacts_data_quality.contracts.checks import CheckDefinitionLanguage

SnippetOrigin = Literal["legacy", "migrated"]
HighlightFunction = Callable[[str, Lexer, object], str]
LexerFactory = Callable[[], Lexer]
FormatterFactory = Callable[..., object]

LEGACY_SNIPPET_TITLE = "Legacy Snippet"
MIGRATED_SNIPPET_TITLE = "Migrated Snippet"
SNIPPETS_ARTIFACT_FILENAME = "snippets.json"


@dataclass(frozen=True)
class CodeSnippet:
    """Structured machine-readable snippet used by report and review tooling."""

    check_id: str
    origin: SnippetOrigin
    definition_language: CheckDefinitionLanguage | None
    path: str
    start_line: int
    end_line: int
    code: str

    def as_payload(self) -> dict[str, Any]:
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
    """One syntax-highlighted code snippet shown in the report."""

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


def build_snippet_artifact(
    check_ids: Collection[str],
    *,
    catalog: CheckCatalog | None = None,
    legacy_source_root: Path | None = None,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Collect structured snippets keyed by canonical check id."""
    snippets_by_check = collect_code_snippets(
        check_ids,
        catalog=catalog,
        legacy_source_root=legacy_source_root,
    )
    return {
        "checks": {
            check_id: [snippet.as_payload() for snippet in snippets]
            for check_id, snippets in sorted(snippets_by_check.items())
        }
    }


def write_snippet_artifact(
    snippet_artifact: dict[str, dict[str, list[dict[str, Any]]]],
    output_dir: Path,
) -> Path:
    """Write the canonical machine-readable snippet artifact to disk."""
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
) -> dict[str, list[CodeSnippet]]:
    """Collect structured legacy and migrated snippets per check."""
    selected_catalog = catalog or get_default_check_catalog()
    selected_check_ids = set(check_ids)
    migrated_snippets = _collect_migrated_snippets(selected_check_ids, selected_catalog)
    legacy_snippets = _collect_legacy_snippets(
        selected_check_ids,
        catalog=selected_catalog,
        legacy_source_root=legacy_source_root,
    )

    snippets_by_check: dict[str, list[CodeSnippet]] = {}
    for check_id in sorted(set(migrated_snippets) | set(legacy_snippets)):
        snippets: list[CodeSnippet] = []
        snippets.extend(migrated_snippets.get(check_id, ()))
        snippets.extend(legacy_snippets.get(check_id, ()))
        if snippets:
            snippets_by_check[check_id] = snippets
    return snippets_by_check


def build_code_snippet_panels(
    snippet_artifact: dict[str, dict[str, list[dict[str, Any]]]],
) -> dict[str, list[dict[str, str]]]:
    """Build report-ready HTML panels from the structured snippet artifact."""
    panels_by_check: dict[str, list[dict[str, str]]] = {}
    for check_id, snippets in snippet_artifact.get("checks", {}).items():
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


def _collect_migrated_snippets(
    check_ids: set[str],
    catalog: CheckCatalog,
) -> dict[str, tuple[CodeSnippet, ...]]:
    """Collect migrated code snippets for Python and DSL checks."""
    snippets = _collect_python_snippets(check_ids, catalog)
    _merge_snippets(
        snippets,
        _collect_dsl_snippets_from_resources(
            default_dsl_check_pack_resources(),
            check_ids,
        ),
    )
    return snippets


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
                origin="migrated",
                definition_language="python",
                path=_relative_path_within_repository(Path(source_file)),
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
    resolved_path = _resolve_traversable_path(path)
    return {
        check_id: (
            CodeSnippet(
                check_id=check_id,
                origin="migrated",
                definition_language="dsl",
                path=_relative_path_within_repository(resolved_path),
                start_line=span.start_line,
                end_line=span.end_line,
                code=span.text,
            ),
        )
        for check_id, span in _extract_dsl_blocks(path).items()
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
) -> dict[str, tuple[CodeSnippet, ...]]:
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
        return {}

    resolved_root = legacy_source_root or resolve_legacy_source_root()
    if resolved_root is None:
        raise RuntimeError(
            "Could not resolve the legacy source tree required for legacy snippets. "
            "Set LEGACY_SOURCE_ROOT or pass legacy_source_root explicitly."
        )

    try:
        module_paths = resolve_legacy_module_paths(resolved_root)
    except RuntimeError as exc:
        raise RuntimeError(
            "Legacy snippet extraction requires a legacy source tree containing "
            "Perl data-quality modules."
        ) from exc

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

    if unresolved_check_ids:
        raise RuntimeError(
            "Could not locate legacy snippets for these legacy-backed checks: "
            + ", ".join(unresolved_check_ids)
        )
    return snippets


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


def _extract_dsl_blocks(path: Traversable) -> dict[str, _TextSpan]:
    """Extract raw YAML check blocks so inline comments are preserved."""
    lines = path.read_text(encoding="utf-8").splitlines()
    blocks: dict[str, _TextSpan] = {}
    current_id: str | None = None
    current_lines: list[str] = []
    current_start_line = 0
    id_pattern = re.compile(r"^\s*-\s+id:\s+([^\s#]+)\s*$")

    for line_number, line in enumerate(lines, start=1):
        match = id_pattern.match(line)
        if match:
            if current_id is not None:
                blocks[current_id] = _TextSpan(
                    text=textwrap.dedent("\n".join(current_lines)).strip(),
                    start_line=current_start_line,
                    end_line=line_number - 1,
                )
            current_id = match.group(1)
            current_lines = [line]
            current_start_line = line_number
            continue
        if current_id is not None:
            current_lines.append(line)

    if current_id is not None:
        blocks[current_id] = _TextSpan(
            text=textwrap.dedent("\n".join(current_lines)).strip(),
            start_line=current_start_line,
            end_line=len(lines),
        )

    return blocks


def _resolve_traversable_path(path: Traversable) -> Path:
    """Resolve a traversable resource to a concrete file-system path."""
    resource_path = getattr(path, "name", None)
    if not resource_path:
        raise RuntimeError("Could not resolve DSL definitions resource path.")
    return Path(str(path)).resolve()


def _relative_path_within_repository(path: Path) -> str:
    """Return a repository-relative path for one local source file."""
    return _relative_path_from_root(path, root=_repository_root())


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


def _repository_root() -> Path:
    """Return the repository root used for repository-relative paths."""
    return Path(__file__).resolve().parents[2]


def _panel_title_for(origin: str) -> str:
    """Return the report-facing panel title for one snippet origin."""
    if origin == "legacy":
        return LEGACY_SNIPPET_TITLE
    if origin == "migrated":
        return MIGRATED_SNIPPET_TITLE
    raise ValueError(f"Unsupported snippet origin {origin!r}.")


def _lexer_for_snippet(snippet: dict[str, Any]) -> Lexer:
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
        "Unsupported migrated snippet definition language "
        f"{definition_language!r} for origin {origin!r}."
    )


def _highlight_snippet(text: str, lexer: Lexer) -> str:
    """Return syntax-highlighted HTML for one source excerpt."""
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
