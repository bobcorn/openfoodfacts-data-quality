from __future__ import annotations

import os
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal

from openfoodfacts_data_quality.checks.legacy import legacy_code_template_key
from openfoodfacts_data_quality.contracts.checks import LegacyCheckIdentity

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping

_PERL_LANGUAGE: Literal["perl"] = "perl"
_LEGACY_MODULE_GLOB = "DataQuality*.pm"


@dataclass(frozen=True, slots=True)
class LegacySourceMatch:
    """One legacy subroutine that emits a normalized data-quality tag template."""

    path: Path
    subroutine_name: str
    start_line: int
    end_line: int
    code_template: str
    code: str


@dataclass(frozen=True, slots=True)
class LegacySubroutineRecord:
    """Structured legacy subroutine metadata extracted from one Perl module."""

    path: Path
    subroutine_name: str
    start_line: int
    end_line: int
    code: str
    emitted_templates: tuple[str, ...]

    @property
    def line_span(self) -> int:
        """Return the inclusive line span covered by the subroutine."""
        return self.end_line - self.start_line + 1


@dataclass(frozen=True, slots=True)
class LegacySourceIndex:
    """Index legacy-source subroutines by the templates they emit."""

    matches_by_template_key: Mapping[str, tuple[LegacySourceMatch, ...]]

    @classmethod
    def build(cls, module_paths: Iterable[Path]) -> LegacySourceIndex:
        """Build a reusable source index from the Perl legacy source tree."""
        matches_by_template_key: dict[
            str, dict[tuple[str, int, int, str], LegacySourceMatch]
        ] = {}

        for record in collect_legacy_subroutine_records(module_paths):
            for code_template in record.emitted_templates:
                concrete_match = LegacySourceMatch(
                    path=record.path,
                    subroutine_name=record.subroutine_name,
                    start_line=record.start_line,
                    end_line=record.end_line,
                    code_template=code_template,
                    code=record.code,
                )
                matches_by_template_key.setdefault(
                    legacy_code_template_key(code_template),
                    {},
                )[
                    (
                        str(record.path),
                        concrete_match.start_line,
                        concrete_match.end_line,
                        code_template,
                    )
                ] = concrete_match

        return cls(
            matches_by_template_key=MappingProxyType(
                {
                    template_key: tuple(
                        match
                        for _, match in sorted(
                            matches.items(),
                            key=lambda item: item[0],
                        )
                    )
                    for template_key, matches in matches_by_template_key.items()
                }
            )
        )

    def matches_for_identity(
        self,
        legacy_identity: LegacyCheckIdentity,
    ) -> tuple[LegacySourceMatch, ...]:
        """Return all indexed legacy matches for one check identity."""
        return self.matches_by_template_key.get(
            legacy_code_template_key(legacy_identity.code_template),
            (),
        )


def resolve_legacy_module_paths(legacy_source_root: Path) -> tuple[Path, ...]:
    """Return the sorted Perl data-quality modules found under one legacy root."""
    product_opener_dir = legacy_source_root / "lib" / "ProductOpener"
    module_paths = tuple(
        sorted(
            candidate
            for candidate in product_opener_dir.glob(_LEGACY_MODULE_GLOB)
            if candidate.is_file()
        )
    )
    if module_paths:
        return module_paths
    raise RuntimeError(
        "Legacy source analysis requires Perl modules matching "
        f"{product_opener_dir / _LEGACY_MODULE_GLOB}."
    )


def resolve_legacy_source_root() -> Path | None:
    """Resolve the legacy server source tree for Tree-sitter-based analysis."""
    configured_root = os.getenv("LEGACY_SOURCE_ROOT")
    repo_root = Path(__file__).resolve().parents[2]
    candidates = [
        Path(configured_root) if configured_root else None,
        repo_root.parent / "openfoodfacts-server",
        Path("/opt/product-opener"),
    ]
    for candidate in candidates:
        if candidate is None:
            continue
        try:
            resolve_legacy_module_paths(candidate)
        except RuntimeError:
            continue
        return candidate
    return None


def collect_legacy_subroutine_records(
    module_paths: Iterable[Path],
) -> tuple[LegacySubroutineRecord, ...]:
    """Return structured records for legacy subroutines that emit quality tags."""
    parser = _get_perl_parser()
    records: list[LegacySubroutineRecord] = []

    for path in sorted(module_paths):
        source = path.read_bytes()
        tree = parser.parse(source)
        for subroutine in _iter_subroutine_nodes(tree.root_node):
            subroutine_name = _subroutine_name(subroutine, source)
            block = _subroutine_block(subroutine)
            if subroutine_name is None or block is None:
                continue

            emitted_templates = tuple(
                sorted(
                    {
                        code_template
                        for expression_statement in _iter_expression_statements(block)
                        for code_template in _code_templates_for_expression(
                            expression_statement,
                            source,
                        )
                    }
                )
            )
            if not emitted_templates:
                continue

            records.append(
                LegacySubroutineRecord(
                    path=path,
                    subroutine_name=subroutine_name,
                    start_line=subroutine.start_point.row + 1,
                    end_line=subroutine.end_point.row + 1,
                    code=source[subroutine.start_byte : subroutine.end_byte]
                    .decode("utf-8")
                    .strip(),
                    emitted_templates=emitted_templates,
                )
            )

    return tuple(records)


@cache
def _get_perl_parser() -> Any:
    """Return a cached Tree-sitter parser configured for Perl."""
    try:
        from tree_sitter_language_pack import get_parser
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Legacy snippet extraction requires tree-sitter-language-pack. "
            'Install the app dependencies with `pip install -e ".[app,dev]"`.'
        ) from exc

    try:
        return get_parser(_PERL_LANGUAGE)
    except Exception as exc:  # pragma: no cover - native package failure surface
        raise RuntimeError(
            "Could not create the Perl Tree-sitter parser required for legacy "
            "snippet extraction."
        ) from exc


def _iter_subroutine_nodes(root_node: Any) -> Iterator[Any]:
    """Yield every top-level or nested Perl subroutine declaration node."""
    yield from (
        node
        for node in _iter_nodes(root_node)
        if getattr(node, "type", None) == "subroutine_declaration_statement"
    )


def _iter_expression_statements(node: Any) -> Iterator[Any]:
    """Yield expression statements contained within one Perl subtree."""
    yield from (
        descendant
        for descendant in _iter_nodes(node)
        if getattr(descendant, "type", None) == "expression_statement"
    )


def _iter_nodes(root_node: Any) -> Iterator[Any]:
    """Depth-first traversal over a Tree-sitter node subtree."""
    stack = [root_node]
    while stack:
        node = stack.pop()
        yield node
        stack.extend(reversed(_children(node)))


def _subroutine_name(subroutine_node: Any, source: bytes) -> str | None:
    """Return the declared subroutine name."""
    for child in _children(subroutine_node):
        if getattr(child, "type", None) == "bareword":
            return _node_text(child, source)
    return None


def _subroutine_block(subroutine_node: Any) -> Any | None:
    """Return the body block of one Perl subroutine declaration."""
    for child in _children(subroutine_node):
        if getattr(child, "type", None) == "block":
            return child
    return None


def _code_templates_for_expression(
    expression_statement: Any,
    source: bytes,
) -> tuple[str, ...]:
    """Return normalized data-quality templates emitted by one expression."""
    named_children = _named_children(expression_statement)
    if not named_children:
        return ()

    expression = named_children[0]
    function_name = _function_name(expression, source)
    if expression.type == "function_call_expression" and function_name == "add_tag":
        arguments = _function_arguments(expression)
        if len(arguments) < 3 or not _is_data_quality_bucket(arguments[1], source):
            return ()
        code_template = _template_from_expression(arguments[2], source)
        if code_template is None or not code_template.startswith("en:"):
            return ()
        return (code_template,)

    if (
        expression.type == "ambiguous_function_call_expression"
        and function_name == "push"
    ):
        arguments = _function_arguments(expression)
        if len(arguments) < 2 or not _is_data_quality_push_target(arguments[0], source):
            return ()
        code_template = _template_from_expression(arguments[1], source)
        if code_template is None or not code_template.startswith("en:"):
            return ()
        return (code_template,)

    return ()


def _function_name(expression: Any, source: bytes) -> str | None:
    """Return the invoked function name for one call-like expression."""
    for child in _children(expression):
        if getattr(child, "type", None) == "function":
            return _node_text(child, source)
    return None


def _function_arguments(expression: Any) -> tuple[Any, ...]:
    """Return the argument expressions for one Perl function call."""
    for child in _children(expression):
        if getattr(child, "type", None) == "list_expression":
            return _named_children(child)
    named_children = _named_children(expression)
    if named_children and getattr(named_children[0], "type", None) == "function":
        return named_children[1:]
    return ()


def _is_data_quality_bucket(node: Any, source: bytes) -> bool:
    """Return whether one add_tag bucket argument targets data-quality tags."""
    bucket = _template_from_expression(node, source)
    if bucket is not None:
        return bucket.startswith("data_quality_")
    return _scalar_name(node, source) == "data_quality_tags"


def _is_data_quality_push_target(node: Any, source: bytes) -> bool:
    """Return whether one push target points to a data-quality tag array."""
    if getattr(node, "type", None) != "array":
        return False

    hash_expression = _array_hash_element_expression(node)
    if hash_expression is None:
        return False

    named_children = _named_children(hash_expression)
    if len(named_children) != 2:
        return False

    base, key = named_children
    if (
        getattr(base, "type", None) != "scalar"
        or _scalar_name(base, source) != "product_ref"
    ):
        return False

    if getattr(key, "type", None) == "autoquoted_bareword":
        key_name = _node_text(key, source)
        return key_name.startswith("data_quality_") and key_name.endswith("_tags")

    if getattr(key, "type", None) == "scalar":
        return _scalar_name(key, source) == "data_quality_tags"

    return False


def _array_hash_element_expression(node: Any) -> Any | None:
    """Return the underlying hash element expression of one Perl array target."""
    for child in _children(node):
        if getattr(child, "type", None) != "varname":
            continue
        for grandchild in _children(child):
            if getattr(grandchild, "type", None) != "block":
                continue
            for block_child in _children(grandchild):
                if getattr(block_child, "type", None) != "expression_statement":
                    continue
                named_children = _named_children(block_child)
                if not named_children:
                    continue
                candidate = named_children[0]
                if getattr(candidate, "type", None) == "hash_element_expression":
                    return candidate
    return None


def _template_from_expression(node: Any, source: bytes) -> str | None:
    """Normalize one supported Perl expression into the shared template form."""
    node_type = getattr(node, "type", None)
    if node_type == "binary_expression":
        named_children = _named_children(node)
        if len(named_children) != 2:
            return None
        left, right = named_children
        operator = source[left.end_byte : right.start_byte].decode("utf-8").strip()
        if operator != ".":
            return None
        left_template = _template_from_expression(left, source)
        right_template = _template_from_expression(right, source)
        if left_template is None or right_template is None:
            return None
        return left_template + right_template

    if node_type == "string_literal":
        return _string_literal_text(node, source)

    if node_type == "interpolated_string_literal":
        return _interpolated_string_text(node, source)

    if node_type == "scalar":
        scalar_name = _scalar_name(node, source)
        if scalar_name is None:
            return None
        return "${" + scalar_name + "}"

    if node_type == "hash_element_expression":
        return _hash_element_placeholder(node, source)

    if node_type == "array_element_expression":
        return _array_element_placeholder(node, source)

    if node_type in {
        "ambiguous_function_call_expression",
        "function_call_expression",
    }:
        return _function_call_template(node, source)

    if node_type in {"expression_statement", "parenthesized_expression"}:
        named_children = _named_children(node)
        if len(named_children) == 1:
            return _template_from_expression(named_children[0], source)

    return None


def _string_literal_text(node: Any, source: bytes) -> str:
    """Return the raw contents of one single-quoted Perl string literal."""
    return source[node.start_byte + 1 : node.end_byte - 1].decode("utf-8")


def _interpolated_string_text(node: Any, source: bytes) -> str:
    """Return the normalized template for one interpolated Perl string."""
    content_node = next(
        (
            child
            for child in _children(node)
            if getattr(child, "type", None) == "string_content"
        ),
        None,
    )
    if content_node is None:
        return _string_literal_text(node, source)

    parts: list[str] = []
    cursor = content_node.start_byte
    for child in _children(content_node):
        child_type = getattr(child, "type", None)
        if cursor < child.start_byte:
            parts.append(source[cursor : child.start_byte].decode("utf-8"))
        if child_type == "not-interpolated":
            parts.append(_node_text(child, source))
            cursor = child.end_byte
            continue

        placeholder = _template_from_expression(child, source)
        if placeholder is None:
            return source[node.start_byte + 1 : node.end_byte - 1].decode("utf-8")
        parts.append(placeholder)
        cursor = child.end_byte
    if cursor < content_node.end_byte:
        parts.append(source[cursor : content_node.end_byte].decode("utf-8"))
    return "".join(parts)


def _hash_element_placeholder(node: Any, source: bytes) -> str | None:
    """Return one placeholder for a supported Perl hash access expression."""
    placeholder_name = _hash_element_placeholder_name(node, source)
    if placeholder_name is None:
        return None
    return "${" + placeholder_name + "}"


def _array_element_placeholder(node: Any, source: bytes) -> str | None:
    """Return one placeholder for a supported Perl array indexing expression."""
    named_children = _named_children(node)
    if len(named_children) != 2:
        return None

    container, index = named_children
    container_name = _array_container_name(container, source)
    if container_name is None:
        return None

    index_text = _node_text(index, source)
    if not index_text:
        return None
    placeholder_name = _placeholder_name_from_segments((container_name, index_text))
    return "${" + placeholder_name + "}"


def _function_call_template(node: Any, source: bytes) -> str | None:
    """Return one placeholder for a supported Perl function-call expression."""
    if _function_name(node, source) != "substr":
        return None

    arguments = _function_arguments(node)
    if len(arguments) != 2:
        return None

    value_expression, offset_expression = arguments
    if _node_text(offset_expression, source).strip() != "3":
        return None

    value_name = _scalar_name(value_expression, source)
    if value_name is None:
        return None
    if value_name.endswith("_no_lc"):
        return "${" + value_name + "}"
    return "${" + value_name + "_no_lc}"


def _hash_key_name(node: Any, source: bytes) -> str | None:
    """Return one hash key as a normalized placeholder name."""
    node_type = getattr(node, "type", None)
    if node_type == "autoquoted_bareword":
        return _node_text(node, source)
    if node_type == "scalar":
        return _scalar_name(node, source)
    if node_type == "number":
        return _node_text(node, source)
    return None


def _hash_element_placeholder_name(node: Any, source: bytes) -> str | None:
    """Return one stable placeholder name for a Perl hash-access expression."""
    segments = _hash_element_segments(node, source)
    if segments is None:
        return None
    if segments and segments[0] == "product_ref":
        segments = segments[1:]
    if not segments:
        return None
    return _placeholder_name_from_segments(segments)


def _hash_element_segments(node: Any, source: bytes) -> tuple[str, ...] | None:
    """Return flattened identifier-like segments for one hash-access expression."""
    named_children = _named_children(node)
    if len(named_children) != 2:
        return None

    base, _ = named_children
    key_name = _hash_key_name(named_children[1], source)
    if key_name is None:
        return None

    base_scalar_name = _scalar_name(base, source)
    if base_scalar_name is not None:
        return (base_scalar_name, key_name)
    if getattr(base, "type", None) == "hash_element_expression":
        base_segments = _hash_element_segments(base, source)
        if base_segments is None:
            return None
        return (*base_segments, key_name)
    return None


def _array_container_name(node: Any, source: bytes) -> str | None:
    """Return the plain container name for one Perl array access node."""
    text = _node_text(node, source)
    if text.startswith("$"):
        return text[1:]
    return None


def _placeholder_name_from_segments(segments: tuple[str, ...]) -> str:
    """Return one valid placeholder identifier from structured expression segments."""
    normalized_segments = [
        _sanitize_placeholder_segment(segment) for segment in segments if segment
    ]
    placeholder_name = (
        "_".join(segment for segment in normalized_segments if segment) or "value"
    )
    if placeholder_name[0].isdigit():
        return "_" + placeholder_name
    return placeholder_name


def _sanitize_placeholder_segment(segment: str) -> str:
    """Return one identifier-safe token derived from a Perl expression fragment."""
    normalized = "".join(
        character if character.isalnum() or character == "_" else "_"
        for character in segment
    ).strip("_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized


def _scalar_name(node: Any, source: bytes) -> str | None:
    """Return the plain variable name for one simple Perl scalar node."""
    text = _node_text(node, source)
    if text.startswith("${") and text.endswith("}"):
        return text[2:-1]
    if not text.startswith("$"):
        return None

    named_children = _named_children(node)
    if (
        len(named_children) == 1
        and getattr(named_children[0], "type", None) == "varname"
    ):
        return _node_text(named_children[0], source)
    return None


def _node_text(node: Any, source: bytes) -> str:
    """Return the UTF-8 text covered by one Tree-sitter node."""
    return source[node.start_byte : node.end_byte].decode("utf-8")


def _children(node: Any) -> tuple[Any, ...]:
    """Return one node's child tuple with stable Any typing."""
    return tuple(getattr(node, "children", ()))


def _named_children(node: Any) -> tuple[Any, ...]:
    """Return one node's named-child tuple with stable Any typing."""
    return tuple(getattr(node, "named_children", ()))
