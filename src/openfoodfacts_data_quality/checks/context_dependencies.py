from __future__ import annotations

import ast
import inspect
import textwrap
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast

from openfoodfacts_data_quality.context.paths import path_spec_for
from openfoodfacts_data_quality.contracts.context import CHECK_INPUT_SURFACES

if TYPE_CHECKING:
    from openfoodfacts_data_quality.checks.registry import CheckBinding

_CallableParams = ParamSpec("_CallableParams")
_CallableReturn = TypeVar("_CallableReturn")
_CONTEXT_PATH_DEPENDENCIES_ATTR = (
    "__openfoodfacts_data_quality_context_path_dependencies__"
)
_CONTEXT_SECTION_NAMES = frozenset({"product", "flags", "category_props", "nutrition"})


@dataclass(frozen=True, slots=True)
class InferredContextDependency:
    """One normalized context dependency inferred from Python check code."""

    path: str
    source: str


@dataclass(frozen=True, slots=True)
class UnsupportedContextDependencyPattern:
    """One unsupported context-helper pattern that would hide dependencies."""

    message: str


def depends_on_context_paths(
    *paths: str,
) -> Callable[
    [Callable[_CallableParams, _CallableReturn]],
    Callable[_CallableParams, _CallableReturn],
]:
    """Attach normalized context dependency metadata to one helper function."""
    normalized_paths = _normalize_context_paths(paths)

    def decorator(
        function: Callable[_CallableParams, _CallableReturn],
    ) -> Callable[_CallableParams, _CallableReturn]:
        setattr(function, _CONTEXT_PATH_DEPENDENCIES_ATTR, normalized_paths)
        return function

    return decorator


def validate_check_context_contract(binding: CheckBinding) -> None:
    """Reject Python checks whose declared dependencies miss inferred usage."""
    inferred_dependencies = infer_check_context_dependencies(binding)
    missing_dependencies = [
        dependency
        for dependency in inferred_dependencies
        if dependency.path not in binding.required_context_paths
    ]
    if not missing_dependencies:
        return

    missing_details = ", ".join(
        f"{dependency.path} ({dependency.source})"
        for dependency in missing_dependencies
    )
    raise ValueError(
        f"Python check {binding.id} is missing declared context paths: "
        f"{missing_details}. Add them to @check(..., requires=...)."
    )


def infer_check_context_dependencies(
    binding: CheckBinding,
) -> tuple[InferredContextDependency, ...]:
    """Infer normalized context dependencies from one decorated Python check."""
    source = textwrap.dedent(inspect.getsource(binding.evaluator))
    module = ast.parse(source)
    function_name = binding.evaluator.__name__
    function_node = next(
        (
            node
            for node in module.body
            if isinstance(node, ast.FunctionDef) and node.name == function_name
        ),
        None,
    )
    if function_node is None:
        raise ValueError(
            f"Could not locate the Python AST for check evaluator {binding.id}."
        )
    if not function_node.args.args:
        raise ValueError(f"Python check evaluator {binding.id} must accept a context.")

    collector = _ContextDependencyCollector(
        context_parameter=function_node.args.args[0].arg,
        globals_namespace=binding.evaluator.__globals__,
    )
    collector.visit(function_node)
    if collector.unsupported_patterns:
        unsupported_details = "; ".join(
            pattern.message for pattern in collector.unsupported_patterns
        )
        raise ValueError(
            f"Python check {binding.id} uses unsupported context-helper patterns: "
            f"{unsupported_details}. Annotate the helper with "
            "@depends_on_context_paths(...) or pass leaf values instead."
        )
    return tuple(collector.dependencies)


def _normalize_context_paths(paths: tuple[str, ...]) -> tuple[str, ...]:
    """Validate context dependency paths while preserving first seen order."""
    normalized_paths: list[str] = []
    seen: set[str] = set()
    for path in paths:
        spec = path_spec_for(path)
        if spec is None:
            supported_surfaces = ", ".join(CHECK_INPUT_SURFACES)
            raise ValueError(
                f"Unknown normalized context dependency path {path!r}. "
                f"Expected a declared path supported by: {supported_surfaces}."
            )
        if path in seen:
            continue
        seen.add(path)
        normalized_paths.append(path)
    return tuple(normalized_paths)


def _context_dependency_paths_for(function: object) -> tuple[str, ...]:
    """Return helper-level normalized context dependency metadata when present."""
    raw_metadata = getattr(function, _CONTEXT_PATH_DEPENDENCIES_ATTR, ())
    if not isinstance(raw_metadata, tuple):
        return ()
    metadata_items = cast(tuple[object, ...], raw_metadata)
    if not all(isinstance(item, str) for item in metadata_items):
        return ()
    if not metadata_items:
        return ()
    return tuple(cast(str, item) for item in metadata_items)


class _ContextDependencyCollector(ast.NodeVisitor):
    """Collect direct and helper-level normalized context dependencies."""

    def __init__(
        self,
        *,
        context_parameter: str,
        globals_namespace: dict[str, object],
    ) -> None:
        self._context_parameter = context_parameter
        self._globals_namespace = globals_namespace
        self._context_aliases: dict[str, str] = {}
        self._seen_paths: set[str] = set()
        self.dependencies: list[InferredContextDependency] = []
        self.unsupported_patterns: list[UnsupportedContextDependencyPattern] = []

    def visit(self, node: ast.AST) -> Any:
        if isinstance(node, ast.AnnAssign):
            context_path = (
                self._context_path_from_expression(node.value)
                if node.value is not None
                else None
            )
            self._update_aliases_for_assignment(node.target, context_path)
        return super().visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        context_path = self._context_path_from_expression(node.value)
        for target in node.targets:
            self._update_aliases_for_assignment(target, context_path)
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        path = self._context_path_from_expression(node)
        if path is not None and path_spec_for(path) is not None:
            self._record_dependency(
                path,
                source=f"direct access to context.{path}",
            )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        helper = self._resolve_runtime_object(node.func)
        context_object_arguments = tuple(
            path
            for argument in (*node.args, *(keyword.value for keyword in node.keywords))
            if (path := self._context_object_path(argument)) is not None
        )
        helper_dependencies = _context_dependency_paths_for(helper)
        if helper_dependencies and any(
            self._references_context(argument)
            for argument in (*node.args, *(keyword.value for keyword in node.keywords))
        ):
            helper_name = getattr(helper, "__qualname__", repr(helper))
            for path in helper_dependencies:
                self._record_dependency(
                    path,
                    source=f"helper dependency via {helper_name}",
                )
        elif context_object_arguments:
            self._record_unsupported_pattern(
                self._unsupported_helper_message(
                    node.func,
                    context_object_arguments,
                )
            )
        self.generic_visit(node)

    def _record_dependency(self, path: str, *, source: str) -> None:
        if path in self._seen_paths:
            return
        self._seen_paths.add(path)
        self.dependencies.append(InferredContextDependency(path=path, source=source))

    def _record_unsupported_pattern(self, message: str) -> None:
        if any(pattern.message == message for pattern in self.unsupported_patterns):
            return
        self.unsupported_patterns.append(
            UnsupportedContextDependencyPattern(message=message)
        )

    def _context_path_from_expression(self, node: ast.expr | None) -> str | None:
        if node is None:
            return None
        if isinstance(node, ast.Name):
            if node.id == self._context_parameter:
                return ""
            return self._context_aliases.get(node.id)
        if not isinstance(node, ast.Attribute):
            return None

        base_path = self._context_path_from_expression(node.value)
        if base_path is None:
            return None
        if base_path:
            return f"{base_path}.{node.attr}"
        if node.attr in _CONTEXT_SECTION_NAMES:
            return node.attr
        return None

    def _references_context(self, node: ast.AST) -> bool:
        for child in ast.walk(node):
            if isinstance(child, ast.expr) and self._context_path_from_expression(
                child
            ):
                return True
            if isinstance(child, ast.Name) and child.id == self._context_parameter:
                return True
        return False

    def _context_object_path(self, node: ast.AST) -> str | None:
        if not isinstance(node, ast.expr):
            return None
        path = self._context_path_from_expression(node)
        if path == "" or path in _CONTEXT_SECTION_NAMES:
            return path
        if path and path_spec_for(path) is None:
            return path
        return None

    def _update_aliases_for_assignment(
        self,
        target: ast.expr,
        context_path: str | None,
    ) -> None:
        bound_names = tuple(_bound_name_targets(target))
        if context_path is not None:
            for name in bound_names:
                self._context_aliases[name] = context_path
            return
        for name in bound_names:
            self._context_aliases.pop(name, None)

    def _resolve_runtime_object(self, node: ast.expr) -> object | None:
        if isinstance(node, ast.Name):
            return self._globals_namespace.get(node.id)
        if isinstance(node, ast.Attribute):
            owner = self._resolve_runtime_object(node.value)
            if owner is None:
                return None
            return getattr(owner, node.attr, None)
        return None

    def _unsupported_helper_message(
        self,
        function_node: ast.expr,
        context_object_arguments: tuple[str, ...],
    ) -> str:
        helper_name = self._call_target_name(function_node)
        rendered_arguments = ", ".join(
            "context" if path == "" else f"context.{path}"
            for path in context_object_arguments
        )
        return f"{helper_name} receives {rendered_arguments}"

    def _call_target_name(self, node: ast.expr) -> str:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            owner_name = self._call_target_name(node.value)
            return f"{owner_name}.{node.attr}"
        return "<call>"


def _bound_name_targets(target: ast.expr) -> tuple[str, ...]:
    """Return bound local names introduced by one simple assignment target."""
    if isinstance(target, ast.Name):
        return (target.id,)
    if isinstance(target, (ast.Tuple, ast.List)):
        names: list[str] = []
        for element in target.elts:
            names.extend(_bound_name_targets(element))
        return tuple(names)
    return ()
