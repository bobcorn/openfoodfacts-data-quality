from __future__ import annotations

import ast
import inspect
import textwrap
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, ParamSpec, TypeVar, cast

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
    """One normalized-context dependency inferred from Python check code."""

    path: str
    source: str


def depends_on_context_paths(
    *paths: str,
) -> Callable[
    [Callable[_CallableParams, _CallableReturn]],
    Callable[_CallableParams, _CallableReturn],
]:
    """Attach normalized-context dependency metadata to one helper function."""
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
    """Infer normalized-context dependencies from one decorated Python check."""
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
    return tuple(collector.dependencies)


def _normalize_context_paths(paths: tuple[str, ...]) -> tuple[str, ...]:
    """Validate context dependency paths while preserving first-seen order."""
    normalized_paths: list[str] = []
    seen: set[str] = set()
    for path in paths:
        spec = path_spec_for(path)
        if spec is None:
            supported_surfaces = ", ".join(CHECK_INPUT_SURFACES)
            raise ValueError(
                f"Unknown normalized-context dependency path {path!r}. "
                f"Expected a declared path supported by: {supported_surfaces}."
            )
        if path in seen:
            continue
        seen.add(path)
        normalized_paths.append(path)
    return tuple(normalized_paths)


def _context_dependency_paths_for(function: object) -> tuple[str, ...]:
    """Return helper-level normalized-context dependency metadata when present."""
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
    """Collect direct and helper-level normalized-context dependencies."""

    def __init__(
        self,
        *,
        context_parameter: str,
        globals_namespace: dict[str, object],
    ) -> None:
        self._context_parameter = context_parameter
        self._globals_namespace = globals_namespace
        self._seen_paths: set[str] = set()
        self.dependencies: list[InferredContextDependency] = []

    def visit_Attribute(self, node: ast.Attribute) -> None:
        path = self._context_path_from_attribute(node)
        if path is not None and path_spec_for(path) is not None:
            self._record_dependency(
                path,
                source=f"direct access to context.{path}",
            )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        helper = self._resolve_runtime_object(node.func)
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
        self.generic_visit(node)

    def _record_dependency(self, path: str, *, source: str) -> None:
        if path in self._seen_paths:
            return
        self._seen_paths.add(path)
        self.dependencies.append(InferredContextDependency(path=path, source=source))

    def _context_path_from_attribute(self, node: ast.Attribute) -> str | None:
        segments: list[str] = []
        current: ast.expr = node
        while isinstance(current, ast.Attribute):
            segments.append(current.attr)
            current = current.value
        if not isinstance(current, ast.Name) or current.id != self._context_parameter:
            return None
        segments.reverse()
        if len(segments) < 2 or segments[0] not in _CONTEXT_SECTION_NAMES:
            return None
        return ".".join(segments)

    def _references_context(self, node: ast.AST) -> bool:
        for child in ast.walk(node):
            if isinstance(child, ast.Name) and child.id == self._context_parameter:
                return True
        return False

    def _resolve_runtime_object(self, node: ast.expr) -> object | None:
        if isinstance(node, ast.Name):
            return self._globals_namespace.get(node.id)
        if isinstance(node, ast.Attribute):
            owner = self._resolve_runtime_object(node.value)
            if owner is None:
                return None
            return getattr(owner, node.attr, None)
        return None
