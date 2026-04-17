"""Supported wheel metadata surface for packaged data quality checks."""

from __future__ import annotations

import hashlib
from functools import cache
from importlib import import_module
from importlib.resources import files
from typing import TYPE_CHECKING

from off_data_quality.checks._sources import (
    default_dsl_check_pack_resources,
    default_python_check_pack_module_names,
)

if TYPE_CHECKING:
    from importlib.resources.abc import Traversable

_RUNTIME_FINGERPRINT_PACKAGE_PATHS = (
    "_scalars.py",
    "contracts/enrichment.py",
    "contracts/structured.py",
)


def packaged_dsl_check_pack_resources() -> tuple[Traversable, ...]:
    """Return the DSL check pack resources packaged in the installed wheel."""
    return default_dsl_check_pack_resources()


def packaged_python_check_pack_module_names() -> tuple[str, ...]:
    """Return the Python check pack modules packaged in the installed wheel."""
    return default_python_check_pack_module_names()


@cache
def packaged_runtime_fingerprint() -> str:
    """Return a stable fingerprint of packaged runtime contract inputs."""
    digest = hashlib.sha256()
    package_root = files("off_data_quality")
    for relative_path in _RUNTIME_FINGERPRINT_PACKAGE_PATHS:
        digest.update(relative_path.encode("utf-8"))
        digest.update(
            _resource_under_package_root(package_root, relative_path).read_bytes()
        )
    return digest.hexdigest()


def packaged_module_path(module_name: str) -> str:
    """Return the wheel-relative source path for one packaged Python module."""
    if not module_name.startswith("off_data_quality"):
        raise ValueError(f"Unsupported non-packaged module {module_name!r}.")
    module = import_module(module_name)
    module_file = getattr(module, "__file__", None)
    module_path = module_name.replace(".", "/")
    if isinstance(module_file, str) and module_file.endswith("__init__.py"):
        return f"{module_path}/__init__.py"
    return f"{module_path}.py"


def packaged_dsl_check_pack_resource_path(resource: Traversable) -> str:
    """Return the wheel-relative path for one packaged DSL check resource."""
    resource_name = getattr(resource, "name", None)
    if not isinstance(resource_name, str) or not resource_name:
        raise RuntimeError("Could not resolve packaged DSL check resource name.")
    return f"off_data_quality/checks/packs/dsl/{resource_name}"


def _resource_under_package_root(
    package_root: Traversable,
    relative_path: str,
) -> Traversable:
    """Resolve one package-relative path under the wheel root."""
    return package_root.joinpath(*relative_path.split("/"))


__all__ = [
    "packaged_dsl_check_pack_resource_path",
    "packaged_dsl_check_pack_resources",
    "packaged_module_path",
    "packaged_python_check_pack_module_names",
    "packaged_runtime_fingerprint",
]
