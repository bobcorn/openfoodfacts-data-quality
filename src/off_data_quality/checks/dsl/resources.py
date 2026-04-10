from __future__ import annotations

from importlib.resources import files
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from importlib.resources.abc import Traversable


def dsl_check_pack_resources() -> tuple[Traversable, ...]:
    """Return the packaged YAML files for all shipped DSL check packs."""
    resources_root = files("off_data_quality.checks.packs.dsl")
    pack_files = [
        resource
        for resource in resources_root.iterdir()
        if resource.is_file() and resource.name.endswith(".yaml")
    ]
    return tuple(sorted(pack_files, key=lambda resource: resource.name))


def dsl_schema_resource() -> Traversable:
    """Return the packaged structural schema for DSL check packs."""
    return files("off_data_quality.checks.dsl").joinpath(
        "schema",
        "definitions.schema.json",
    )
