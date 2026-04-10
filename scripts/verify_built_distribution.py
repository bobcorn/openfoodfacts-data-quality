"""Smoke-test the installed distribution without coupling to unstable runtime APIs."""

from __future__ import annotations

import importlib
from importlib import metadata
from importlib.resources import files

DIST_NAME = "openfoodfacts-data-quality"
PACKAGE_NAME = "off_data_quality"
REQUIRED_RESOURCE_PATHS = (
    "py.typed",
    "checks/packs/dsl/global_checks.yaml",
    "checks/packs/dsl/canada_checks.yaml",
    "checks/dsl/schema/definitions.schema.json",
)


def main() -> None:
    """Validate that the built wheel installs and keeps required packaged data."""
    importlib.import_module(PACKAGE_NAME)
    package_version = metadata.version(DIST_NAME)
    package_root = files(PACKAGE_NAME)

    missing_paths = [
        relative_path
        for relative_path in REQUIRED_RESOURCE_PATHS
        if not package_root.joinpath(relative_path).is_file()
    ]
    if missing_paths:
        missing_list = ", ".join(missing_paths)
        raise RuntimeError(
            "Installed distribution is missing required packaged resources: "
            f"{missing_list}"
        )

    print(f"Verified installed distribution {DIST_NAME} {package_version}.")


if __name__ == "__main__":
    main()
