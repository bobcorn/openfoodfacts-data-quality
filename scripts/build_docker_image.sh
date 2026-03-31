#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

image_tag="${1:-openfoodfacts-data-quality:local}"

docker build \
  -t "${image_tag}" \
  "${repo_root}"
