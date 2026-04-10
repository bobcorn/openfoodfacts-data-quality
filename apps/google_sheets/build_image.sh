#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEFAULT_IMAGE_NAME="google-sheets-demo:local"
IMAGE_NAME="${GOOGLE_SHEETS_IMAGE_NAME:-$DEFAULT_IMAGE_NAME}"
CLIENT_ID="${1:-${GOOGLE_SHEETS_CLIENT_ID:-}}"
API_KEY="${2:-${GOOGLE_SHEETS_API_KEY:-}}"
PROJECT_NUMBER="${3:-${GOOGLE_SHEETS_CLOUD_PROJECT_NUMBER:-}}"

if [[ -z "${CLIENT_ID}" || -z "${API_KEY}" || -z "${PROJECT_NUMBER}" ]]; then
  echo "Usage: $0 <google-client-id> <google-api-key> <google-cloud-project-number>" >&2
  echo "Or set GOOGLE_SHEETS_CLIENT_ID, GOOGLE_SHEETS_API_KEY," >&2
  echo "and GOOGLE_SHEETS_CLOUD_PROJECT_NUMBER in the environment." >&2
  exit 1
fi

docker build \
  --build-arg "GOOGLE_SHEETS_CLIENT_ID=${CLIENT_ID}" \
  --build-arg "GOOGLE_SHEETS_API_KEY=${API_KEY}" \
  --build-arg "GOOGLE_SHEETS_CLOUD_PROJECT_NUMBER=${PROJECT_NUMBER}" \
  -f "${ROOT_DIR}/apps/google_sheets/Dockerfile" \
  -t "${IMAGE_NAME}" \
  "${ROOT_DIR}"
