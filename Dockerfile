ARG SERVER_BASE_IMAGE=ghcr.io/bobcorn/openfoodfacts-server:main-debaa5a59b
FROM ${SERVER_BASE_IMAGE} AS runtime-base

USER root

ARG SERVER_BASE_IMAGE
ARG PYTHON_VERSION=3.14.3

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && /root/.local/bin/uv python install ${PYTHON_VERSION}

WORKDIR /opt/openfoodfacts-data-quality

COPY pyproject.toml README.md /opt/openfoodfacts-data-quality/
COPY src /opt/openfoodfacts-data-quality/src
RUN /root/.local/bin/uv venv --seed --python ${PYTHON_VERSION} /opt/openfoodfacts-data-quality/.venv \
    && /opt/openfoodfacts-data-quality/.venv/bin/pip install --no-cache-dir '/opt/openfoodfacts-data-quality[app]'

COPY migration /opt/openfoodfacts-data-quality/migration
COPY ui /opt/openfoodfacts-data-quality/ui
COPY config /opt/openfoodfacts-data-quality/config
COPY scripts /opt/openfoodfacts-data-quality/scripts

RUN chmod +x /opt/openfoodfacts-data-quality/migration/legacy_backend/off_runtime.pl \
    && mkdir -p /opt/openfoodfacts-data-quality/data

ENV PYTHONUNBUFFERED=1 \
    LEGACY_BACKEND_FINGERPRINT="${SERVER_BASE_IMAGE}" \
    PRODUCT_OPENER_FLAVOR_SHORT="off" \
    PYTHONPATH="/opt/openfoodfacts-data-quality/src:/opt/openfoodfacts-data-quality" \
    PATH="/root/.local/bin:${PATH}"

EXPOSE 8000

CMD ["/opt/openfoodfacts-data-quality/.venv/bin/python", "-m", "migration.cli"]

FROM runtime-base AS demo

COPY examples /opt/openfoodfacts-data-quality/examples

ENV SOURCE_SNAPSHOT_PATH="/opt/openfoodfacts-data-quality/examples/data/products.duckdb" \
    PORT="8000" \
    BATCH_SIZE="1000" \
    BATCH_WORKERS="1" \
    LEGACY_BACKEND_WORKERS="1" \
    MISMATCH_EXAMPLES_LIMIT="20" \
    CHECK_PROFILE="full" \
    SOURCE_DATASET_PROFILE="full" \
    PARITY_STORE_PATH="/opt/openfoodfacts-data-quality/data/parity_store/parity.duckdb"

FROM runtime-base AS runtime
