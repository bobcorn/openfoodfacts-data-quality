FROM python:3.14-slim AS wheel-builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /workspace

COPY pyproject.toml README.md /workspace/
COPY src /workspace/src

RUN python -m pip install --no-cache-dir --upgrade pip build \
    && python -m build --wheel --outdir /dist


FROM python:3.14-slim AS runtime

ARG GOOGLE_SHEETS_DEMO_GOOGLE_CLIENT_ID=""
ARG GOOGLE_SHEETS_DEMO_GOOGLE_API_KEY=""
ARG GOOGLE_SHEETS_DEMO_GOOGLE_CLOUD_PROJECT_NUMBER=""

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    GOOGLE_SHEETS_DEMO_GOOGLE_CLIENT_ID=${GOOGLE_SHEETS_DEMO_GOOGLE_CLIENT_ID} \
    GOOGLE_SHEETS_DEMO_GOOGLE_API_KEY=${GOOGLE_SHEETS_DEMO_GOOGLE_API_KEY} \
    GOOGLE_SHEETS_DEMO_GOOGLE_CLOUD_PROJECT_NUMBER=${GOOGLE_SHEETS_DEMO_GOOGLE_CLOUD_PROJECT_NUMBER}

WORKDIR /app

COPY --from=wheel-builder /dist/openfoodfacts_data_quality-*.whl /tmp/

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir /tmp/openfoodfacts_data_quality-*.whl

RUN groupadd --system appuser \
    && useradd --system --gid appuser --create-home --home-dir /home/appuser appuser \
    && mkdir -p /app/examples/google_sheets_demo /app/examples/google_sheets_demo/web \
    && chown -R appuser:appuser /app
COPY examples/google_sheets_demo/__init__.py /app/examples/google_sheets_demo/__init__.py
COPY examples/google_sheets_demo/api.py /app/examples/google_sheets_demo/api.py
COPY examples/google_sheets_demo/data_sources.py /app/examples/google_sheets_demo/data_sources.py
COPY examples/google_sheets_demo/models.py /app/examples/google_sheets_demo/models.py
COPY examples/google_sheets_demo/server.py /app/examples/google_sheets_demo/server.py
COPY examples/google_sheets_demo/workflow.py /app/examples/google_sheets_demo/workflow.py
COPY examples/google_sheets_demo/web/index.html /app/examples/google_sheets_demo/web/index.html
COPY examples/google_sheets_demo/web/app.js /app/examples/google_sheets_demo/web/app.js
COPY examples/google_sheets_demo/web/style.css /app/examples/google_sheets_demo/web/style.css

USER appuser

EXPOSE 8501

CMD ["python", "-m", "examples.google_sheets_demo.server"]
