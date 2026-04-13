FROM python:3.14-slim AS wheel-builder

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \

WORKDIR /workspace

COPY pyproject.toml README.md /workspace/
COPY src /workspace/src

RUN python -m pip install --no-cache-dir --upgrade pip build \
    && python -m build --wheel --outdir /dist


FROM python:3.14-slim AS runtime

ARG GOOGLE_SHEETS_CLIENT_ID=""
ARG GOOGLE_SHEETS_API_KEY=""
ARG GOOGLE_SHEETS_CLOUD_PROJECT_NUMBER=""

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    GOOGLE_SHEETS_CLIENT_ID=${GOOGLE_SHEETS_CLIENT_ID} \
    GOOGLE_SHEETS_API_KEY=${GOOGLE_SHEETS_API_KEY} \
    GOOGLE_SHEETS_CLOUD_PROJECT_NUMBER=${GOOGLE_SHEETS_CLOUD_PROJECT_NUMBER} \
    LOG_INCLUDE_SOURCE=false

WORKDIR /app

COPY --from=wheel-builder /dist/openfoodfacts_data_quality-*.whl /tmp/

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir /tmp/openfoodfacts_data_quality-*.whl Jinja2==3.1.6 \
    groupadd --system appuser \
    && useradd --system --gid appuser --create-home --home-dir /home/appuser appuser \
    && mkdir -p /app/ui/static /app/ui/templates /app/apps/google_sheets/static /app/apps/google_sheets/templates \
    && chown -R appuser:appuser /app

COPY apps/__init__.py /app/apps/__init__.py
COPY ui/__init__.py /app/ui/__init__.py
COPY ui/rendering.py /app/ui/rendering.py
COPY ui/templates /app/ui/templates
COPY ui/static /app/ui/static
COPY apps/google_sheets/__init__.py /app/apps/google_sheets/__init__.py
COPY apps/google_sheets/api.py /app/apps/google_sheets/api.py
COPY apps/google_sheets/data_sources.py /app/apps/google_sheets/data_sources.py
COPY apps/google_sheets/models.py /app/apps/google_sheets/models.py
COPY apps/google_sheets/server.py /app/apps/google_sheets/server.py
COPY apps/google_sheets/workflow.py /app/apps/google_sheets/workflow.py
COPY apps/google_sheets/templates/index.html.j2 /app/apps/google_sheets/templates/index.html.j2
COPY apps/google_sheets/static/app.js /app/apps/google_sheets/static/app.js
COPY apps/google_sheets/static/google_sheets.css /app/apps/google_sheets/static/google_sheets.css

USER appuser

EXPOSE 8501

CMD ["python", "-m", "apps.google_sheets.server"]
