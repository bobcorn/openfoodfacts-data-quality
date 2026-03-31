# Local Development

[Documentation](../index.md) / [Guides](index.md) / Local Development

For most work, use the Docker-based flow from the repository root.

## Quick Start

```bash
cp .env.example .env
docker compose up --build
```

This:

- builds the local runtime image
- mounts the configured DuckDB snapshot
- persists the reference result cache
- writes report artifacts into `artifacts/`

## Main Local Inputs

- `.env`
  Local runtime configuration.
- `config/check-profiles.toml`
  Named check subsets for application runs.
- `DATABASE_PATH`
  The source DuckDB snapshot used by the application.

## Useful Development Loops

- Edit profiles in `config/check-profiles.toml` when you want to narrow or widen the active check set.
- Use the bundled sample snapshot for a fast first run.
- Switch to a richer local snapshot when you need more representative parity behavior.

## Local Python Environment

If you want to run tools outside Docker:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[app,dev]"
```

Use the Docker flow for parity-backed development. It provides the legacy backend runtime in a controlled environment.

[Back to Guides](index.md) | [Back to Documentation](../index.md)
