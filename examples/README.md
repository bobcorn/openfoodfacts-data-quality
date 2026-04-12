# Examples

This directory provides the same walkthroughs in two forms:

- runnable Python scripts in `scripts/` using `py:percent`
- paired Jupyter notebooks in `notebooks/` for demos and step-by-step exploration

The scripts are the review-friendly source of truth. The notebooks are paired
views kept in sync with Jupytext.

## Run the scripts

Install the optional example dependencies:

```bash
.venv/bin/python -m pip install -e ".[examples]"
```

Run any example from the repository root:

```bash
.venv/bin/python examples/scripts/basic_usage.py
.venv/bin/python examples/scripts/input_formats.py
.venv/bin/python examples/scripts/jurisdiction_filtering.py
```

## Run the notebooks

Install the optional notebook dependencies:

```bash
.venv/bin/python -m pip install -e ".[notebook]"
```

Launch JupyterLab from the repository root:

```bash
.venv/bin/jupyter lab
```

Then open:

- `examples/notebooks/basic_usage.ipynb`
- `examples/notebooks/input_formats.ipynb`
- `examples/notebooks/jurisdiction_filtering.ipynb`

## Keep the paired files in sync

This directory uses a local `jupytext.toml` so the pairing stays scoped to the
examples only.

If you edit one side manually, resync the pair with:

```bash
make sync-examples
```

The repository `pre-commit` config also runs `jupytext --sync` for these
examples, so `pre-commit install --install-hooks` keeps the paired files
aligned automatically on commits.

The notebooks are intentionally committed without saved outputs so diffs stay
small and reviewable.
