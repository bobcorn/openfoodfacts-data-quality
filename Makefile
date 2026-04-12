VENV_BIN := $(if $(wildcard .venv/bin/python),.venv/bin,)

PYTHON ?= $(if $(VENV_BIN),$(VENV_BIN)/python,python)
PRE_COMMIT ?= $(if $(VENV_BIN),$(VENV_BIN)/pre-commit,pre-commit)
PYTEST ?= $(if $(VENV_BIN),$(VENV_BIN)/pytest,pytest)
RUFF ?= $(if $(VENV_BIN),$(VENV_BIN)/ruff,ruff)
MYPY ?= $(if $(VENV_BIN),$(VENV_BIN)/mypy,mypy)
VULTURE ?= $(if $(VENV_BIN),$(VENV_BIN)/vulture,vulture)
IMPORT_LINTER ?= $(if $(VENV_BIN),$(VENV_BIN)/lint-imports,lint-imports)
PYRIGHT ?= npm exec --yes --package pyright@1.1.408 -- pyright --project pyrightconfig.json
JSCPD ?= npm exec --yes --package jscpd@4.0.8 -- jscpd

PYTHON_TARGETS := apps migration scripts src tests
VULTURE_TARGETS := apps migration scripts src
JSCPD_TARGETS := apps migration scripts src tests
COVERAGE_ARGS := --cov=src/off_data_quality --cov=migration --cov=apps --cov-report=term-missing:skip-covered --cov-report=xml
CLEAN_DIRS := .mypy_cache .pytest_cache .ruff_cache .scannerwork artifacts build dist htmlcov src/openfoodfacts_data_quality.egg-info
CLEAN_FILES := .coverage coverage.xml
CACHE_DIRS := data/reference_result_cache
EXAMPLE_NOTEBOOK_SOURCES := examples/scripts/basic_usage.py examples/scripts/input_formats.py examples/scripts/jurisdiction_filtering.py

.PHONY: build check clean clean-cache coverage deadcode distclean dupcheck format format-check importlint install-hooks lint pyright quality sync-examples test typecheck

format:
	$(RUFF) check --fix $(PYTHON_TARGETS)
	$(RUFF) format $(PYTHON_TARGETS)

format-check:
	$(RUFF) format --check $(PYTHON_TARGETS)

lint:
	$(RUFF) check $(PYTHON_TARGETS)

importlint:
	$(IMPORT_LINTER) --config pyproject.toml

test:
	$(PYTEST) -q

coverage:
	$(PYTEST) $(COVERAGE_ARGS) -q

check:
	$(MAKE) format-check
	$(MAKE) lint
	$(MAKE) importlint
	$(MAKE) coverage

typecheck:
	$(MYPY)

deadcode:
	$(VULTURE) $(VULTURE_TARGETS)

pyright:
	$(PYRIGHT)

dupcheck:
	$(JSCPD) --config .jscpd.json $(JSCPD_TARGETS)

quality:
	@status=0; \
	$(MAKE) check || status=$$?; \
	$(MAKE) typecheck || status=$$?; \
	$(MAKE) deadcode || status=$$?; \
	$(MAKE) pyright || status=$$?; \
	$(MAKE) dupcheck || status=$$?; \
	exit $$status

install-hooks:
	$(PRE_COMMIT) install --install-hooks

sync-examples:
	$(PYTHON) -m jupytext --sync $(EXAMPLE_NOTEBOOK_SOURCES)

build:
	rm -rf build dist src/openfoodfacts_data_quality.egg-info
	$(PYTHON) -m build

clean:
	rm -rf $(CLEAN_DIRS)
	rm -f $(CLEAN_FILES)
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f \( -name '*.pyc' -o -name '*.pyo' -o -name '.DS_Store' \) -delete

clean-cache: clean
	rm -rf $(CACHE_DIRS)

distclean: clean-cache
	rm -rf .venv
