# Project Overview and Scope

[Documentation](../index.md) / [Project](index.md) / Project Overview and Scope

This repository supports migration, review, and maintenance work for Open Food Facts data quality checks.

It combines:

- a Python-owned runtime for migrated checks
- a defined path for parity validation against the legacy backend
- report and artifact output for review

## What The Repository Contains

- a reusable library under `src/openfoodfacts_data_quality/`
- a parity and reporting application under `app/`
- operational scripts for validation, sample refresh, and migration planning

## Current Coverage

- migrated checks can live in one shared runtime
- simple checks can be expressed in a small DSL, while more complex checks can remain Python
- parity-backed execution is supported as a regular workflow
- the project can emit both a reviewer-facing report and machine-readable artifacts

## Current Limits

- it is not a full replacement for every legacy data quality rule
- it is not a stable public product API in the sense of a finished platform
- parity-backed workflows still depend on the legacy backend

## Repository Split

The main split is between reusable runtime logic and migration application logic.

- `src/` owns the check system and the public library surface
- `app/` owns orchestration, reference loading, parity, and reporting

This split allows the migrated check runtime to remain reusable even when the parity application evolves.

[Back to Project](index.md) | [Back to Documentation](../index.md)
