# Legacy Inventory

[Documentation](../index.md) / [Operations](index.md) / Legacy Inventory

The legacy inventory workflow exists for migration planning, not for parity execution.

## Export

The main entrypoint is:

```bash
python scripts/export_legacy_inventory.py --legacy-source-root /path/to/openfoodfacts-server
```

This inspects the legacy Perl data-quality modules and writes:

- `artifacts/legacy_inventory/legacy_families.json`
- `artifacts/legacy_inventory/estimation_sheet.csv`

## What The Export Produces

### `legacy_families.json`

This is the machine-readable inventory artifact. It groups legacy emitted code templates into families and records:

- source locations
- placeholder information
- structural signals from the source subroutine
- source fingerprinting data

### `estimation_sheet.csv`

This is the planning file. Its structure is intentionally flat and does not mirror every detail from the JSON artifact.

## Assessment Application

`scripts/apply_inventory_assessment.py` applies planning decisions from `assessment.json` back onto the estimation sheet.

This separation preserves a distinction between the machine-derived inventory and the human planning layer:

- export generates facts
- assessment records judgments
- apply writes those judgments into the planning sheet

## Role In The Repository

This workflow is documented alongside the runtime because it supports migration planning. It is not used in routine check execution.

[Back to Operations](index.md) | [Back to Documentation](../index.md)
