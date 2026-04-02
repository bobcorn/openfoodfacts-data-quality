# Legacy Inventory

[Documentation](../index.md) / [Operations](index.md) / Legacy Inventory

The legacy inventory workflow supports migration planning and stays separate from parity execution.

## Export

Entry point:

```bash
.venv/bin/python scripts/export_legacy_inventory.py --legacy-source-root /path/to/openfoodfacts-server
```

This inspects the legacy Perl data quality modules and writes:

- `artifacts/legacy_inventory/legacy_families.json`
- `artifacts/legacy_inventory/estimation_sheet.csv`

## Outputs

### `legacy_families.json`

Machine readable inventory artifact. It groups legacy emitted code templates into migration families and records:

- source locations
- placeholder information
- structural signals from the legacy subroutine
- counts of unsupported data-quality emission shapes seen in the same source family
- source fingerprinting data

### `estimation_sheet.csv`

Flat CSV scaffold for planning. It is simpler than the JSON artifact and meant for human estimation work.
It includes the source span columns and a derived `cluster_id` column so spreadsheet-based estimates can group families that share the same legacy snippet. The assessment apply step preserves that grouping key.

## Assessment

`scripts/apply_inventory_assessment.py` applies planning decisions from `assessment.json` back onto the estimation sheet.

Use the same `.venv` workflow for this step:

```bash
.venv/bin/python scripts/apply_inventory_assessment.py
```

The workflow keeps facts, judgments, and sheet updates in separate artifacts:

- export generates facts
- assessment records judgments
- apply writes those judgments into the planning sheet

## Role

This workflow sits alongside the runtime because it supports the migration effort. It is not part of routine check execution or the public library API.

The export and report snippet workflows share the same legacy source analysis module in `app/legacy_source.py`.

## Next

- [Project Overview and Scope](../project/overview-and-scope.md)
- [Roadmap and Open Questions](../project/roadmap-and-open-questions.md)
- [System Overview](../architecture/system-overview.md)
