# Legacy Inventory

[Documentation](../index.md) / [Operations](index.md) / Legacy Inventory

The legacy inventory workflow supports migration planning and stays separate from parity execution.

## Export

Entry point:

```bash
python scripts/export_legacy_inventory.py --legacy-source-root /path/to/openfoodfacts-server
```

This inspects the legacy Perl data quality modules and writes:

- `artifacts/legacy_inventory/legacy_families.json`
- `artifacts/legacy_inventory/estimation_sheet.csv`

## Export Outputs

### `legacy_families.json`

Machine readable inventory artifact. It groups legacy emitted code templates into migration families and records:

- source locations
- placeholder information
- structural signals from the legacy subroutine
- source fingerprinting data

### `estimation_sheet.csv`

Flat CSV scaffold for planning. It is simpler than the JSON artifact and meant for human estimation work.

## Assessment

`scripts/apply_inventory_assessment.py` applies planning decisions from `assessment.json` back onto the estimation sheet.

The workflow keeps facts, judgments, and sheet updates in separate artifacts:

- export generates facts
- assessment records judgments
- apply writes those judgments into the planning sheet

## Repository Role

This workflow sits alongside the runtime because it supports the migration effort. It is not part of routine check execution or the public library API.

## Next Reads

- [Project Overview and Scope](../project/overview-and-scope.md)
- [Roadmap and Open Questions](../project/roadmap-and-open-questions.md)
- [System Overview](../architecture/system-overview.md)
