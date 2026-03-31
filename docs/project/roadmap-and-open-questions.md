# Roadmap and Open Questions

[Documentation](../index.md) / [Project](index.md) / Roadmap and Open Questions

This page lists the areas most likely to change next.

## Likely Next Steps

- migrate more checks into the shared packaged runtime
- refine parity behavior for legacy-backed checks
- improve the documentation and contributor workflow around check authoring
- stabilize the public library surface once its boundaries are clear

## Open Design Questions

### How far should the DSL expand?

The DSL is intentionally small. Expanding it too far would start hiding logic that is easier to read in Python.

### How should enriched data be exposed through the library?

Some checks need backend-derived data, but not every enriched field should become part of the long-lived normalized contract. The enriched surface already exists, but its long-term library boundary is still a design decision.

### How should full-corpus runs be executed?

Local Docker runs are appropriate for development and small validation loops. It is still open whether full snapshots should run in a different environment.

### How much debugging detail should the report include?

The report currently focuses on summary output. Adding more debugging detail may help investigation, but it also increases noise.

### When should the public API be treated as stable?

The raw and enriched library surfaces are already explicit, but they may still change before they should be documented as long-lived external interfaces.

## Risk Areas

### Full-corpus execution

Running parity-oriented workflows on snapshots with millions of products changes the practical constraints of the system. A setup that works for local or sample runs may not be efficient enough for full snapshots.

### Full-corpus parity investigation

Even a small mismatch rate can produce a large absolute number of differences on a full snapshot. Investigation and reporting need to stay usable at that scale.

### Repository boundaries

The project still needs clear boundaries between:

- shared runtime contracts
- parity application logic
- one-off migration planning workflows

[Back to Project](index.md) | [Back to Documentation](../index.md)
