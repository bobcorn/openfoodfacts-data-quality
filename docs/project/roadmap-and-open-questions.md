# Roadmap and Open Questions

[Documentation](../index.md) / [Project](index.md) / Roadmap and Open Questions

Project-level decisions still in motion and the areas most likely to change next.

## Near-Term Priorities

- migrate more legacy-backed checks into the shared packaged runtime
- keep tightening parity behavior where legacy comparison is expected
- improve onboarding and contributor workflow around check authoring
- make the library-facing boundaries clearer as the runtime matures

## Open Questions

### DSL Scope

The current DSL is intentionally narrow. Expanding it too far would hide logic that is easier to review in Python. The main question is not whether the DSL can do more, but which additions would still keep migrated logic obvious and auditable.

### Enriched API Stability

The repository already exposes explicit enriched snapshots through the public library APIs. The open question is which enriched fields deserve to be treated as long-lived shared contracts and which should remain app-local or backend-adjacent details.

### Full-Corpus Execution

The Docker flow is appropriate for local development and modest validation loops. It is still open whether full-snapshot parity runs should remain local, move to CI-like automation, or live in a different execution environment altogether.

### Report Detail

The current report is intentionally summary-first. More debugging detail could help investigation, but too much detail risks turning the main report into a noisy artifact that is harder to review quickly.

### Public API Stability

The raw and enriched APIs are explicit today. The unresolved question is when they should be documented and supported as durable public interfaces rather than as prototype-era public APIs.

## Risk Areas

### Full-Corpus Performance

A workflow that feels lightweight on sample data can behave very differently on millions of products. Batch sizing, cache behavior, legacy backend throughput, and report usability all become more sensitive at that scale.

### Parity Investigation At Scale

Even low mismatch rates can produce large absolute mismatch volumes on full snapshots. The artifact and report model needs to remain useful when the debugging scope grows.

### Runtime/App Boundary Drift

The repository depends on a clean split between:

- reusable runtime contracts
- parity-only application behavior
- one-off migration-planning workflows

That split is already present in the codebase, but it will need continued discipline as the project grows.

## Next Reads

- [Project Overview and Scope](overview-and-scope.md)
- [Parity Pipeline](../architecture/parity-pipeline.md)
- [Configuration and Artifacts](../operations/configuration-and-artifacts.md)
