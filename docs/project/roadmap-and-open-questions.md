# Roadmap and Open Questions

[Documentation](../index.md) / [Project](index.md) / Roadmap and Open Questions

Project decisions and areas most likely to change.

## Near Term Priorities

- migrate more checks compared against legacy behavior into the shared packaged runtime
- keep tightening parity behavior where legacy comparison is expected
- improve onboarding and contributor workflow around check authoring
- make the library boundaries clearer as the runtime matures

## Open Questions

### DSL Scope

The DSL stays narrow by design. Expanding it too far would hide logic that is easier to review in Python. The open question is which additions would keep migrated logic obvious and auditable.

### Enriched API Stability

The repository already exposes explicit enriched snapshots through the public library APIs. The open question is which enriched fields deserve to be treated as durable shared contracts and which should remain local to `app/` or close to the backend.

### Full Corpus Execution

The Docker flow is appropriate for local development and modest validation loops. It is still open whether whole snapshot parity runs should remain local, move to CI style automation, or run in a different environment.

### Report Detail

The report stays at summary level. More debugging detail could help investigations. Too much detail would make the main report harder to review quickly.

### Public API Stability

The raw and enriched APIs are explicit today. The open question is when they should be documented and supported as durable public interfaces.

## Risk Areas

### Full Corpus Performance

A workflow that is lightweight on sample data can behave very differently on millions of products. Batch sizing, cache behavior, legacy backend throughput, and report usability all become more sensitive at that scale.

### Parity Investigation At Scale

Even low mismatch rates can produce large absolute mismatch volumes on full snapshots. The artifact and report model needs to remain useful when the debugging scope grows.

### Runtime/App Boundary Drift

The repository depends on a clean split between:

- reusable runtime contracts
- application behavior used only for parity
- dedicated migration planning workflows

The split is already present in the codebase. It will need continued discipline as the project grows.

## Next Reads

- [Project Overview and Scope](overview-and-scope.md)
- [Parity Pipeline](../architecture/parity-pipeline.md)
- [Configuration and Artifacts](../operations/configuration-and-artifacts.md)
