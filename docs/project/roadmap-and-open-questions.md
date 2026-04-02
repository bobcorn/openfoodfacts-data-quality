# Roadmap and Open Questions

[Documentation](../index.md) / [Project](index.md) / Roadmap and Open Questions

Project decisions and areas most likely to change.

## Priorities

- migrate more checks compared against legacy behavior into the shared packaged runtime
- keep tightening parity behavior where legacy comparison is expected
- improve the local check authoring workflow
- keep the explicit raw, enriched, and reference contracts aligned as new checks land

## Open Questions

### DSL Scope

The DSL stays narrow by design. Expanding it too far would hide logic that is easier to review in Python. The open question is which additions would keep migrated logic obvious and auditable.

### Enriched Surface Growth

The repository already exposes an explicit enriched contract owned by the Python runtime. The open question is which additional fields, if any, deserve to widen that contract as more migrated checks land.

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

The split is already present in the codebase. It still needs discipline as the project grows.

## Next

- [Project Overview and Scope](overview-and-scope.md)
- [Application Run Flow](../architecture/application-run-flow.md)
- [Configuration and Artifacts](../operations/configuration-and-artifacts.md)
