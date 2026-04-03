[Back to documentation index](../index.md)

# About the runtime model

The runtime model explains where check execution happens and which contract a
migrated check receives.

## Why the runtime is split

The shared runtime lives under `src/openfoodfacts_data_quality/`. It owns:

- packaged check definitions
- the [check catalog](migrated-checks.md#packaged-checks) and check metadata
- input projection and context building
- the public [Python library APIs](../how-to/use-the-python-library.md)

The application layer lives under `app/`. It adds the parts that exist only
for full application runs:

- DuckDB source loading
- dataset profile selection for one run
- [reference data](reference-data-and-parity.md#why-the-reference-path-exists)
  resolution
- [strict comparison](reference-data-and-parity.md#strict-comparison)
- [`RunResult`](../reference/data-contracts.md#runresult) accumulation
- parity store persistence and
  [report artifact generation](../reference/report-artifacts.md)

`app/` builds on the shared runtime. The shared runtime does not depend on
`app/`.

## Input surfaces

An input surface is the contract by which product data enters the runtime.

The runtime supports two input surfaces: `raw_products` and
`enriched_products`.

### Raw product runs

`raw_products` means the check can run from the public
[source snapshot](../reference/glossary.md#source-snapshot) alone. The
migrated runtime builds its context from
[raw product rows](../reference/data-contracts.md#rawproductrow) and does not
need enriched data for that check.

### Enriched product runs

`enriched_products` means the check depends on stable enriched data that is not
present in raw public rows. In application runs, that data is materialized
through the [reference path](reference-data-and-parity.md#why-the-reference-path-exists)
and projected into the enriched contract owned by Python. In direct library
usage, callers can provide
[enriched snapshots](../reference/data-contracts.md#enrichedsnapshotresult)
explicitly.

The selected surface changes:

- which checks are eligible for a run
- which data the runtime must prepare
- whether an application run needs the
  [reference path](reference-data-and-parity.md#why-the-reference-path-exists)

```mermaid
flowchart TB
    subgraph CALLERS["Entry points"]
        A["Raw API"]
        B["Enriched API"]
        C["Application layer"]
    end

    subgraph INPUTS["Runtime inputs"]
        D["Raw rows"]
        E["Enriched snapshots"]
    end

    subgraph RT["Shared runtime"]
        F["NormalizedContext"]
        G["Packaged checks<br/>Python or DSL"]
        H["Findings"]
        F --> G --> H
    end

    A --> D
    B --> E
    C --> D
    C --> E
    D --> F
    E --> F
```

## Input surface and dataset profile are different

The application uses two independent selection axes:

- the check input surface, which decides which contract each check can read
- the dataset profile, which decides which source rows enter one run

Example: one run can use `SOURCE_DATASET_PROFILE=smoke` to read a small sample
of products while `CHECK_PROFILE=raw_products` still keeps the run on checks
that support the raw surface.

Changing the dataset profile changes run coverage. It does not make a raw check
into an enriched check, and it does not change the valid
`NormalizedContext` paths for that check.

## NormalizedContext

Checks do not read raw DuckDB rows or backend payloads directly. They read
`NormalizedContext`.

`NormalizedContext` is the shared runtime contract consumed by migrated checks.
The runtime converts different input shapes into one structure owned by Python
with stable field names and stable dotted paths.

This keeps check logic independent from source specific shapes. Raw and
enriched runs still share one execution model.

## Why this boundary matters

Growing `NormalizedContext` affects several parts of the system. The change
reaches
[check selection](../reference/check-metadata-and-selection.md#selection-inputs),
[DSL](migrated-checks.md#definition-languages) usage,
[helper annotations](../reference/check-metadata-and-selection.md#dependency-invariant),
and tests.

Treat `NormalizedContext` as a stable boundary, not as an incidental helper
shape.

## Related information

- [About migrated checks](migrated-checks.md)
- [About reference data and parity](reference-data-and-parity.md)
- [Data contracts](../reference/data-contracts.md)

[Back to documentation index](../index.md)
