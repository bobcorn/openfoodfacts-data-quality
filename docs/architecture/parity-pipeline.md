# Parity Pipeline

[Documentation](../index.md) / [Architecture](index.md) / Parity Pipeline

One application run from source snapshot to rendered report.

## Pipeline Overview

```mermaid
flowchart TB
    subgraph INPUT["Input"]
        A["DuckDB Source"]
    end

    subgraph PREP["Run Preparation"]
        B["Resolve Snapshot Metadata"]
        C["Load Active Profile"]
        D["Configure Run Strategy"]
        B --> C
        C --> D
    end

    subgraph SOURCE["Batch Source"]
        E["Read Source Batches"]
        F["Prepare Backend Payload"]
        E --> F
    end

    subgraph EXT["External Dependency"]
        G["Legacy Backend Runtime"]
    end

    subgraph REF["Reference Path"]
        H["Resolve Reference Results"]
        I["Normalize Reference Findings"]
        J["Provide Enriched Snapshots"]
        H --> I
        H --> J
    end

    subgraph MIG["Migrated Runtime"]
        K["Build Check Contexts"]
        L["Run Python Checks"]
        M["Run DSL Checks"]
        N["Normalize Migrated Findings"]
        K --> L
        K --> M
        L --> N
        M --> N
    end

    subgraph PAR["Parity"]
        O["Compare Findings"]
        P["Accumulate Run Summary"]
        O --> P
    end

    subgraph ART["Output Artifacts"]
        Q["Emit Machine Readable Artifacts"]
    end

    subgraph PRES["Presentation"]
        R["Render Report Site"]
        S["Preview Site"]
        R --> S
    end

    A --> B
    D --> E
    E --> K
    F --> H
    G -.-> H
    I --> O
    J --> K
    N --> O
    P --> Q
    P --> R
```

The table below restates the same flow stage by stage.

| Stage | Nodes In The Diagram | Role In The Flow |
| --- | --- | --- |
| Input | DuckDB Source | Provides the source snapshot consumed by the run. |
| Run preparation | Resolve Snapshot Metadata, Load Active Profile, Configure Run Strategy | Determines the source snapshot id, selected checks, required input surface, and whether reference data is needed. |
| Batch source | Read Source Batches, Prepare Backend Payload | Streams ordered source rows and shapes the payload sent to the legacy backend path when reference data is required. |
| External dependency | Legacy Backend Runtime | Executes the trusted Perl boundary that materializes enriched reference data and legacy finding tags. |
| Reference path | Resolve Reference Results, Normalize Reference Findings, Provide Enriched Snapshots | Reuses cache when possible, normalizes reference findings, and exposes enriched snapshots for parity or enriched surface runs. |
| Migrated runtime | Build Check Contexts, Run Python Checks, Run DSL Checks, Normalize Migrated Findings | Builds normalized contexts, executes the selected migrated checks, and turns the results into comparable findings. |
| Parity | Compare Findings, Accumulate Run Summary | Applies strict multiset comparison and aggregates batch results into run level parity data. |
| Output artifacts | Emit Machine Readable Artifacts | Writes the machine readable outputs used for inspection and downstream review. |
| Presentation | Render Report Site, Preview Site | Builds the static report site and serves it locally for review. |

## Run Preparation

The pipeline resolves:

- the source snapshot id
- the active check profile
- the required input surface
- whether reference results are needed at all

The active profile determines the check set, input surface, and parity baselines in scope.

## Source Batches

Source rows are streamed from DuckDB in ordered batches. The same source reader contract is used for the bundled sample and for larger snapshots that follow the same schema.

## Reference Results

If the run needs parity findings or enriched snapshots:

- raw rows are projected into the explicit legacy backend input contract
- cached reference results are reused when possible
- missing results are materialized through persistent legacy backend workers

If the run does not need reference data, this branch is skipped.

## Legacy Backend

Parity runs compare migrated output against the behavior of the current trusted backend.

For that reason the pipeline still materializes:

- enriched snapshots from the legacy side
- legacy emitted check tags from the legacy side

The dependency is deliberate. Parity validation still relies on the current legacy backend.

That includes parity `raw_products` runs. Even when migrated contexts are built from raw rows, the reference side still comes from legacy emitted tags. Only runtime only runs can skip the backend entirely. See [Legacy Backend Image](../operations/legacy-backend-image.md).

## Migrated Contexts

The migrated runtime builds normalized contexts from:

- raw rows for `raw_products`
- enriched snapshots for `enriched_products`

That keeps the execution engine independent from source specific shapes.

## Check Execution

The shared execution engine loads the selected evaluators and runs them on the normalized contexts. Python and DSL checks are executed through one unified path.

## Parity Comparison

The parity layer normalizes reference and migrated outputs into observed findings and compares them with strict multiset equality over:

- product id
- observed code
- severity

This comparison is stricter than matching on check id alone.

## Outputs

Batch level parity results are accumulated into one run summary. The completed run then produces:

- a static HTML report
- `parity.json`
- `snippets.json`
- a bundled JSON export archive

The current renderer is scoped to migration runs compared under parity. Runtime only checks are supported by the shared runtime and catalog, but they are not yet part of the report presentation model.

## Next Reads

- [Reading The Report](../getting-started/reading-the-report.md)
- [Configuration and Artifacts](../operations/configuration-and-artifacts.md)
- [Legacy Backend Image](../operations/legacy-backend-image.md)
- [System Overview](system-overview.md)
