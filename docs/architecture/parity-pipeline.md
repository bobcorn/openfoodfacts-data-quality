# Parity Pipeline

[Documentation](../index.md) / [Architecture](index.md) / Parity Pipeline

This is the application flow from one source snapshot to one report.

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
        Q["Emit Machine-Readable Artifacts"]
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

## 1. Prepare The Run

The pipeline resolves:

- the source snapshot id
- the active check profile
- the required input surface
- whether reference results are needed at all

## 2. Read Source Batches

Source rows are streamed from DuckDB in ordered batches. This uses the same pipeline on both the bundled sample and larger snapshots.

## 3. Resolve Reference Results When Needed

If the run needs parity-backed findings or enriched snapshots:

- raw rows are projected into the explicit legacy backend input contract
- cached reference results are reused when possible
- missing results are materialized through persistent legacy backend workers

If the run does not need reference-side data, this branch is skipped.

## Legacy Backend Dependency

Parity-backed runs compare migrated output against artifacts produced by the current legacy backend.

For that reason the pipeline still materializes:

- enriched snapshots from the legacy side
- legacy-emitted check tags from the legacy side

The containerized runtime makes this dependency reproducible across local and CI runs.

## 4. Build Migrated Contexts

The migrated runtime builds normalized contexts from:

- raw rows for `raw_products`
- enriched snapshots for `enriched_products`

## 5. Run The Selected Checks

The shared execution engine runs the selected Python and DSL evaluators and emits migrated findings.

## 6. Compare Reference And Migrated Output

The parity layer normalizes both sides into observed findings and compares them with strict multiset equality over:

- product id
- observed code
- severity

## 7. Emit Artifacts

The completed run produces:

- a static HTML report
- `parity.json`
- `snippets.json`
- a JSON export archive

The report emphasizes exact totals and bounded examples rather than embedding every finding.

[Back to Architecture](index.md) | [Back to Documentation](../index.md)
