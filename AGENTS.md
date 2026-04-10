# AGENTS.md

## Mission

This repository exists to migrate Open Food Facts data quality checks from legacy Perl into a modern Python system without losing behavioral fidelity.

The project combines LLM-assisted migration, strict parity validation against the legacy backend, and new Canada specific rules. The goal is a reusable, testable validation framework that preserves trusted logic, supports local DuckDB-based analysis, and leaves behind a repeatable migration blueprint.

When rules conflict, optimize in this order:

1. behavioral correctness
2. parity with the legacy system where parity is expected
3. the cleanest architecturally sound solution that fits the real problem
4. readability and maintainability
5. reuse across the shared runtime, library surface, and migration tooling

## Role Of This File

Use this file as the default operating guide for agents working in this repository. It exists to reduce unnecessary exploration, keep changes inside the right boundaries, and make validation expectations explicit. Keep it concise and operational, not a second copy of the documentation.

## Canonical Sources

Use the right source of truth for the right question.

- Code, tests, executable config, `Makefile`, `pyproject.toml`, and CI definitions are the operational source of truth.
- `docs/reference/glossary.md` is the source of truth for terminology and naming.
- `docs/explanation/` and `docs/how-to/` are the sources of truth for architecture and workflow details.
- `AGENTS.md` is the source of truth for agent behavior expectations and repository-specific working rules.

If sources diverge, follow the more specific canonical source for the question at hand and treat executable repository state as higher priority than prose.

## Execution Checklist

Before making a non-trivial change:

1. Identify the correct ownership boundary.
2. Define the clean solution before touching code.
3. Widen scope only if it removes a real structural problem.
4. Use the correct execution environment, especially the repository local `venv` for Python work.
5. Validate in proportion to real risk.
6. Update affected documentation when behavior or contracts changed.
7. Report what was run, what was skipped, and why.

## Core Principles

- Choose the cleanest, simplest solution that fully solves the real problem, not the fastest patch.
- Favor readability over cleverness, dense idioms, or extreme compactness.
- Keep responsibilities separated. Prefer high cohesion, low coupling, explicit contracts, and protected invariants.
- Avoid unnecessary duplication, but do not abstract too early.
- Make failures surface early and clearly.
- Prefer small, incremental, reversible changes when they still support the correct design.
- Widen scope enough to remove local hacks or clean up nearby debt the change must rely on, but do not turn a focused task into a broad refactor without a clear reason.
- Do not ship hacky workarounds or knowingly increase technical debt unless the user explicitly asks for a temporary stopgap.
- Measure before optimizing. Do not sacrifice clarity for irrelevant micro-optimizations.

## Performance And Complexity

Keep computational complexity in mind whenever writing or reshaping logic.

- Know the rough order of growth of the code you introduce.
- Reason in terms of expected input scale, not only tiny examples.
- Reduce asymptotic complexity before chasing constant-factor wins.
- Choose data structures deliberately and avoid repeated work or unnecessary nested scans.
- Consider I/O, allocations, database work, external processes, caching, memoization, and precomputation only in terms of the real workload.
- Reassess prior assumptions when data scale changes materially.

## Repository Shape And Boundaries

High-level ownership:

- `src/off_data_quality/`: reusable library surface, runtime, packaged checks, contracts, and shared APIs
- `migration/`: orchestration, source loading, reference loading, strict comparison, artifacts, and reporting
- `apps/google_sheets/`: Google Sheets browser workflow and app-specific packaging
- `ui/`: shared templates, static assets, and rendering helpers for report and app entrypoints
- `tests/`: regression and behavioral test suite
- `config/`: runtime and check selection configuration
- `docs/`: explanation pages, how-to guides, reference documentation, and project notes
- `scripts/`: repository utilities such as DSL validation
- `artifacts/`: generated output, not maintained by hand source

Important subareas:

- `migration/source/`: source snapshot loading and validation
- `migration/run/`: run preparation, batching, orchestration, profiles, scheduling, accumulation, serialization
- `migration/reference/`: reference loading, normalization, and caching for the legacy baseline
- `migration/legacy_source.py`: shared legacy source analysis for report snippets and inventory export
- `migration/parity/`: strict comparison logic
- `migration/report/`: report rendering and output shaping
- `src/off_data_quality/checks/`: packaged Python and DSL checks plus shared metadata

Boundary rules:

- Keep reusable execution behavior in `src/off_data_quality/`. Keep source loading, legacy backend integration, reference loading, strict comparison, and report rendering in `migration/`.
- Treat checks as packaged definitions, not ad hoc migration or app logic.
- Preserve the check metadata model. Be careful with `parity_baseline`, jurisdictions, supported surfaces, required context paths, and `legacy_identity`.
- Respect the normalized context contract. Extend it intentionally instead of introducing one off helper only shapes.
- Keep configuration based check selection in `config/check-profiles.toml` or the profile loading path, not in scattered conditionals.
- Keep artifact formats stable unless the change explicitly intends to evolve them.
- Keep Canada specific behavior clearly scoped.
- Use the DSL for readable boolean predicates over approved normalized fields. Use Python when the logic needs loops, helper driven logic, aggregation, multi step reasoning, or dynamic emitted codes.
- Treat changes to contracts, context construction, or check metadata as cross-cutting work.

## Working Defaults

- Python target: 3.14
- Main toolchain: `ruff`, `pytest`, `coverage`, `mypy`, `vulture`, `pyright`, `jscpd`
- For repository Python work, prefer the repository local virtual environment first.
- If `.venv` exists, use its executables such as `.venv/bin/python`, `.venv/bin/pip`, `.venv/bin/pytest`, and related tools.
- Do not install packages into system Python or rely on the global interpreter for repository work.
- If no suitable local virtual environment exists, stop and ask how to proceed rather than falling back to system Python by default.
- When creating commits, use Conventional Commits format without a scope. Example: `feat: add reference cache observer`.
- Docker is the preferred path for runs that may need reference results and for validation that spans multiple components.

## Branch And PR Workflow

- Do not push directly to `main`. Work on a topic branch and open a pull request against `main`.
- `main` uses squash merges. Treat the pull request title as the final Conventional Commit message without a scope, and expect local topic branches to need `git branch -D` after merge because their original commits are not ancestors of `main`.
- `AGENTS.md` is local agent guidance in this workspace. Do not add or commit it unless the user explicitly asks for that.

Canonical commands:

```bash
cp .env.example .env
docker compose up --build
```

```bash
cd apps/google_sheets
cp .env.example .env
docker compose up --build
```

```bash
.venv/bin/python -m pip install -e ".[app,dev]"
```

```bash
make quality
.venv/bin/pytest -q tests/test_some_area.py
```

## Validation Policy

Use targeted validation while iterating. Before calling non-trivial work done, run the full repository toolchain unless there is a concrete reason you cannot. The default completion target for any non-documentation change is `make quality`.

Rules:

- `pytest` is the floor, not the finish line. Completed work should end with a fully green pass across both gated and non-gated tooling.
- The only routine exception to `make quality` is a purely documentation-only diff that touches only Markdown or other non-executable guidance files, cannot affect runtime behavior, tests, typing, packaging, executable configuration, or tooling behavior, and is validated by checking internal consistency plus affected file paths, commands, and references; if skipped, say explicitly that `make quality` was skipped because the diff was documentation-only.
- If a change can affect end to end migration flow, reference loading, strict comparison, report generation, artifact production, or other runtime wiring that spans multiple components, run the Docker-based flow before calling the work done.
- If the full toolchain or Docker flow is skipped, say exactly what was skipped and why.
- Do not run `make clean` or other cache-clearing commands by default.
- Never add `noqa`-style directives, blanket ignores, or weakened tool settings just to get a green result. If a suppression appears genuinely necessary, stop and ask for approval with the rationale.

## Terminology And Naming

`docs/reference/glossary.md` is the source of truth for terminology and naming. Follow it instead of duplicating or improvising vocabulary here, and do not introduce casual synonyms for established concepts.

Preserve these conceptual distinctions:

- `reference` is the parity side runtime data and support layer
- `legacy backend` is the Perl execution boundary
- `legacy` is provenance or raw legacy-emitted codes, not a generic name for reference side runtime payloads
- `migrated` is the Python side of parity findings, outputs, and implementation behavior
- `implementation` is current-repository code provenance in snippet artifacts

- `DSL` is preferred in prose
- `dsl` stays lowercase in technical names and literal values
- If a new name blurs these boundaries, rename it.

## Documentation And Natural Language

Keep code, comments, docs, and agent guidance aligned with reality.

- If a confirmed code change affects observable behavior, contracts, workflows, outputs, architecture, or naming, update the relevant documentation in the same task, including `docs/`, `README.md`, comments, docstrings, or `AGENTS.md` when relevant.
- Do not leave behind comments or docstrings that describe behavior that is no longer true.
- Do not update documentation speculatively for changes that are not actually being kept.
- Prefer small, targeted updates in the canonical location rather than scattering overlapping explanations.

When writing natural language anywhere in the repository, optimize for signal and keep the prose human, minimal, and clear.

- Treat signal as the information that helps the reader understand behavior, intent, constraints, decisions, or action.
- Treat noise as anything that can be removed, shortened, or simplified without losing useful information.
- Aim for near-zero noise. In the ideal case, the text carries only signal.
- Remove not only disposable sentences, but also disposable phrasing inside sentences.
- If a sentence can say the same thing with less noise, rewrite it.
- Keep prose compact, but not choppy or staccato. Allow only enough connective tissue to keep the text natural and readable.
- Prefer direct, specific, situated language, precise nouns and verbs, and comments that explain reasons, constraints, or edge cases.
- Prefer short, nominal headings that name the subject or action.
- Prefer ordinary, non-hyphenated phrasing when it is equally clear or clearer. Use hyphenated compounds only when the hyphenated form is standard, necessary, or materially clearer.
- Prefer the simplest clear phrasing over idiomatic, figurative, or stylistically embellished language.
- Prefer prose over lists unless the content is genuinely list-shaped.

Avoid:

- rhetorical flourishes, constructed contrasts, or theatrical emphasis
- idiomatic, figurative, poetic, ornate, or rhetorically elevated phrasing when a simpler sentence says the same thing
- neat binary framing such as `not X but Y`, mirrored phrasing such as `both X and Y`, or `on the one hand / on the other hand`
- overly perfect parallelism, recurring triads, or long chains of coordinated items in one sentence
- metadiscourse, filler, template phrases, pseudo-academic tone, generic wording detached from the actual code, repeated sentence templates, or canned conclusions
- explanatory or sentence-like headings when a shorter heading is sufficient
- colon-heavy writing, dash-heavy parentheticals, frequent parenthetical asides, stylistic or unnecessary hyphenated compounds, or punctuation used as scaffolding more than meaning

## Scope And Escalation

Prefer the clean solution within a controlled scope.

- It is good to widen the change slightly when that is the only clean way to avoid a workaround in the touched area.
- It is good to pay down nearby technical debt when doing so keeps the result simpler, safer, and easier to maintain.
- It is not good to turn a focused task into a broad refactor unless the broader work is required for correctness or design integrity.
- Favor proportional cleanup. Fix the structure the change must rely on, not every imperfect thing nearby.

Stop and ask for guidance when:

- multiple clean designs are possible and the tradeoff is long-lived or product-defining
- the best fix requires widening the scope substantially beyond the original task
- a deeper structural issue is real, but the right direction is not objectively clear from repository context
- the requested change appears to require knowingly increasing technical debt
- a suppression, tooling exception, or other policy escape hatch seems necessary

When escalating, explain:

- what the structural issue is
- what the clean options are
- which option you recommend and why
- what decision is needed before proceeding

## Independent Reasoning

The user makes the final decision. The agent should still provide an autonomous, evidence-based recommendation.

- Reason from the codebase, tests, repository rules, and software engineering judgment.
- Do not assume the user is right just because they questioned a design choice.
- Do not mirror the user's framing if the evidence points elsewhere.
- Explain your reasoning clearly, including when you disagree with a proposed direction.
- Remain open to correction without giving up independent judgment.

## Useful Reference Docs

- `README.md`
- `docs/index.md`
- `docs/reference/glossary.md`
- `docs/how-to/run-the-project-locally.md`
- `docs/how-to/author-checks.md`
- `docs/how-to/validate-changes.md`
- `docs/explanation/system-architecture.md`
- `docs/explanation/migration-runs.md`
- `docs/explanation/migrated-checks.md`
- `apps/google_sheets/README.md`
- `docs/reference/check-metadata-and-selection.md`

## Maintenance Notes

- Add guidance only if it materially changes agent behavior or prevents recurring mistakes.
- Prefer compressing or deleting stale sections over growing the file indefinitely.
- Do not duplicate long-form architecture, project status, or glossary content that already lives elsewhere in `docs/`.
