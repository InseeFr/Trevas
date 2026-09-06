# Provenance — work breakdown

> Companion to [`20260728_01_provenance.md`](./20260728_01_provenance.md) (strategy),
> [`20260728_02_provenance-output-and-tests.md`](./20260728_02_provenance-output-and-tests.md)
> (IR + corpus + operator catalogue §6) and
> [`20260729_01_vtl-fixture-directives.md`](./20260729_01_vtl-fixture-directives.md)
> (fixture format).

When a step lands in the tree, mark it `[x]` in the table. Do not add a
progress paragraph.

Goal: **full VTL-ML/DL coverage** per the catalogue in spec 02 §6 — every
operator either has a corpus golden + extraction, or is explicitly structural
(no provenance). Until a row is green, the extractor must **fail loudly**
(`unsupported: …`), never invent lineage.

Two phases:

1. **Architecture** (PR 1–16) — IR, harness, corpus ladder 01–17, RDF view,
   retire listeners. **Done.**
2. **Completeness** (PR 17–40) — full catalogue + BPE + rich RDF + UDO
   inlining + fixture tooling. Same shape: *fixture(s) + implementation →
   green*. Provenance coverage is **independent of Trevas execution support**:
   if the engine throws, the extractor still derives structure/lineage (oracle
   failure → full `PendingOp` derive, same as pivot).

## Phase 1 — Architecture (done)

| Done | PR | Capability | Turns green |
|------|----|------------|-------------|
| [x] | 1 | Corpus harness (DOT import, `GraphAssert`, golden self-check) | self-check |
| [x] | 2 | `VtlBaseVisitor<Void>` + structure oracle + identity assignment | 01 |
| [x] | 3 | Component-wise dataset ops (`+`, `*`, …; scalar literals not nodes) | 02, 13 |
| [x] | 4 | Expression nodes (calc) | 03 |
| [x] | 5 | Condition edges (filter, sub) | 04, 14 |
| [x] | 6 | Projection & rename | 05, 06 |
| [x] | 7 | Clause chaining + anonymous intermediates | chain-filter-calc |
| [x] | 8 | Aggr | 07 |
| [x] | 9 | Join (empty body) | 08 |
| [x] | 10 | Set ops | 09, 10, 11 |
| [x] | 11 | Analytic | 12 |
| [x] | 12 | Check/ruleset (`check_datapoint` … `all`) | 16 |
| [x] | 13 | User-defined operators (black-box) | 17 |
| [x] | 14 | Pivot + table-form `$input` | 15 |
| [x] | 15 | RDF view: IR → `Program` → `RDFUtils` | own tests |
| [x] | 16 | Delete listeners; `Provenance.run` | — |

## Phase 2 — Full VTL coverage

Index: spec 02 §6. Corpus case ids below are **new** folders under `tests/`
unless noted. Add a §5 lineage rule before or with the first PR that needs it.

### Wave A — Scalar expressions (reference-level)

Most of §6.5–6.6 does **not** need new `PendingOp` variants: the enclosing
expression node already `dependsOn` referenced variables. The work is
SupportCheck + RHS walk (allow the AST; still forbid nested dataset clauses).

| Done | PR | Capability | Turns green |
|------|----|------------|-------------|
| [x] | 17 | Scalar-expr framework: recursive allow-list in calc/filter/sub/aggr args; `cast`; `if`/`nvl`/`case` | 18-cast-if |
| [x] | 18 | String ops in expressions (`substr`, `trim`/`||`, `replace`, `instr`, `length`, …) | 19-string |
| [x] | 19 | Numeric *functions* + comparison helpers (`abs`/`round`/…, `between`, `isnull`, `in`/`not_in`, `match_characters`) | 20-numeric-cmp |
| [x] | 20 | Date/time *scalars* in expressions (`datediff`, `getyear`, …) | 21-datetime-scalar |

### Wave B — Remaining dataset producers (clauses / ops)

| Done | PR | Capability | Turns green |
|------|----|------------|-------------|
| [x] | 21 | Dataset-level component-wise (§5.13 complete): `abs(ds)`, boolean/`\|\|` between datasets, dataset `if`/`nvl` | 22-ds-scalar |
| [x] | 22 | Membership `#` (component → mono-measure dataset) | 23-membership |
| [x] | 23 | `unpivot` + `customPivot` (same producer family; pure derive OK) | 24-unpivot, 32-custom-pivot |
| [x] | 24 | `apply` clause | 25-apply |
| [x] | 25 | Join **body** (filter/calc/keep/drop/rename/aggr inside join) | 26-join-body |
| [x] | 26 | Join kinds gap-fill (`full_join`, `cross_join`) if not already green via 08 | 27-join-kinds |
| [x] | 27 | Aggr gaps: `having`, `group except` / `group all` as needed by Trevas | 28-aggr-having |

### Wave C — Validation & definitions

| Done | PR | Capability | Turns green |
|------|----|------------|-------------|
| [x] | 28 | `check_datapoint` output modes `invalid` / `all_measures` (schema + edges) | 29-check-modes |
| [ ] | 29 | Simple `check` (validation) | 30-check-simple |
| [ ] | 30 | `define hierarchical ruleset` + `hierarchy` + `check_hierarchy` | 31-hierarchy |
| [x] | 31 | `define structure` — **skipped**: not in Trevas `Vtl.g4` (stay unsupported until parser) | — |

### Wave D — Time-series & misc producers

| Done | PR | Capability | Turns green |
|------|----|------------|-------------|
| [ ] | 32 | Time-series producers (`fill_time_series`, `flow_to_stock`, `stock_to_flow`, `timeshift`, `time_agg`) | 33-timeseries |
| [ ] | 33 | `exists_in` (dataset-level producer) | 34-exists-in |
| [ ] | 34 | `eval` (external routine, black-box like UDO) | 35-eval |
| [ ] | 35 | Scalar assignment (`x := 1+1`) — IR `kind=scalar` + implement | 36-scalar-assign |
| [x] | 36 | Distance / leftover grammar ops after A–D (audit `Vtl.g4` vs SupportCheck); known gap `symdiff` | 37-symdiff |

### Wave E — Integration & complete surface

| Done | PR | Capability | Turns green |
|------|----|------------|-------------|
| [ ] | 37 | Re-enable `RDFTest.bpeTest` (BPE end-to-end via `Provenance.run`) | bpe RDF |
| [ ] | 38 | Richer RDF view (`wasDerivedFrom` / expression nodes; extend beyond minimal SDTH) | own tests |
| [ ] | 39 | UDO body inlining (walk body; replace black-box §5.17) | own tests + 17-udf |
| [ ] | 40 | Migrate `InputDirectives` → `vtl-test-utils` | harness still green |

**Done when:** SupportCheck has no intentional gaps against `Vtl.g4` statement /
expression / function alternatives in the catalogue; every §6 producer/scalar
has a green corpus (or structural no-op); BPE RDF green; rich RDF + UDO
inlining landed; unknown syntax still throws (forward-compat). Engine
unsupported-ops are not an excuse to leave provenance red.

## Why this cut

- **Wave A first** — BPE (and most real scripts) fail on `cast` / `substr` / `if`
  inside calc/filter long before they need hierarchy or time-series. Same IR
  model (expression nodes); cheap lineage, high unblocking value.
- **Join body separate from empty join** — empty-body is done (PR-9); body is
  clause-chaining *inside* a multi-operand frame (anonymous intermediates +
  multiple parents). Deserves its own review.
- **Check modes ≠ new check family** — `invalid`/`defaults` change output
  schema; keep isolated from hierarchical validation.
- **Time-series / hierarchy / eval** after the common ML surface — rarer in
  day-to-day scripts, heavier structure rules (spec 02 §6 still says “add”).
- **BPE before rich RDF / UDO inline** — integration gate on the minimal SDTH
  path; 38–39 deepen the same surface. Unit goldens remain the coverage ladder.
- **Engine ≠ provenance** — `unpivot`, hierarchy, time-series, etc. may throw at
  eval; goldens still assert derived IR (oracle catch → full derive).

## Principles

1. **Small units of work** — each PR one concern, independently green.
2. **Fail loudly until covered** — partial coverage during the climb is fine
   *because it is explicit*; never emit a plausible-but-wrong graph.
3. **The corpus is the coverage ladder** — every `tests/*/` folder runs; red
   count = backlog. Shape: *implementation + N cases turning green*.
4. **Catalogue is the definition of “tout”** — spec 02 §6; grammar audit (PR-36)
   catches drift vs `Vtl.g4`. Execution support in `vtl-engine` is orthogonal.
5. **Corpus pattern every coverage PR** — `tests/NN-slug/{input.vtl,expected.dot}`;
   §5 rule added with the PR; `own tests` only for RDF/view layers.

## Mechanisms

- **Let them fail.** The harness runs every `tests/*/` folder unconditionally.
- **Extractor.** `ProvenanceExtractor.extract(script, inputs) → ProvGraph`.
  Public SDTH path: `Provenance.run` → extract → `SdthProgramView` → `RDFUtils`.
- **Grammar walk.** Parse → SupportCheck (`ScriptSymbols`) → structure oracle →
  `ProvenanceVisitor`. `PendingOp` + `StructureDeriver` / `EdgeLinker`.
- **Golden self-check.** Fixture lint without extraction (from PR-1).

## Notes (phase 1)

**PR-1…16** as above. Entry point: `fr.insee.vtl.prov.Provenance.run`. IR package:
`fr.insee.vtl.prov2`. `$input` parsing: `InputDirectives` (→ `vtl-test-utils` in
PR-40).

## Embedded decisions (flag if you disagree)

- **Walk: `VtlBaseVisitor<Void>` + mutable `ProvGraph`.**
- **Sealed `PendingOp` + `StructureDeriver` / `EdgeLinker`.**
- **`ScriptSymbols`** filled once at support-check.
- **Oracle:** run-once-and-read-bindings; on eval failure derive whole LHS from
  `PendingOp` (never mix).
- **Scalar ops in expressions** stay reference-level (one expression node, not
  full AST) unless a later PR explicitly deepens granularity.
- **Unsupported stems** stay stable: `define`, `scalar`, `arithmetic`, `clause`,
  `calc`, `aggr`, `join`, `set`, `check`, `functions` (extend only when a new
  family needs a distinct stem).

## Locked product decisions (2026-09-06)

1. **Périmètre = catalogue §6 + grammar audit**, même si Trevas throw à l’exéc.
   Oracle failure → pure derive (pattern pivot).
2. **Assignation scalaire** → IR `kind=scalar` (PR-35); pas d’ignore.
3. **`customPivot`** → même famille que `unpivot` (PR-23).
4. **Wave E entière obligatoire** — BPE + RDF riche + UDO inlining + migrate
   `$input` (PR 37–40). Pas de polish « optionnel » sur ce chemin.
