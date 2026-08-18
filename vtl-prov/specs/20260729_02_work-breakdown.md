# Provenance refactor — work breakdown for review

> Companion to [`20260728_01_provenance.md`](./20260728_01_provenance.md) (strategy),
> [`20260728_02_provenance-output-and-tests.md`](./20260728_02_provenance-output-and-tests.md)
> (IR + corpus) and [`20260729_01_vtl-fixture-directives.md`](./20260729_01_vtl-fixture-directives.md)
> (fixture format).

When a step lands in the tree, mark it `[x]` in the table. Do not add a
progress paragraph.

| Done | PR | Capability | Turns green |
|------|----|------------|-------------|
| [x] | 1 | Corpus harness (DOT import, `GraphAssert`, SPI stub, golden self-check) | self-check |
| [ ] | 2 | Statement walk + structure oracle (run-once, read bindings) + identity assignment; move SPI out of `ProvenanceTests` | 01 |
| [ ] | 3 | Component-wise dataset ops | 02, 13 |
| [ ] | 4 | Expression nodes (calc) | 03 |
| [ ] | 5 | Condition edges (filter, sub) | 04, 14 |
| [ ] | 6 | Projection & rename | 05, 06 |
| [ ] | 7 | Clause chaining + anonymous intermediates | chain-filter-calc |
| [ ] | 8 | Aggr | 07 |
| [ ] | 9 | Join | 08 |
| [ ] | 10 | Set ops | 09, 10, 11 |
| [ ] | 11 | Analytic | 12 |
| [ ] | 12 | Check/ruleset (resolve provisional schema of 16) | 16 |
| [ ] | 13 | User-defined operators | 17 |
| [ ] | 14 | Pivot + table-form `$input` parsing | 15 |
| [ ] | 15 | RDF view: IR → `Program` → `RDFUtils` (same triples; see [`20260808_01_rdf-compatibility-view.md`](./20260808_01_rdf-compatibility-view.md)) | own tests |
| [ ] | 16 | Delete `ProvenanceListener` / `VariableGraphListener`, migrate `run()` | — |

## Principles

1. **Small units of work** — each PR one concern, independently green.
2. **No complete feature set required** — partial coverage is fine *because it is
   explicit*: an extractor must **fail loudly** ("unsupported: pivot") on syntax
   it does not handle, never emit a plausible-but-wrong graph.
3. **The corpus is the coverage ladder** — every corpus case runs and **fails
   until its extraction is implemented** (no skip/allowlist mechanism: red is the
   honest state, and the failing count is the visible backlog). Every extraction
   PR has the same reviewable shape: *implementation + N cases turning green*.

## Mechanisms

- **Let them fail.** The harness runs every `tests/*/` folder unconditionally.
  Unimplemented cases fail (the stub throws `UnsupportedOperation`); a PR's
  functional delta is the set of cases it turns green. Note: the module's CI
  stays red until extraction lands — acceptable on the feature branch; revisit
  only if it must merge to `develop` before extraction is complete.
- **Extractor SPI.** The harness depends on a minimal interface
  (`ProvenanceExtractor: (script, inputs) → graph`), stubbed to throw
  `UnsupportedOperation` until extraction PRs land. Assertions compare graphs in
  DOT shape (vertex→attrs, edge→attrs, as sets) via `jgrapht-io` — the richer
  `ProvGraph` IR class is *not* needed by the harness and arrives with the first
  extraction PR. The SPI (and a `ProvGraph` → test `Graph` adapter) moves out of
  `ProvenanceTests` in PR-2 — main sources cannot implement a nested test type.
- **Golden self-check.** The harness also lints the corpus itself, with no
  extraction involved: every `expected.dot` imports; every node has `kind`;
  variable `dataset` attrs match id prefixes; edge endpoints are declared nodes;
  `$input` directives parse; datasets used in the golden exist in the directives;
  the script's statements are consistent with the golden's statement indices.
  This runs green from day one and catches hand-maintenance drift.

## Notes

**PR-1** lives in `vtl-prov/src/test` under **`fr.insee.vtl.prov2.tests`**
(`ProvenanceTests`, `Graph`, `GraphAssert`) so legacy `fr.insee.vtl.prov` stays
untouched until PR-16. One-liner `$input` only (table form = PR-14). The
directive parser later migrates to `vtl-test-utils` (spec 20260729_01 §6/§8).
Richer RDF than today's triples is a later view, not PR-15.

## Embedded decisions (flag if you disagree)

- **Oracle in PR-2:** run-once-and-read-bindings (least invasive); a minimal
  engine hook can replace it later without touching the graph layer
  (spec 20260728_01 §structure-oracle).
- **Clause chaining is its own PR (7),** after single clauses — anonymous
  intermediates are the fiddly part and deserve isolated review.
- **Unsupported syntax throws** from day one (principle 2).

## Open questions

- Scalar assignment (`x := 1 + 1;`) — node `kind=scalar`? Not yet modelled in
  spec 20260728_02, and **not** in the corpus. `13-scalar-mult` is
  `ds2 := ds1 * 3` (dataset × literal), covered by PR-3.
- When exactly the fixture parser migrates out to `vtl-test-utils` (after PR-14,
  once table form exists?).
