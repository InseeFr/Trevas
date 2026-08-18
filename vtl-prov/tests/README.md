# Provenance golden test corpus

Hand-authored fixtures for the provenance graph IR. See the specs:
`../specs/20260728_01_provenance.md` and `../specs/20260728_02_provenance-output-and-tests.md`.

Each case is a folder with:

- `input.vtl` — the VTL script, with its input structures declared inline as
  `$input` directives (see below), so the case is self-contained.
- `expected.dot` — the provenance graph as Graphviz **DOT** (the assertion; also
  renders to a picture for review).

## Inline dataset directives

> Canonical definition: [`../specs/20260729_01_vtl-fixture-directives.md`](../specs/20260729_01_vtl-fixture-directives.md)
> (external sources, `vtl-test-utils` module, TCK vision). This is a quick reference.

Input (and optionally expected-output) structures are declared in VTL comments, so
each fixture is a single self-contained `.vtl`. A directive is `$<keyword> <target>`
inside a `//` or `/* */` comment. The keyword set is **open** — a parser dispatches
on it and ignores unknown directives (forward-compatible). Two are defined:

- `$input <name>` — structure (and optional data / source) of a binding dataset.
- `$output <name>` — expected structure/data of a result. Orthogonal to
  `expected.dot` (which asserts the provenance *graph*, not the data); handy for
  cases where the result structure is computed (join, aggr).

### One-liner form (structure only — the default)

```vtl
// $input ds1: id STRING IDENTIFIER, var1 INTEGER MEASURE, var2 INTEGER MEASURE
ds2 := ds1;
```

Each column is `name TYPE ROLE`, comma-separated, with an optional trailing
`key=value` tail for future attributes (`me1 NUMBER MEASURE null=true`) — the same
shape as the DOT attribute lists used in `expected.dot`.

### Table form (when you want data)

Three header rows (names / types / roles), then optional data rows after a
`|---|` separator (matching `../docs/model-v1.md`):

```vtl
/* $input ds1
 * | id         | var1    | var2    |
 * | STRING     | INTEGER | INTEGER |
 * | IDENTIFIER | MEASURE | MEASURE |
 * |------------|---------|---------|
 * | 1          | 10      | 11      |
 */
```

Data is optional — provenance is built from the declared structure, so include
rows only for data-dependent cases (`pivot`/`unpivot`) or executable examples.

## `expected.dot` conventions

Graphviz DOT, imported/compared via `jgrapht-io`. Full spec in
`../specs/20260728_02_provenance-output-and-tests.md` §4. In short:

**Ids (deterministic — no UUIDs), always quoted:**

| Entity | Id | Example |
|---|---|---|
| binding dataset | `{name}@0` | `"ds1@0"` |
| dataset produced by statement N | `{name}@{N}` | `"ds_res@1"` |
| anonymous clause intermediate | `#s{N}.{k}` | `"#s1.1"` |
| variable instance | `{datasetId}.{comp}` | `"ds_res@1.var_sum"` |
| expression node | `e{N}.{k}` | `"e1.2"` |

**Nodes** — `"id" [kind=<k>, …];` with `kind` ∈ `dataset | variable | expression`.
Attributes: variable → `dataset` (membership), `role`, `type`; dataset → `src`,
`anon=true`; expression → `src`.

**Edges** — `"from" -> "to" [op=<clause>, role=condition];`. **Every edge is a
`dependsOn`** (dependent → dependency). `op` names the clause/operator;
`role=condition` marks predicates/selectors (absent = value/operand flow).
Unannotated: `"a" -> "b";`.

**Where expression nodes appear.** Only for *explicitly written scalar
expressions*: `calc`/`aggr`/analytic args and `filter`/`sub` predicates. The
entity they define (`var_sum`, or the filtered dataset) points at the expression
node; the expression node points at the variables it references (literals are not
nodes at reference-level). Dataset-level operators with no written per-column
expression (assignment, arithmetic, set ops, join) link instances **directly**.

**Pass-through columns.** A column an operator leaves untouched gets a direct
`dependsOn` to the same-named input column, annotated with the clause `op`.

Ordering is irrelevant — fixtures are imported and compared as sets, and `//`
comments are ignored, so keep section comments for readability.
