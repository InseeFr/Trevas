# Provenance golden test corpus

Hand-authored fixtures for the provenance graph IR. See the specs:
`../specs/20260728_01_provenance.md` and `../specs/20260728_02_provenance-output-and-tests.md`.

Each case is a folder with:

- `input.vtl` — the VTL script, **with its input structures declared inline** as
  `$input` directives (see below), so the case is self-contained.
- `expected.facts` — the golden graph as a flat fact list (the assertion).

No extraction code exists yet; these are pure fixtures that pin down the *output*.

## Inline dataset directives

> Canonical definition: [`../specs/20260729_01_vtl-fixture-directives.md`](../specs/20260729_01_vtl-fixture-directives.md)
> (external sources, `vtl-test-utils` module, TCK vision). This is a quick reference.

Input (and optionally expected-output) structures are declared in VTL comments, so
each fixture is a single self-contained `.vtl`. A directive is `$<keyword> <target>`
inside a `//` or `/* */` comment. The keyword set is **open** — a parser dispatches
on it and ignores unknown directives (forward-compatible). Two are defined:

- `$input <name>` — structure (and optional data) of a binding dataset.
- `$output <name>` — expected structure/data of a result. Orthogonal to
  `expected.facts` (which asserts the provenance *graph*, not the data); handy for
  cases where the result structure is computed (join, aggr).

### One-liner form (structure only — the default)

```vtl
// $input ds1: id STRING IDENTIFIER, var1 INTEGER MEASURE, var2 INTEGER MEASURE
ds2 := ds1;
```

Each column is `name TYPE ROLE`, comma-separated. Columns may carry an optional
trailing `key=value` tail for future attributes (same positional-core +
open-annotations shape as `expected.facts` edges):

```
// $input ds1: id STRING IDENTIFIER, me1 NUMBER MEASURE null=true
```

### Table form (when you want data)

Three header rows (names / types / roles), then optional data rows after a
`|---|` separator — matching the convention in `../docs/model-v1.md`:

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

## `expected.facts` conventions

One `subject predicate object [annotations]` fact per line. Comments (`#`) and
blank lines are cosmetic (stripped before comparison); assertion is set-equality
over the remaining lines. Whitespace between tokens is insignificant.

**Ids (deterministic — no UUIDs):**

| Entity | Id | Example |
|---|---|---|
| binding dataset | `{name}@0` | `ds1@0` |
| dataset produced by statement N | `{name}@{N}` | `ds_res@1` |
| anonymous clause intermediate | `#s{N}.{k}` | `#s1.1` |
| variable instance | `{datasetId}.{comp}` | `ds_res@1.var_sum` |
| expression node | `e{N}.{k}` | `e1.2` |

**Node typing** — `subject a <kind>`, kind ∈ `dataset | variable | expression`.

**Node properties** (lowercase predicate): `dataset` (membership), `role`, `type`,
`src` (defining source fragment), `anon`.

**Edges** — the single relation `dependsOn` (dependent → dependency), with
optional annotations:
- `op=<clause/operator>` — the operation responsible (`assign`, `+`, `calc`,
  `filter`, `rename`, `aggr`, …).
- `role=condition` — a predicate/selector dependency (filter/sub predicate,
  setdiff exclusion, analytic partition/order). Absence = value/operand flow.

**Where expression nodes appear.** Only for *explicitly written scalar
expressions*: `calc`/`aggr`/analytic args and `filter`/`sub` predicates. The
entity they define (`var_sum`, or the filtered dataset) `dependsOn` the
expression node; the expression node `dependsOn` the variables it references
(literals are not nodes at reference-level). Dataset-level operators with no
written per-column expression (assignment, arithmetic, set ops, join) link
instances **directly** with an `op=` annotation — the produced dataset instance
itself is the value of that expression, so no separate node is made.

**Pass-through columns.** A column an operator leaves untouched gets a direct
`dependsOn` to the same-named input column, annotated with the clause `op`.
