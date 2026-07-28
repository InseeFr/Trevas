# Provenance golden test corpus

Hand-authored fixtures for the provenance graph IR. See the specs:
`../specs/20260728_01_provenance.md` and `../specs/20260728_02_provenance-output-and-tests.md`.

Each case is a folder with:

- `input.vtl` — the VTL script.
- `structure.json` — declared structure of every input (binding) dataset, so the
  case is self-contained and deterministic.
- `expected.facts` — the golden graph as a flat fact list (the assertion).

No extraction code exists yet; these are pure fixtures that pin down the *output*.

## `structure.json`

Object keyed by dataset name; each component gives its `role` and `type`:

```json
{
  "ds1": {
    "id":   { "role": "IDENTIFIER", "type": "STRING" },
    "var1": { "role": "MEASURE",    "type": "INTEGER" }
  }
}
```

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
