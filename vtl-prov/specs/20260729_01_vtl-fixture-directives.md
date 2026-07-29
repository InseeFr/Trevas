# VTL fixture directives (`$input` / `$output`)

> Extracted from the provenance corpus work
> ([`20260728_02_provenance-output-and-tests.md`](./20260728_02_provenance-output-and-tests.md) §3).
> That corpus is the first *consumer*; this document owns the directive format so
> it can grow into a shared `vtl-test-utils` module and, eventually, a VTL **TCK**
> (Technology Compatibility Kit).

## 1. Motivation

A VTL test case needs a script **and** the structure/data of its inputs (and
often the expected outputs). Keeping those in side files (`structure.json`, CSVs)
scatters a case across the filesystem and couples it to one harness.

Instead, declare everything **inline in the `.vtl` itself**, inside comments, so
a fixture is a single self-contained, engine-agnostic file:

```vtl
// $input ds1: id STRING IDENTIFIER, var1 INTEGER MEASURE, var2 INTEGER MEASURE
ds2 := ds1[calc var_sum := var1 + var2];
```

Because the declarations live in comments, the file is still valid VTL: any
engine can run it, and any tooling can read the directives. This unlocks:

- **provenance corpus** — inputs + `expected.facts` (the current use);
- **engine unit tests** — inputs + `$output`, asserted by the engine test-suite;
- **cross-engine conformance (TCK)** — the same fixtures run against any VTL
  engine to demonstrate compatibility;
- **executable docs** — examples that are guaranteed to run.

## 2. The directive system

A directive is `$<keyword> <target> …` appearing in a `//` line comment or a
`/* … */` block comment. The **keyword set is open**: a parser dispatches on the
keyword and **ignores unknown directives** (forward-compatible — an older parser
tolerates a newer fixture). Defined keywords:

| Keyword | Meaning |
|---|---|
| `$input <name>` | structure (and optional data / source) of a binding dataset |
| `$output <name>` | expected structure (and optional data) of a result |

Reserved for later (illustrative, not yet specified): `$engine`, `$note`,
`$expect`, `$define`.

`$input` and `$output` share the **same grammar** — they differ only in meaning
(a supplied binding vs. an expected result). Everything in §3–§4 applies to both.

## 3. Declaring structure

```
directive := '$' KEYWORD name [':' structure] ['=' source]
```

At least one of `structure` / `source` must be present.

### 3.1 One-liner (structure only — the default)

```vtl
// $input ds1: id STRING IDENTIFIER, var1 INTEGER MEASURE, var2 INTEGER MEASURE
```

- `structure` is a comma-separated list of columns.
- A column is `name TYPE ROLE`, followed by an **optional trailing `key=value`
  tail** for future attributes — the grammar never changes to add one:

```
// $input ds1: id STRING IDENTIFIER, me1 NUMBER MEASURE null=true domain=positive
```

`TYPE` ∈ VTL scalar types (`STRING`, `INTEGER`, `NUMBER`, `BOOLEAN`, `DATE`, …).
`ROLE` ∈ `IDENTIFIER | MEASURE | ATTRIBUTE`.

> The `positional core + open key=value tail` shape is deliberately the **same**
> as the provenance `expected.facts` edge annotations — one mental model across
> the whole test substrate.

### 3.2 Table (structure + optional data)

Three header rows — names / types / roles — then optional data rows after a
`|---|` separator (matching `../docs/model-v1.md`):

```vtl
/* $input ds1
 * | id         | var1    | var2    |
 * | STRING     | INTEGER | INTEGER |
 * | IDENTIFIER | MEASURE | MEASURE |
 * |------------|---------|---------|
 * | 1          | 10      | 11      |
 * | 2          | 11      | 10      |
 */
```

Data is optional: provenance is built from declared structure, so include rows
only for data-dependent operators (`pivot`/`unpivot`) or executable examples.

## 4. External sources (the extensible part)

`= <source>` loads a dataset from outside the comment. `:` introduces an inline
structure, `=` a source; both may appear (use the inline structure to supply
roles/types a raw source lacks):

```vtl
// $input ds1 = ./data/ds1.csv
// $input ds1: id STRING IDENTIFIER, var1 INTEGER MEASURE = ./data/ds1.csv
```

Source forms (paths are resolved relative to the fixture file). The set is
**pluggable** — resolved by scheme/extension, so new loaders need no grammar
change:

| Source | Example | Notes |
|---|---|---|
| CSV | `= ./ds1.csv` | header = names; types/roles from inline structure or inference |
| Parquet | `= ./ds1.parquet` | carries its own schema |
| JSON | `= ./ds1.json` | array-of-objects |
| SDMX DSD | `= sdmx(./DSD_BPE.xml, BPE_DETAIL)` | structure from a DSD (reuses `vtl-sdmx`) |
| _(future)_ | `= gen(rows=1000, seed=1)` | generated data |

Disambiguation is unambiguous because sources sit after `=`; a bare `:` body is
always an inline structure.

## 5. Parsing rules (for implementers)

1. Scan the source for comment spans (`//…`, `/* … */`); strip the leading ` * `
   continuation marker in block comments.
2. Within comment text, find lines/blocks beginning with `$KEYWORD`.
3. Dispatch on keyword; **ignore unknown keywords** (log at debug).
4. Parse `name`, optional `: structure`, optional `= source`.
5. `structure` is one-liner (comma list) or, if the directive is a block comment
   containing a markdown table, the 3-row table (+ optional data rows).
6. The runnable script is simply the file — comments are ignored by the VTL
   engine, so no stripping is required to execute it.

Determinism: directive order does not matter; a fixture with N `$input`s yields a
name→dataset map.

## 6. `vtl-test-utils` module (sketch)

A small, dependency-light module that turns a fixture file into runnable inputs
and assertions. Illustrative API:

```java
VtlFixture fixture = VtlFixture.parse(Path.of("03-calc/input.vtl"));

Map<String, Dataset> inputs = fixture.inputs();      // materialized from $input
String script            = fixture.script();          // the file (or directives stripped)
Map<String, Dataset> exp = fixture.expectedOutputs(); // from $output, if present

// run against any engine
inputs.forEach((n, ds) -> engine.put(n, ds));
engine.eval(script);

fixture.assertOutputs(engine);   // compares results to $output structures/data
```

- **Loaders** for `= source` are registered by scheme/extension (CSV, Parquet,
  SDMX…), so the module is open to new formats.
- Structure-only `$input` (no data) materializes an **empty** dataset with the
  declared schema — enough for structure-level checks (and for provenance).
- Lives alongside `vtl-model`/`vtl-engine`; depends on neither a specific
  processing engine nor a specific assertion framework.

## 7. TCK vision

A **TCK** is a language-agnostic corpus of these fixtures plus a thin runner SPI
an engine implements. Because each fixture is self-describing, the same files can
certify any VTL engine:

- **Conformance profile** — `$input` + `$output`: run the script, assert the
  result equals `$output`. Demonstrates operator-level compatibility.
- **Provenance profile** — `$input` + `expected.facts`: assert the emitted
  provenance graph (Trevas-specific for now; the fixture format is shared).

Fixtures are grouped by operator/feature (mirroring the provenance corpus and the
VTL operator catalogue) so coverage is legible and gaps are obvious.

## 8. Relationship to the provenance corpus

The directive format is the shared substrate; assertions layer on top:

- provenance → `expected.facts` (the `dependsOn` graph);
- conformance → `$output` (result data/structure).

`vtl-prov/tests/` is the first corpus. When `vtl-test-utils` exists, the parser
and loaders move there and the provenance tests depend on it.

## 9. Open questions

- **Type/role inference** for raw CSV/JSON when no inline structure is given —
  infer, or require an explicit structure? (Lean: require, to stay deterministic.)
- **Data literal syntax** in tables — how to write null, dates, decimals, empty
  string unambiguously (e.g. `_` or `null` for null?).
- **`$output` equality semantics** — set vs ordered rows; float tolerance;
  identifier-keyed comparison.
- **Multiple statements / intermediate outputs** — may a fixture assert on a
  transient dataset, or only persistent/final ones?
- **Module home** — new `vtl-test-utils` module vs. a test-scoped package first,
  promoted once stable.
- **Directive vs. VTL-DL** — should `$input` structures ever be expressed with
  real VTL `define structure` instead of a comment DSL? (Comment DSL keeps
  fixtures engine-agnostic and lets us carry data; VTL-DL cannot carry data.)
