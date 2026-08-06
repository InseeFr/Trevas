# 00 — Strategy: partial first, not big-bang

## Recommendation

Ship **P0 (scalar + opaque `dataset` UDO)** first, then structured types and edge cases. Do **not** attempt full VTL-DL type fidelity in one PR.

Opaque `dataset` params/returns are in P0 because real scripts need them — not a deferred P2 luxury.

## Why not all at once

### 1. The hard part is not “define/call”

Parsing and DAG reordering already work (`visitDefOperator`, tests in `DagDefineStatementsTest`). What remains is:

- materializing an operator artefact in bindings
- resolving calls to that artefact before/instead of native method lookup
- argument defaulting (`default`, `_` / `OPTIONAL`)
- evaluating the body in a scoped bindings environment
- typing the signature

That last item explodes in surface area.

### 2. Full type syntax is a separate product

VTL DL allows parameter/return types such as:

- basic scalars (`integer`, `number`, `string`, …)
- constrained scalars (`integer {0,1}`, `number [value >= 0]`)
- `dataset { identifier <string> Id, measure <number> Me }`
- `component` / `measure` / `attribute` / `viral attribute`
- `set <scalarType>`
- `ruleset` / `datapoint_on_variables` / `hierarchical_on_…`
- operator types (`T1 * T2 -> T3`) in the meta-syntax of the reference manual

Trevas today largely works with **Java classes** (`Long`, `Double`, `Dataset`, …) plus `DataStructure` roles. There is no first-class parser/checker for the DL type AST used in UDO signatures. Building that *as a prerequisite* of UDOs would stall the feature for months.

### 3. Invocation grammar is already constrained

In Trevas’ grammar, call arguments are:

```antlr
parameter: varID | constant | OPTIONAL ;
```

So call sites cannot pass arbitrary expressions (`add(1+2, foo*3)`). That is a VTL grammar choice Trevas already mirrors. A “complete” UDO story still lives inside that constraint — another reason not to over-engineer the first cut.

### 4. Runtime backends should stay untouched

Dataset bodies should compile to the same `DatasetExpression` / `ProcessingEngine` path as inline VTL. Scalar bodies should compile to `ResolvableExpression`. **No new `ProcessingEngine` methods** are required for P0–P1. Spark rides along for free if the body only uses already-supported operators.

Touching Spark / PE for UDOs would be a smell that the design leaked across layers.

### 5. Rulesets prove the incremental path

Rulesets shipped with a model object + define visitor + invoke visitor + executor. Types and valuedomain resolution were tightened later. UDOs should copy that cadence.

## What “partial” means concretely

**In P0 we support:**

- `define operator` with **basic scalar** and **opaque `dataset`** params/return (or inferred return)
- optional `default <constant>` (scalars)
- body = any expression already supported by Trevas under the parameter bindings (including dataset ops)
- invocation via existing `operatorID(…)` / `callDataset`
- free variables from outer scope
- collision policy — see [08-open-questions.md](./08-open-questions.md)

**In P0 we explicitly do *not* support (fail clearly):**

- structured enforcement of `dataset {…}` (may parse; full check in P1)
- `component` / `set` / `ruleset` parameter or return types
- scalar constraints (`[…]`, `{…}`, nullability DL modifiers)
- nested `define operator` inside a body
- recursion (default: reject in P1)
- shadowing built-in operator names — reject

Failing with `UnimplementedException` / `InvalidArgumentException` and a precise message is better than silently mis-typing.

## Why this still matches Trevas architecture

Partial support does **not** mean a shortcut that bypasses layers:

- visitor still only dispatches
- `semantics/udo/*Executor` still owns VTL meaning (signature check, defaults, scoped eval)
- mechanical work stays in existing PE / scalar natives

P0 is a **scope cut on the type surface**, not a layering shortcut.

## Success metric for P0

These scripts run end-to-end on in-memory engine (and Spark if the body only uses PE-backed ops):

```vtl
define operator add (x integer default 0, y integer default 0)
   returns number is
      x + y
end operator;

res := add(1);
res2 := add(1, 2);
```

```vtl
y := 4;
define operator max_with_y (x integer)
   returns number is
      if x > y then x else y
end operator;
max_res := max_with_y(2);
```

(Second example already exists as a DAG ordering fixture — it should become an execution fixture.)
