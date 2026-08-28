# 08 — Decisions & pitfalls

Locked for P0 unless marked otherwise. Do not reopen mid-implementation without updating [10](./10-implementation.md).

## 1. Free variable binding time — **DECIDED (P0)**

A free var is a name in the body that is not a parameter. **Invoke-time lookup** in the bindings passed to `resolve` for names not snapshotted at define. After DAG reorder, free vars exist when the call runs. Params shadow outer names.

**P3:** free variables **already bound** when `define operator` runs are snapshotted into `UdoDefinition.closureBindings` and override invoke-time values (S5). Names unbound at define still use invoke-time lookup (S2).

## 2. Name collisions — **DECIDED (P0)**

**Reject** if bindings already contain that key (`AlreadyDefinedException`, E6), **or** if the name matches a known native/global registry entry (E8). No silent overwrite. Parser keywords (`abs`, …) fail parse before this check — E8 uses a plain `IDENTIFIER` pre-registered as a native via `registerMethod` (test-only; production UDOs are **not** registered).

## 3. Recursion — **DECIDED**

**P0:** no guard (may stack-overflow). **P1:** active-name set / call stack → clear error.

## 4. `OPTIONAL` / `_` for natives vs UDOs — **DECIDED (P0)**

Implement `_` for **UDOs only**. Leave native `callDataset` behaviour unchanged (may NPE / error) — out of UDO scope.

## 5. Doc typo `max1 returns boolean` — **DECIDED (P0)**

**Enforce** declared return types. D3 uses corrected type; D4 rejects the literal doc signature.

## 6. Model module purity — **DECIDED (P0)**

Engine-side `UdoDefinition` in bindings (may hold ANTLR `ExprContext`). Keep `vtl-model` free of parser types unless serialization later needs a DTO. **No** `UserDefinedOperator` class in `vtl-model` for P0 — see [02-model](./02-model.md).

## 6a. Invoke: resolve operator id like a variable — **REVISED (review)**

Spike used `registerMethod` + trampoline `Method` + `FunctionExpression` so calls looked like natives. That **goes around the bindings** and makes scoping / closures moot.

Target:

1. Source of truth = `UdoDefinition` in bindings only.
2. Call: resolve `name` in the **current** expression bindings; if `UdoDefinition` → `UdoFunctionExpression` (`ResolvableExpression`); else natives.
3. Fork stays in `visitCallDataset` (raw params for `_`), **not** inside `invokeFunction`.
4. Do **not** use `DatasetScalarFunctionExecutor` for UDOs.
5. No ThreadLocal trampoline — `resolve` evals the body with a child map.
6. Unknown `foo(...)` → `FunctionNotFoundException` (same as natives). Bare `foo` → `UndefinedVariableException` (including free vars in the body).

See [01-architecture](./01-architecture.md), [04-invoke](./04-invoke.md).

## 6b. `integer` ⊆ `number` — **DECIDED (P0)**

Allow via existing `TypeChecking` (D1: `returns number` with integer body). Do not require exact `Long`/`Double` identity for `number` formals / returns. See [06-types](./06-types.md).

## 6c. Closing keyword — **DECIDED (P0)**

Implement `end operator` as in Trevas / SDMX ANTLR (`END OPERATOR`). Ignore RM prose variants such as “end define operator”.

## 6d. Clause outer bindings — **DECIDED (P0)**

`ClauseVisitor` merges outer script bindings into the component expression map so UDO scalar params (`threshold`, `factor`) resolve inside `filter` / `calc`. Components still shadow outer names when equal.

## 7. Component-level calls / `compExpr`

`genericOperatorsComponent` / `callComponent` is already commented out in `Vtl.g4`. UDO calls go through the **normal expression path** (`visitCallDataset` → `ResolvableExpression`). If `compExpr` is removed from the grammar, nothing UDO-specific should break — do not add a component-only hook.

## 8. Evaluation of defaults

Defaults are constants in the grammar (`DEFAULT constant`). No expression defaults in P0 (and not in Trevas grammar). Good.

## 9. Public API / docs tone

Document as **partial support** with a subset table. Avoid claiming VTL-DL completeness.

## 11. Mixed signatures (dataset + scalar, …)

**Grammar: yes.** Each `parameterItem` has its own `inputParameterType` independently (`scalarType | datasetType | …`). Nothing requires homogeneous params. Examples already in P0 tests: DS1 (`dataset` + `integer`), DS5 (`dataset` + `integer`).

**Must support in P0** — falls out naturally once both scalar and opaque `dataset` types are accepted; no separate feature flag.

### Pitfall: `ds[keep s]` is not “keep by string value”

```antlr
keepOrDropClause: (KEEP | DROP) componentID (COMMA componentID)* ;
componentID: IDENTIFIER (MEMBERSHIP IDENTIFIER)? ;
```

In `define operator keep_in_ds (ds dataset, s string) … ds[keep s]`, the `s` after `keep` is a **component identifier literal** (column named `s`), not the runtime value of the string parameter. Dynamic column selection via a `string` param is **not** what this syntax means, and Trevas should not invent that semantics.

Valid mixed uses bind the scalar into an **expression** position, e.g. `ds[filter long1 > threshold]` or `ds[calc long1 := long1 * factor]`.

## 12. No deferred expression / predicate parameters (`filterBy`-style HOFs)

**Wanted pattern (not VTL):**

```vtl
define operator filterBy (ds dataset, cond ???) returns dataset is
   ds[filter cond]
end operator;
out := filterBy(ds, long1 > 10);
```

### Call-site grammar

| Grammar | `parameter` production |
|---------|------------------------|
| **Official SDMX** (`vtl/v2.1/.../Vtl.g4`) | `expr \| OPTIONAL` — arbitrary expressions allowed as args |
| **Trevas** | `varID \| constant \| OPTIONAL` — **stricter dialect** |

So `filterBy(ds, long1 > 10)` **parses in official VTL**, **does not parse in Trevas today**.

Aligning Trevas on `expr` would be a grammar fix (worth considering for compliance), but it still would **not** give HOFs.

### Why it still is not `filterBy`

UDO formal types (`inputParameterType`) are only value types:

`scalar | dataset | set | ruleset | component`

There is **no** expression / predicate / lambda type. Arguments are **evaluated in the caller**, then bound as values. Even with `expr` args:

- `long1 > 10` is computed in the outer scope (needs `long1` as a variable there), yielding a scalar boolean (or error)
- binding that boolean into `ds[filter cond]` only keeps all rows or none — it does **not** inject a per-row predicate into the clause

VTL has no quote/thunk for unevaluated expressions.

### How to do “filterBy” in VTL

1. **Inline clause**: `out := ds[filter long1 > 10];`
2. **UDO with fixed predicate**: body hard-codes the condition
3. **UDO with scalar knobs**: parameterize values, not the predicate AST —  
   `filter_long1_gt(ds, t)` → `ds[filter long1 > t]`
4. **Host-side**: generate VTL text / API outside the language

### Trevas roadmap note

- Document as **language limitation** (no HOF / deferred expr), not only a Trevas gap.
- Optional separate issue: widen Trevas `parameter` to `expr | OPTIONAL` to match SDMX (general call compliance) — **orthogonal** to UDO P0 and still insufficient for `filterBy`.
