# 08 — Decisions & pitfalls

Locked for P0 unless marked otherwise. Do not reopen mid-implementation without updating [10](./10-implementation.md).

## 1. Free variable binding time — **DECIDED (P0)**

**Invoke-time lookup** in current bindings. After DAG reorder, free vars exist when the call runs; body resolves names dynamically. Params shadow outer names.

## 2. Name collisions — **DECIDED (P0)**

**Reject** if bindings already contain that key, **or** if the name matches a known native/global registry entry. No silent overwrite. Covered by E6 (bindings) and E8 (registry). Note: parser keywords (`abs`, …) fail parse before this check — E8 uses a plain `IDENTIFIER` pre-registered via `registerMethod`.

## 3. Recursion — **DECIDED**

**P0:** no guard (may stack-overflow). **P1:** active-name set / call stack → clear error.

## 4. `OPTIONAL` / `_` for natives vs UDOs — **DECIDED (P0)**

Implement `_` for **UDOs only**. Leave native `callDataset` behaviour unchanged (may NPE / error) — out of UDO scope.

## 5. Doc typo `max1 returns boolean` — **DECIDED (P0)**

**Enforce** declared return types. D3 uses corrected type; D4 rejects the literal doc signature.

## 6. Model module purity — **DECIDED (P0)**

Engine-side `UdoDefinition` in bindings (may hold ANTLR `ExprContext`). Keep `vtl-model` free of parser types unless serialization later needs a DTO. **No** `UserDefinedOperator` class in `vtl-model` for P0 — see [02-model](./02-model.md).

## 6a. Invoke via FunctionExpression / Method trampoline — **DECIDED (P0)**

Validated spike:

1. Source of truth = `UdoDefinition` in bindings.
2. Define also `registerMethod(name, UdoTrampoline.invokeN)`.
3. Call sites build `UdoFunctionExpression` → `Method.invoke` → trampoline re-enters `ExpressionVisitor`.
4. UDO fork stays in `visitCallDataset` (raw params for `_`), **not** inside `invokeFunction`.
5. Do **not** use `DatasetScalarFunctionExecutor` for UDOs (no mono-measure lift in P0).
6. Trampoline may use ThreadLocal CallSite in P0; refine later if needed.

See [01-architecture](./01-architecture.md), [04-invoke](./04-invoke.md).

## 6b. `integer` ⊆ `number` — **DECIDED (P0)**

Allow via existing `TypeChecking` (D1: `returns number` with integer body). Do not require exact `Long`/`Double` identity for `number` formals / returns. See [06-types](./06-types.md).

## 6c. Closing keyword — **DECIDED (P0)**

Implement `end operator` as in Trevas / SDMX ANTLR (`END OPERATOR`). Ignore RM prose variants such as “end define operator”.

## 6d. Clause outer bindings — **DECIDED (P0)**

`ClauseVisitor` merges outer script bindings into the component expression map so UDO scalar params (`threshold`, `factor`) resolve inside `filter` / `calc`. Components still shadow outer names when equal.

## 7. Component-level calls

Commented grammar (`genericOperatorsComponent` / `callComponent`) suggests component-level UDO invoke is unfinished upstream. **Out of scope** until grammar is revived.

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
