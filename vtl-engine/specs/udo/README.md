# Roadmap — User Defined Operators (UDO)

**Location:** `vtl-engine/specs/udo/` (engine module specs).  
**Acceptance:** `UserDefinedOperatorTest` · **Walkthrough:** `UdoPatternWalkthroughTest`

Implementation spec for **User Defined Operators** (VTL 2.1) in Trevas.

## Verdict

**Partial support (P0):** scalars + opaque `dataset` + mixed signatures. Not full VTL-DL.

**Runtime pattern (locked after spike):** define → `UdoDefinition` in bindings + trampoline `Method` in registry → call → `UdoFunctionExpression` (`FunctionExpression`) → `Method.invoke` → body via `ExpressionVisitor`. See [01-architecture](./01-architecture.md).

## Working method (mandatory)

**At every implementation step, start with tests.** Catalog IDs in [09](./09-test-catalog.md) must exist and fail for the right reason before prod for that slice; then implement only what turns those IDs green. Details: [07](./07-testing.md), [10](./10-implementation.md).

```
for each slice:
  1. write / harden JUnit for that slice’s IDs   → red
  2. minimal engine change                         → those IDs green
  3. next slice only when current IDs pass
```

## Status (Aug 2026)

| Step | State |
|------|-------|
| Specs + pattern lock | ✅ |
| Acceptance suite (D/S/DS/E + E8) | ✅ green (DS4 `@Disabled`) |
| Pattern walkthrough | ✅ `UdoPatternWalkthroughTest` |
| Define / invoke / trampoline | ✅ in engine |
| User-facing Docusaurus | ⬜ slice B8 |
| Structured `dataset {…}` (DS4) | ⬜ P1 |

```bash
mvn -pl vtl-engine -Dtest=UserDefinedOperatorTest,UdoPatternWalkthroughTest test
```

## Pattern at a glance

```
define operator add (…) is x + y end operator;
  AssignmentVisitor.visitDefOperator
    → UdoDefineExecutor.define → UdoDefinition
    → bindings.put("add", udo)
    → reject if name in bindings OR native/global registry
    → registerMethod("add", UdoTrampoline.invokeN)

res := add(1, 2);
  GenericFunctionsVisitor.visitCallDataset
    → bindings.get("add") instanceof UdoDefinition
    → UdoInvokeExecutor (defaults / _ / arity)  // needs raw parameter ctx
    → UdoFunctionExpression extends FunctionExpression
    → resolve: Trampoline.enter → Method.invoke → ExpressionVisitor(body) → exit
```

Do **not** route UDOs through `DatasetScalarFunctionExecutor` (no mono-measure lift in P0).  
`invokeFunction` stays native-only — UDO fork stays in `visitCallDataset` (raw `_` args).

## For reviewers (coherence lock)

1. **Artefact** = `UdoDefinition` in bindings (source of truth); trampoline `Method` is dispatch only — [02](./02-model.md)
2. **Invoke** = `FunctionExpression` / `Method.invoke` via `UdoFunctionExpression` — [04](./04-invoke.md)
3. **Collisions** = bindings **or** registry name → reject (E6, E8) — [08 §2](./08-open-questions.md)
4. **Types** = scalars + opaque `dataset`; structured `{…}` not enforced — [06](./06-types.md)
5. **Clause scope** = outer bindings merged into `ClauseVisitor` (scalar params in filter/calc)
6. **Not in P0** = HOF, component/set/ruleset types, constraints, scalar→dataset lift, PE API changes

## In scope (P0)

| Capability | Example |
|------------|---------|
| Typed scalars + `default` + `_` | `add(x integer default 0, y integer default 0)` |
| `returns` or inference | `returns number` / omit |
| Free variables (after DAG) | body uses script-level `y` |
| Opaque `dataset` in/out | filter / calc / union recipes |
| Mixed signatures | `ds dataset, threshold integer` |
| UDO → UDO | body calls another UDO |

## Limitations

| Limitation | Consequence |
|------------|-------------|
| No HOF / predicate injection | no `filterBy(ds, age >= 18)` |
| `ds[keep s]` ≠ dynamic keep | `s` is a literal component name |
| Structured `dataset {…}` | opaque until P1 (DS4) |
| Trampoline uses `Object` arity + ThreadLocal | spike-validated; refine later if needed |
| Trevas call args | `varID\|const\|_` vs official `expr` — orthogonal |

## Current state in Trevas

| Layer | Status |
|-------|--------|
| Parse / DAG | ✅ |
| `UdoDefinition` + `UdoParameter` | ✅ |
| `visitDefOperator` + registry collision | ✅ |
| `UdoTrampoline` + `registerMethod` | ✅ |
| `visitCallDataset` UDO branch | ✅ |
| `UdoInvokeExecutor` + `UdoFunctionExpression` | ✅ |
| Clause outer-bindings merge | ✅ |
| Rich DL types / DS4 | ❌ P1+ |

## Documents

| File | Content |
|------|---------|
| [00-strategy.md](./00-strategy.md) | Why partial |
| [01-architecture.md](./01-architecture.md) | **Locked call path** (bindings + Method + FunctionExpression) |
| [02-model.md](./02-model.md) | `UdoDefinition` |
| [03-define.md](./03-define.md) | Define + register trampoline |
| [04-invoke.md](./04-invoke.md) | Invoke via FunctionExpression |
| [05-phases.md](./05-phases.md) | P0→P3 |
| [06-types.md](./06-types.md) | Type matrix |
| [07-testing.md](./07-testing.md) | Test strategy |
| [08-open-questions.md](./08-open-questions.md) | Locked decisions |
| [09-test-catalog.md](./09-test-catalog.md) | Catalog |
| [10-implementation.md](./10-implementation.md) | Steps aligned on this pattern |

## Definition of Done (P0)

| Principle | How |
|-----------|-----|
| Pattern | bindings artefact + trampoline Method + FunctionExpression body eval |
| Tested | `UserDefinedOperatorTest` green; DS4 skipped; walkthrough green |
| Modular | visitors thin; `semantics/udo/*` owns meaning; no PE API change |
| Docs | this package + optional Docusaurus (B8) |
