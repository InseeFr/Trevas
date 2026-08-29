# Roadmap — User Defined Operators (UDO)

**Location:** `vtl-engine/specs/udo/`  
**Acceptance:** `UserDefinedOperatorTest`

Implementation spec for **User Defined Operators** (VTL 2.1) in Trevas.

## Verdict

**Partial support (P0):** scalars + opaque `dataset` + mixed signatures. Not full VTL-DL.

**Runtime pattern:** define → `UdoDefinition` in bindings only → call resolves the operator id like a variable → `UdoFunctionExpression` (`ResolvableExpression`) evals the body. No trampoline `Method` in the native registry. See [01-architecture](./01-architecture.md).

## Working method

**Tests first.** Unit-test a hardcoded `UdoDefinition` / `UdoFunctionExpression` before more visitor wiring. Catalog IDs: [09](./09-test-catalog.md), [07](./07-testing.md), [10](./10-implementation.md).

## Status (Aug 2026)

| Step | State |
|------|-------|
| Specs (target path) | ✅ this package |
| **Step 0** — hardcoded `UdoFunctionExpression` unit tests | ✅ `UdoFunctionExpressionTest`, `UdoInvokeExecutorTest` |
| **Step 1** — define without registry (E1, E2, E6, E8) | ✅ |
| **Step 2** — invoke via bindings lookup (D1, D3, S1) | ✅ `UdoCallLookupTest` |
| **Steps 3–7** — catalog through `UdoFunctionExpression` | ✅ |
| User-facing Docusaurus (step 8) | ✅ |
| **P1** — DS4, recursion guard (E9, E9-b), Spark IT | ✅ |
| **P2** — component params, wildcards, viral attribute | ✅ |
| **P3** — ruleset params, scalar set guard, closure (S5) | ✅ |
| Hygiene — `UdoTypes`, return inference, specs sync | ✅ |

```bash
mvn -pl vtl-engine -Dtest=UdoFunctionExpressionTest,UdoInvokeExecutorTest,UdoCallLookupTest,UdoDatasetTypeParserTest,UdoComponentTypeParserTest,UdoRulesetTypeParserTest,UdoStructureCheckTest,UserDefinedOperatorTest,UdoPatternWalkthroughTest,ValidationFunctionsTest test
```

## Pattern at a glance (target)

```
define operator add (…) is x + y end operator;
  AssignmentVisitor.visitDefOperator
    → UdoDefineExecutor.define → UdoDefinition
    → reject if name in bindings (AlreadyDefined) OR native registry
    → bindings.put("add", udo)
    → no registerMethod

res := add(1, 2);
  GenericFunctionsVisitor.visitCallDataset
    → currentBindings.get("add") instanceof UdoDefinition
    → wire defaults / _ / arity
    → UdoFunctionExpression extends ResolvableExpression
    → resolve: child map → ExpressionVisitor(body)
```

Do **not** route UDOs through `DatasetScalarFunctionExecutor`.  
`invokeFunction` stays native-only — UDO fork stays in `visitCallDataset` (raw `_` args).

## For reviewers

1. **Artefact** = `UdoDefinition` in bindings; not a `vtl-model` DTO (ANTLR body) — [02](./02-model.md)
2. **Invoke** = `ResolvableExpression`, operator id like a variable — [04](./04-invoke.md)
3. **Collisions** = E6 `AlreadyDefinedException` / E8 native — [08 §2](./08-open-questions.md)
4. **Types** = reuse `TypeChecking`; this file only lists rejected syntax — [06](./06-types.md)
5. **Clause scope** = outer bindings merged into `ClauseVisitor`
6. **Not in P0** = HOF, component/set/ruleset types, constraints, scalar→dataset lift, PE API changes, lexical closures

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
| Structured `dataset {…}` | P1 (DS4) — opaque param still accepted without signature |
| Trevas call args | `varID\|const\|_` vs official `expr` — orthogonal |

## Documents

| File | Content |
|------|---------|
| [00-strategy.md](./00-strategy.md) | Why partial |
| [01-architecture.md](./01-architecture.md) | Target call path |
| [02-model.md](./02-model.md) | `UdoDefinition` |
| [03-define.md](./03-define.md) | Define (bindings only) |
| [04-invoke.md](./04-invoke.md) | Invoke via `ResolvableExpression` |
| [05-phases.md](./05-phases.md) | P0 vs backlog |
| [06-types.md](./06-types.md) | Syntax accept/reject |
| [07-testing.md](./07-testing.md) | Hardcoded unit tests first |
| [08-open-questions.md](./08-open-questions.md) | Decisions |
| [09-test-catalog.md](./09-test-catalog.md) | Catalog |
| [10-implementation.md](./10-implementation.md) | Next steps after review |
