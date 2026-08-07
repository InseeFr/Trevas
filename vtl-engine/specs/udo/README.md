# Roadmap — User Defined Operators (UDO)

**Location:** `vtl-engine/specs/udo/` (engine module specs).  
**Tests:** `src/test/java/.../UserDefinedOperatorTest.java`

Implementation spec for **User Defined Operators** (VTL 2.1) in Trevas.

## Verdict

**Partial support first**: **P0 = scalars + opaque `dataset`** (param and/or return) + mixed signatures. Not the full DL type system in one go.

Parsing and the DAG are already in place. The gap is `define` → binding → invocation.

**Execution plan (test-first):** [10-implementation.md](./10-implementation.md)  
**Test catalog (métier + technique):** [09-test-catalog.md](./09-test-catalog.md) + `UserDefinedOperatorTest`

## Working method (mandatory)

**At every implementation step, start with tests.** The slice’s acceptance cases (catalog IDs in [09](./09-test-catalog.md)) must exist and **fail for the right reason** before any production code for that slice. Then implement only what is needed to make those tests pass. Details: [07-testing.md](./07-testing.md), [10-implementation.md](./10-implementation.md).

```
for each slice:
  1. write / harden the JUnit cases for that slice’s IDs   → red
  2. implement the minimal engine change                     → those IDs green
  3. do not start the next slice until the current IDs pass
```

No `feat(udo)` without the corresponding red tests. Skip a dedicated “test-only” commit only when the cases already exist from an earlier gate (baseline suite).

## Status (Aug 2026)

| Step | State |
|------|-------|
| Specs + review locks | ✅ |
| **20 acceptance tests written** (DS4 disabled) | ✅ |
| Tests green | ❌ expected until prod |
| **Review gate** | ⏳ catalog + slice map |
| Implementation | ⬜ after team OK |

```bash
mvn -pl vtl-engine -Dtest=UserDefinedOperatorTest test   # fails today — normal
```

## For reviewers (coherence lock)

Before coding, confirm these P0 locks (details in [08](./08-open-questions.md)):

1. **Artefact** = engine `UdoDefinition` in bindings (not a `vtl-model` DTO yet) — [02](./02-model.md)
2. **Types** = scalars + opaque `dataset` + mixed; structured `{…}` accepted but not enforced — [03](./03-define.md), [06](./06-types.md)
3. **Layers** = thin visitors → `semantics/udo/*` → existing `ExpressionVisitor` / PE — [01](./01-architecture.md)
4. **Not in P0** = HOF/predicates, component/set/ruleset types, constraints, scalar→dataset lift, PE API changes
5. **Assignability** = reuse `TypeChecking` (`integer` ⊆ `number` for D1)

## What we will do

1. Register `define operator` in bindings (same pattern as rulesets)
2. Resolve `operatorID(…)` to that artefact **before** natives
3. Evaluate the body in a parameter scope (+ free vars) via existing `ExpressionVisitor`
4. Support scalars, opaque `dataset`, and mixed signatures (`dataset` + scalar)
5. Ship in granular commits: **tests first → then implem** per slice (see [Working method](#working-method-mandatory))

No new `ProcessingEngine` API: the body reuses the current runtime (in-memory / Spark).

## In scope (P0)

| Capability | Example |
|------------|---------|
| Typed scalars + `default` + `_` | `add(x integer default 0, y integer default 0)` |
| `returns` or inference | `returns number` / omit |
| Free variables (after DAG) | body uses script-level `y` |
| Opaque `dataset` in/out | `filter` / `calc` / `union` recipes |
| Mixed signatures | `ds dataset, threshold integer` |
| UDO → UDO | body calls another UDO (`varID` / const / `_` only) |

## Limitations (assumed)

| Limitation | Consequence |
|------------|-------------|
| No expression / predicate injection (HOF) | no `filterBy(ds, age >= 18)` |
| `ds[keep s]` ≠ dynamic keep | `s` is a literal component name, not a string value |
| Structured `dataset {…}` | parses OK; structure enforce → P1 |
| No `component` / `set` / ruleset / scalar constraints | P2–P3 |
| Trevas call args stricter than official VTL | `varID\|const\|_` vs official `expr` — orthogonal fix |
| Doc example `max1 returns boolean` | spec typo; Trevas enforces the real type |

See also [08-open-questions.md](./08-open-questions.md).

## Current state in Trevas

| Layer | Status |
|-------|--------|
| Parse (`define operator`, `operatorID(…)`) | ✅ grammar present |
| DAG (`Identifier.Type.OPERATOR`, free vars, ignore params) | ✅ `DAGBuildingVisitor.visitDefOperator` |
| Define runtime (`AssignmentVisitor.visitDefOperator`) | ❌ missing |
| Model (`UdoDefinition` engine-side in bindings) | ❌ missing |
| Invoke (`GenericFunctionsVisitor.visitCallDataset`) | ⚠️ natives only |
| VTL DL types (rich signatures) | ❌ not implemented |
| Spark-specific | ➖ not needed for MVP |

VTL reference: `vtl/v2.1/docs/reference_manual/vtl_dl_udo.rst` (same predicate limit in 2.2).

## Documents

| File | Content |
|------|---------|
| [00-strategy.md](./00-strategy.md) | Why partial; big-bang risks |
| [01-architecture.md](./01-architecture.md) | visitor → semantics → PE |
| [02-model.md](./02-model.md) | `UdoDefinition` (engine artefact) |
| [03-define.md](./03-define.md) | Registration |
| [04-invoke.md](./04-invoke.md) | Invocation + binding |
| [05-phases.md](./05-phases.md) | Phases P0→P3 |
| [06-types.md](./06-types.md) | Type matrix |
| [07-testing.md](./07-testing.md) | Test plan |
| [08-open-questions.md](./08-open-questions.md) | Locked P0 decisions + pitfalls |
| [09-test-catalog.md](./09-test-catalog.md) | Test catalog |
| [10-implementation.md](./10-implementation.md) | TDD commits + checkboxes |

## Existing analogy

Closest production pattern: **rulesets**

1. `AssignmentVisitor.visitDefDatapointRuleset` → model object → `bindings.put(name, artefact)`
2. Invoke visitor → read artefact → `*Executor`

UDOs follow the same path.

## Coherence / Definition of Done (P0)

The package stays **coherent** if we stick to:

| Principle | How |
|-----------|-----|
| Modular | visitors dispatch only; `semantics/udo/*` owns meaning; no PE API change |
| Tested | **Each step starts with tests** the implem must resolve ([Working method](#working-method-mandatory)); 20 P0 cases in [09](./09-test-catalog.md); all green = P0 DoD |
| Efficient | body = `ExpressionVisitor` re-entry; no second runtime |
| No fluff | each slice ships only what its tests require; skip a test-only commit if cases already exist |

**P0 DoD:** commits 1b→7b + 8 green; no code for constraints / component / HOF / structured enforce; [08](./08-open-questions.md) decisions stay locked.

If S4 is already green after 2b (natural re-entry), **skip 7a/7b**.
