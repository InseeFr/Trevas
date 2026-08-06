# 07 — Testing strategy

## Where we are

| Item | Status |
|------|--------|
| Roadmap specs `00`–`10` | ✅ |
| Acceptance suite `UserDefinedOperatorTest` | ✅ **20 tests written** (DS4 `@Disabled`) |
| Production UDO code | ❌ **not started** |
| Tests green | ❌ **expected red** until slice 1b+ |

**Next gate:** review [09-test-catalog.md](./09-test-catalog.md) → then execute [10-implementation.md](./10-implementation.md).

---

## Test-first approach (mandatory)

```
1. Catalog entry (09) — precise expected behaviour
2. JUnit method (UserDefinedOperatorTest) — red
3. Review / sign-off
4. feat(udo) slice — minimal prod
5. Same IDs green — check off 09 + 10
```

No `feat(udo)` before the corresponding tests exist and fail for the **right reason** (not `FunctionNotFound` when testing arity on a defined UDO).

---

## Test layers

| Layer | Location | When | Purpose |
|-------|----------|------|---------|
| **Acceptance** | `UserDefinedOperatorTest` | P0 gate | End-to-end via `VtlScriptEngine` — product bar |
| **Unit** | `semantics/udo/*Test` | optional per slice | `UdoDefineExecutor` / `UdoInvokeExecutor` edge cases |
| **DAG regression** | `DagDefineStatementsTest` | already green | Reorder only — S2 also proves **execution** |
| **Spark IT** | `vtl-spark4` | P1 | Dataset UDO body on PE |

P0 **DoD = acceptance only** (20 green). Unit tests are optional helpers, not a substitute.

---

## Categories (review lens)

| Tag | IDs | Reviewer asks |
|-----|-----|---------------|
| **Doc** | D1–D4 | “Do we match VTL RM intent?” |
| **Métier** | DS1–DS5 | “Do real recipes (filter/calc/union/scale) work?” |
| **Technique** | S1–S4 | “Are engine semantics correct (scope, DAG, `_`, nest)?” |
| **Erreurs** | E1–E7 | “Do we fail clearly, not as unknown function?” |

Full matrix: [09-test-catalog.md](./09-test-catalog.md).

---

## Running & CI

```bash
# Full acceptance suite
mvn -pl vtl-engine -Dtest=UserDefinedOperatorTest test

# Single ID while developing
mvn -pl vtl-engine -Dtest=UserDefinedOperatorTest#testD1AddTwoArgs test
```

Pre-implementation: CI **may** fail on this class — document in PR. Post-P0: must be green in `vtl-engine` CI.

---

## Assertion conventions

| Result | Assert |
|--------|--------|
| Scalar | `assertThat(value).isEqualTo(expected)` — use `Long`/`Double`/`Boolean`/`String` literals matching engine |
| Dataset | `getDataAsMap()` + `containsExactlyInAnyOrder` or targeted `anySatisfy` |
| Error | `assertThatThrownBy(...)` — prefer message fragment over exception class alone |
| Invoke errors | `hasMessageNotContaining("not found")` when UDO is defined (E3–E7) |

---

## After P0

| Phase | New tests |
|-------|-----------|
| P1 | DS4 enabled; recursion guard; optional Spark IT |
| P2+ | component params, constraints — new catalog section |
