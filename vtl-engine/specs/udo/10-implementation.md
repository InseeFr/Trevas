# 10 — Implementation plan (pattern-aligned steps)

**Pattern (locked):** [01-architecture](./01-architecture.md) — bindings `UdoDefinition` + trampoline `Method` + `UdoFunctionExpression`.

**Test-first:** each step below starts from catalog IDs ([09](./09-test-catalog.md)); implement only what turns those IDs green.

---

## Current status

| Milestone | State |
|-----------|-------|
| Pattern spike + walkthrough | ✅ |
| P0 acceptance green (DS4 skipped) | ✅ |
| Specs aligned on pattern | ✅ (this package) |
| Docusaurus user docs | ⬜ B8 |
| P1 DS4 / recursion / Spark IT | ⬜ |

---

## Steps (define → invoke → harden)

Each **feat** step assumes tests for its IDs exist (write/red first if missing).

### Step 1 — Model + define + collisions

**Tests first:** E1, E2, E6, E8  
**Implement:**

- [x] `UdoDefinition` / `UdoParameter`
- [x] `UdoDefineExecutor` (P0 type subset)
- [x] `AssignmentVisitor.visitDefOperator`
- [x] Reject bindings collision **and** registry collision
- [x] `bindings.put` + `registerMethod(trampoline)`

**Green:** E1 E2 E6 E8

---

### Step 2 — Invoke via FunctionExpression

**Tests first:** D1, D3, S1, S5 (and walkthrough scalar)  
**Implement:**

- [x] `UdoTrampoline` + `UdoFunctionExpression`
- [x] `UdoInvokeExecutor` (minimal: all args present)
- [x] `GenericFunctionsVisitor.visitCallDataset` UDO fork (raw params)
- [x] Scoped `ExpressionVisitor` in trampoline

**Green:** D1 D3 S1 S5 · `UdoPatternWalkthroughTest` scalar

**Do not** call `DatasetScalarFunctionExecutor` for UDOs.

---

### Step 3 — Defaults, `_`, arity

**Tests first:** D2, S3, E3–E5, E7  
**Implement:**

- [x] Default fill / `OPTIONAL` in `UdoInvokeExecutor`
- [x] Arity and type mismatch messages (not `FunctionNotFound`)

**Green:** D2 S3 E3 E4 E5 E7

---

### Step 4 — Free variables

**Tests first:** S2  
**Implement:**

- [x] Child scope = outer bindings copy + params (invoke-time free vars)

**Green:** S2

---

### Step 5 — Opaque dataset + mixed + clause scope

**Tests first:** DS1–DS3, DS5  
**Implement:**

- [x] `dataset` formal / return
- [x] Mixed signatures
- [x] `ClauseVisitor` merge outer bindings (scalar params in filter/calc)

**Green:** DS1 DS2 DS3 DS5 · walkthrough dataset filter  
**Leave:** DS4 `@Disabled`

---

### Step 6 — Return type enforcement

**Tests first:** D4  
**Implement:**

- [x] Declared `returns` check in trampoline (`integer` ⊆ `number`)

**Green:** D4

---

### Step 7 — Nested UDO

**Tests first:** S4  
**Implement:** only if not already green after Step 2 (re-entrant lookup)

- [x] S4 green via re-entry (no extra slice needed)

---

### Step 8 — User docs

**Commit:** `docs(udo): document partial UDO support`

- [ ] Docusaurus: pattern summary, métier examples, limitations
- [ ] Link to `specs/udo/README.md`

---

## P0 completion criteria

```bash
mvn -pl vtl-engine -Dtest=UserDefinedOperatorTest,UdoPatternWalkthroughTest test
# SUCCESS — DS4 skipped
```

| Category | IDs |
|----------|-----|
| Doc | D1–D4 |
| Technique | S1–S4 |
| Métier | DS1–DS3, DS5 |
| Erreurs | E1–E8 |

---

## P1+ outline

| Step | Focus | Tests |
|------|--------|-------|
| P1 | Structured `dataset {…}` | DS4 |
| P1 | Recursion guard | new E* |
| P1 | Optional: replace ThreadLocal trampoline with bound Method | — |
| P1 | Spark IT | new IT |
| later | component, constraints | new catalog |

---

## Commit style

```
test(udo): <IDs> — <why>
feat(udo): <behaviour> — <IDs green>
docs(udo): <what>
```
