# 10 — Implementation plan (test-first)

**Principle:** no production UDO code until the acceptance suite is **complete, precise, and red for the right reasons**. Then: **test commit (if needed) → feat commit** per slice.

Catalog reference: [09-test-catalog.md](./09-test-catalog.md)  
Testing policy: [07-testing.md](./07-testing.md)

---

## Current status

| Milestone | State |
|-----------|-------|
| Specs + decisions | ✅ done |
| Acceptance tests (20 P0) | ✅ written in JUnit |
| Tests green | ❌ all fail (no prod) |
| Review gate | ⏳ **you are here** |
| Slice 1b+ (prod) | ⬜ after review OK |

---

## Phase A — Test suite gate (before any prod)

**Goal:** reviewers approve [09](./09-test-catalog.md) slice map + JUnit assertions.

### A0 — Baseline (done)

- [x] Roadmap `00`–`10`
- [x] `UserDefinedOperatorTest` — 20 P0 methods + DS4 `@Disabled`
- [x] Catalog D/S/DS/E matrix + métier/technique tags

### A1 — Harden acceptance (optional, pre-review)

**Commit:** `test(udo): tighten acceptance assertions and catalog`

- [ ] Verify E3–E7 use `hasMessageNotContaining("not found")` when UDO is defined
- [ ] Add E8/E9 (native / ruleset collision) if reviewers require — see [09 § Planned errors](./09-test-catalog.md)
- [ ] Add `@Tag` or method naming already maps 1:1 to catalog IDs (document in 09)
- [ ] Run suite — confirm **FAIL** (not compilation error)
- [ ] Team sign-off on slice map below

**No `feat(udo)` in Phase A.**

---

## Phase B — Implementation (TDD slices)

Ritual per **feat** commit:

```
[ ] only prod for this slice
[ ] listed catalog IDs green
[ ] check IDs in 09 checklist
[ ] no Docusaurus until slice 8
```

### B1 — Define + define errors

| Step | Commit | IDs green | Prod |
|------|--------|-----------|------|
| 1a | `test(udo): define errors E1 E2 E6` | (optional if A1 done) | — |
| 1b | `feat(udo): register define operator in bindings` | **E1, E2, E6** | `UdoDefinition`, `AssignmentVisitor.visitDefOperator`, `UdoDefineExecutor`, `UdoTypeSupport` subset |

Specs: [02](./02-model.md), [03](./03-define.md), [08 §2](./08-open-questions.md)

---

### B2 — Scalar invoke (happy path)

| Step | Commit | IDs green | Prod |
|------|--------|-----------|------|
| 2a | `test(udo): scalar invoke D1 D3 S1 S5` | review only | — |
| 2b | `feat(udo): invoke scalar UDOs` | **D1, D3, S1, S5** | `UdoInvokeExecutor`, `GenericFunctionsVisitor` UDO branch, scoped `ExpressionVisitor` |

---

### B3 — Defaults, `_`, arity

| Step | Commit | IDs green | Prod |
|------|--------|-----------|------|
| 3a | `test(udo): defaults and arity D2 S3 E3-E7` | review only | — |
| 3b | `feat(udo): UDO defaults optional and arity checks` | **D2, S3, E3, E4, E5, E7** | default + `_` handling in `UdoInvokeExecutor` |

---

### B4 — Free variables

| Step | Commit | IDs green | Prod |
|------|--------|-----------|------|
| 4a | `test(udo): free variable S2` | review only | — |
| 4b | `feat(udo): free variables in UDO body` | **S2** | invoke-time outer binding lookup ([08 §1](./08-open-questions.md)) |

---

### B5 — Métier: opaque dataset recipes

| Step | Commit | IDs green | Prod |
|------|--------|-----------|------|
| 5a | `test(udo): dataset recipes DS1-DS3 DS5` | review only | — |
| 5b | `feat(udo): opaque dataset params and returns` | **DS1, DS2, DS3, DS5** | `dataset` formal check; mixed signatures |

DS4 stays `@Disabled`.

---

### B6 — Return type enforcement

| Step | Commit | IDs green | Prod |
|------|--------|-----------|------|
| 6a | `test(udo): returns mismatch D4` | review only | — |
| 6b | `feat(udo): enforce declared return type` | **D4** | `returns` vs body ([06 assignability](./06-types.md)) |

---

### B7 — Nested UDO (if needed)

After 2b, run **S4**. If green → **skip B7**.

| Step | Commit | IDs green |
|------|--------|-----------|
| 7a/7b | `test` + `feat(udo): nested UDO S4` | **S4** |

---

### B8 — Documentation

**Commit:** `docs(udo): document partial UDO support`

- [ ] Docusaurus: supported subset, métier examples (DS1/DS5), limitations
- [ ] [09](./09-test-catalog.md) P0 checklist all `[x]`
- [ ] [README](./README.md) layer table → define/invoke ✅

---

## P0 completion criteria

```bash
mvn -pl vtl-engine -Dtest=UserDefinedOperatorTest test
# → BUILD SUCCESS, 20 tests, 0 failures, 1 skipped (DS4)
```

| Category | IDs | Must be green |
|----------|-----|---------------|
| Doc | D1–D4 | ✅ |
| Technique | S1–S4 | ✅ |
| Métier | DS1–DS3, DS5 | ✅ |
| Erreurs | E1–E7 | ✅ |

---

## P1+ (outline)

| Slice | Focus | Tests |
|-------|--------|-------|
| P1a/b | Structured `dataset {…}` | DS4 |
| P1 | Recursion guard | new E* |
| P1 | Spark IT | new IT class |
| later | component, constraints | new catalog section |

---

## Commit style

```
test(udo): <IDs> — <why>
feat(udo): <behaviour> — <IDs green>
docs(udo): <what>
```

Skip **a** commits when tests already exist from Phase A.
