# 10 — Implementation plan

**Target pattern:** [01-architecture](./01-architecture.md) — bindings `UdoDefinition` + `UdoFunctionExpression` (`ResolvableExpression`). No trampoline.

**Test-first:** hardcoded expression tests first ([07](./07-testing.md)), then catalog IDs ([09](./09-test-catalog.md)).

---

## Current status

| Milestone | State |
|-----------|-------|
| Spike (trampoline) + acceptance green | ✅ in this PR (to replace) |
| Specs retargeted after review | ✅ |
| Hardcoded `UdoFunctionExpression` unit tests | ⬜ |
| Drop `registerMethod` / `UdoTrampoline` | ✅ |
| E6 → `AlreadyDefinedException` | ✅ |
| Docusaurus | ⬜ |
| P1 DS4 / recursion / Spark IT | ⬜ |

---

## Next steps (review follow-up)

### Step 0 — Control the expression (absolute first)

**Tests first:** hardcoded `UdoDefinition` (body as parsed expr or a tiny script snippet bound by hand) + `UdoFunctionExpression.resolve` — scalar add, defaults, type mismatch, free var in the resolve map.

**Implement:** `UdoFunctionExpression extends ResolvableExpression` evals the body; no `Method.invoke`.

### Step 1 — Define without registry

**Tests:** E1, E2, E6, E8  
**Implement:** `bindings.put` only; E6 = `AlreadyDefinedException`; E8 = native collision; **no** `registerMethod`. **Done.**

### Step 2 — Invoke via bindings lookup

**Tests:** D1, D3, S1  
**Implement:** `visitCallDataset` resolves **current** bindings; arg wiring for present args; no `DatasetScalarFunctionExecutor`.

### Step 3–7 — keep existing catalog

Defaults/`_` (D2, S3, E3–E5, E7), free vars (S2), dataset recipes (DS1–3, DS5), returns (D4), nested (S4). Behaviour stays; implementation goes through the new expression.

### Step 8 — User docs

Docusaurus when the path is stable.

---

## P1+ (backlog, not a commitment)

| Focus | Tests |
|-------|--------|
| Structured `dataset {…}` | DS4 |
| Recursion guard | new E* |
| Spark IT | new IT |
| Optional: snapshot closures at define | new S* |
