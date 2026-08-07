# 05 — Phases

## Overview

| Phase | Scope | Effort | PE changes |
|-------|-------|--------|------------|
| **P0** | Scalars **+ opaque `dataset`** (param and/or return) | M | none |
| **P1** | Structured `dataset {…}` enforce, define-time body check, recursion guard, Spark IT | M–L | none |
| **P2** | `component` params, richer structure checks, viral attrs edge cases | L | none expected |
| **P3** | Constraints, `set`, ruleset types | XL | none |

**P0 includes dataset.** Opaque `dataset` is required from day one (real scripts pass/return datasets). Structured constraints and component types can follow in P1/P2 without blocking reusable dataset helpers.

Tests first — full catalog: [09-test-catalog.md](./09-test-catalog.md) (P0: D/S/DS1–3+DS5/E1–E8; DS4 disabled).

**Runtime pattern (locked):** bindings `UdoDefinition` + trampoline `Method` + `UdoFunctionExpression` — [01](./01-architecture.md), steps [10](./10-implementation.md).

---

## P0 — Scalar + opaque dataset (**shipped** Aug 2026)

### In

- Basic scalar params / optional `returns` / inferred return
- `default <constant>` and `_` / `OPTIONAL`
- Free variables after DAG reorder
- **Opaque `dataset` as parameter and/or return type** (`instanceof Dataset` check only)
- Body = any expression Trevas already supports under param bindings (filter, calc, union, …)
- Invoke via `name(args)` with `varID` / `constant` / `_` through **FunctionExpression / Method trampoline**
- Reject name collision with bindings **or** native/global registry (E6, E8)
- Clause bodies: outer bindings merged so scalar params resolve in `filter` / `calc`
- In-memory acceptance + walkthrough tests

### Out of P0 (still fail clearly)

- Structured `dataset { identifier <…> … }` enforcement (parse OK, check deferred → P1)
- `component` / `set` / `ruleset` parameter types
- Scalar constraints / nullability DL syntax
- Scalar-UDO auto-promotion onto datasets (native-style lift)
- provenance / Jackson
- recursion guard (P1)
- User-facing Docusaurus (slice B8)

### Acceptance

- [x] `UserDefinedOperatorTest` green for D*, S*, DS1–DS3, DS5, E1–E8 (DS4 `@Disabled`)
- [x] `UdoPatternWalkthroughTest` green
- [x] No `ProcessingEngine` API change

---

## P1 — Structure + harden

- Enforce `dataset { componentConstraint* }` against actual `DataStructure` (DS4)
- Define-time body type-check (placeholders)
- Recursion detection
- Spark IT for dataset UDO bodies
- Optional: replace ThreadLocal trampoline with bound `Method` if needed
- Defaults / `_` already in **P0** — only polish if gaps remain

---

## P2 — Component-level & attribute edge cases

- `component` / role-typed parameters (needs component visitor context)
- Viral attribute policy inside UDO bodies

---

## P3 — Full DL fidelity

- Scalar constraints, `set <T>`, ruleset types as args
- Only if scripts demand it

---

## Why this order

```
value
  ▲
  │            P3 ── exotic DL types
  │       P2 ── component
  │  P1 ── structured dataset checks
  │ P0 ── scalar + opaque dataset   ← shipped (pattern locked)
  └──────────────────► complexity
```

Opaque dataset UDOs are the packaging mechanism for reusable pipeline snippets (`filter`/`calc`/`join` recipes). Temporary assignments do **not** replace them when the same recipe is invoked multiple times or shared across scripts.
