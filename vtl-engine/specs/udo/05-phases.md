# 05 — Phases

## Overview

| Phase | Scope | Effort | PE changes |
|-------|-------|--------|------------|
| **P0** | Scalars **+ opaque `dataset`** (param and/or return) | M | none |
| **P1** | Structured `dataset {…}` enforce, define-time body check, recursion guard, Spark IT | M–L | none |
| **P2** | `component` params, richer structure checks, viral attrs edge cases | L | none expected |
| **P3** | Constraints, `set`, ruleset types | XL | none |

**P0 includes dataset.** Opaque `dataset` is required from day one (real scripts pass/return datasets). Structured constraints and component types can follow in P1/P2 without blocking reusable dataset helpers.

Tests first — full catalog: [09-test-catalog.md](./09-test-catalog.md) (20 P0 cases: doc, métier, technique, errors).

---

## P0 — Scalar + opaque dataset (ship this first)

### In

- Basic scalar params / optional `returns` / inferred return
- `default <constant>` and ideally `_` / `OPTIONAL`
- Free variables after DAG reorder
- **Opaque `dataset` as parameter and/or return type** (`instanceof Dataset` check only)
- Body = any expression Trevas already supports under param bindings (filter, calc, union, …)
- Invoke via `name(args)` with `varID` / `constant` / `_`
- In-memory engine tests from the catalog (D*, S*, DS1–DS3, E*)

### Out of P0 (still fail clearly)

- Structured `dataset { identifier <…> … }` enforcement (parse OK, check deferred → P1)
- `component` / `set` / `ruleset` parameter types
- Scalar constraints / nullability DL syntax
- Scalar-UDO auto-promotion onto datasets (native-style lift)
- provenance / Jackson
- recursion guard (P1)

### Acceptance

- All enabled tests in `UserDefinedOperatorTest` for D*, S*, DS1–DS3, E* green
- No `ProcessingEngine` API change

---

## P1 — Structure + harden

- Enforce `dataset { componentConstraint* }` against actual `DataStructure` (DS4)
- Define-time body type-check (placeholders)
- Recursion detection
- Spark IT for dataset UDO bodies
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
  │ P0 ── scalar + opaque dataset   ← needed now
  └──────────────────► complexity
```

Opaque dataset UDOs are the packaging mechanism for reusable pipeline snippets (`filter`/`calc`/`join` recipes). Temporary assignments do **not** replace them when the same recipe is invoked multiple times or shared across scripts.
