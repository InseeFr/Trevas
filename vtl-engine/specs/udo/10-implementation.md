# 10 — Implementation plan

**Target pattern:** [01-architecture](./01-architecture.md) — bindings `UdoDefinition` + `UdoFunctionExpression` (`ResolvableExpression`). No trampoline.

**Test-first:** hardcoded expression tests first ([07](./07-testing.md)), then catalog IDs ([09](./09-test-catalog.md)).

---

## Current status

| Milestone | State |
|-----------|-------|
| `UdoFunctionExpression` + bindings lookup (no trampoline) | ✅ |
| Catalog P0–P3 + aux (37 acceptance tests) | ✅ |
| `UdoTypes` — shared scalar parsing / assignability | ✅ |
| Omitted `returns` inference on `getType()` | ✅ |
| Docusaurus user docs | ✅ |
| P1 DS4 / recursion (E9, E9-b) / Spark IT | ✅ |
| P2 component params, wildcards, viral attribute | ✅ |
| P3 ruleset params, scalar set guard, closure (S5) | ✅ |

---

## Backlog (when scripts demand it)

| Focus | Notes |
|-------|-------|
| `set <T>` runtime | rejected at define today (P3-3); no invoke path |
| Scalar constraints at define | `scalarTypeConstraint` → `UnimplementedException` |
| Value domains | `valueDomain` types |
| Ruleset constraints | `on variable` / `on vd` in ruleset formals |
| E10 | define UDO same name as existing ruleset |

Compile body at `define` time is **not** planned without typed placeholders for all bindings (formals, closure, free vars) — a naive attempt broke inference and dataset typing.

---

## Historical steps (P0 rewrite — done)

Steps 0–8 documented the migration from the trampoline spike to `UdoFunctionExpression`. All catalog IDs listed in [09](./09-test-catalog.md) are green on `feat/udo`.
