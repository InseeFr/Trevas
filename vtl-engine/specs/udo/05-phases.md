# 05 — Phases

P0 is the only committed slice: **scalar + opaque `dataset`**, no PE API change. P1+ is a backlog, not a contract.

Tests first: [09-test-catalog.md](./09-test-catalog.md). Runtime path: [01-architecture.md](./01-architecture.md).

| Phase | Scope | Status |
|-------|--------|--------|
| **P0** | Scalars + opaque `dataset` (param and/or return) | in this PR; rewrite invoke to `ResolvableExpression` (review) |
| **P1** | Structured `dataset {…}` (DS4), recursion guard, Spark IT | ✅ |
| **P2** | `component` params, richer structure / viral attrs | next — guard: P2-1 rejects `measure`/`component` at define |
| **P3** | Constraints, `set`, ruleset types | only if scripts demand it |

### P0 in

- Basic scalar params / optional `returns` / inferred return
- `default <constant>` and `_` / `OPTIONAL`
- Free variables after DAG reorder (invoke-time lookup)
- Opaque `dataset` as parameter and/or return
- Body = any expression Trevas already supports under param bindings
- Invoke: resolve operator id in current bindings → `UdoFunctionExpression`
- Reject name collision with bindings (`AlreadyDefinedException`) or native registry
- Clause bodies: outer bindings merged so scalar params resolve in `filter` / `calc`

### P0 out

- Structured `dataset {…}` enforcement (DS4)
- `component` / `set` / `ruleset` parameter types, scalar constraints
- Scalar-UDO auto-promotion onto datasets
- Lexical closures (snapshot bindings at define)
- Recursion guard, provenance

Opaque dataset UDOs package reusable `filter`/`calc`/`join` recipes. Temporary assignments do not replace them when the same recipe is invoked multiple times or shared across scripts.
