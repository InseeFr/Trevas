# 04 — Invoke path (`ResolvableExpression`)

## Entry point (target)

Fork in `visitCallDataset` **before** visiting all parameters (natives still visit eagerly). Resolve the operator id in the **current** expression bindings (the map the visitor already holds — needed for nested calls / later closures), not always `ENGINE_SCOPE`:

```java
String name = ctx.operatorID().getText();
Object binding = currentBindings.get(name); // visitor map, not only ENGINE_SCOPE
if (binding instanceof UdoDefinition udo) {
  return wireArgs(udo, ctx, exprVisitor, fromContext(ctx)); // defaults / _
}
List<ResolvableExpression> parameters =
    ctx.parameter().stream().map(exprVisitor::visit).toList();
return invokeFunction(name, parameters, fromContext(ctx)); // natives only
```

**Do not** move the UDO check into `invokeFunction`: that API only receives visited expressions and cannot see `_` / `OPTIONAL`.

## Argument wiring

(Today: `UdoInvokeExecutor` — keep as a helper, not a second invoke stack.)

1. **Arity / optionality** against formals (extra args → error; missing mandatory → error).
2. For each formal:
   - actual `varID` / `constant` → visit, type-check via existing `TypeChecking` / `checkInstanceOf`, collect expression
   - actual `OPTIONAL` → default if optional, else error (E7)
   - missing trailing → default if optional, else error (E3)
3. Return `new UdoFunctionExpression(udo, resolvedArgs, position)`.

## `UdoFunctionExpression` (extends `ResolvableExpression`)

Implements the general expression contract — **not** a fake `FunctionExpression` over a trampoline `Method`.

- `getType()` → declared `returns` or `Object` if inferred.
- `resolve(context)`:
  1. Child map = copy of `context` + formal names → evaluated args
  2. `new ExpressionVisitor(child, PE, engine).visit(body)`
  3. `body.resolve(child)`
  4. If `returns` declared → assignability check (`integer` ⊆ `number` via `TypeChecking`)
  5. Return result

No ThreadLocal CallSite. Unit-test this with a **hardcoded** `UdoDefinition` (no `define operator` parse) before wiring the visitor.

## Why not FunctionExpression / Method

The spike reused `FunctionExpression` → `Method.invoke` so UDOs looked like natives. That required `registerMethod` + a trampoline, which bypasses bindings. Target: same *idea* (a `ResolvableExpression` you `resolve`), without pretending there is a Java method.

| Path | Mechanism |
|------|-----------|
| Native | `findMethod` → `FunctionExpression` → real Java method |
| UDO | bindings hit → `UdoFunctionExpression` → VTL body |

Do **not** send UDOs through `DatasetScalarFunctionExecutor` (no mono-measure lift in P0).

## Scalar vs dataset

- Scalar / dataset body results both OK when signature matches.
- P0: no scalar-UDO auto-lift onto dataset actuals.
- Clause bodies (`filter` / `calc`) need outer bindings visible — `ClauseVisitor` merges them (scalar params like `threshold`).

## Nested UDO

Body may call another UDO: re-entrant `visitCallDataset` resolves the callee in the **current** (copied) bindings. Matches VTL (body = any expr). Covered by S4 (`quadruple` → `twice`).

## Errors

| Case | Signal |
|------|--------|
| Not UDO, not native (`foo(...)`) | `FunctionNotFoundException` — same as natives. Bare `foo` stays `UndefinedVariableException`. |
| Wrong arg type | message with expected/got (`TypeChecking`) |
| Missing / extra args | arity message (not “not found”) |
| `_` without default | E7 |
| Return mismatch | message contains declared VTL type (e.g. `boolean`) |
| Define vs existing binding | E6 `AlreadyDefinedException` |
| Define vs native registry | E8 `conflicts with native function` |
