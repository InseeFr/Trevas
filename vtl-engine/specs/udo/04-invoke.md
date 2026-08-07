# 04 — Invoke path (FunctionExpression + Method)

## Entry point (as implemented)

Fork in `visitCallDataset` **before** visiting all parameters (natives still visit eagerly):

```java
String name = ctx.operatorID().getText();
Object binding = engine.getBindings(ENGINE_SCOPE).get(name);
if (binding instanceof UdoDefinition udo) {
  return UdoInvokeExecutor.invoke(udo, ctx, exprVisitor, engine, fromContext(ctx));
}
List<ResolvableExpression> parameters =
    ctx.parameter().stream().map(exprVisitor::visit).toList();
return invokeFunction(name, parameters, fromContext(ctx)); // natives only
```

**Do not** move the UDO check into `invokeFunction`: that API only receives visited expressions and cannot see `_` / `OPTIONAL`.

## `UdoInvokeExecutor` steps

1. **Arity / optionality** against formals (extra args → error; missing mandatory → error).
2. For each formal:
   - actual `varID` / `constant` → visit, type-check, collect expression
   - actual `OPTIONAL` → default if optional, else error (E7)
   - missing trailing → default if optional, else error (E3)
3. Return `new UdoFunctionExpression(udo, resolvedArgs, position)`.

## `UdoFunctionExpression` (extends `FunctionExpression`)

- Super ctor: `new VtlMethod(UdoTrampoline.methodForArity(n))` with `Object` parameter types (skips strict `checkInstanceOf` on `Object`).
- `getType()` → declared `returns` or `Object` if inferred.
- `resolve(context)`:
  1. `UdoTrampoline.enter(udo, context)`
  2. `super.resolve(context)` → evaluates args → `Method.invoke(null, args)`
  3. `UdoTrampoline.exit()` in `finally`

## `UdoTrampoline.dispatch`

1. Read CallSite (`udo`, outer bindings).
2. Child map = copy of outer + formal names → actual values.
3. `new ExpressionVisitor(child, PE, engine).visit(body)`.
4. `body.resolve(child)`.
5. If `returns` declared → assignability check (`integer` ⊆ `number` allowed).
6. Return result.

ThreadLocal CallSite is the P0 bridge (no per-UDO bytecode). Acceptable for the locked pattern; replace later only if needed.

## Why FunctionExpression

Reuses the same resolve machinery as natives (`Method.invoke` + evaluated args) without sending UDOs through `DatasetScalarFunctionExecutor` lift.

| Path | Mechanism |
|------|-----------|
| Native | `findMethod` → `FunctionExpression` → real Java method |
| UDO | bindings hit → `UdoFunctionExpression` → trampoline Method → VTL body |

## Scalar vs dataset

- Scalar / dataset body results both OK when signature matches.
- P0: no scalar-UDO auto-lift onto dataset actuals.
- Clause bodies (`filter` / `calc`) need outer bindings visible — `ClauseVisitor` merges them (scalar params like `threshold`).

## Nested UDO

Body may call another UDO: re-entrant `visitCallDataset` finds the callee in (copied) outer bindings. S4.

## Errors

| Case | Signal |
|------|--------|
| Not UDO, not native | `FunctionNotFoundException` |
| Wrong arg type | message with expected/got |
| Missing / extra args | arity message (not “not found”) |
| `_` without default | E7 |
| Return mismatch | message contains declared VTL type (e.g. `boolean`) |
| Define vs registry | E8 `conflicts with native function` |
