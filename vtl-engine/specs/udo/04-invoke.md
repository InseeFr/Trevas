# 04 — Invoke path

## Entry point

`GenericFunctionsVisitor.visitCallDataset` today:

```java
List<ResolvableExpression> parameters =
    ctx.parameter().stream().map(exprVisitor::visit).collect(Collectors.toList());
return invokeFunction(ctx.operatorID().getText(), parameters, fromContext(ctx));
```

Change to:

```java
String name = ctx.operatorID().getText();
Object maybeUdo = engine.getBindings(ENGINE_SCOPE).get(name);
if (maybeUdo instanceof UdoDefinition udo) { // engine artefact — see 02-model
  return UdoInvokeExecutor.invoke(udo, ctx.parameter(), exprVisitor, engine, fromContext(ctx));
}
// existing native path
return invokeFunction(name, parameters, fromContext(ctx));
```

Pass the raw `parameter` contexts (not only visited expressions) so `OPTIONAL` can be detected without pretending `_` is a value expression.

Today `exprVisitor.visit(parameter)` likely breaks or mis-handles `OPTIONAL` — confirm and fix as part of UDO work (natives may already be unable to use `_`).

## `UdoInvokeExecutor.invoke` steps

1. **Arity / optionality**
   - Count provided args (including `OPTIONAL` placeholders).
   - For each formal parameter in order:
     - if actual present and not `OPTIONAL` → visit to `ResolvableExpression`, check type
     - if actual is `OPTIONAL` or missing trailing optional → use default constant expression
     - if missing mandatory → error
   - Reject extra args.

2. **Build child bindings**
   - Copy or wrap parent `ENGINE_SCOPE`.
   - Put each parameter name → resolved value **or** bind `ResolvableExpression`s and resolve lazily consistently with the rest of the engine.
   - Prefer: resolve actuals against parent bindings, then put **values** (or dataset expressions) into a child map used for body resolve — simplest mental model.

3. **Evaluate body**
   - `ExpressionVisitor` on body with child bindings (+ same `ProcessingEngine` / engine).
   - Result is `ResolvableExpression`; resolve if the caller expects a value (assignment path already resolves).

4. **Return type check**
   - If `returns` declared, assert compatibility (`number` accepting `integer` result, etc. — reuse `TypeChecking` rules).
   - If undeclared, use body type.

5. **Cleanup**
   - Child bindings discarded; outer scope unchanged.

## Scalar vs dataset results

- If body yields a scalar expression → return it (assignment stores scalar).
- If body yields `DatasetExpression` → return it; PE already behind the body operators.
- P0 may restrict signatures to scalars even if the body could return a dataset — keep signature and body aligned.

## Promotion / mono-measure (P1)

VTL often allows scalar operators to apply to datasets. Natives get this via `DatasetScalarFunctionExecutor`. For UDOs with scalar signature called with dataset actuals:

- **P0:** type error
- **P1:** optional lifting — either reject still, or implement a dedicated lift that maps the UDO body over measures (complex; do not underestimate)

Do not silently route UDO calls through `DatasetScalarFunctionExecutor`.

## Nested calls

`udoA` body may call `udoB` if `udoB` is in bindings. Works naturally via re-entrant `visitCallDataset`. Recursion: see open questions (default reject via call-stack guard).

## Errors (suggested messages)

| Case | Exception |
|------|-----------|
| Unknown operator (not UDO, not native) | existing `FunctionNotFoundException` |
| Wrong arg type | `InvalidTypeException` / `InvalidArgumentException` |
| Missing mandatory arg | `InvalidArgumentException` |
| `OPTIONAL` without default | `InvalidArgumentException` |
| Return type mismatch | `InvalidTypeException` |
| Unsupported signature type used at define | `UnimplementedException` with pointer to roadmap phase |
