# 03 — Define path (`define operator`)

## Entry point (target)

```java
@Override
public Object visitDefOperator(VtlParser.DefOperatorContext ctx) {
  UdoDefinition udo = UdoDefineExecutor.define(ctx, engine);
  Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
  String name = udo.getName();
  Positioned pos = fromContext(ctx);

  if (bindings.containsKey(name)) {
    throw new AlreadyDefinedException(/* operator id */, pos); // E6
  }
  if (engine.getRegisteredMethods().containsKey(name)
      || engine.getRegisteredGlobalMethods().containsKey(name)) {
    throw … "conflicts with native function"; // E8
  }

  bindings.put(name, udo);
  return udo;
}
```

Visitor stays thin. Signature validation lives in `UdoDefineExecutor`.

**Do not** `registerMethod` the UDO. The operator id is resolved like a variable at the call site ([04](./04-invoke.md)). Registering a trampoline `Method` goes around the bindings and makes closure / scoped lookup moot.

## Grammar note

Close with `end operator` (`END OPERATOR`). Ignore RM prose “end define operator”.

## `UdoDefineExecutor.define` steps

1. Read `operatorID`.
2. Parse each `parameterItem`: unique `varID`, P0 type subset, optional `DEFAULT constant` (type-checked).
3. Parse `RETURNS` if present; else `returnType = null`.
4. Type gate:
   - accept bare / structured-looking `dataset` as **opaque**
   - reject `component` / `set` / `ruleset` / scalar constraints
5. Capture body `expr` (do not evaluate).
6. Return `UdoDefinition` (holds `engine` for later re-entry).

Light checks only at define. Full body type-check → P1.

## After define (side effects)

| Action | Why |
|--------|-----|
| `bindings.put(name, udo)` | Source of truth for invoke (like a dataset / ruleset) |
| Reject bindings collision | E6 — `AlreadyDefinedException` |
| Reject registry collision | E8 — never shadow a native |
| **No** `registerMethod` | Call site looks up the binding, then evaluates a `ResolvableExpression` |

## Defaults

If `default` present → parameter optional at call site (`_` or omitted trailing). Default must match formal type.

## DAG

Unchanged: `DAGBuildingVisitor.visitDefOperator` already tracks `OPERATOR` + free-var deps and reorders. P0 free-var semantics remain **invoke-time** lookup ([08 §1](./08-open-questions.md)).

## What not to do

- Do not execute the body at define.
- Do not write parameter names into outer bindings.
- Do not register a trampoline `Method` under the operator name.
- Do not skip the registry collision check (E8 still applies: cannot `define operator` over a native).
