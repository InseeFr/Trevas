# 03 — Define path (`define operator`)

## Entry point (as implemented)

```java
@Override
public Object visitDefOperator(VtlParser.DefOperatorContext ctx) {
  UdoDefinition udo = UdoDefineExecutor.define(ctx, engine);
  Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
  String name = udo.getName();

  if (bindings.containsKey(name)) {
    throw … "name already bound";
  }
  if (engine.getRegisteredMethods().containsKey(name)
      || engine.getRegisteredGlobalMethods().containsKey(name)) {
    throw … "conflicts with native function";
  }

  bindings.put(name, udo);
  engine.registerMethod(name, UdoTrampoline.methodForArity(udo.getParameters().size()));
  return udo;
}
```

Visitor stays thin. Signature validation lives in `UdoDefineExecutor`.

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

## After define (mandatory side effects)

| Action | Why |
|--------|-----|
| `bindings.put(name, udo)` | Source of truth for invoke |
| `registerMethod(name, trampoline)` | Enables `FunctionExpression` / `Method.invoke` path |
| Reject registry collision **before** register | E8 — never shadow a native |

## Defaults

If `default` present → parameter optional at call site (`_` or omitted trailing). Default must match formal type.

## DAG

Unchanged: `DAGBuildingVisitor.visitDefOperator` already tracks `OPERATOR` + free-var deps and reorders. P0 free-var semantics remain **invoke-time** lookup ([08 §1](./08-open-questions.md)).

## What not to do

- Do not execute the body at define.
- Do not write parameter names into outer bindings.
- Do not rely on registry alone (always keep `UdoDefinition` in bindings).
- Do not skip the registry collision check when registering the trampoline.
