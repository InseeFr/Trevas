# 03 — Define path (`define operator`)

## Entry point

Mirror rulesets in `AssignmentVisitor`:

```java
@Override
public Object visitDefOperator(VtlParser.DefOperatorContext ctx) {
  UdoDefinition udo = UdoDefineExecutor.define(ctx, expressionVisitor, engine);
  Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
  String name = udo.getName();
  assertNameAvailable(bindings, name, fromContext(ctx));
  bindings.put(name, udo);
  return udo;
}
```

Visitor stays thin. All validation lives in `UdoDefineExecutor`.

## Grammar note

Trevas / official ANTLR close with `end operator` (`END OPERATOR`). Some RM prose says “end define operator”; **implement against the grammar**, not the prose variant.

## `UdoDefineExecutor.define` steps

1. **Read name** from `operatorID`.
2. **Parse parameters** from `parameterItem+`:
   - `varID` → name (unique; else error)
   - `inputParameterType` → via `UdoTypeSupport.parseInputType` (**P0: basic scalars + opaque `dataset`**)
   - optional `DEFAULT constant` → visit with `ConstantVisitor`, check type compatibility (scalars only in practice; opaque dataset defaults are not expected)
3. **Parse return type** if `RETURNS` present; else leave empty for inference at invoke.
4. **Type subset gate:**
   - **Accept** bare `dataset` (opaque) — in P0
   - **Accept** structured `dataset { … }` syntax but **do not enforce** structure in P0 (same as DS4 deferred → P1; treat as opaque)
   - **Reject** immediately: `component` / `set` / `ruleset` / scalar constraints / nullability modifiers
5. **Capture body** `expr` (do not fully evaluate).
6. **Optional define-time checks** (P0: skip heavy probe):
   - Light: parameter uniqueness + type subset + default type match.
   - Full body type-check with placeholders → **P1**.
7. **Return** `UdoDefinition`.

### P0 recommendation on define-time body check

**Light check only** at define.  
**Defer** full body type-check to first invoke.  
Document in release notes: P0 may accept a define that later fails on invoke.

## Defaults

VTL rule: if `default` is present, parameter is optional at call site.

Constraints from the reference manual:

- default value type must match parameter type
- parameter names unique

Call-site `_` / `OPTIONAL` token must map to “use default”; if no default → error.

## DAG interaction

No change expected: `visitDefOperator` already:

- emits `Identifier.Type.OPERATOR`
- ignores parameter names as dependencies
- includes free vars from the body as `VARIABLE` dependencies
- reorders parent `DefineExpressionContext`

Execution order after preprocessor: defines run before calls that depend on them; free-var assignments run before the define if the define body references them **as dependencies of the define statement**.

Note: the DAG attaches free-var deps to the **define** statement, so:

```vtl
res := max_with_y(b);
b := 2;
define operator max_with_y (x integer) returns … is if x > y then x else y end operator;
y := 4;
```

is reordered to assign `b`, `y`, then define, then call. That means at **define** time `y` already exists in bindings, and at **invoke** time too. Good for either capture strategy; P0 still uses **invoke-time** free-var lookup ([08 §1](./08-open-questions.md)).

## What not to do in define

- Do not execute the body for side effects.
- Do not write parameter names into outer bindings.
- Do not register into `NativeFunctionRegistry`.
