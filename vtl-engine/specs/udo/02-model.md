# 02 — Model (`UdoDefinition`)

## P0 decision (locked)

**Source of truth:** engine-side `UdoDefinition` in script bindings (looked up like a variable).  
**Not in the native method registry.** Dispatch is `UdoFunctionExpression` (`ResolvableExpression`), not a trampoline `Method`.

**No `vtl-model` DTO in P0.** The body is an ANTLR `ExprContext` and we keep a `VtlScriptEngine` handle for re-entry / PE. Putting that in `vtl-model` would pull the parser into the model module. Same choice as rulesets: engine artefact stored in bindings. A later model type, if any, would be a thin interface (name + signature) without the parse tree.

```java
// fr.insee.vtl.engine.semantics.udo.UdoDefinition
public final class UdoDefinition {
  private final String name;
  private final List<UdoParameter> parameters;
  private final /* nullable */ Class<?> returnType; // null → infer at invoke
  private final VtlParser.ExprContext body;
  private final VtlScriptEngine engine;             // for body re-entry / PE
}
```

```java
public final class UdoParameter {
  private final String name;
  private final Class<?> type;       // scalars or Dataset.class
  private final Object defaultValue; // if default clause present
  private final boolean optional;    // true iff default clause present
}
```

Factories: `UdoParameter.mandatory(...)`, `UdoParameter.withDefault(...)`.

## Body representation

**Parse subtree** (`ExprContext`). Define does not evaluate the body. Invoke re-enters `ExpressionVisitor` with a child map (params + outer bindings).

## Free variables

A **free variable** is a name used in the body that is **not** a parameter — e.g. `y` in:

```vtl
define operator max_with_y (x integer) returns number is
   if x > y then x else y
end operator;
```

P0 resolves free vars at **invoke time** in the bindings passed to `resolve` ([08 §1](./08-open-questions.md)). Parameters shadow outer names. This is **not** a lexical closure: if `y` is reassigned between define and call, the call sees the new value. Snapshot-at-define (real closures) is later; invoke-time matches DAG reorder (S2).

## Binding + registry namespace

| Existing | New define | P0 |
|----------|------------|----|
| absent in bindings **and** native registries | UDO | ok → `bindings.put` only |
| any binding (var / ruleset / UDO) | same name | **error** `AlreadyDefinedException` (E6) |
| native or global registry key | same name | **error** (E8) — do not shadow a native |

Keywords like `abs` are rejected by the **parser** (`operatorID` = `IDENTIFIER`) before collision checks — test E8 with a plain identifier pre-registered as a native via `registerMethod`.

### Invoke lookup

1. Current bindings `get(name) instanceof UdoDefinition` → UDO path  
2. else native / global `findMethod` path  
3. else `FunctionNotFoundException` (call syntax `name(...)`, same as natives)

## Typing in P0

| VTL | Java |
|-----|------|
| `integer` | `Long.class` |
| `number` | `Double.class` |
| `string` | `String.class` |
| `boolean` | `Boolean.class` |
| `date` / `time_period` / `duration` | same as `cast` if available |
| `dataset` (opaque) | `Dataset.class` |

Unsupported productions → fail at **define**. See [06-types](./06-types.md).  
Omitted `returns` → infer from body result type at invoke (P0).  
Assignability uses existing `TypeChecking` / `checkInstanceOf` — not a parallel table.
