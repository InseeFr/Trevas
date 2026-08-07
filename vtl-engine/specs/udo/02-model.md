# 02 — Model (`UdoDefinition`)

## P0 decision (locked)

**Source of truth:** engine-side `UdoDefinition` in `ENGINE_SCOPE` bindings.  
**Dispatch hook:** trampoline `java.lang.reflect.Method` registered under the same name (not a second source of truth).

No `vtl-model` DTO in P0.

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

**Parse subtree** (`ExprContext`). Define does not evaluate the body. Invoke re-enters `ExpressionVisitor` with a child map (params + outer bindings). Free vars resolve at **invoke time** ([08 §1](./08-open-questions.md)).

## Trampoline (not the model)

`UdoTrampoline.invoke0…invoke8` — public static methods with `Object` parameters. Used only so `FunctionExpression` can call `Method.invoke`. CallSite (`udo` + outer bindings) is set via ThreadLocal around invoke ([04](./04-invoke.md)).

## Binding + registry namespace

| Existing | New define | P0 |
|----------|------------|----|
| absent in bindings **and** registries | UDO | ok → put binding + `registerMethod` |
| any binding (var / ruleset / UDO) | same name | **error** (E6) |
| native or global registry key | same name | **error** (E8) |

Keywords like `abs` are rejected by the **parser** (`operatorID` = `IDENTIFIER`) before collision checks — test E8 with a plain identifier pre-registered via `registerMethod`.

### Invoke lookup

1. `bindings.get(name) instanceof UdoDefinition` → UDO path  
2. else native / global `findMethod` path

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
