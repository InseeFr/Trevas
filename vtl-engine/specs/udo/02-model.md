# 02 — Model (`UdoDefinition`)

## P0 decision (locked)

**Put a single engine-side artefact in bindings** — no `vtl-model` type in P0.

```java
// vtl-engine: fr.insee.vtl.engine.semantics.udo.UdoDefinition
public final class UdoDefinition {
  private final String name;
  private final List<UdoParameter> parameters;
  private final /* nullable */ Class<?> returnType; // P0: Java class; null = infer at invoke
  private final Positioned position;
  private final VtlParser.ExprContext body;         // parse subtree; not evaluated at define
}
```

```java
public final class UdoParameter {
  private final String name;
  private final Class<?> type;              // P0 subset (scalars + Dataset.class for opaque dataset)
  private final /* nullable */ Object defaultValue; // constant only in P0
  private final boolean optional;           // true iff default present (VTL rule)
}
```

Stored in `ENGINE_SCOPE` under the operator name, same way rulesets are stored.

**Why engine-side:** the body is an ANTLR `ExprContext`. `vtl-model` must stay parser-free. A DTO (`UserDefinedOperator` in `vtl-model`) is optional later for Jackson / SDMX — not a P0 deliverable. See [08 §6](./08-open-questions.md).

Lookup: `bindings.get(name) instanceof UdoDefinition`.

## Body representation

**P0 = Option A (parse subtree).** Define captures the body context; invoke re-enters `ExpressionVisitor` with a child scope. Free vars resolve at invoke time ([08 §1](./08-open-questions.md)).

Do **not** compile the body to `ResolvableExpression` at define time in P0 (Option B stays a possible later optimization).

## Binding namespace

Operators share the flat `ENGINE_SCOPE` bindings map with variables and rulesets.

Collision policy ([08 §2](./08-open-questions.md)):

| Existing binding | New define | P0 |
|------------------|------------|----|
| absent | UDO | ok |
| variable | UDO same name | **error** |
| ruleset | UDO same name | **error** |
| UDO | UDO same name | **error** |
| native function name | UDO same name | **error** |

Invocation lookup order in `visitCallDataset`:

1. UDO in bindings (`UdoDefinition`)
2. native / global method registry

## Typing in P0

Map basic scalar tokens to existing Trevas Java classes (same mapping as `cast`):

| VTL | Java |
|-----|------|
| `integer` | `Long.class` |
| `number` | `Double.class` |
| `string` | `String.class` |
| `boolean` | `Boolean.class` |
| `date` | `Instant.class` (if cast mapping exists; else reject → P1) |
| `time_period` | `Interval.class` (same) |
| `duration` | `PeriodDuration.class` (same) |
| `dataset` (opaque) | `Dataset.class` (or `DatasetExpression` at invoke) |

Unsupported type syntax → clear exception at **define** time (fail fast), not at invoke time. See [06-types](./06-types.md).

If `returns` is omitted: infer from body at first invoke (P0). Define-time probe with placeholders = P1 ([03-define](./03-define.md)).
