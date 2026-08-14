# 06 — Type support matrix

Not a parallel type system. Assignability for UDO args / `returns` goes through existing `TypeChecking` / `checkInstanceOf` (`integer` ⊆ `number` already works — D1).

This file only lists **which signature syntax** P0 accepts vs rejects at **define**.

## P0 accept

| Syntax | Notes |
|--------|-------|
| `integer` / `number` / `string` / `boolean` | Same Java classes as the rest of the engine |
| `date` / `time_period` / `duration` | If `cast` already maps them; else reject until then |
| `dataset` | Opaque (`instanceof Dataset` / `DatasetExpression`) |
| `dataset { … }` | **Accepted as opaque** — no structure check (DS4 = P1) |
| omitted `returns` | Infer from body at invoke |

## P0 reject at define

| Syntax | Until |
|--------|-------|
| `component` / `measure` / `attribute` / … | P2 |
| `set <T>`, ruleset types | later |
| scalar constraints (`integer {0,1}`, `[value >= 0]`, nullability) | later |

```
UnimplementedException: UDO parameter type 'component' is not supported yet
```

Do not silently ignore constraint syntax if the grammar attaches it.

## Assignability (reuse engine)

| Case | |
|------|--|
| `integer` → `number` formal / `returns` | allow |
| `number` → `integer` | reject |
| exact scalar match | allow |
| `null` / `Object` | same as other expressions |
| dataset ↔ scalar | reject |
| opaque dataset ↔ opaque dataset | allow |

No extra table in UDO code — call `TypeChecking` / `checkInstanceOf`.
