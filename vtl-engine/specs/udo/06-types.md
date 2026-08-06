# 06 — Type support matrix

Based on VTL DL (`vtl_dl_udo.rst`) vs Trevas today.

## Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | P0 |
| 🔜 | P1 |
| 📦 | P2 |
| 🧊 | P3 / later |
| ❌ | out of scope unless demanded |

## Input parameter types

| Syntax | Phase | Notes |
|--------|-------|-------|
| `integer` / `number` / `string` / `boolean` | ✅ | Map to existing Java classes |
| `date` / `time` / `time_period` / `duration` | ✅/🔜 | Include if cast mapping exists; else 🔜 |
| `scalar` | 🔜 | Top type — only if useful |
| `integer {0,1}` / `number [value >= 0]` | 🧊 | Needs constraint evaluator |
| `(not) null` modifiers | 🧊 | |
| `dataset` | ✅ | Opaque (`instanceof Dataset`) — required in P0 |
| `dataset { … }` | 🔜 | Structural checks in P1 |
| `component` / `measure` / … | 📦 | Component visitor context |
| `set <T>` | 🧊 | |
| `ruleset` / `datapoint_…` / `hierarchical_…` | 🧊 | Pass ruleset artefacts as args |

## Return types

| Syntax | Phase |
|--------|-------|
| basic scalar | ✅ |
| omitted (`returns` absent) | ✅ infer from body |
| opaque `dataset` | ✅ |
| structured `dataset {…}` | 🔜 |
| `component` | 📦 |

## Assignability (P0 rules) — **locked**

Reuse `TypeChecking` (same rules as the rest of the engine):

| Case | P0 |
|------|----|
| `integer` (`Long`) actual / body → `number` formal / `returns` | **allow** (`TypeChecking` already treats number widenings) — needed for D1 (`returns number` + `x + y` integers) |
| `number` → `integer` | **reject** |
| exact scalar match | allow |
| `null` / unknown (`Object`) | follow existing expression rules |
| dataset actual → scalar formal | **reject** |
| scalar actual → opaque `dataset` formal | **reject** |
| opaque `dataset` ↔ opaque `dataset` | allow (`instanceof Dataset` / `DatasetExpression`) |

Do not invent a parallel assignability table for UDOs.

## Fail-fast policy

Type productions **not** in the active phase must throw at **define** time (e.g. `component`, constraints):

```
UnimplementedException: UDO parameter type 'component' is not supported yet
(see vtl-engine/specs/udo — phase P2)
```

Structured `dataset { … }` is **accepted as opaque** in P0 (no structure check). Enforcement = P1 (DS4). Do not silently ignore scalar constraints if the grammar attaches them — reject until P3.
