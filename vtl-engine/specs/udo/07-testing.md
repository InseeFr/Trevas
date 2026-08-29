# 07 — Testing strategy

**Absolute first step** (before more visitor wiring): unit-test the expression/model with a **hardcoded** `UdoDefinition` — no parse of `define operator`. Replace the hardcoded instance with the dynamically defined one once define/invoke are wired.

Same idea as the provenance corpus: script-level cases as VTL (+ later `.vtl` fixtures), plus engine-level unit tests.

## Layers

| Layer | Location | Purpose |
|-------|----------|---------|
| **Unit (model)** | `expressions/UdoFunctionExpressionTest`, `semantics/udo/UdoInvokeExecutorTest`, `UdoDatasetTypeParserTest`, `UdoStructureCheckTest`, `UdoComponentTypeParserTest` | Hardcoded UDO → `resolve`; parser / structure / component checks in isolation |
| **Acceptance** | `UserDefinedOperatorTest` | Product bar (D/S/DS/E) via `engine.eval` |
| **Walkthrough** | `UdoPatternWalkthroughTest` | Bindings artefact + call path |
| **DAG** | `DagDefineStatementsTest` | Reorder; S2 covers execution |
| **Fixtures (later)** | `.vtl` files, prov-style `$input` | Optional; do not block P0 on `vtl-test-utils` |

## Test-first

```
0. Hardcoded UdoDefinition + UdoFunctionExpression  → green (controls the base)
1. Catalog / JUnit for the slice IDs                 → red
2. Minimal engine change                             → those IDs green
3. Check off 09 / 10
```

## Running

```bash
mvn -pl vtl-engine -Dtest=UdoFunctionExpressionTest,UdoInvokeExecutorTest,UdoDatasetTypeParserTest,UdoStructureCheckTest,UdoComponentTypeParserTest,UdoCallLookupTest,UserDefinedOperatorTest,UdoPatternWalkthroughTest test
```

IDE: run as JUnit on module `vtl-engine` (factory fallback if SPI `vtl` is null).

## Categories

| Tag | IDs |
|-----|-----|
| Doc | D1–D4 |
| Recipes | DS1–DS5 |
| Technique | S1–S4 |
| Errors | E1–E8 |
