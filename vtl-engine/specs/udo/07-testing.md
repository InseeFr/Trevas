# 07 — Testing strategy

## Where we are

| Item | Status |
|------|--------|
| Specs `00`–`10` (pattern-locked) | ✅ |
| `UserDefinedOperatorTest` | ✅ green (DS4 skipped) |
| `UdoPatternWalkthroughTest` | ✅ green — step through layers |
| Production P0 path | ✅ in engine |

## Test-first (mandatory for further slices)

```
1. Catalog / JUnit for the slice IDs → red
2. Implement pattern step (see 10) → green
3. Check off 09 / 10
```

## Test layers

| Layer | Location | Purpose |
|-------|----------|---------|
| **Acceptance** | `UserDefinedOperatorTest` | Product bar (D/S/DS/E) |
| **Walkthrough** | `UdoPatternWalkthroughTest` | Breakpoints: define → FunctionExpression → trampoline |
| **DAG** | `DagDefineStatementsTest` | Reorder; S2 covers execution |
| **Spark IT** | P1 | Dataset UDO on PE |

## Pattern checkpoints (walkthrough)

Assert after define:

- `bindings.get(name) instanceof UdoDefinition`
- `getRegisteredMethods()` contains trampoline `UdoTrampoline.invokeN`

Assert after call:

- result value / dataset rows
- path used `Method.invoke` (debug: break in `UdoTrampoline.dispatch`)

## Running

```bash
mvn -pl vtl-engine -Dtest=UserDefinedOperatorTest,UdoPatternWalkthroughTest test
mvn -pl vtl-engine -Dtest=UdoPatternWalkthroughTest#walkthrough_scalarAdd_viaFunctionExpressionAndMethodInvoke test
```

IDE: run as JUnit on module `vtl-engine` (factory fallback if SPI `vtl` is null).

## Categories

| Tag | IDs |
|-----|-----|
| Doc | D1–D4 |
| Métier | DS1–DS5 |
| Technique | S1–S4 |
| Erreurs | E1–E8 |

## After P0

| Phase | Tests |
|-------|-------|
| P1 | Enable DS4; recursion; optional Spark IT |
| P2+ | New catalog section |
