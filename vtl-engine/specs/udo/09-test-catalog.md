# 09 — Test catalog

**Executable suite:** `vtl-engine/src/test/java/fr/insee/vtl/engine/visitors/UserDefinedOperatorTest.java`  
**Walkthrough:** `UdoPatternWalkthroughTest`

**Status:** acceptance **green** on `feat/udo` (P0–P3 catalog). Runtime path: [01](./01-architecture.md).

---

## Test-first contract

| Rule | Meaning |
|------|---------|
| **Red first** | New IDs (P1+) have a JUnit method **before** the implem that makes them green |
| **One slice ↔ one reason** | Each `feat(udo)` commit turns green **only** the IDs listed for that slice |
| **Acceptance = script** | `UserDefinedOperatorTest` is the product bar (not unit tests alone) |
| **Recipes + technique** | Recipes = reusable VTL pipelines; technique = engine semantics (DAG, scope, errors) |
| **Precise asserts** | Scalars: exact value + Java type; datasets: row sets; errors: not `FunctionNotFound` when UDO exists |
| **Unit first** | Hardcoded `UdoDefinition` / `UdoFunctionExpression` before more visitor wiring |

---

## Coverage map

| Category | IDs | Count | Role |
|----------|-----|-------|------|
| **Doc / RM** | D1–D4 | 4 | VTL 2.1 reference examples (incl. corrected typo) |
| **Technique** | S1–S5 | 5 | Inference, free vars, `_`, nested UDO, closure snapshot |
| **Recipes** | DS1–DS5 | 5 | Pipeline recipes (filter, calc, union, scale) |
| **Errors** | E1–E8 | 8 | Define + invoke failure modes (incl. registry collision) |
| **P1** | DS4, E9, E9-b | 3 | Structured `dataset {…}`, direct + mutual recursion guard |
| **P2** | P2-2 … P2-7 | 6 | component params, wildcards, viral attribute |
| **P3** | P3-1 … P3-3 | 3 | ruleset params, scalar set guard |
| **Aux** | S6, DS4-neg | 2 | string/boolean smoke, structured dataset mismatch |
| **Total enabled** | | **37** | P0 + P1 + P2 + P3 + aux |

---

## Slice → test matrix (P0) — historical → done

| Slice | Commit type | IDs that turn green | Prod scope | State |
|-------|-------------|---------------------|------------|-------|
| **1** | feat | E1, E2, E6, E8 | define + collisions + bindings only | ✅ |
| **2** | feat | D1, D3, S1 | scalar invoke via FunctionExpression | ✅ |
| **3** | feat | D2, S3, E3–E5, E7 | defaults + `_` + arity | ✅ |
| **4** | feat | S2 | free vars @ invoke | ✅ |
| **5** | feat | DS1–DS3, DS5 | opaque `dataset` + clause merge | ✅ |
| **6** | feat | D4 | `returns` enforcement | ✅ |
| **7** | feat | S4 | UDO → UDO re-entry | ✅ |
| **8** | docs | — | Docusaurus user docs | ⬜ B8 |

Aligned with [10-implementation](./10-implementation.md) steps 1–8.

---

## Doc / RM (D*)

Conformance with VTL 2.1 UDO examples (`vtl_dl_udo.rst`).

| ID | JUnit | Slice | Script essence | Expected |
|----|-------|-------|----------------|----------|
| **D1** | `testD1AddTwoArgs` | 2 | `add(x int default 0, y int default 0) returns number`; `res := add(1, 2)` | `res == 3L` (`Long`); `integer` body assignable to `returns number` |
| **D2** | `testD2AddDefaults` | 3 | same `add`; `one := add(5)`; `zero := add()` | `one == 5L`, `zero == 0L` |
| **D3** | `testD3Max1CorrectedReturnType` | 2 | `max1(x,y) returns integer`; `max1(3,7)` | `res == 7L` (RM typo fixed: not `boolean`) |
| **D4** | `testD4Max1DocTypoRejected` | 6 | literal RM: `returns boolean` + `if x > y then x else y` | **throws** at define or invoke; message mentions `boolean` |

---

## Technique (S*)

Engine behaviour — not tied to a RM example.

| ID | JUnit | Slice | What it proves | Setup / script | Expected |
|----|-------|-------|----------------|----------------|----------|
| **S1** | `testS1InferredReturn` | 2 | Omit `returns` | `twice(x) is x+x`; `twice(21)` | `42L`; inferred scalar type |
| **S2** | `testS2FreeVariable` | 4 | Free var + **DAG reorder** | call before define; `y := 4` after define; body uses `y` | `max_res == 4L` (not 2) |
| **S3** | `testS3OptionalUnderscore` | 3 | `_` → default | `add(10, _)` with defaults 0 | `10L` |
| **S4** | `testS4NestedUdoCall` | 7 | UDO calls UDO | `quadruple` body = `twice(x)+twice(x)` | `12L` |
| **S5** | `testS5LexicalClosureSnapshot` | P3 | Closure snapshot at define | `y := 2` before define; host sets `y := 4` before call | `max_res == 2L` (not 4) |
| **S6** | `testS5StringAndBoolean` | 2 | `string` / `boolean` returns | `shout`, `is_adult` | typed scalar results |

### S2 detail (DAG — critical)

Input order (intentionally wrong):

```vtl
max_res := max_with_y(b);   // uses operator + free var y
b := 2;
define operator max_with_y (x integer) returns number is
   if x > y then x else y
end operator;
y := 4;
```

Preprocessor must reorder to: `b`, `y`, **define**, **call**. At invoke, `y == 4`, `x == 2` → `max_res == 4`.

### S5 detail (closure)

Free variables **bound at define** are snapshotted into `UdoDefinition`. At invoke, snapshot values override the current bindings for those names. Variables not yet bound at define still resolve at invoke (S2). S5 uses a host reassignment between define and call (DAG forbids `y := 2` and `y := 4` in one script).

---

## Recipes (DS*)

Reusable **transformation recipes** — why UDOs exist in production scripts.

| ID | JUnit | Slice | Intent | Body pattern | Assert |
|----|-------|-------|--------|--------------|--------|
| **DS1** | `testDs1FilterRecipe` | 5 | Filter rows by threshold | `ds[filter long1 > threshold]` | 2 rows from `DatasetSamples.ds1` (Toto, Franck); not Hadrien/Nico |
| **DS2** | `testDs2CalcRecipe` | 5 | Enrich with derived measure | `ds[calc long1_x2 := long1 * 2]` | 3 rows; each has `long1_x2 == 2 * long1` |
| **DS3** | `testDs3UnionTwoDatasets` | 5 | Union two inputs | `union(a, b)` | 3 rows `{a,1}`, `{b,2}`, `{c,3}` |
| **DS5** | `testDs5DatasetAndScalar` | 5 | Parameterise a calc (scale) | `ds[calc long1 := long1 * factor]` with `factor integer` | Nico row: `long1 == 60` (20×3) |
| **DS4** | `testDs4StructuredDatasetType` | P1 | Structure contract | typed `dataset { id, long1 }` | matching dataset accepted |
| **DS4-neg** | `testDs4StructuredDatasetMismatchRejected` | P1 | Structure mismatch | wrong measure name vs signature | error mentions component |

### DS1 row expectation (reference)

After `keep_long1_gt(ds1, 25)` — ids **Toto** (`long1=30`) and **Franck** (`long1=100`) only.

### DS5 note

Models “apply same recipe with different factor” without duplicating VTL — shared library pattern (host loads defs, script calls `scale_long1(ds, 3)`).

---

## Errors (E*)

| ID | JUnit | Slice | Phase | Trigger | Must **not** be | Expected failure |
|----|-------|-------|-------|---------|-------------------|------------------|
| **E1** | `testE1DuplicateParam` | 1 | define | `(x integer, x integer)` | — | error mentions `x` / duplicate |
| **E2** | `testE2WrongDefaultType` | 1 | define | `x integer default "nope"` | — | error mentions `integer` / type |
| **E3** | `testE3MissingMandatory` | 3 | invoke | `add2(1)` two mandatory params | `FunctionNotFound` | arity / missing arg |
| **E4** | `testE4TooManyArgs` | 3 | invoke | `id(1, 2)` one param | `FunctionNotFound` | too many args |
| **E5** | `testE5TypeMismatch` | 3 | invoke | `id("nope")` for `integer` | `FunctionNotFound` | type mismatch |
| **E6** | `testE6NameCollision` | 1 | define | `add := 1` then `define operator add` | silent overwrite | exception |
| **E7** | `testE7OptionalWithoutDefault` | 3 | invoke | `add2(1, _)` no default on `y` | `FunctionNotFound` | `_` without default |
| **E8** | `testE8NativeRegistryCollision` | 1 | define | plain `IDENTIFIER` pre-`registerMethod`'d then `define operator` same name | silent overwrite | reject registry collision |
| **E9** | `testE9DirectRecursionRejected` | P1 | invoke | `recurse(x)` body | stack overflow | `recursive` message |
| **E9-b** | `testE9bMutualRecursionRejected` | P1 | invoke | `ping` ↔ `pong` (separate defines) | stack overflow | `recursive` message |

**E8 note:** do not use keyword `abs` (parse error). Register a non-keyword name via `registerMethod` before define.

## P2 (component params)

| ID | JUnit | Phase | Trigger | Expected |
|----|-------|-------|---------|----------|
| **P2-2** | `testP2ScaleByComponentParam` | invoke | `(m measure < integer >)` + `ds[calc scaled := m * factor]` | scaled measure column |
| **P2-3** | `testP2ComponentRoleMismatchRejected` | invoke | identifier passed for `measure` formal | `role` in message |

Call site passes a **variable** bound to `Structured.Component` (Trevas grammar: `varID` only, not `ds#col`).

| ID | JUnit | Phase | Trigger | Expected |
|----|-------|-------|---------|----------|
| **P2-4** | `testP2WildcardDatasetSignature` | invoke | `measure < integer > _` in dataset param | matching mono-measure dataset accepted |
| **P2-5** | `testP2WildcardDatasetMissingMeasureRejected` | invoke | id-only dataset vs `measure _` | `exactly one` in message |
| **P2-6** | `testP2ViralAttributeComponentParam` | invoke | `viral attribute < string >` formal in filter | one matching row |
| **P2-7** | `testP2ViralAttributeWildcardDataset` | invoke | `viral attribute < string > _*` in dataset param | dataset with viral attr accepted |

Wildcards: `_` (exactly one), `_+` (one or more), `_*` (zero or more) on non-predetermined components.

## P3 (ruleset / set)

| ID | JUnit | Phase | Trigger | Expected |
|----|-------|-------|---------|----------|
| **P3-1** | `testP3RulesetParamCheckDatapoint` | invoke | `(rs ruleset)` + `check_datapoint(ds, rs)` in body | same result as direct `check_datapoint` |
| **P3-2** | `testP3RulesetTypeMismatchRejected` | invoke | scalar passed for `ruleset` formal | `ruleset` in message |
| **P3-3** | `testP3ScalarSetParamRejectedAtDefine` | define | `set < integer >` formal | `scalar set` in message |

Call site passes a **variable** bound to `DataPointRuleset` or `HierarchicalRuleset` (same as component params: `varID` only).

### Deferred (catalog only)

| ID | Category | Trigger | Expected |
|----|----------|---------|----------|
| **E10** | define | define UDO same name as existing ruleset | reject (same policy as E6) — add when needed |

---

## Checklist (P0 sign-off)

```
[x] Enabled tests exist in UserDefinedOperatorTest (D/S/DS1–3+DS5/E1–E8)
[x] Each ID maps to exactly one JUnit method
[x] D* — RM examples covered (D4 = reject typo)
[x] S* — DAG (S2), inference (S1), _ (S3), nested (S4)
[x] DS* — filter, calc, union, mixed param, structured DS4
[x] E* — define + invoke errors incl. E8 registry + E9/E9-b recursion
[x] Pattern walkthrough green
[x] mvn -pl vtl-engine -Dtest=UserDefinedOperatorTest,UdoPatternWalkthroughTest → SUCCESS
[x] Docusaurus user docs (B8)
```

---

## Doc note on `max1`

Official example uses `returns boolean` with an integer body — spec typo. D3 = corrected; D4 = literal signature rejected.
