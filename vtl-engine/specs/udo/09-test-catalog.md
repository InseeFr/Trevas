# 09 — Test catalog

**Executable suite:** `vtl-engine/src/test/java/fr/insee/vtl/engine/visitors/UserDefinedOperatorTest.java`

**Run:**

```bash
mvn -pl vtl-engine -Dtest=UserDefinedOperatorTest test
```

**Status today:** suite **written, all red** (no prod). **Gate before coding:** reviewers sign off this catalog + [10](./10-implementation.md) slice map.

---

## Test-first contract

| Rule | Meaning |
|------|---------|
| **Red first** | Every P0 ID has a JUnit method **before** the implem slice that makes it green |
| **One slice ↔ one reason** | Each `feat(udo)` commit turns green **only** the IDs listed for that slice |
| **Acceptance = script** | `UserDefinedOperatorTest` is the product bar (not unit tests alone) |
| **Métier + technique** | Métier = reusable VTL recipes; technique = engine semantics (DAG, scope, errors) |
| **Precise asserts** | Scalars: exact value + Java type; datasets: row sets; errors: not `FunctionNotFound` when UDO exists |

---

## Coverage map

| Category | IDs | Count | Role |
|----------|-----|-------|------|
| **Doc / RM** | D1–D4 | 4 | VTL 2.1 reference examples (incl. corrected typo) |
| **Technique** | S1–S4 | 4 | Inference, free vars, `_`, nested UDO |
| **Métier** | DS1–DS5 | 5 | Pipeline recipes (filter, calc, union, scale) |
| **Erreurs** | E1–E7 | 7 | Define + invoke failure modes |
| **P1** | DS4 | 1 | Structured `dataset {…}` (`@Disabled`) |
| **Total P0** | | **20** | All enabled except DS4 |

---

## Slice → test matrix (P0)

| Slice | Commit type | IDs that must turn green | Prod scope |
|-------|-------------|--------------------------|------------|
| **0** | baseline | — (all red) | specs only |
| **1a** | test | E1, E2, E6 hardened | none |
| **1b** | feat | E1, E2, E6 | define + `UdoDefinition` in bindings |
| **2a** | test | D1, D3, S1, S5 review | none |
| **2b** | feat | D1, D3, S1, S5 | scalar invoke + scope |
| **3a** | test | D2, S3, E3–E5, E7 review | none |
| **3b** | feat | D2, S3, E3–E5, E7 | defaults + `_` + arity |
| **4a** | test | S2 review | none |
| **4b** | feat | S2 | free vars @ invoke |
| **5a** | test | DS1–DS3, DS5 review | none |
| **5b** | feat | DS1–DS3, DS5 | opaque `dataset` + mixed |
| **6a** | test | D4 review | none |
| **6b** | feat | D4 | `returns` enforcement |
| **7** | test+feat if needed | S4 | UDO → UDO re-entry |
| **8** | docs | all P0 checked | user docs |

If tests already exist and assert correctly in slice **0**, skip redundant **a** commits ([10](./10-implementation.md)).

---

## Doc / RM (D*)

Conformance with VTL 2.1 UDO examples (`vtl_dl_udo.rst`).

| ID | JUnit | Slice | Script essence | Expected |
|----|-------|-------|----------------|----------|
| **D1** | `testD1AddTwoArgs` | 2b | `add(x int default 0, y int default 0) returns number`; `res := add(1, 2)` | `res == 3L` (`Long`); `integer` body assignable to `returns number` |
| **D2** | `testD2AddDefaults` | 3b | same `add`; `one := add(5)`; `zero := add()` | `one == 5L`, `zero == 0L` |
| **D3** | `testD3Max1CorrectedReturnType` | 2b | `max1(x,y) returns integer`; `max1(3,7)` | `res == 7L` (RM typo fixed: not `boolean`) |
| **D4** | `testD4Max1DocTypoRejected` | 6b | literal RM: `returns boolean` + `if x > y then x else y` | **throws** at define or invoke; message mentions `boolean` |

---

## Technique (S*)

Engine behaviour — not tied to a RM example.

| ID | JUnit | Slice | What it proves | Setup / script | Expected |
|----|-------|-------|----------------|----------------|----------|
| **S1** | `testS1InferredReturn` | 2b | Omit `returns` | `twice(x) is x+x`; `twice(21)` | `42L`; inferred scalar type |
| **S2** | `testS2FreeVariable` | 4b | Free var + **DAG reorder** | call before define; `y := 4` after define; body uses `y` | `max_res == 4L` (not 2) |
| **S3** | `testS3OptionalUnderscore` | 3b | `_` → default | `add(10, _)` with defaults 0 | `10L` |
| **S4** | `testS4NestedUdoCall` | 7 | UDO calls UDO | `quadruple` body = `twice(x)+twice(x)` | `12L` |

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

---

## Métier (DS*)

Reusable **transformation recipes** — why UDOs exist in production scripts.

| ID | JUnit | Slice | Métier intent | Body pattern | Assert |
|----|-------|-------|---------------|--------------|--------|
| **DS1** | `testDs1FilterRecipe` | 5b | **Filtrer** rows by threshold | `ds[filter long1 > threshold]` | 2 rows from `DatasetSamples.ds1` (Toto, Franck); not Hadrien/Nico |
| **DS2** | `testDs2CalcRecipe` | 5b | **Enrichir** with derived measure | `ds[calc long1_x2 := long1 * 2]` | 3 rows; each has `long1_x2 == 2 * long1` |
| **DS3** | `testDs3UnionTwoDatasets` | 5b | **Fusionner** two inputs | `union(a, b)` | 3 rows `{a,1}`, `{b,2}`, `{c,3}` |
| **DS5** | `testDs5DatasetAndScalar` | 5b | **Paramétrer** a calc (scale) | `ds[calc long1 := long1 * factor]` with `factor integer` | Nico row: `long1 == 60` (20×3) |
| **DS4** | `testDs4StructuredDatasetType` | P1 | Structure contract | typed `dataset { id, long1 }` | `@Disabled` until P1 |

### DS1 row expectation (reference)

After `keep_long1_gt(ds1, 25)` — ids **Toto** (`long1=30`) and **Franck** (`long1=100`) only.

### DS5 métier note

Models “apply same recipe with different factor” without duplicating VTL — shared library pattern (host loads defs, script calls `scale_long1(ds, 3)`).

---

## Erreurs (E*)

| ID | JUnit | Slice | Phase | Trigger | Must **not** be | Expected failure |
|----|-------|-------|-------|---------|-------------------|------------------|
| **E1** | `testE1DuplicateParam` | 1b | define | `(x integer, x integer)` | — | error mentions `x` / duplicate |
| **E2** | `testE2WrongDefaultType` | 1b | define | `x integer default "nope"` | — | error mentions `integer` / type |
| **E3** | `testE3MissingMandatory` | 3b | invoke | `add2(1)` two mandatory params | `FunctionNotFound` | arity / missing arg |
| **E4** | `testE4TooManyArgs` | 3b | invoke | `id(1, 2)` one param | `FunctionNotFound` | too many args |
| **E5** | `testE5TypeMismatch` | 3b | invoke | `id("nope")` for `integer` | `FunctionNotFound` | type mismatch |
| **E6** | `testE6NameCollision` | 1b | define | `add := 1` then `define operator add` | silent overwrite | exception |
| **E7** | `testE7OptionalWithoutDefault` | 3b | invoke | `add2(1, _)` no default on `y` | `FunctionNotFound` | `_` without default |

### Planned errors (catalog only — add in slice 1a if missing)

| ID | Category | Trigger | Expected |
|----|----------|---------|----------|
| **E8** | define | `define operator abs` when native `abs` exists | reject collision with native |
| **E9** | define | define UDO same name as existing ruleset | reject (same policy as E6) |

Not in JUnit yet — add in **1a** if reviewers want native/ruleset collision explicit.

---

## Checklist (P0 sign-off)

Copy for review PR:

```
[ ] 20 enabled tests exist in UserDefinedOperatorTest
[ ] Each ID maps to exactly one JUnit method
[ ] D* — RM examples covered (D4 = reject typo)
[ ] S* — DAG (S2), inference (S1), _ (S3), nested (S4)
[ ] DS* — filter, calc, union, mixed param (DS4 disabled)
[ ] E* — define errors before invoke errors where applicable
[ ] Slice map in 10 agreed with team
[ ] mvn -pl vtl-engine -Dtest=UserDefinedOperatorTest → FAIL (expected pre-impl)
```

---

## Doc note on `max1`

Official example uses `returns boolean` with an integer body — spec typo. D3 = corrected; D4 = literal signature rejected.
