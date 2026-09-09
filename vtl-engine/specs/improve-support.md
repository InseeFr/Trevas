# Improve VTL 2.1 TCK support

Baseline Spark 3 after CSV TimePeriod + `group except`: **123 / 183**.
After P0 Integer/Number + set operators (`setdiff`/`intersect`/`symdiff`): **132 / 183**.
After Join body (`filter`/`calc`/`apply`/`keep`/`drop`/`rename`, incl. unary legacy): **136 / 183**.
After `exists_in`: **139 / 183**.
After viral attribute nulls-first min on Spark: **141 / 183** pass, **42** fails (local Spark 3 TCK).

## Method

Source of truth for each operator: official VTL 2.1 reference (`vtl/v2.1/docs/…`) — Result type, Behavior / Semantics, then typical behaviours. TCK examples validate; they do not override the manual when they conflict (document the conflict).

Implement generically for the operator (scalar / component / dataset, aggregate vs analytic when the manual uses the same Result type). Avoid one-off casts that only silence a single fixture.

When a theme is done: unit tests (engine + Spark 3/4 when PE changes), related TCK subset green, update this doc (baseline, theme status, “Out of scope / done”), and flip the matching rows in `docs/docs/user-guide/coverage/*.mdx` (+ FR i18n).

Order: non-date work first (by TCK cases unlocked), date / time work last.
Within each block, higher impact first, then cost / dependencies.

Measure: `mvn test -pl coverage -am` then `python3 coverage/scripts/render_tck_job_summary.py`.

## Overview

### Non-date (do first)

| Priority | Theme | Cases ≈ | Effort | Status |
|----------|--------|------:|--------|--------|
| P0 | Integer vs Number fidelity (`sum`/`mod`/`round`/`trunc`/windows) | 8 | M | done |
| P1 | Set operators (`setdiff`/`intersect`/`symdiff`) | 4 | M | done |
| P1 | Join (structure + rows) | 4 | M | done |
| P1 | `exists_in` | 3 | S | done |
| P1 | Viral / null attributes in aggregation | 2–3 | M | done |
| P2 | Validation `check` / `check_datapoint` | 3 | M | |
| P2 | `hierarchy` (+ `check_hierarchy`) | 3–4 | L | |
| P3 | Misc non-date (unpivot, if datasets, `in` valuedomain, random, median, log) | 7 | S–M | |

### Date / time (do last)

| Priority | Theme | Cases ≈ | Effort |
|----------|--------|------:|--------|
| T0 | `fill_time_series` | 8 | L |
| T1 | SDMX TimePeriod + `timeshift` / `period_indicator` | 6 | L |
| T1 | Date / duration extractors and converters | 6 | M |
| T2 | `flow_to_stock` / `stock_to_flow` | 4 | M |
| T3 | `time_agg` | 1 | M |

Counts overlap a bit (e.g. hierarchy and check_hierarchy). Non-date ceiling ≈ 30+ cases before touching time.

## Non-date — P0 / P1 / P2 / P3

### 1. Integer vs Number type fidelity (~8) — done

Follow each operator’s official type rules (not TCK majority vote):

- `sum`: Result type is always `number` (aggregate and analytic). Integer measures promote to Number.
- `mod`: Integer×Integer → Integer; otherwise Number (Behavior paragraph).
- `round` / `trunc`: omitted `numDigit` → Integer; otherwise Number (Semantics). Watch Spark SQL shortcuts that return Double.
- Other numeric ops: same pattern as Addition / Multiplication / Division docs.

TCK harness hack (2.1 only): a few `sum` examples disagree Integer vs Number for the same pattern ([sdmx-twg/vtl#708](https://github.com/sdmx-twg/vtl/issues/708); clarified in 2.2 via [PR #713](https://github.com/sdmx-twg/vtl/pull/713)). `TckStructureComparison` soft-matches Long↔Double **only** on that allowlist; every other operator stays strict. Drop the hack when fixtures target 2.2.

### 2. Set operators (~4) — done

`setdiff` (2), `intersect` (1), `symdiff` (1): same structure; compare by identifier keys;
leftmost datapoint on collision; no attribute propagation.

Implemented in `SetOperatorsExecutor` + PE (`executeSetDiff` / `executeIntersect` / `executeSymDiff`)
for InMemory and Spark 3/4 (`left_anti` / `left_semi`). TCK set leaves green (4/4).

### 3. Join (~4) — done

ex_4/ex_5 structure, ex_6/ex_7 rows. Root cause: `joinBody` was parsed but ignored.

Implemented `JoinBodyExecutor`: filter → apply|calc|aggr → keep|drop → rename on the
virtual result (still carrying `alias#name`), then automatic alias stripping.
`apply` expands pairwise over homonym measures. Unary `inner_join` (legacy TCK;
manual 2.1 asks for ≥2 operands) is accepted as identity + body.

TCK Join leaves green (7/7). Baseline after Join: **136 / 183** (47 fails).

### 4. `exists_in` (~3) — done

Match on common identifier value combos; result = Id(op1) + `bool_var`.
`retain` = `all` (default) / `true` / `false`. Constraint: one operand’s identifiers
contain the other’s.

Implemented in `ExistsInExecutor` via PE project + intersect / setdiff + calc + union.
TCK Exists-in leaves green (3/3). Baseline after exists_in: **139 / 183** (44 fails).

### 5. Attributes in aggregation (~2–3) — done

`avg(DS)` / `aggr … group by` with viral attributes: reduction is nulls-first `min`
(empty CSV → null ⇒ group result null). In-memory already matched; Spark SQL `min`
skipped nulls and produced wrong values (e.g. `"A"`).

Fixed `minNullsFirst` in Spark 3/4 `convertAggregation` for `MinAggregationExpression`.
TCK Aggregate invocation ex_3/ex_4 green. Baseline: **141 / 183** (42 fails).

### 6. Validation `check` / `check_datapoint` (~3)

Structure (column order, presence of `Me_1` in output, roles) and ruleid/errorcode mapping.

### 7. `hierarchy` (~3) and `check_hierarchy` (~1)

Roll-up unimplemented. `check_hierarchy` already blows up on Spark (unresolved valuedomain column). Large semantics + ruleset work — lower priority than smaller wins above despite similar case count.

### 8. Misc non-date (~7)

| Case | Suspected issue |
|------|-----------------|
| Unpivot ex_1 | `Id_2` not an identifier — unpivot semantics over measures A/B/C |
| if-then-else ex_1 | dataset if: mono-measure names (`bool_var` vs …) |
| Element of ex_3 | `in` valuedomain → visitor NPE |
| Random ex_1/ex_2 | non-deterministic / not wired on Spark |
| Median ex_1 | row mismatch |
| Logarithm ex_2 | numeric / null mismatch |

Pick up as you go once items 1–7 are stable.

## Date / time — last

Do this block only after the non-date wave. Prefer SDMX TimePeriod parsing before operators that need real intervals.

### T1. Lexical SDMX TimePeriod + `timeshift` / `period_indicator` (~6)

Today CSV/Spark keep `"2010"`, `"2010Q1"`, `"2010M1/2010M12"` as String typed Interval/Time. Enough for group by, not enough for time ops.

- SDMX parser → `Interval` (year, quarter, month, ranges).
- Wire `timeshift` (Date / Time / TimePeriod) and `period_indicator` (A/Q/… codes).
- TCK comparison round-trip (expected output is often still lexical codes, not ISO Interval).

Prerequisite for fill_time_series and flow/stock.

### T0. `fill_time_series` (~8)

All “Fill time series” ex_1…ex_8 fails: `UnimplementedException`.

- Implement the operator (single / all, period bounds).
- Needs a correct TimePeriod model (T1).
- Highest time impact, but blocked on parsing — hence after T1 in practice, even though case count is higher.

### T2. Date / duration converters (~6)

One case each, same TemporalFunctions area:
`getmonth`, `datediff`, `dateadd`, `daytoyear`, `daytomonth`, `yeartoday`.

Many are stubs / Unimplemented. Follow after ISO date CSV parsing (already in place).

### T3. `flow_to_stock` / `stock_to_flow` (~4)

Structure and/or rows. Needs ordered TimePeriod; do after the SDMX parser.

### T4. `time_agg` (~1)

Time aggregation ex_1: Unimplemented. Last among time ops.

## Out of scope (done recently)

- CSV loading for TimePeriod / DATE / TIME / DURATION + INTEGER `2.0`
- Spark `Interval` / `PeriodDuration` / `OffsetDateTime` → StringType
- `group except` (complement of identifiers)
- Integer/Number fidelity for `sum` / `mod` / `round` / `trunc` (+ analytic where Result type matches); 2.1 sum Long↔Double TCK allowlist in `TckStructureComparison`
- Set operators `setdiff` / `intersect` / `symdiff` (engine + Spark 3/4)
- Join body clauses (`filter` / `calc` / `apply` / `keep`/`drop` / `rename`) + unary legacy join
- `exists_in` (retain all/true/false)
- Viral attribute aggregation: Spark `min` nulls-first (align with in-memory / TCK)

## Working method

1. One theme = one PR (or two if SDMX parser + fill_time_series).
2. Always a Trevas unit test + re-run the related TCK subset.
3. Do not classify business fails as “fixture” (null attribute, Long/Double type).
4. Recompute the impact table after each wave (TCK zip and engine both move).
5. Keep this roadmap and any operator “support” notes in sync when a theme lands.

## Suggested next wave (non-date only)

1. Aggr-clause `having` / source-group aggregates (~1)
2. Validation `check` / `check_datapoint` (~3)

Then remaining P2/P3 toward about **144 / 183**, then SDMX parser → `fill_time_series` → remaining time ops for **150+**.
