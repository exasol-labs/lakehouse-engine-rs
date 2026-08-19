# Feature: Unity Catalog E2E Harness — Delta Query Result Coverage

End-to-end coverage of the actual rows a query returns over the seeded Delta fixtures, run through the
same `unity-e2e` stack and virtual schema as `e2e-harness/unity-catalog-e2e-harness`.

## Background

* **This delta is issue #359.** It adds ONE scenario and amends no recorded clause. The added scenario
  makes the Delta suite's timestamp declared-type expectations version-aware and EXACT, so the Delta
  half of the precision change is asserted rather than tolerated. No fixture, row set, value, refusal,
  pruning, or JSON-rendering expectation changes.
* **The recorded Delta declared-type expectations survive the change WITHOUT being amended, because
  `assert_col_type` matches by the space-stripped PREFIX.** `TIMESTAMP(6)` starts with `TIMESTAMP`, so
  `date_timestamp_ntz` in the type-widening scenario and `TIMESTAMP_COL`/`TIMESTAMP_NTZ_COL` in the
  varied-types scenario keep passing on both version arms unchanged. That tolerance is exactly why a
  second, EXACT assertion is needed: without it the Delta path could regress to bare `TIMESTAMP` on a
  2025.x engine with the suite still green.
* **This suite is deliberately NOT matrixed over Exasol versions.** `e2e-unity` stays single-version
  (`e2e-harness/e2e-harness` records why), so the added assertion is version-aware for correctness
  when a developer points the stack at an 8.x image, not because CI exercises both arms here.
* **The expected declared type comes from the ONE shared helper** `e2e-harness/e2e-harness` introduces,
  read from the live session. This suite MUST NOT carry a second version-to-precision table.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A Delta timestamp column's declared Exasol type is asserted exactly at the engine's precision

* *GIVEN* the `stats_all_types` Delta table's `timestamp_col` (Delta `timestamp`) and `timestamp_ntz_col` (Delta `timestamp without time zone`) columns, and the `type_widening` table's `date_timestamp_ntz` column
* *AND* the running engine's own version, read from the live session through the ONE shared helper `e2e-harness/e2e-harness` introduces
* *WHEN* the harness reads each column's `COLUMN_TYPE` from `SYS.EXA_ALL_COLUMNS`
* *THEN* each of the three columns' declared type SHALL equal EXACTLY `TIMESTAMP(6)` when the helper's expected precision is 6 and EXACTLY `TIMESTAMP` when it is 3, matched in full rather than by the space-stripped prefix the recorded assertions use
* *AND* the recorded prefix-tolerant expectations for those same columns SHALL be left in place unchanged, because they still hold on both arms — this scenario ADDS the exact check rather than tightening theirs
* *AND* the assertion SHALL cover BOTH Delta timestamp type names, so a change that gated only one of the two Unity type names would fail here rather than pass by covering the other
* *AND* no row value, JSON rendering, refusal, or pruning expectation of this feature SHALL change
* *AND* the scenario SHALL FAIL, not skip, when no live `unity-e2e` stack is available, per this repo's E2E contract
<!-- /DELTA:NEW -->
</content>
