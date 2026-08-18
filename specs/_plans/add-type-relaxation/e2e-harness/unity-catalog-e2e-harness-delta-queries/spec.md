# Feature: Unity Catalog E2E Harness — Delta Query Result Coverage

End-to-end coverage of the actual rows a query returns over the seeded Delta fixtures — delete-free,
deletion-vector, column-mapped, partitioned, join/aggregate, type-widened, and unplannable-type
tables — run through the same `unity-e2e` stack and virtual schema as
`e2e-harness/unity-catalog-e2e-harness`.

## Background

* **This delta is issue #349.** `unity.delta_e2e.type_widening` moves from the refused set to the
  queryable set, so the reader-feature refusal scenario loses one of its two fixtures and a new
  scenario asserts the rows the widened table actually returns. No new fixture, Makefile target, or
  test tier is added: these scenarios extend the existing `make test-e2e-unity` suite in the same
  `e2e_unity_test.rs` binary.
* **The vendored fixture already carries everything this needs, and MUST NOT be modified.**
  `scripts/unity/fixtures/PROVENANCE.md` records the vendored delta-kernel-rs tables as *"read
  fixtures — never mutated"*. The `type-widening` fixture's commit 0 declares thirteen columns at
  their NARROW types and commits one data file whose Parquet columns are physically narrow; its
  commit 2 widens all thirteen declared types in `metaData.schemaString`, records each change under
  `delta.typeChanges`, and commits a second data file at the WIDE types. Two `add` actions, two data
  files, no `remove` — both files are live at the current snapshot, so a single query reads one
  narrow file and one wide file. That is exactly the straddling shape
  `datafusion-scan/type-relaxation` exists to prove, and no fixture byte needs to change to get it.
* **What DOES have to change is `scripts/unity/seed.sh`, which registers only 3 of the 13 columns.**
  For an EXTERNAL Delta table the engine reads schema and protocol from the Delta log, but the Unity
  Catalog column list is what `createVirtualSchema` enumerates — a column absent from it is not
  selectable from Exasol at all. Registering all thirteen at their WIDENED types is what makes the
  fixture's coverage reachable; it adds no fixture bytes and mutates nothing vendored.
* **The widened `float` column's value is the f32's exact double expansion, not the decimal literal.**
  The narrow file stores `3.4` as a 32-bit float, and widening to `double` preserves those bits
  exactly rather than re-parsing the decimal text — so the returned value is `3.4`'s single-precision
  representation expanded to double, and an assertion written against the literal `3.4` would fail on
  a correct read. This is a property of IEEE 754 widening, not of this engine, and it is recorded
  here so the expectation is set once rather than rediscovered as a bug.
* **`unshredded_variant` keeps its refusal unchanged.** `variantType-preview` stays off the
  allow-list, so the reader-feature refusal scenario keeps a live fixture and the gate keeps an
  end-to-end assertion.
* **Four Delta widening pairs are NOT reachable through this fixture and are covered below the E2E
  tier.** `byte` → `short`, `byte` → `int`, and `short` → `int` all tag `int32` on both sides in this
  engine's vocabulary and are therefore invisible in any logical schema, and `short` → `long` has no
  fixture column. Covering them would need a new Delta table authored by a Spark + delta-spark
  one-shot job this stack does not have. `datafusion-scan/type-relaxation` covers all four against
  purpose-written Parquet files at the scan tier, which exercises the same cast at the same seam for
  a fraction of the cost.
* **Two of the fixture's thirteen recorded changes are outside the Delta protocol's supported list,
  and `vs-adapter/delta-type-mapping`'s validation refuses exactly those two columns.** `byte_decimal`
  (`byte` → `decimal(4,1)`) and `short_decimal` (`short` → `decimal(6,1)`) both derive `k1` as
  NEGATIVE against the protocol's `Byte`/`Short`/`Int` → `Decimal(10+k1,k2)` base of precision 10 —
  `10+k1=4` and `10+k1=6` respectively — so `k1 >= k2 >= 0` fails for both. This is not a defect in
  the fixture or the validation: the fixture is vendored from `delta-kernel-rs` v0.26.0 test data
  under the `typeWidening-preview` (not the finalized `typeWidening`) name, and its preview-era
  decimal target was derived per-source-type rather than from the finalized `Decimal(10+k1,k2)` /
  `Decimal(20+k1,k2)` bases the current protocol specifies. The other eleven columns, including
  `int_decimal` (`decimal(11,1)`, `k1=1`) and `long_decimal` (`decimal(21,1)`, `k1=1`), are within the
  supported list and stay queryable. `byte_decimal` and `short_decimal` are refused per column — the
  same existing mechanism `vs-adapter/delta-type-mapping` already uses for `binary`, `map`, `struct`,
  and `variant` — so a query selecting either of them fails naming the column and both types, while
  the rest of the table, including its other eleven widened columns, stays queryable.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A Delta table using an unsupported reader feature fails the query loud

* *GIVEN* the seeded fixture whose Delta `protocol` action declares a reader feature this engine does
  not implement — `unity.delta_e2e.unshredded_variant` (`variantType-preview`)
* *WHEN* the suite issues a query against it
* *THEN* the query SHALL fail with the reader's plan-time gating error naming the refused feature by
  its Delta protocol name — `variantType-preview` — and MUST NOT return a row
* *AND* the error MUST NOT be a type-mapping error naming a column, because the table's reader
  feature is refused BEFORE its schema is mapped, and a column-typed error here would prove the gate
  ran too late
* *AND* the error MUST NOT cite issue #349, because type widening is no longer refused and a closed
  issue cited in a shipped refusal reads as an unfixed gap with no owner
* *AND* the failure MUST arrive as a SQL error rather than as a crashed UDF VM — checked by a
  follow-up query surviving on the same connection — so an unsupported table is a diagnosable refusal
  rather than an abnormal exit
* *AND* the error text MUST NOT contain any credential value
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A type-widened Delta table returns its current wider types across the widening boundary

* *GIVEN* the seeded fixture `unity.delta_e2e.type_widening`, whose `readerFeatures` list is
  `timestampNtz` and `typeWidening-preview` — both now allow-listed — and whose two live data files
  are one written BEFORE the widening at the narrow physical types and one written after at the wide
  ones
* *AND* all thirteen of its columns registered in Unity Catalog at their WIDENED types
* *WHEN* the suite issues `SELECT` naming the ELEVEN protocol-supported columns explicitly, plus
  `SELECT COUNT(*)`, and separately issues a `SELECT` naming each of the two refused columns
* *THEN* `SELECT COUNT(*)` SHALL return 2 and the eleven-column projection SHALL return both rows, so
  the pre-widening file is read rather than skipped or failed
* *AND* each of the eleven columns SHALL arrive as its WIDENED declared Exasol type: `byte_long` and
  `int_long` as `DECIMAL(20,0)`; `float_double`, `byte_double`, `short_double`, and `int_double` as
  `DOUBLE PRECISION`; `decimal_decimal_same_scale` as `DECIMAL(20,2)`;
  `decimal_decimal_greater_scale` as `DECIMAL(20,5)`; `int_decimal` as `DECIMAL(11,1)`; `long_decimal`
  as `DECIMAL(21,1)`; and `date_timestamp_ntz` as `TIMESTAMP`
* *AND* `byte_decimal` and `short_decimal` SHALL each fail their `SELECT` with the per-column refusal
  naming the column and both its Delta types (`byte`/`decimal(4,1)` and `short`/`decimal(6,1)`),
  because their recorded `delta.typeChanges` entries derive a NEGATIVE `k1` against the protocol's
  `Decimal(10+k1,k2)` base and are therefore outside the supported list — and MUST NOT return a row
  or a NULL value
* *AND* the PRE-WIDENING row SHALL return its real logged values widened to those types rather than
  NULL — `byte_long` as 1, `int_long` as 2, `int_decimal` as 3.0, `long_decimal` as 4.0, and
  `date_timestamp_ntz` as the midnight instant of its logged date — because a NULL here is exactly
  the silent-wrong-value outcome the reader-feature refusal previously existed to prevent
* *AND* the POST-WIDENING row SHALL return values only the wide types can hold — `byte_long` and
  `int_long` as 9223372036854775807, `long_decimal` as 123456789012345678.9 — so a scan that read
  both files at the narrow width would fail rather than pass on coincidentally-equal values
* *AND* `float_double`'s pre-widening value SHALL be asserted as the stored 32-bit float's exact
  double expansion, and the suite MUST NOT assert the decimal literal `3.4`, because widening
  preserves the single-precision bits rather than re-parsing the text
* *AND* the suite SHALL capture the generated pushdown SQL for at least one of the eleven-column
  queries and assert it drives the scan UDF, so a silent fallback to an unaccelerated wrapper fails
  the suite rather than passing on correct rows
* *AND* the error text of any failure in this scenario MUST NOT contain any credential value
<!-- /DELTA:NEW -->
