# Feature: Type Relaxation

Reads a data file whose physical column type is narrower than the table's current logical type by casting each file's column up at scan time. This delta changes no recorded description. The paragraph here is plan-local context only.

## Background

* This delta amends ONE scenario and ADDS ONE scenario, and is issue #407. The cast mechanism, the
  supported-set table, and both format conformance arguments are unchanged. The read behavior gains
  exactly ONE addition. The next bullet states it.
* **The recorded sentence that the overflow asymmetry is unreachable from this feature is
  SUPERSEDED.** The recorded Background states that every pair in the supported set is a widening,
  so no value can overflow either cast site. `MERGE_SCHEMA = 'FALSE'` makes the NARROWING direction
  reachable for the first time. The declaration carries the sampled footer's narrow type. A later
  file's physical column is wider, so the delegated `DefaultPhysicalExprAdapter` inserts a
  NARROWING cast. That read-side cast uses `safe: false` and returns a clean error rather than a
  wrong value. This plan wants that answer, and its E2E suite pins it. The emit
  boundary's `safe: true` policy stays UNREACHABLE, because the read-side cast fails before any
  overflowing value reaches it. The amended scenario below carries this supersession normatively, so
  the merged library holds one statement rather than two contradictory ones.
* **Type relaxation gains a THIRD writer. That writer is this engine.** Delta calls the shape type
  widening. Iceberg calls it type promotion. The direct-storage catalog kind has no table format
  and therefore no writer to record a change. The ENGINE decides the current logical type itself
  by folding the files' Parquet footers. The recorded read answer covers it unchanged, because the
  scan side reads whatever the logical schema declares and never asks which writer produced it.
* **The supported pair set gains its FIRST production consumer and therefore its first production
  home.** Today the set exists as this feature's 13-row table plus the test list
  `supported_relaxation_pairs` in `crates/lakehouse-engine/src/scan/type_relaxation_tests.rs`. The
  fold must decide, for two concrete Arrow types, whether one widens to the other, so it needs the
  set as production code. The test list STAYS the concrete pin it is today. It is asserted AGAINST
  the production owner rather than generated from it. A generated list would assert only
  what its generator accepts and would withdraw the pin
  `arrow_castability_pins_every_supported_relaxation_pair` exists to hold.
* **The fold decides a wider type. It does not decide castability.** `arrow::compute::can_cast_types`
  stays the castability oracle at scan time and is not consulted by the fold, because it also
  accepts narrowing casts no format permits. The recorded reason that the supported set is decided
  at PLAN time rather than by the cast holds identically for this third writer.
* **The fold introduces no pair.** It answers only over the rows this feature already records. A
  pair outside them fails the fold rather than being widened by some other rule. Adding a pair
  stays this feature's decision, made once, in its table.
* Apache Iceberg and Delta specification check: NOT implicated by the addition. The fold reads raw
  Parquet footers and claims conformance to neither format. Its pair set is this feature's own.
  This feature already grounds that set in the Apache Iceberg table specification's § Schema
  Evolution promotion table (rows 1 to 3) and in the Delta Lake protocol's § Reader Requirements
  for Type Widening. The new writer adds no row, so neither grounding changes.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A narrow physical column binds to the current wider logical type and is cast per file

* *GIVEN* a scan spec whose logical schema declares a column at the table's CURRENT type — the type after a Delta type widening, an Iceberg type promotion, or a direct-storage footer fold
* *AND* two assigned files, one written BEFORE the change whose physical Parquet column carries the narrow source type, and one written AFTER whose physical column carries the current type
* *WHEN* the scan UDF reads both files in one shard
* *THEN* the UDF SHALL register the DataFusion table schema from the scan spec's `LogicalField` list and MUST NOT infer it from any data file, so the column's declared type is the CURRENT one for both files
* *AND* the column-binding adapter SHALL resolve the narrow physical field to that logical column by its binding key alone — field-id, declared physical name, or identity — and MUST NOT require, test, or compare the physical field's Arrow data type, because a binding that depended on type equality would fail on exactly the file this scenario exists for
* *AND* the delegated `DefaultPhysicalExprAdapter` SHALL insert the physical-to-logical cast into the physical expression tree, so every filter, projection, aggregate, and join key evaluated by DataFusion sees the column at its CURRENT type rather than its physical one
* *AND* the emitted rows from the OLD file SHALL carry that file's real values widened to the current type, and the emitted rows from the NEW file SHALL carry theirs unchanged, so the two files return one consistent column
* *AND* the cast SHALL be decided PER FILE from that file's own footer schema, so a shard straddling the change needs no per-shard grouping by physical layout
* *AND* this resolution SHALL hold identically for an Iceberg scan, a Delta scan, and a direct-storage scan, because all three populate the same `LogicalField` list and install the same adapter — the scan side MUST NOT branch on table format, and MUST NOT branch on whether a writer or the engine decided the current type
* *AND* a logical type NARROWER than a file's physical column SHALL reach that same read-side cast in the NARROWING direction, SUPERSEDING the recorded statement that the overflow asymmetry is unreachable from this feature, because the direct-storage sample-one-footer mode declares the sampled file's narrow type over files that may be wider
* *AND* that narrowing cast SHALL return a CLEAN ERROR naming the column and both types rather than a wrong value, because the read-side site carries `safe: false`, and the emit boundary's `safe: true` policy SHALL stay UNREACHABLE for it, because the read-side cast fails before any overflowing value reaches the boundary
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: The supported pair set answers a plan-time widening question from one production owner

* *GIVEN* the 13-row supported-set table above, and a plan-time caller holding two concrete Arrow types for one column read from two Parquet footers
* *WHEN* that caller asks which of the two types the column resolves to
* *THEN* exactly ONE production item SHALL answer, taking the two Arrow types and returning the WIDER one when either ordering is a row of the supported set, and returning NO answer otherwise
* *AND* the existing test list `supported_relaxation_pairs` SHALL STAY the concrete 17-entry pin it is today and SHALL be asserted AGAINST that production item, so every listed pair resolves to its wider member and a curated refusal set returns no answer
* *AND* the item and the test list SHALL be checked AGAINST each other rather than one GENERATED from the other, because a list generated from the item would assert only what the item already accepts and would withdraw the arrow-castability pin that exists to fail when an arrow-cast upgrade withdraws a pair
* *AND* a row added to the supported-set table with NO matching rule in that item SHALL FAIL the suite, so the table, its production owner, and the concrete pin cannot drift apart in any direction
* *AND* the item SHALL answer over the CONCRETE types rather than over row names, so a decimal row's precision and scale conditions are evaluated on the actual pair — `decimal(10,2)` with `decimal(12,2)` resolves to `decimal(12,2)` by row 3, and `decimal(10,2)` with `decimal(20,5)` resolves to `decimal(20,5)` by row 12
* *AND* it SHALL return NO answer for two types whose widening is supported in NEITHER direction, so a caller reports a conflict rather than guessing, and `decimal(12,2)` with `decimal(10,5)` SHALL be such a pair because neither ordering satisfies a row
* *AND* it SHALL return the shared type unchanged for two EQUAL types, so an unevolved column costs no special case at the caller
* *AND* it MUST NOT consult `arrow::compute::can_cast_types`, because that function also accepts narrowing casts no format permits, which is the recorded reason the supported set is decided at plan time
* *AND* it MUST NOT add, remove, or parameterize a row of the supported-set table, so `long` → `double` stays absent and this scenario changes no scan-time answer
* *AND* every existing assertion of this feature's test suite MUST pass with no change to any expected value, because the item restates the rows the suite already pins
<!-- /DELTA:NEW -->
