# Feature: DataFusion Scan Execution — Partial Aggregate Output

The scan UDF partial-aggregate path: computing node-local aggregates inside
DataFusion, emitting per-shard partial results in a form the Exasol wrapper SQL
can merge into the final query result.

## Background

<!-- DELTA:NEW -->
* **This delta is issue #399's blocking prerequisite, and it also fixes pre-existing mismatches
  #399 did not originally cover.** Issue #399's Scope section states that the partial-aggregate path
  is not in scope, a note written before the 0.26.0 release bundled row validation into the same
  bump. It adds ONE scenario and changes none. The partial-aggregate column contract, the merge
  shapes, and the adapter's declared partial types are all unchanged.
* **The SDK bump turns four silent mismatches on this path into query failures.**
  `exasol-udf-sdk` 0.26.0 validates every `Value` row against the declared output column before
  buffering it (`check_output_row`, `column_accepts` in `exa-udf-runtime`'s `rowset.rs`). The
  earlier bridge coerced a mismatched cell instead. `column_accepts` rejects `Value::Int64` and
  `Value::Numeric` in a `Double` column, `Value::Numeric` in an `Int32`/`Int64` column,
  `Value::Double` in a `Numeric` column, and `Value::String` in any numeric column.
* **Four concrete pre-existing mismatches reach those rules.** `partial_emits_items`
  (`adapter/pushdown/grouped_agg.rs`) declares `AvgSum`, `StatSum` and `StatSumSq` as
  `DOUBLE PRECISION`, while `partial_select_items` (`scan/partial_agg.rs`) emits a bare
  `SUM(<arg>)`, so DataFusion keeps the argument type: `AVG(<iceberg long>)` produces
  `Value::Int64` and `AVG(<iceberg decimal>)` produces `Value::Numeric`, both into a `Double`
  column. `SUM` over a `decimal(p,s)` with `p` at least 27 widens past 36 and produces
  `Value::String` into the declared `DECIMAL(36,s)`. `MIN`/`MAX` over a `decimal(p,0)` with `p` at
  most 18 produces `Value::Numeric` into a column Exasol bins to `ExaType::Int64`. An aggregate
  reached only nested inside a scalar is declared by the hardcoded `NESTED_AGGREGATE_PLAN_TYPE`
  (`adapter/pushdown/scalar_over_agg.rs`), the literal `DOUBLE PRECISION`, while its DataFusion
  expression yields `Decimal128` over decimal arguments, so `SELECT ROUND(SUM(dec_a * dec_b), 2)`
  produces `Value::Numeric` into a `Double` column.
* **Existing coverage misses all four because every AVG and STDDEV case uses a `double` column.**
  `Float64` into a `Double` column is the one pairing the current end-to-end tests exercise.
* **The fix reuses the emit-boundary coercion rather than casting per aggregate kind.** The root
  cause is the same back-door leak issue #399 removes: the adapter decides the declared type and
  the scan independently produces a value type, with nothing enforcing agreement. Reading the
  declared `ExaType` from `UdfContext::output_column` and coercing the Arrow column to it before
  the per-cell conversion gives both emit paths one authority and one rule.
* **Group-key columns keep their existing stringification.** They are declared
  `VARCHAR(2000000)` and `value_to_gk_string` already produces `Value::String`, which
  `column_accepts` allows. Routing them through an Arrow cast instead would change the group-key
  text and therefore the merge identity, so the coercion applies only to the partial-aggregate
  columns.
* **The adapter needs no change.** The declared partial types are already the authority; only the
  scan's conformance to them is new.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Every emitted partial-aggregate cell matches its declared output column

* *GIVEN* a partial-aggregate scan whose declared output columns include a `DOUBLE PRECISION` `AvgSum`, `StatSum` or `StatSumSq` column, a `DECIMAL(36,s)` `Sum` column, a `MIN`/`MAX` column declared with its source column's Exasol type, and a `DOUBLE PRECISION` column the adapter declares for an aggregate reached only nested inside a scalar
* *AND* a DataFusion result batch whose aggregate column types diverge from those declarations, for example an `Int64` sum for `AVG(<iceberg long>)`, a `Decimal128` sum for `AVG(<iceberg decimal>)`, a `Decimal128(37,s)` sum over a wide decimal, a `Decimal128(p,0)` minimum for a column declared `DECIMAL(p,0)` with `p` at most 18, and a `Decimal128` value for the nested aggregate declared `DOUBLE PRECISION`
* *WHEN* the scan UDF builds the partial row
* *THEN* the UDF SHALL coerce each partial-aggregate column to the Arrow type the declared `ExaType` from `UdfContext::output_column` requires, before converting the cell to a `Value`
* *AND* the emitted `Value` variant SHALL be one the SDK's `column_accepts` rule admits for that declared column, so no partial-aggregate query fails with an `output column … is … but the value is …` error
* *AND* a partial-aggregate column whose declared `Numeric` reports an absent `precision` or `scale`, or one outside what `Decimal128` represents, SHALL fail the call naming that column, under the same rule the Arrow `emit_batch` path applies, so neither path substitutes a string value into a numeric column
* *AND* `AVG`, `STDDEV`, `STDDEV_POP`, `VARIANCE` and `VAR_POP` over an integer or decimal column SHALL return the same values they returned before the SDK bump
* *AND* the UDF MUST NOT add a per-aggregate-kind cast to the partial SELECT SQL, because the declared column is the one authority and reading it covers every kind at once
* *AND* group-key columns SHALL keep their existing `value_to_gk_string` stringification, unchanged and uncoerced, so the merge identity of a group is unaffected
* *AND* a shard with no matching rows SHALL keep emitting its existing null partial row, whose `Value::Int64` counters and `Value::Null` cells the declared columns already admit
<!-- /DELTA:NEW -->
