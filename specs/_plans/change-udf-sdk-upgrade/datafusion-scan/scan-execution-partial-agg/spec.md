# Feature: DataFusion Scan Execution — Partial Aggregate Output

The scan UDF partial-aggregate path: computing node-local aggregates inside
DataFusion, emitting per-shard partial results in a form the Exasol wrapper SQL
can merge into the final query result.

## Background

<!-- DELTA:NEW -->
* The declared-`Numeric` payload rule is owned by `datafusion-scan/scan-execution-value-conversion`
  and applied identically on both emit paths. Since `exasol-udf-sdk` 0.28.1 that rule covers an
  out-of-range `precision` or `scale` only, because `ExaType::Numeric` no longer holds an absent one
  (issue #405).
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Every emitted partial-aggregate cell matches its declared output column

* *GIVEN* a partial-aggregate scan whose declared output columns include a `DOUBLE PRECISION` `AvgSum`, `StatSum` or `StatSumSq` column, a `DECIMAL(36,s)` `Sum` column, a `MIN`/`MAX` column declared with its source column's Exasol type, and a `DOUBLE PRECISION` column the adapter declares for an aggregate reached only nested inside a scalar
* *AND* a DataFusion result batch whose aggregate column types diverge from those declarations, for example an `Int64` sum for `AVG(<iceberg long>)`, a `Decimal128` sum for `AVG(<iceberg decimal>)`, a `Decimal128(37,s)` sum over a wide decimal, a `Decimal128(p,0)` minimum for a column declared `DECIMAL(p,0)` with `p` at most 18, and a `Decimal128` value for the nested aggregate declared `DOUBLE PRECISION`
* *WHEN* the scan UDF builds the partial row
* *THEN* the UDF SHALL coerce each partial-aggregate column to the Arrow type the declared `ExaType` from `UdfContext::output_column` requires, before converting the cell to a `Value`
* *AND* the emitted `Value` variant SHALL be one the SDK's `column_accepts` rule admits for that declared column, so no partial-aggregate query fails with an `output column … is … but the value is …` error
* *AND* a partial-aggregate column whose declared `Numeric` reports a `precision` or `scale` outside what `Decimal128` represents SHALL fail the call naming that column, under the same rule the Arrow `emit_batch` path applies, so neither path substitutes a string value into a numeric column
* *AND* the absent-payload half of that rule SHALL be deleted rather than left unreachable, because `ExaType::Numeric` no longer holds an absent `precision` or `scale` (`datafusion-scan/scan-execution-value-conversion`)
* *AND* `AVG`, `STDDEV`, `STDDEV_POP`, `VARIANCE` and `VAR_POP` over an integer or decimal column SHALL return the same values they returned before the SDK bump
* *AND* the UDF MUST NOT add a per-aggregate-kind cast to the partial SELECT SQL, because the declared column is the one authority and reading it covers every kind at once
* *AND* group-key columns SHALL keep their existing `value_to_gk_string` stringification, unchanged and uncoerced, so the merge identity of a group is unaffected
* *AND* a shard with no matching rows SHALL keep emitting its existing null partial row, whose `Value::Int64` counters and `Value::Null` cells the declared columns already admit
<!-- /DELTA:CHANGED -->
