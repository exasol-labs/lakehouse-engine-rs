# Feature: End-to-End Harness — Scan Correctness

End-to-end assertions that the lakehouse VS query path returns correct ROWS — projection, filter,
LIMIT, oversubscribed shard fan-out, partition pruning, nested JSON columns, and timestamp
precision — against a local Exasol Docker container, run through the same
`e2e-harness/e2e-harness` provisioning and shared harness definition. Split out of that feature
once its scenario count crossed this library's per-spec organization threshold; that sibling
feature keeps harness provisioning, the fail-fast contract, row-cap/fetch-paging mechanics, and
the Exasol-version CI gate.

## Background

<!-- DELTA:NEW -->
* **This delta is issue #399.** It adds ONE scenario and no seed fixture. No existing scenario
  changes.
* **The scenario proves value correctness once the spec carries no type list.** Issue #399 deletes
  the scan spec's copy of the declared emit types, so the scan reads its declared output type from
  `UdfContext::output_column` alone. What this scenario asserts is that a query over a wide type
  mix still returns the seeded values, against a scan spec that carries no `emit_exa_types` key.
* **No recurring test asserts the declared list against the runtime list, by deliberate choice.**
  That agreement is the SDK and SLC's own contract: `language-container-rs` populates the
  output-column accessors from the same call-site `EMITS` clause the engine parsed. A permanent
  assertion here would re-test that upstream guarantee rather than this repo's logic. It is proven
  once instead, at implementation time, against the local Exasol Docker container, as a one-time
  gate on the no-fallback design.
* **Two seeded tables already span the type mix.** `typed_distinct_probe` (`E2E_TYPED_TABLE`)
  carries Iceberg `long`, `decimal`, `double`, `string`, `date`, `timestamp`, and `boolean` columns.
  `complex_probe` (`E2E_COMPLEX_TABLE`) carries `list`, `struct`, and `map` columns, which the
  adapter declares `VARCHAR(2000000)` and the scan renders as JSON.
* **A scale-0 narrow-decimal CAST is the only reachable probe for the Int32 bin.** Exasol bins a
  scale-0 `DECIMAL(p,0)` with `p` ≤ 9 to `ExaType::Int32`. No catalog-declared column in these
  fixtures produces one: an Iceberg `int` declares `DECIMAL(10,0)`, which bins to `ExaType::Int64`.
  A projected `CAST(<col> AS DECIMAL(5,0))` declares `DECIMAL(5,0)` through
  `exasol_type_from_json`, so the scenario reaches the Int32 bin through the select list.
* **`Binary` needs no separate fixture.** The adapter declares `Binary`, `LargeBinary`, and
  `FixedSizeBinary` as `VARCHAR(2000000)`, the same declaration the nested types carry. The
  coercion dispatches on the declared `ExaType`, never on the source Arrow type, so the nested-type
  columns exercise the identical code path.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The scan returns correct values across the type mix with no spec-carried emit types

* *GIVEN* the seeded `typed_distinct_probe` and `complex_probe` Iceberg tables served through the virtual schema
* *AND* a query whose select list spans `long`, `decimal`, `double`, `string`, `date`, `timestamp`, and `boolean` columns, a projected `CAST(<col> AS DECIMAL(5,0))`, and the `list`, `struct`, and `map` columns the adapter declares `VARCHAR(2000000)`
* *WHEN* the query runs end to end against the local Exasol Docker container
* *THEN* `EXPLAIN VIRTUAL` SHALL show the generated `EMITS (...)` clause declaring one Exasol type per select-list item
* *AND* the generated scan spec JSON MUST NOT contain the key `emit_exa_types`, because the scan carries no second copy of that declaration
* *AND* the query SHALL return the seeded values for every column, with the nested columns returned as the JSON documents `datafusion-scan/nested-json-rendering` specifies
* *AND* the run SHALL exercise the `ExaType` variants `Int64`, `Int32`, `Numeric`, `Double`, `String`, `Date`, `Timestamp`, and `Boolean`, so the context-driven coercion is exercised for every variant the scan can meet on this fixture set rather than for a single column type
<!-- /DELTA:NEW -->
