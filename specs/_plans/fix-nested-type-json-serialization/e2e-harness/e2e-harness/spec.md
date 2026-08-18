# Feature: End-to-End Harness

End-to-end test suite that exercises the full lakehouse VS query path — from Exasol SQL
through the adapter and scan UDF to Iceberg Parquet files in MinIO — verifying
correctness of projection, filter, and Iceberg file-pruning pushdown against a local
Exasol Docker container. The harness installs `LAKEHOUSE_SCAN` as a SCALAR EMIT script
and `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET distributor script. See
`e2e-harness/e2e-harness-grouped-agg` for grouped-aggregate and nested-aggregate E2E
scenarios.

## Background

* **This delta is issue #350.** It adds ONE scenario and ONE seed fixture: an Iceberg table carrying
  populated `list`, `struct`, and `map` columns, queried end to end so the JSON rendering
  `datafusion-scan/nested-json-rendering` specifies is proven against real Parquet data rather than
  against a hand-built Arrow array. No existing scenario changes.
* **No existing Iceberg seed helper writes a nested column.** Every `seed_*` function in
  `crates/lakehouse-engine/tests/common/seed.rs` declares primitive `NestedField`s only, which is why
  the gap this delta closes went untested: the pre-existing unit assertions for the JSON fallback used
  a ZERO-FIELD struct, a shape that sidesteps every field-wise code path.
* **The probe table needs the non-string-keyed map case, which is the one shape the JSON encoder
  cannot render without stringification.** The Iceberg spec permits any key type
  (https://iceberg.apache.org/spec/#nested-types), and `arrow-json`'s map encoder rejects every
  non-`Utf8` key outright, so a fixture without such a column would leave the map-key contract
  covered by unit tests alone.
* **Every requested shape IS writable, including `map<int, string>`, which refutes a stale comment in
  the seed module.** `crates/lakehouse-engine/tests/common/seed.rs` states that complex list/struct
  columns *"are not written here because iceberg-rust does not expose a struct/list writer"*. A live
  probe wrote `list<string>`, `list<int>`, `struct<street, city>`, `map<string, string>`,
  `map<int, string>`, and `list<struct<a: int>>` into one Iceberg Parquet file with iceberg-rust 0.10
  and parquet 58. That comment is corrected by this delta, not worked around.
* **The real obstacle is nested FIELD-ID REASSIGNMENT, and the existing seed helpers cannot absorb
  it.** `iceberg-rest-fixture` assigns fresh field ids on `create_table`, and
  `common::seed::overlay_iceberg_field_ids` repairs only TOP-LEVEL ids, matching them by name — nested
  ids keep the values the test authored. Feeding a batch built from the AUTHORED schema therefore fails
  with `DataInvalid => Field id 9 not found in struct array`. The fixture MUST build its Arrow batch
  from `iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())` AFTER
  `create_table` returns. `create_and_append_files` takes its batches up front and so cannot do this,
  which is why a nested-type seed needs its own create-then-write path rather than that helper.
* **The derived Arrow schema is already correct and carries a `PARQUET:field_id` on every nested
  field** — list elements, struct fields, and map key/value alike — which is what makes the nested
  field-id binding `datafusion-scan/nested-json-rendering` relies on implementable.
* **The new binary MUST be added to the `test-e2e` make target.** That target enumerates its test
  binaries explicitly, so a new E2E binary that is not listed never runs in the suite gate.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: An Iceberg table's list, struct, and map columns return valid JSON end to end

* *GIVEN* a seeded Iceberg table carrying a primitive control column plus populated nested columns — a `list<string>`, a `list<int>`, a `struct<street: string, city: string>`, a `map<string, string>`, a `map<int, string>`, and a `list<struct<a: int>>` — each with at least one fully-populated row, one row whose nested value is NULL, and one row exercising an empty collection and a null member
* *WHEN* a query selects those columns through the virtual schema
* *THEN* `createVirtualSchema` SHALL declare every nested column as `VARCHAR(2000000)`, verifiable through `SYS.EXA_ALL_COLUMNS`
* *AND* the query SHALL SUCCEED for every nested column, so the recorded `sqlCode 22002` physical-to-logical cast failure for `struct` and `map` is gone
* *AND* every returned nested value SHALL parse as JSON and SHALL equal the exact document `datafusion-scan/nested-json-rendering` specifies for that value — a JSON array for each list, an object keyed by FIELD NAME for the struct, an object keyed by the STRINGIFIED key for both maps
* *AND* the `list<string>` value SHALL return `["hello","world"]` with QUOTED elements, so the recorded Arrow display text `[hello, world]` is gone
* *AND* a NULL nested value SHALL return SQL NULL rather than the text `null`, `{}`, or `[]`
* *AND* the `map<int, string>` value SHALL return its integer keys as JSON object names, proving the stringification path against real Parquet data
* *AND* a WHERE predicate, a GROUP BY key, an ORDER BY key, and `COUNT(DISTINCT)` over a nested column SHALL each return the rows an equivalent comparison over the rendered JSON string returns, so the column behaves as the `VARCHAR(2000000)` Exasol declared for it in every pushdown shape
* *AND* the WHERE case SHALL be written as a REGRESSION test with a discriminating fixture — a predicate matching ONE of several rows, plus a conjunction of a primitive predicate and a nested one — because a predicate over a `list` column today returns EVERY row (`datafusion-scan/nested-json-rendering`), so a fixture whose predicate matches every row would pass against the bug it exists to catch
* *AND* the new E2E binary SHALL be listed in the `test-e2e` make target, so the suite gate runs it
<!-- /DELTA:NEW -->
