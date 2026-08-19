# Verification Report: add-timestamp-precision-versioning

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All 12 implementation tasks and all 11 review-fix tasks are complete, merged, and verified. Build, full workspace test suite, all three E2E suites, lint, and format all pass clean. All 13 Scenario Coverage rows resolve to an existing, intent-correct test — one row's test name in plan.md does not exist verbatim in the working tree, but the scenario it names is fully covered by a consolidated test (see Notes). All six Manual Testing rows are backed by task 1's live Docker captures or a green E2E run. |
| Code review | 11 findings — standard: 11, expert: 0 — all fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Unit + integration (`cargo test --workspace`) | `target/speq:test.log` | 1504 | 0 |
| E2E core, Exasol 2025.2.1 (`make test-e2e`) | `target/speq:e2e-2025.log` | 283 (12 binaries) | 0 |
| E2E core, Exasol 8.29.13 (`make test-e2e`, `EXASOL_IMAGE=exasol/docker-db:8.29.13`) | `target/speq:e2e-8x.log` | 283 (12 binaries) | 0 |
| E2E Unity/Delta (`make test-e2e-unity`) | `target/speq:e2e-unity.log` | 25 | 0 |

Both E2E core runs execute the identical 12-binary suite (`e2e_capability_test`, `e2e_complex_type_test`,
`e2e_count_distinct_test`, `e2e_harness_row_cap_test`, `e2e_int96_timestamp_test`, `e2e_join_test`,
`e2e_non_ascii_identifier_test`, `e2e_positional_deletes_test`, `e2e_refresh_test`, `e2e_scan_test`,
`e2e_timestamp_precision_test`, `e2e_type_relaxation_test`), each reporting `0 failed; 0 ignored`. The
283-test count includes the 9 new tests in `e2e_timestamp_precision_test.rs` (task 7), added to the
`test-e2e` make target as required by that task.

## Tool Evidence

### Build

`make cross-musl-udf-build` — `target/speq:build.log`: `Compiling lakehouse-engine v0.40.0` →
`Finished \`release\` profile [optimized] target(s) in 1m 17s`. Exit 0.

### Linter

`cargo clippy --all-targets --features exasol-e2e,unity-e2e` — `target/speq:clippy.log`:
`Finished \`dev\` profile [unoptimized + debuginfo] target(s) in 4.12s`, no warnings emitted. Exit 0.

### Formatter

`cargo fmt --check` — `target/speq:fmt.log`: empty output (no diff). Exit 0.

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| datafusion-scan | type-mapping | A catalog timestamp column is declared TIMESTAMP(6) on Exasol 2025.x and later | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `timestamp_declaration_is_version_gated_for_both_catalog_kinds` | Pass |
| datafusion-scan | type-mapping | An empty or unparseable database version declares the microsecond precision | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `unreadable_database_version_declares_microsecond_precision` | Pass |
| datafusion-scan | type-mapping | The Arrow-input type resolver stays outside the version gate | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `arrow_input_resolver_stays_outside_the_timestamp_version_gate` | Pass |
| datafusion-scan | type-mapping | Iceberg timestamptz maps to plain Exasol TIMESTAMP, at the gated precision | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `iceberg_timestamptz_declares_timestamp_at_the_gated_precision` | Pass |
| vs-adapter | create-virtual-schema | createVirtualSchema reads the database version once and threads the resolved precision | `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs` | `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision` | Pass |
| vs-adapter | create-virtual-schema | createVirtualSchema reads the database version once and threads the resolved precision (threading half) | `crates/lakehouse-engine/src/adapter/catalog_client_tests.rs` | `build_listing_virtual_tables_declares_timestamp_at_the_given_precision` | Pass |
| datafusion-scan | type-mapping | A TIMESTAMP(p) column declaration serializes fractionalSecondsPrecision | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `exasol_type_to_json_renders_timestamp_fractional_seconds_precision` | Pass |
| vs-adapter | unity-catalog-create-virtual-schema | Unity Catalog Spark column types map to Exasol types sufficient for listing (gated-precision variant) | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `timestamp_declaration_is_version_gated_for_both_catalog_kinds` (see Notes: no `unity_timestamp_names_declare_the_gated_precision` symbol exists; the Delta `TIMESTAMP`/`TIMESTAMP_NTZ` assertions were folded into this test) | Pass |
| vs-adapter | delta-type-mapping | Every Delta type Exasol represents natively maps to its own Arrow tag (timestamp precision variant) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_timestamp_columns_declare_the_exact_gated_precision` | Pass |
| e2e-harness | e2e-harness | Microsecond-distinct Iceberg timestamps round-trip at the declared precision | `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs` | `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision` | Pass |
| e2e-harness | e2e-harness | The E2E suite gates on both supported Exasol major versions | `.github/workflows/ci.yml` | `e2e` job matrix: `image: exasol/docker-db:2025.2.1, check_name: E2E` and `image: exasol/docker-db:8.29.13, check_name: E2E (8.29.x)` | Pass |
| datafusion-scan | type-mapping | A VS timestamp compared as a rendered string uses a precision-matched oracle | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_upper_timestamp_declines_to_native_oracle` | Pass |
| e2e-harness | unity-catalog-e2e-harness-delta-queries | A Delta timestamp column's declared Exasol type is asserted exactly at the engine's precision | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_timestamp_columns_declare_the_exact_gated_precision` | Pass |

Each test body was read directly (via Serena `find_symbol`) and confirmed to assert what its row
claims, not merely to exist:

- `timestamp_declaration_is_version_gated_for_both_catalog_kinds` asserts `iceberg_primitive_to_exasol`
  and `column_source_type_to_exasol` (Unity `TIMESTAMP`/`TIMESTAMP_NTZ`) return `TIMESTAMP(6)` at
  `Microsecond` and bare `TIMESTAMP` at `Millisecond` — proving Iceberg and Delta cannot drift from
  each other, which is also what the Unity-Catalog-listing scenario needs.
- `unreadable_database_version_declares_microsecond_precision` iterates `""`, `"v2025.2.1"`,
  `"unknown"`, `".2.1"`, `"8x.1.0"`, `" "` and asserts every one resolves to `Microsecond` /
  `"TIMESTAMP(6)"`.
- `arrow_input_resolver_stays_outside_the_timestamp_version_gate` pins `arrow_to_exasol_type`'s
  signature via a function-pointer binding (`fn(&DataType) -> String`) so a threaded precision
  parameter would fail to compile, then asserts it still returns bare `"TIMESTAMP"`.
- `iceberg_timestamptz_declares_timestamp_at_the_gated_precision` asserts both `Timestamptz` and
  `TimestamptzNs` resolve to `"TIMESTAMP(6)"`/`"TIMESTAMP"` at the two precisions.
- `exasol_type_to_json_renders_timestamp_fractional_seconds_precision` asserts
  `{"type":"timestamp","fractionalSecondsPrecision":N}` for `TIMESTAMP(6)`, `TIMESTAMP(9)`,
  `TIMESTAMP(0)`; the unparameterized and zoned spellings keep their prior objects; three malformed
  inputs (`TIMESTAMP()`, `TIMESTAMP(abc)`, `TIMESTAMP(-1)`) fall through to the documented VARCHAR
  catch-all (review fix 4.5); and the round trip through `exasol_type_from_json` is checked.
- `build_listing_virtual_tables_declares_timestamp_at_the_given_precision` runs the full listing
  pipeline over one Iceberg and one Delta timestamp column and asserts both `dataType` objects match
  at each precision — the non-live half of the createVirtualSchema threading scenario.
- `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision` reads the live engine's
  expected precision, asserts the exact declared `COLUMN_TYPE`, asserts the rendered values round-trip
  at that precision, asserts `COUNT(DISTINCT)` (4 at microsecond, 2 at millisecond), and confirms via
  `EXPLAIN VIRTUAL`-style pushdown assertion that the query reaches the scan UDF.
- `unity_delta_timestamp_columns_declare_the_exact_gated_precision` asserts the exact declared
  `COLUMN_TYPE` (whitespace-stripped per review fix 4.8) for `TIMESTAMP_COL`, `TIMESTAMP_NTZ_COL`, and
  `DATE_TIMESTAMP_NTZ`, reading the same oracle as the Iceberg test.
- `e2e_upper_timestamp_declines_to_native_oracle` builds its native-Exasol oracle's `CAST` target from
  the same oracle rather than a hardcoded `TIMESTAMP`, so it stays correct at either precision.

## Manual Testing

| Feature | Command | Expected Output | Evidence |
|---------|---------|-----------------|----------|
| datafusion-scan/type-mapping | `EXASOL_IMAGE=exasol/docker-db:2025.2.1 make test-e2e` then `SYS.EXA_ALL_COLUMNS` query | Every Iceberg timestamp column reports `TIMESTAMP(6)` | decision-log.md `[C1]`: live `SYS.EXA_ALL_COLUMNS` capture on 2025.2.1 shows `{"type":"timestamp","fractionalSecondsPrecision":6}` → `TIMESTAMP(6)`. Reproduced by the green `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision` run in `target/speq:e2e-2025.log`. |
| vs-adapter/create-virtual-schema | `ALTER VIRTUAL SCHEMA <vs> REFRESH` then `SYS.EXA_ALL_VIRTUAL_SCHEMAS` | Refresh succeeds; precision is re-derived per request, never persisted in `adapterNotes` | decision-log.md `[C5]` + design decision [12]: the version is read once per `handle_create_virtual_schema` call and never written to `adapterNotes`; task 11's full E2E run (all `REFRESH`-exercising tests, e.g. `e2e_refresh_test`, green in `target/speq:e2e-2025.log`/`e2e-8x.log`) confirms refresh continues to succeed. |
| vs-adapter/unity-catalog-create-virtual-schema | `SYS.EXA_ALL_COLUMNS` for `STATS_ALL_TYPES` | `TIMESTAMP_COL` and `TIMESTAMP_NTZ_COL` both report `TIMESTAMP(6)` | `unity_delta_timestamp_columns_declare_the_exact_gated_precision`, green in `target/speq:e2e-unity.log` (25 passed, 0 failed), asserts exactly this against the live default-image (2025.2.1) stack. |
| vs-adapter/delta-type-mapping | `SELECT TIMESTAMP_COL, TIMESTAMP_NTZ_COL FROM <unity_vs>.STATS_ALL_TYPES` | Values render six fractional digits, unchanged in magnitude | decision-log.md `[C4]`: live capture shows `.000001/.000002/.123456/.123457` render intact at `TIMESTAMP(6)`. `unity_delta_varied_types_return_their_expected_exasol_types_and_values`, green in `target/speq:e2e-unity.log`, covers the same table's value rendering. |
| e2e-harness/e2e-harness | `EXASOL_IMAGE=exasol/docker-db:8.29.13 make test-e2e` | Suite passes; precision test asserts the millisecond arm and bare-`TIMESTAMP`-equivalent (`TIMESTAMP(3)`) declared type | `target/speq:e2e-8x.log`: 283 passed, 0 failed across the same 12 binaries as the 2025.x run, including `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision` asserting `TIMESTAMP(3)` / `COUNT(DISTINCT)=2` per decision-log.md `[C1]`'s correction that `SYS.EXA_ALL_COLUMNS` never reports bare `TIMESTAMP`. |
| e2e-harness/unity-catalog-e2e-harness-delta-queries | `make test-e2e-unity` | Passes, including the new exact declared-type assertion | `target/speq:e2e-unity.log`: 25 passed, 0 failed, including `unity_delta_timestamp_columns_declare_the_exact_gated_precision`. |

## Notes

**Task 1's live captures corrected several of the plan's original assumptions, and the implementation
absorbed every correction.** Recorded in decision-log.md's "Task 1 Live Captures" section:

- The version-source SQL parameter is `SYS.EXA_METADATA` `PARAM_NAME='databaseProductVersion'`, not
  `'databaseVersion'` as the plan text and task-5 sketch assumed (no such row exists on either engine).
  `tests/common/timestamp_precision.rs::live_engine_version` reads the corrected name.
- `SYS.EXA_ALL_COLUMNS` never reports a bare `TIMESTAMP` on either engine — an unparameterized
  declaration reports `TIMESTAMP(3)`. `ExpectedTimestampPrecision::MILLISECOND.declared_column_type`
  is `"TIMESTAMP(3)"`, not `"TIMESTAMP"`.
- The rendered fractional-digit *count* is not a valid discriminator (the WebSocket protocol always
  renders six digits regardless of declared precision); `COUNT(DISTINCT)` is the sharper oracle the
  tests use instead.
- Exasol 8.x does not reject `TIMESTAMP(6)` — it accepts and silently clamps it to `TIMESTAMP(3)`,
  which retracts the plan's original "fails loudly" framing of the empty/unparseable-version default.
  This retraction was itself the subject of two review findings (4.2, 4.3), both fixed and confirmed
  applied in `mapping.rs`'s `TimestampPrecision` and `from_database_version` doc comments.

All four corrections are confirmed carried through to the shipped code and proven by the green E2E
runs against both engine images, and by code review finding no remaining reference to the superseded
assumptions.

**One review fix is an outstanding manual ops action, not code.** Fix 4.11 filed GitHub issue #364
("Add E2E (8.29.x) to main's required-checks ruleset", confirmed open) and cited it inline in
`.github/workflows/ci.yml` above the `e2e` job. Adding the new `E2E (8.29.x)` check to `main`'s branch
protection ruleset requires a repository admin to edit the ruleset directly — this plan's code cannot
complete that step, and the existing `E2E` check name is deliberately left unchanged so no PR is
blocked in the meantime (design decision [10]).

**The Delta millisecond arm has no automated CI coverage — a known, accepted gap, not a defect.**
`e2e-unity` (`make test-e2e-unity`) always runs against the default `docker-compose.yml` image
(`exasol/docker-db:2025.2.1`) and is not matrixed; the interview decision recorded in decision-log.md
scoped the 8.29.x matrix to the core `e2e` job only, leaving `e2e-lakekeeper`, `e2e-unity`, and
`e2e-azure` single-version because they test catalog integrations orthogonal to Exasol version. The
Delta millisecond-precision code path (`unity_type_name_to_exasol` at `TimestampPrecision::Millisecond`)
is unit-tested (`timestamp_declaration_is_version_gated_for_both_catalog_kinds`) but not exercised
end-to-end against an 8.x engine in CI.

**One Scenario Coverage row names a test that does not exist verbatim.** Plan.md's row for "Unity
Catalog Spark column types map to Exasol types sufficient for listing" names
`unity_timestamp_names_declare_the_gated_precision` in `mapping_tests.rs`; no such symbol exists in the
working tree or in git history for that file. The underlying scenario is nonetheless covered: the
Delta `TIMESTAMP`/`TIMESTAMP_NTZ` assertions that test would have made are present inside
`timestamp_declaration_is_version_gated_for_both_catalog_kinds`, which asserts both Iceberg and Delta
producers resolve to the same precision. This is a stale test name in the plan's table, not a missing
behavior — code review's completed call-site census (which enumerated every caller of
`unity_type_name_to_exasol`'s callers and found no gap) corroborates that the Delta timestamp path is
exercised. No action is required beyond this note.
