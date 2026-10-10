# Decision Log: add-delta-pruning-e2e

## Interview

**Q:** Delta tables are reachable only through `CATALOG_KIND = 'UNITY_CATALOG'`, and `make test-e2e` starts no Unity Catalog container. Only `make test-e2e-unity` does, through `make unity-up` (the `docker-compose.unity.yml` overlay plus `scripts/unity/seed.sh`). Where should the Delta pruning test run?
**A:** Move Unity Catalog into `make test-e2e`. `make test-e2e` brings up Unity Catalog with `make unity-up`, and `e2e_pruning_test` registers the Delta table there. This meets the acceptance criterion literally. The plan also covers the Makefile target, the CI job that runs the default E2E suite (the `e2e-unity` job's cargo line stays flag-identical to the Makefile), how the table is registered in Unity Catalog with every selected column, and no feature gate that would skip the test.

**Q:** How should the Delta fixture be written with controlled row groups, pages, and file paths?
**A:** Hand-write the `_delta_log`. The data files reuse the Parquet encoder in `tests/common/raw_parquet.rs` with the pruning writer properties. The test writes the commit JSON itself: `protocol`, `metaData`, and `add` actions with per-file `stats` and the partition value of `p`. No new dependency. The plan states how stats correctness is asserted and checks the fixture against the Delta protocol.

## Design Decisions

### [1] The Delta copy reuses the Hive copy's data files, paths, and partition layout

- **Decision:** The Delta table at `s3://warehouse/delta_pruning/pruning_cases` holds one data file per fixture label. Each data file is byte-identical to the Hive copy's file for that label: the fixture batch without `p`, encoded with `pruning_writer_properties()`. The `add` paths are `p=a/a1.parquet`, `p=b/b1.parquet`, and `p=__HIVE_DEFAULT_PARTITION__/n1.parquet`, and `partitionValues` is `{"p": "a"}`, `{"p": "b"}`, or `{"p": null}`. The table declares `ts` as `timestamp_ntz`, so its protocol is reader version 3 and writer version 7 with `timestampNtz` in both `readerFeatures` and `writerFeatures`.
- **Alternatives:** Author the table with Apache Spark and delta-spark, the route `PROVENANCE.md` names for bespoke Delta tables (rejected in the interview: a second Spark job for rows the Rust harness already defines). Add the `deltalake` crate as a dev-dependency (rejected in the interview: a new dependency for one commit). Declare `ts` as Delta `timestamp` (rejected: the Parquet column is timezone-naive, and the protocol requires `isAdjustedToUTC = true` for `timestamp`, so the Delta copy would need different bytes from the Hive copy). Point the `add` paths at the Hive copy's objects as absolute paths (rejected: it couples two fixtures that the direct-storage enumeration and the Delta log must each own).
- **Rationale:** One encoding for both copies keeps the row-group and page shape identical by construction, and the fixture-shape test checks it on each copy. The protocol rules the fixture follows:
  - § Add File and Remove File: `path` is "A relative path to a data file from the root of the table", `partitionValues` is "A map from partition column to value for this logical file" and required, `size` is required, and `stats` is optional.
  - § Data Files: "Data files can be stored in the root directory of the table or in any non-hidden subdirectory" and "Actual partition values for a file must be read from the transaction log". The Hive-style directories follow the reference implementation's convention, and the vendored `basic_partitioned` table uses the same `letter=__HIVE_DEFAULT_PARTITION__/` directory with `{"letter": null}`.
  - § Partition Value Serialization: "Partition values are stored as strings". The NULL partition uses JSON `null`, the form the vendored `basic_partitioned` table (Delta Lake 2.1.1) writes and the shipped reader already resolves.
  - § Consistency Between Table Metadata and Data Files: "Values for all partition columns present in the schema MUST be present for all files in the table". Every `add` carries `p`.
  - § Primitive Types: `timestamp without time zone` "When this is stored in a parquet file, its `isAdjustedToUTC` must be set to `false`. To use this type, a table must support a feature `timestampNtz`." § Timestamp without timezone: "the table must have Reader Version 3 and Writer Version 7. A feature name `timestampNtz` must exist in the table's `readerFeatures` and `writerFeatures`." The engine's reader-feature gate allows `timestampNtz`.
  - Data files omit `p`. § Materialize Partition Columns: without that writer feature, "writers are not required to write partition columns to data files".
- **Promotes to ADR:** no

### [2] One shared writer drops and rewrites a hand-written Delta table on every run

- **Decision:** A new test module `tests/common/delta_log.rs` owns the Delta log JSON. One function writes a whole one-commit table: it deletes every object under the table location, PUTs each data file, and writes `_delta_log/00000000000000000000.json` with `protocol`, `metaData`, and one `add` per file. It records each file's byte length as `size` and derives the protocol from the columns: reader 1 and writer 2, or reader 3 and writer 7 with `timestampNtz` when a column is `timestamp_ntz`. `seed_delta_extra_types_table` in `e2e_unity_test.rs` moves onto it, with the same protocol, columns, and `add`.
- **Alternatives:** Write the pruning log inline in `pruning_fixture.rs` and leave the inline log in `e2e_unity_test.rs` (rejected: two modules would encode the same log format, and a protocol fix would need two edits). Rewrite the objects in place on every run (rejected: § Creation of New Log Entries says "Writers MUST never overwrite an existing log entry", and § Data Files says "Data files MUST be uniquely named and MUST NOT be overwritten"). Write only when commit 0 is absent (rejected: a changed fixture would meet a stale table on a developer's persisted SeaweedFS volume).
- **Rationale:** Dropping the table before writing it makes every run create a new table, so no live log entry or data file is overwritten, and the fixture still converges after a change ("Fixture authoring is idempotent across runs", `specs/testing.md` § Fixtures). The writer hides the log layout, the size bookkeeping, and the protocol choice from both callers.
- **Promotes to ADR:** no

### [3] Per-file statistics cover `numRecords` and `k`, and the fixture-shape test checks them against the footers

- **Decision:** Each `add` carries `stats` with `numRecords`, and with `minValues`, `maxValues`, and `nullCount` for `k`, computed from the fixture rows. The file whose `k` is NULL in every row (`b2`) carries `nullCount` for `k` and no `k` bound. The fixture-shape test reads the committed log and each data file's footer, and asserts per file: the label, the row-group and page shape, `partitionValues.p`, `size` equal to the object's byte length, `numRecords` equal to the footer's row count, and the `k` bounds and null count equal to those over the footer's row-group statistics.
- **Alternatives:** Bounds for every column (rejected: no translatable leaf in the case table reads `s`, `x`, or `ts`, because `x` and `ts` appear only inside `ABS(X) > 0.1` and `SECOND(TS, 3) > 1` and `s` only inside `LIKE` (decision [2] of the #466 plan), so string-prefix and timestamp-format rules would be written and never read). Derive the stats from the written footers (rejected: the shape test would then compare a value with itself).
- **Rationale:** § Per-file Statistics makes statistics optional and lists `numRecords`, `nullCount`, `minValues`, and `maxValues`. For an all-NULL column it says the bounds carry no information. The kernel folds a missing bound to "keep the file" (`delta/delta-file-pruning` Background), so a column without bounds costs pruning and never rows. `nullCount` and `numRecords` for `k` are what the kernel's `IS NULL`, `IS NOT NULL`, and null checks read. Two independent derivations, rows for the log and the writer for the footer, make the comparison a real check.
- **Promotes to ADR:** no

### [4] The test registers the table in Unity Catalog under its own schema through a shared helper

- **Decision:** `register_unity_table`, `wait_for_unity_catalog`, `unity_port`, `unity_catalog_url`, and `UNITY_CATALOG_URI_INTERNAL` move from `e2e_unity_test.rs` to a new `tests/common/unity.rs`, built under `exasol-e2e` and `unity-e2e`. `register_unity_table` takes the namespace and creates its catalog and schema when absent. `e2e_pruning_test` registers `pruning_cases` under `unity.e2e_pruning` with its six columns and `p` as the partition column, and creates the virtual schema `PRUNING_DELTA` with the CONNECTION `PRUNING_DELTA_CREDS` through `create_virtual_schema_with_password`. `scripts/unity/seed.sh` is unchanged.
- **Alternatives:** Register the table in `seed.sh` (rejected: the column list would duplicate the Rust definition that also builds the Delta schema). Register it under the seeded `unity.delta_e2e` (rejected: the pruning virtual schema would enumerate every vendored table, against "Each standalone fixture gets its own namespace and virtual schema", `specs/testing.md` § Fixtures).
- **Rationale:** The Exasol column list of a Unity table comes from the Unity Catalog columns' `type_json` (`types/mapping.rs`), so the registration and the Delta `schemaString` read one column definition in `pruning_fixture.rs`. `seed.sh` restarts Unity Catalog, whose test server keeps its catalog in memory, so the test registers on every run. The CONNECTION carries static SeaweedFS keys, as the Unity suite's own virtual schema does (`specs/testing.md` § Per-run cloud resources).
- **Promotes to ADR:** no

### [5] `make test-e2e` runs `make unity-up`, and the `e2e` CI job starts its stack with the Unity overlay

- **Decision:** The `test-e2e` recipe runs `$(MAKE) unity-up` before its cargo line, the same shape as `test-e2e-unity`, and its cargo line gains no flag. The `e2e` job in `.github/workflows/ci.yml` uses `docker compose -f docker-compose.yml -f docker-compose.unity.yml` for its pull, start, Spark-fixture, log-dump, and stop steps. The `e2e-unity` job and the `test-e2e-unity` cargo line are unchanged.
- **Alternatives:** Run the Delta cases in `e2e_unity_test` under `make test-e2e-unity` (rejected in the interview: the acceptance criterion names `make test-e2e`). Start the Unity Catalog container in `test-e2e` without `seed.sh` (rejected in the interview, which chose `make unity-up`).
- **Rationale:** `unity-up` names `exasol` because the overlay adds the `unitycatalog` host entry to the Exasol container. A stack started without the overlay therefore has its Exasol container recreated on the first `unity-up`. Starting the CI stack with the overlay makes `unity-up` start only the Unity Catalog container and run the seed.
- **Consequences:**
  - Both `e2e` legs, `E2E` and `E2E (8.29.x)`, now read a Unity Delta table. The `e2e-unity` job runs on 2025.x only, so the 8.29.x leg is the first to read a Unity Delta table on 8.29.
  - The `e2e` job pulls the Unity Catalog and AWS CLI images and runs the seed.
  - A local `make test-e2e` after a stack started without the overlay recreates the Exasol container once. Its data volume survives.
- **Promotes to ADR:** no

### [6] The Delta expected sets follow the kernel's SQL WHERE data skipping

- **Decision:** A Delta expected set is listed for every case whose predicate the Delta translator translates fully. These are the same 13 cases that carry an Iceberg set. Partly translated cases carry none and are held to soundness. The sets follow `delta_kernel` 0.26 as read in its source (`scan/data_skipping.rs`, `kernel_predicates/mod.rs`):
  - The skipping predicate is built with `eval_sql_where`, whose doc says "SQL WHERE semantics only keeps rows for which the filter evaluates to TRUE". Each comparison is wrapped in a null check of its column, also under `NOT`.
  - A partition column's value is its exact minimum and maximum ("For partition columns, returns the exact partition value (which serves as both min and max)"), and its null check is `partitionValues_parsed.<col> IS NOT NULL`. A NULL partition value therefore fails every comparison, including the negated equality that `!=` and `NOT IN` produce.
  - A data column's null check is `nullCount != numRecords`, so a column that is NULL in every row of a file fails every comparison. `IS NULL` keeps a file when `nullCount != 0`, and `IS NOT NULL` keeps it when `nullCount != numRecords`.
  - `NOT` is pushed down by De Morgan, and a negated equality keeps a file when its minimum or its maximum differs from the literal.

  | Case | Iceberg | Delta |
  |------|---------|-------|
  | `NOT (K >= 10 AND K <= 20)` | a2, b1, n1, n2 | a2, b1, n1, n2 |
  | `NOT (K > 9 AND K < 21)` | a2, b1, n1, n2 | a2, b1, n1, n2 |
  | `NOT (K < 30 AND P = 'a')` | all six | a2, b1, b2, n1, n2 |
  | `NOT (P > 'a' AND P < 'c')` | a1, a2 | a1, a2 |
  | `NOT (P IN ('a', 'b'))` | n1, n2 | none |
  | `NOT (K IN (10, 20))` | all six | a1, a2, b1, n1, n2 |
  | `NOT (P IN ('a', 'b') OR P IS NULL)` | none | none |
  | `NOT (P BETWEEN 'b' AND 'c')` | a1, a2 | a1, a2 |
  | `NOT (P IS NULL)` | a1, a2, b1, b2 | a1, a2, b1, b2 |
  | `NOT (K IS NULL)` | a1, a2, b1, n1, n2 | a1, a2, b1, n1, n2 |
  | `NOT (P IS NOT NULL)` | n1, n2 | n1, n2 |
  | `NOT (K IS NOT NULL)` | a2, b2 | a2, b2 |
  | `NOT (K <= 1000)` | none | none |

- **Alternatives:** Assert only Sound for Delta (rejected: a change that turns Delta pruning off would pass). Compute the sets with a Rust model of the kernel (rejected: a second copy of the kernel's bound semantics in test code, the drift that ADR `delta-kernel-prunes-adapter-only-translates` rejects for the adapter. Hand-listed labels are explicit and short, as decision [4] of the #466 plan chose for Iceberg).
- **Rationale:** Three rows differ from Iceberg, and each pins a kernel rule: `NOT (K < 30 AND P = 'a')` drops `a1` because the kernel prunes the negated equality `P != 'a'` on the exact partition value, `NOT (P IN ('a', 'b'))` drops the NULL partition under SQL WHERE semantics, and `NOT (K IN (10, 20))` drops the all-NULL `b2`. A live set that differs from a listed set is a finding to explain from the kernel source, never a value to copy into the table. A Sound failure is an engine defect and stops the plan.
- **Promotes to ADR:** no

### [7] The behavior is stated in one new `delta/delta-file-pruning` scenario

- **Decision:** The `delta/delta-file-pruning` delta adds one scenario, `Delta pruning keeps every file with a matching row for every filter shape`. It changes no Background bullet. The test rules for the Delta fixture, the Unity Catalog bring-up, and the CI job go into `specs/testing.md` as direct edits.
- **Alternatives:** Restate the `AND`, `OR`, `NOT`, and `BETWEEN` exactness rules in the Delta Background (rejected: `file-planning/pushdown-file-pruning` already states them and says "Delta pruning follows the same rules", so the scenario cites it).
- **Rationale:** The adapter's Delta behavior does not change. The new scenario states the end-to-end outcome the case table proves, and the scenario title differs from the Iceberg one, so each `/// Scenario:` line names one scenario.
- **Promotes to ADR:** no

## Review Findings
