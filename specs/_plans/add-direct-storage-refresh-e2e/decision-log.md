# Decision Log: add-direct-storage-refresh-e2e

## Interview

**Q:** How should the seven REFRESH cases (A1 to A7) be packaged to stay comprehensive but compact? The issue says one base path and virtual schema per test, but that costs seven fixtures and seven `CREATE VIRTUAL SCHEMA` statements.
**A:** One staged test, two REFRESHes (the recommended option). One base path and one virtual schema. Stage 1 changes everything at once: a new directory `t_new/`, a second file, an extra column and a wider `QTY` in one evolving table, `region=eu` in a Hive table, and an emptied directory. It then runs one REFRESH. Stage 2 adds the incompatible file and asserts that the REFRESH fails and that the other tables still work. Each case keeps its own before and after assertions. This needs the fewest fixtures. The accepted trade-off is that a failure in one case can hide the cases after it.

## Design Decisions

### [1] One staged test with one base path and one virtual schema, against the issue's one-test-per-case layout

- **Decision:** Cases A1 to A7 run in one test, `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema`, over one base path (`s3://warehouse/direct_refresh/`) and one virtual schema. Stage 1 applies the A1, A2, A3, A4, A6, and A7 changes, asserts each case's before-REFRESH state, runs one REFRESH, and asserts each case's after-REFRESH state. Stage 2 applies the A5 change and asserts its before, REFRESH, and after states. Every assertion message starts with its case label (`A1:` to `A7:`), so a failure names its case.
- **Alternatives:** One test, base path, and virtual schema per case, as issue #479 describes. Rejected by the user in the interview: it costs seven fixture sets and seven `CREATE VIRTUAL SCHEMA` statements for the same assertions.
- **Rationale:** The user chose this option. A5 must run last, because a failed REFRESH leaves the virtual schema at its previous declaration, and a later stage would then have nothing to refresh to. A7 sits in stage 1, so its after-REFRESH skip is asserted by a REFRESH that succeeds.
- **Consequences:** A failing case stops the test, so the cases after it report nothing until it is fixed. The case-label prefix keeps the first failure diagnosable. This deviates from the issue's layout sentence, not from any of its assertions: every row of table A keeps its before and after assertion.
- **Promotes to ADR:** no

### [2] Separate tables per case, with A3 and A4 sharing the evolving table

- **Decision:** The staged test uses six table directories. `t_append/` (A2) gains a second file of its own columns, `t_evolve/` (A3 and A4) gains one file that adds `NEW_COL` and stores `QTY` as INT64, `t_hive/` (A6) gains `region=eu/`, `t_emptied/` (A7) loses its data file, `t_new/` (A1) appears, and `t_conflict/` (A5) gains the incompatible file.
- **Alternatives:** Put A2 into the evolving table as well. Rejected: A2 asserts that the declared columns stay unchanged after the REFRESH, which a table that also gains `NEW_COL` cannot show. Split A3 and A4 into two tables. Rejected: the interview answer names one evolving table, and the A3 check on existing columns still runs over `ID` and `LABEL`, which the wider `QTY` does not disturb.
- **Rationale:** A table directory costs one or two small objects, while a virtual schema costs a `CREATE`. One table per incompatible assertion keeps every case's assertion discriminating.
- **Promotes to ADR:** no

### [3] The test resets its base path at the start and leaves the final state in place

- **Decision:** The test deletes every object under `BASE_REFRESH` with a new `delete_fixture_prefix` helper before it writes the initial objects, and it does no cleanup at the end. A new `delete_fixture_object` helper deletes the one data file of `t_emptied/` in stage 1. The fixtures are written by the test, not by `write_all_fixtures`, because the test mutates them. The base path is enumerated by no other test, because its final state fails enumeration (`specs/testing.md` § Fixtures).
- **Alternatives:** A per-run base path with a time suffix. Rejected: it accumulates objects in the SeaweedFS volume that outlives a run. Cleanup at the end of the test. Rejected: a failing assertion skips it, so the start-of-test reset is needed anyway, and the left state lets a developer inspect the last run.
- **Rationale:** `specs/testing.md` requires idempotent fixture authoring across runs. The live probe showed that SeaweedFS keeps an emptied directory as a listed prefix after its last object is deleted. A rerun therefore starts with empty `t_new/`, `t_emptied/`, and other directories. The enumeration skips them, so the served table list at `CREATE` is the same on every run, and the test asserts no exact `SKIPPED_TABLES` list before its first mutation.
- **Consequences:** `specs/testing.md` § Fixtures gains a bullet for mutated fixtures, and its `raw_parquet.rs` bullet names the two delete helpers.
- **Promotes to ADR:** no

### [4] A4 stores two INT64 values: one outside INT32 but inside DECIMAL(10,0), and one outside DECIMAL(10,0)

- **Decision:** The stage-1 file of `t_evolve/` stores `QTY` as INT64 with the values 5,000,000,000 and 50,000,000,000. Before REFRESH, the test asserts that `SELECT ID, QTY` fails with a numeric out-of-range error, and that `SELECT QTY ... WHERE ID = 3` returns 5,000,000,000 unchanged.
- **Alternatives:** One value outside INT32 only, as the issue's A4 row describes. Rejected: live, 5,000,000,000 reads back correctly before REFRESH, because an INT32 column declares `DECIMAL(10,0)`, which holds every value up to 9,999,999,999. The issue's "fails" expectation would not hold, and the test would not discriminate.
- **Rationale:** The two values pin both halves of the observed behavior: a value outside the declared Exasol type fails and is never truncated, and a value inside it reads back exactly. A truncating 32-bit cast would return 705,032,704 for the first value, so the second assertion catches the defect the issue worries about. No observed query shape returned a wrong or NULL value (Live Observations in `plan.md`).
- **Consequences:** The B5 documentation states that a value outside the declared type fails, not that every value outside the narrower physical type fails.
- **Promotes to ADR:** no

### [5] A7 keeps a `_SUCCESS` marker in the emptied directory

- **Decision:** `t_emptied/` holds `file1.parquet` and `_SUCCESS`. Stage 1 deletes only the data file.
- **Alternatives:** Delete every object of the directory, as the issue's A7 row says. Rejected: S3 drops a prefix that holds no object, so the directory would vanish from the listing and appear in no `SKIPPED_TABLES` entry. SeaweedFS keeps the empty prefix, so a test without a marker would pass on SeaweedFS for a reason S3 does not share.
- **Rationale:** A Spark job leaves `_SUCCESS` behind, and the marker makes the skip assertion hold on every S3-compatible store. The B5 documentation states both outcomes: the table disappears, and a directory that still holds an object is listed in `SKIPPED_TABLES`.
- **Promotes to ADR:** no

### [6] The two Iceberg-only refresh scenarios become kind-neutral instead of gaining a direct-storage copy

- **Decision:** The `# Feature` description and the scenarios "Refresh re-enumerates the namespace and returns a refresh response" and "Refresh reflects table and column structure changes" of `vs-adapter/refresh-and-set-properties` name both an Iceberg namespace and a direct-storage base path. Their Iceberg clauses keep their wording. The direct-storage clauses name the partition column and the `SKIPPED_TABLES` entry. The first scenario's persistence clause names `SKIPPED_TABLES` beside `TABLE_MAP`, because the refresh writes both.
- **Alternatives:** Add a direct-storage refresh scenario to `direct-storage/direct-storage-table-discovery` and leave the two scenarios Iceberg-only. Rejected: issue #479 requires the new test to carry the two scenario titles, and a test cannot cite a scenario whose GIVEN names a different catalog kind. Two scenarios about one protocol request would also give the refresh contract two owners.
- **Rationale:** The accepted ADR `vs-refresh-reuses-create-virtual-schema-enumeration` already makes the refresh path kind-neutral. The scenarios now say what the code does. The direct-storage behavior between refreshes is a planning property, so it goes into `direct-storage/direct-storage-table-planning`, and the failed refresh is a discovery property, so it goes into `direct-storage/direct-storage-table-discovery`.
- **Consequences:** The Iceberg dropped-column and renamed-column clauses still have no REFRESH E2E test. That gap predates this plan, and this plan does not widen it.
- **Promotes to ADR:** no

### [7] New scenarios state the observed behavior, including where it differs from the issue's expectation

- **Decision:** The new scenario on a file the declaration cannot hold states that every query on a table with an unfoldable column pair fails, including a query that does not read the column, and that a value inside the declared type reads back. The new scenario on reading current files states that an emptied table returns zero rows without an error before the REFRESH.
- **Alternatives:** Write the issue's expectation ("a query reading `X` fails", "error or zero rows") as the scenario. Rejected: `AGENTS.md` requires that behavior claims come from a live run, and the issue asks to assert whichever behavior the code shows.
- **Rationale:** No observed behavior returns a wrong value, so none is a defect under the issue's out-of-scope rule, and no `fix(...)` issue is needed. The timestamp precision loss found while checking the B4 table is the existing issue #461, which the B4 row links.
- **Promotes to ADR:** no

### [8] The staged test cites only the direct-storage REFRESH scenario whose GIVEN it builds

- **Decision:** Of the existing direct-storage scenarios that name REFRESH, the staged test carries only "A first-level directory holding no data file is skipped, not failed". It does not carry "Two partition keys that fold to the same name fail the refresh", "A file missing the colliding key's segment fails the refresh", or "MERGE_SCHEMA selects one footer or every footer, on both the refresh and the plan path".
- **Alternatives:** Carry every direct-storage scenario that names REFRESH, as the issue's acceptance criterion lists them. Rejected: the staged test builds no partition-key collision and no second virtual schema under `MERGE_SCHEMA = 'FALSE'`, so the line would claim proof the test does not give.
- **Rationale:** `AGENTS.md` requires one `/// Scenario:` line per scenario a test implements. The three scenarios keep their existing proof: `partition_key_collision_with_a_missing_segment_fails_the_refresh` and `merge_schema_false_declares_the_narrow_sampled_type_and_refuses_a_wider_file_column` in `e2e_direct_storage_test.rs`, and the fold-collision tests in `adapter/parquet_directory_tests.rs`.
- **Promotes to ADR:** no

### [9] The README Pushdown bullet is fixed with the tagline and the Sharding bullet

- **Decision:** Task 2.1 also rewrites the README Pushdown bullet, which says the path reaches "Apache Iceberg and Databricks-managed Iceberg".
- **Alternatives:** Fix only the tagline and the Sharding bullet, as B1 lists. Rejected: the Pushdown bullet carries the same wrong claim one line further down, and acceptance criterion AC14 asks for the catalog kinds and table formats to be described correctly.
- **Rationale:** It is the same defect in the same paragraph.
- **Promotes to ADR:** no

## Review Findings

### [1] [plan-review] Direct-storage clauses name the property configuration they hold for

- **Finding:** Three new direct-storage clauses stated behavior for every configuration, and recorded scenarios contradict them in some configurations. Under `MERGE_SCHEMA = 'FALSE'`, `direct-storage/direct-storage-properties` reads one footer per table, so the refresh scenario's added column and the discovery scenario's fold error do not hold. Under `HIVE_PARTITIONING = 'FALSE'`, `direct-storage/direct-storage-hive-partitioning` declares no partition column, so the refresh scenario's new partition column does not hold. On a partitioned table, a partition filter that prunes the string file plans and succeeds, so the planning scenario's "every query on the second table fails" does not hold.
- **Direction change:** The direct-storage GIVEN step of "Refresh reflects table and column structure changes" names a virtual schema that leaves `MERGE_SCHEMA` and `HIVE_PARTITIONING` absent, so both resolve to TRUE per `direct-storage/direct-storage-properties`. The GIVEN of "A refresh that a column pair fails keeps the previous declaration queryable" names a virtual schema that leaves `MERGE_SCHEMA` absent. The second GIVEN step of "Until a refresh, a file the declaration cannot hold fails the query and never returns a wrong value" names an unpartitioned table. Task 1.3 already builds each case this way: `VS_REFRESH` sets neither property, and `t_conflict/` holds its two files with no `key=value` segment. Task 1.3 is unchanged.
- **Promotes to ADR:** no
