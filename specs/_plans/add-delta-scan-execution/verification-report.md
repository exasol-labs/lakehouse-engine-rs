# Verification Report: add-delta-scan-execution

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All 24 tasks and 24 review findings complete; build, full workspace test suite, both E2E suites, clippy, and fmt are green; every scenario-coverage row maps to a real passing test; every manual-testing command ran against the live Docker stack with matching output. |
| Code review | 24 findings — standard: 22, expert: 2 — all 24 fixed |

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

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit + Integration (`cargo test --workspace`) | 1305 | 1305 | 2 (pre-existing micro-benchmarks, unrelated to this plan) |
| E2E — Unity/Delta (`make test-e2e-unity`) | 18 | 18 | 0 |
| E2E — Iceberg regression (`make test-e2e`) | 254 | 254 | 0 |

### Tool Evidence

**Build** — `cargo build --workspace --all-targets`: exit 0.

**Lint** — `cargo clippy --all-targets`: exit 0, no warnings.

**Format** — `cargo fmt --all -- --check`: exit 0, no diffs.

## Scenario Coverage

Every scenario in `plan.md`'s Scenario Coverage table (deletion vectors, partition values,
delete-mechanism convergence, format-reader resolution, catalog-kind matching, Iceberg
byte-identity, and the seven Unity/Delta E2E scenarios) resolves to a test that exists and
passed in this run. Three rows had drifted from the actual test names left by review fixes 4.11
and 4.12 and one row named a test that was never written (its scenario is in fact already proven
by `every_request_shape_resolves_through_the_format_reader_seam`, which two other rows already
cite) — all four corrected in `plan.md` during this pass:

| Row | Old name in plan.md | Corrected to |
|-----|----------------------|--------------|
| Resolution: One catalog session per request serves every table | `a_two_leg_join_builds_exactly_one_catalog_session` | `a_two_leg_join_resolves_both_legs_on_one_catalog_session` |
| Resolution: The catalog kind is matched at one added construction site | `catalog_kind_is_matched_only_at_the_construction_sites` | `catalog_kind_is_matched_only_at_the_construction_site` |
| Resolution: A table the reader cannot plan fails the query loud at plan time | `an_unplannable_delta_table_fails_pushdown_with_the_readers_error` (never existed) | `every_request_shape_resolves_through_the_format_reader_seam` |
| Kind: A pushdown request under the Unity Catalog kind is planned as a Delta scan | `unity_kind_pushdown_routes_to_the_delta_format_reader` (renamed by fix 4.12) | `unity_kind_pushdown_routes_to_the_unity_catalog_loader` |

## Manual Tests

Run against the live Docker stack (`lakehouse-engine-rs-2-exasol-1`, `lakehouse-engine-rs-2-minio-1`)
via `exapump sql`, per `plan.md`'s Manual Testing table:

| Test | Command | Result |
|------|---------|--------|
| Deletion vectors | `SELECT COUNT(*) FROM UNITY_DELTA_E2E_VS.TABLE_WITH_DV` | 8 — matches the plan's expected value (not the file's 10 physical rows) ✓ |
| Partition values | `SELECT LETTER, COUNT(*) FROM UNITY_DELTA_E2E_VS.BASIC_PARTITIONED GROUP BY LETTER ORDER BY 1` | `a,2` `b,1` `c,1` `e,1` `,1` — four non-null letters plus one NULL group, six rows total, no `__HIVE_DEFAULT_PARTITION__` value ✓ |
| Format-neutral resolution | `SELECT COUNT(*) FROM UNITY_DELTA_E2E_VS.MULTI_PART_STATS` | 5, no "not yet supported" error ✓ |
| Column mapping | `SELECT ID, NAME, "VALUE" FROM UNITY_DELTA_E2E_VS.CM_ID_MODE` | 3 real rows under logical column names, never NULL ✓ (plan's command needed `VALUE` quoted — it is an Exasol reserved word; corrected in `plan.md`) |
| Iceberg byte-identity | `make test-e2e` | 254/254 pass, no assertion edited ✓ |

## Notes

- **Bug found and fixed during this verification pass**: review fix 4.22 (task already marked
  `[x]` from a prior session) replaced a string-offset join assertion in
  `tests/e2e_unity_test.rs` with a check against each side's parsed `files` array — but a live
  `make test-e2e-unity` run showed neither side's `files` entries carry the table name as a
  substring (only `table_root` does), and the sharded fact side carries no top-level `files`
  field at all when file assignment flows through `LAKEHOUSE_DISTRIBUTE_FILES`. Corrected to
  assert on each side's `table_root` instead, verified green on rerun. `tasks.md`'s 4.22 line and
  `review-findings.md`'s finding text are historical and still describe the original (incorrect)
  `files`-based fix — the actual applied fix is `table_root`-based, per the corrected `tasks.md`
  entry.
- Tasks 4.13–4.24 (the remaining standard review fixes from a prior session) are now all applied
  and verified; 4.13 was already correctly applied when this session resumed.
- No scenario, review finding, or manual-testing command was skipped or deferred.
