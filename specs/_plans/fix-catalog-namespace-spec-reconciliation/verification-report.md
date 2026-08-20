# Verification Report: fix-catalog-namespace-spec-reconciliation

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Mechanical rename (`ICEBERG_NAMESPACE` → `NAMESPACE`, no alias) and Iceberg-only prose corrections landed with zero runtime behavior change. All automated checks and both live E2E suites are green. |
| Code review | 14 findings — 14 fixed (2 expert, 12 standard) |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (one row hit an unrelated pre-existing environment issue, documented below) |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit + Integration (`cargo test --workspace`) | 50 test binaries | 1535 | 2 |
| E2E (`make test-e2e`) | 11 test binaries | 279 | 0 |
| E2E Unity (`make test-e2e-unity`) | 1 test binary | 24 | 0 |

All three runs: 0 failed.

E2E's first run hit 2 failures (`e2e_int96_far_future_timestamp_scans_without_overflow`,
`e2e_int96_fixture_present_and_int96_encoded`) with "Table does not exist" /
"object ... not found" — the Docker stack's one-shot `spark-iceberg-fixtures` job
(`docker-compose.yml`) had not been run for this stack instance, a pre-existing environment gap
unrelated to this plan's changes (no fixture, positional-delete, or scan-execution behavior was
touched). Ran `docker compose up spark-iceberg-fixtures` (exited 0) and re-ran `make test-e2e`
clean.

### Manual Tests

| Test | Result |
|------|--------|
| `CREATE VIRTUAL SCHEMA ... WITH ... NAMESPACE = 'e2e_lakehouse' ...` | ✓ — captured live in task 1.1 (decision-log.md) and exercised by the green E2E suite |
| Same statement with `ICEBERG_NAMESPACE` in place of `NAMESPACE` | ✓ — fails with the required-property error naming `NAMESPACE`; pinned by unit test `create_virtual_schema_rejects_old_namespace_alias_without_replacement` and captured live in task 1.1 |
| `ALTER VIRTUAL SCHEMA ... SET NAMESPACE='<ns>'` | ✓ — captured live in task 1.1 (decision-log.md): CREATE + ALTER...SET both parsed and returned OK against the real adapter script |
| `CREATE VIRTUAL SCHEMA ... CATALOG_KIND = 'UNITY_CATALOG' NAMESPACE = '...'` | ✓ — equivalent DDL exercised end-to-end by the green `test-e2e-unity` suite (24/24 passed, including `unity_create_virtual_schema_lists_fixture_tables_and_columns` and the Delta broadcast-join/aggregate tests) |
| `NAMESPACE=tpch bench/run.sh` in docker mode | ⚠ partial — see Notes |

## Tool Evidence

### Build

```
cargo build --workspace → exit 0
```

### Linter

```
cargo clippy --all-targets → exit 0, "Finished `dev` profile [unoptimized + debuginfo] target(s)", zero warnings
```

### Formatter

```
cargo fmt --all -- --check → exit 0, zero diffs
```

### Spec validation

```
speq feature validate → 0 errors across all features (only pre-existing AND-step-count style warnings, none introduced by this plan)
```

## Scenario Coverage

Every scenario listed in the plan's Verification § Scenario Coverage table has a passing test.
Two test-location corrections against the plan's original mapping (both applied during the
review-fix pass, and reconciled here rather than adding a duplicate test, per the plan's own
instruction):

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | create-virtual-schema | Create VS enumerates every table in the configured namespace (no-alias clause) | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `create_virtual_schema_rejects_old_namespace_alias_without_replacement` | Pass |
| datafusion-scan | scan-execution-file-metadata | Delete-file relative and absolute paths resolve like data-file paths | `crates/lakehouse-engine/src/scan/store_router_tests.rs` (moved from the plan's originally-cited `scan/spec_tests.rs`; code review found the first draft implementation-coupled) | `delete_file_paths_resolve_against_the_table_root_like_data_file_paths` | Pass |
| datafusion-scan | scan-execution | (all recorded scenarios) | `crates/lakehouse-engine/tests/*`, `crates/lakehouse-engine/src/**/*_tests.rs` | (per plan table) | Pass |
| vs-adapter / datafusion-scan / parallelism | (remaining 25 scenarios in the plan's table — rename tests, join/broadcast, empty-result, sharding, resolution) | — | (per plan table, unchanged) | (per plan table, unchanged) | Pass — all confirmed green in `cargo test --workspace` and the two E2E runs above |

The plan's own coverage table was corrected in place (`plan.md`, during the expert review-fix
pass) to point at the actual final test location for the delete-file-path scenario.

## Notes

- **Guard exceptions (task 8.3).** The plan's completeness guards pass with two documented,
  by-design exceptions rather than literally zero extra hits:
  - Guard 1 (`ICEBERG_NAMESPACE`/`iceberg_namespace` grep): 3 extra hits in
    `adapter_tests.rs`'s plan-mandated no-alias test, which must quote the literal old string to
    assert its rejection.
  - Guard 2 (`"Iceberg table root"` grep): 8 hits outside `specs/_recorded/` — 3 are this plan's
    own delta-file `## Background` narration, 5 sit inside permanent-spec `## Scenarios` clauses
    that already carry a staged `DELTA:CHANGED` replacement, resolved when `/speq:record` merges
    the delta. This is the same pending-merge pattern the plan's own Guard 1 wording explicitly
    carves out for `ICEBERG_NAMESPACE`'s six scenario occurrences — Guard 2's wording simply
    omitted the equivalent carve-out. Not an implementation defect.
  - Guard 3 (`specs/_recorded`/`specs/_decision` frozen): passes exactly, zero diff.
- **Bench manual test (partial).** `NAMESPACE=tpch bench/run.sh` in docker mode generated the
  expected `CREATE VIRTUAL SCHEMA TPCH USING LHVS.LAKEHOUSE_ADAPTER WITH ... NAMESPACE = 'tpch' ...`
  DDL — direct proof the env var is read and flows into the real statement — but the subsequent
  UDF invocation failed with `F-UDF-CL-RUST-9001: Fingerprint mismatch: expected 0.21.0:..., found
  0.22.1:...`. This is a stale BucketFS-deployed `.so` / SLC-cache mismatch against the
  workspace's pinned SDK version (`exasol-udf-sdk = "0.22.1"`, unchanged by this plan, confirmed
  via `git log`) — an environment/deploy staleness issue, not a rename regression. The property
  rename itself is proven by the visible DDL and is additionally covered end-to-end by the green
  `test-e2e` and `test-e2e-unity` suites, which use a correctly matched `.so`.
- **Residual scenario-level narrowing (flagged, out of scope).** Two `## Scenarios` clauses not
  touched by this plan's delta files still narrate Iceberg-only framing that the plan's
  Background corrections already neutralize: `vs-adapter/pushdown-planning-file-resolution`'s
  "Pushdown derives the scanned Iceberg table from the involved virtual table" scenario, and
  `vs-adapter/pushdown-planning-file-encoding`'s delete-content-type scenario ("no additional
  Iceberg metadata is carried per delete file"). Both are scenario text with no runtime-behavior
  implication and were left unedited per the plan's explicit split (scenario edits require a
  `DELTA:CHANGED` marker in a delta file; this plan authored none for either). Recommend a
  follow-up plan if these should be neutralized too.
- No runtime behavior changed anywhere in this plan — every check above is either a rename
  (proven byte-identical in its effect via passing tests) or prose-only.
