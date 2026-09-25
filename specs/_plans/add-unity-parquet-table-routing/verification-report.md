# Verification Report: add-unity-parquet-table-routing

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Unity Catalog now admits and plans `PARQUET` base tables; the scan case-folds identity-bound columns and refuses an unadmitted physical type at rewrite time, for every format. All checklist commands pass. |
| Code review | 17 findings — 17 fixed (15 standard, 2 expert) |

| Check | Status |
|-------|--------|
| Build (`make cross-udf-build`) | ✓ |
| Tests (`cargo test`) | ✓ |
| E2E (`docker compose up -d && make test-e2e`) | ✓ |
| E2E Unity (`make unity-up && make test-e2e-unity`) | ✓ |
| Lint (`cargo clippy --all-targets`, `--features unity-e2e`) | ✓ |
| Format (`cargo fmt --check`) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Unit (lakehouse-engine lib) | `cargo test --workspace` | 1367 | 0 |
| Unit (lakehouse-catalog lib) | `cargo test --workspace` | 196 | 0 |
| Unit (vs-expression lib) | `cargo test --workspace` | 17 | 0 |
| Integration (`catalog_public_surface`) | `cargo test -p lakehouse-catalog --test catalog_public_surface` | 17 | 0 |
| E2E, base stack (16 binaries) | `docker compose up -d && LH_EXASOL_CPUSET=0-1 make test-e2e` | 380+ | 0 |
| E2E, Unity stack | `LH_EXASOL_CPUSET=0-1 make unity-up && make test-e2e-unity` | 28 | 0 |

Note: this host has 4 CPU cores. The Exasol container's default `cpuset 0-3` makes the unrelated
`adapter_detects_container_cpuset` test report `PRECONDITION UNMET` on it (it checks core-count
detection, not scanning). Both E2E runs above used `LH_EXASOL_CPUSET=0-1` to avoid that
host-specific precondition; it is not a code regression.

### Manual Tests

| Test | Result |
|------|--------|
| `unity-catalog-client`: table metadata shows `PARQUET`/`EXTERNAL` and `partition_index` 0/1 | ✓ |
| `unity-catalog-create-virtual-schema`: VS column types match the mapping table | ✓ |
| `unity-parquet-table-planning`: 4 rows returned with correct `YEAR`/`REGION` per directory | ✓ |
| `parquet-directory-seam`: `REGION = 'eu'` count matches fixture (3) | ✓ |
| `delta-table-planning`: `BASIC_PARTITIONED` unchanged (6 rows) | ✓ |
| `pushdown-format-neutral-resolution`: `EXPLAIN VIRTUAL` shows only the 2 `region=eu` files pushed | ✓ |
| `catalog-crate-public-surface-extensions`: 0 failures | ✓ |
| `unity-catalog-e2e-harness-parquet-queries`: `make test-e2e-unity`, 0 failures | ✓ |
| `scan-execution-field-id-projection`: 0 failures, including the case-fold tests | ✓ |
| `type-relaxation`: `SELECT ID, PRICE` fails naming `PRICE`, `Float32`, `Float64`, `widened/` | ✓ |
| `direct-storage-e2e`: covered by the base `make test-e2e` run above, 0 failures | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --all-targets: 0 warnings/errors
cargo clippy --all-targets --features unity-e2e: 0 warnings/errors
```

### Formatter

```
cargo fmt --check: clean, no changes
```

## Scenario Coverage

Every test name listed in the plan's `## Verification > Scenario Coverage` table was confirmed
present in the codebase (automated grep audit) and passes under the suite runs above. Highlights:

| Domain | Feature | Scenario | Test Location | Passes |
|--------|---------|----------|---------------|--------|
| vs-adapter | unity-parquet-table-planning | Files listed through the shared seam, no footer read | `.../format/unity_parquet_format_reader_tests.rs` | Pass |
| vs-adapter | unity-parquet-table-planning | Case-drifted column binds; unadmitted type refused naming the column | same | Pass |
| vs-adapter | unity-catalog-client | Parquet base table admitted with partition columns | `lakehouse-catalog/src/unity/client_tests.rs` | Pass |
| datafusion-scan | scan-execution-field-id-projection | Identity-bound field case-folds; ambiguous fold fails naming every candidate | `scan/field_id_projection_tests.rs` | Pass |
| datafusion-scan | type-relaxation | Physical type outside identity/widening/text is refused only on a rewrite that reads it | `scan/type_relaxation_tests.rs`, `scan/field_id_projection_tests.rs` | Pass |
| unity-e2e | unity-catalog-e2e-harness-parquet-queries | Unity Parquet table listed, queried, and agrees under vended/static credentials | `tests/e2e_unity_test.rs` | Pass |
| e2e-harness | direct-storage-e2e | `MERGE_SCHEMA = 'FALSE'` refuses a wider file column instead of narrowing it | `tests/e2e_direct_storage_test.rs` | Pass |

## Notes

- **Breaking changes are intentional per the plan's Impact section**: a query reading a column
  whose data-file type is outside identity, the supported widening set, and text rendering now
  fails naming the table, column, and both types (every format). Under direct storage
  `MERGE_SCHEMA = 'FALSE'`, a file column wider than the sampled declaration now fails rather than
  silently reading a truncated value.
- **Spec correction found during the fix pass**: task 4.21's fix (admit a primitive file column
  under a nested logical declaration by ordinary admission rather than always refusing it) made the
  `type-relaxation` spec delta's "without a nested descriptor" qualifier on string-tagged text
  admission inaccurate. Corrected in
  `specs/_plans/add-unity-parquet-table-routing/datafusion-scan/type-relaxation/spec.md` to drop
  the qualifier, matching the shipped `admits_as_text` behavior.
- **Known pre-existing spec-validation issues, unrelated to this plan**: `speq feature validate`
  reports errors only in already-recorded specs (`e2e-harness`, `vs-adapter/connection-credentials-azure`)
  that predate this work.
- **Docs follow-up left out of scope on purpose**: `docs/install.md:205` still reads "Unity Catalog
  (Delta tables)" in prose (not a link to the renamed anchor). `docs/catalogs.md`'s heading, intro,
  and summary-table row were all updated to "Delta and Parquet tables" during Group C's work.
