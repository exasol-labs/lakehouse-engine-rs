# Verification Report: fix-absent-table-location-error-consistency

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | The absent-location guard is hoisted above the vended/static split in `resolve_file_list`; both auth modes now reject an empty `loadTable` `location` identically. All three `warehouse == storage location` wording defects are corrected. All checklist gates green: build, host tests, E2E, lint, format. |
| Code review | 6 findings — standard: 6 fixed, expert: 0 |

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
| Host (`cargo test`, workspace) | 1017 | 1017 | 2 |
| E2E (`make test-e2e`, 8 test binaries) | 224 | 224 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p lakehouse-engine absent_table_location` — 1 passed; both `use_vended_credentials` arms report the identical path-independent `UdfError::User` naming `loadTable`/`location` and the table | ✓ |
| `grep -rn "S3 URI of the Iceberg warehouse" . \| grep -vE '/target/\|/\.git/\|specs/_plans/'` — no output | ✓ |
| `cargo test -p lakehouse-engine reconstruct_` then `legacy_empty_root_treats_paths_as_absolute` — all pass unchanged | ✓ |
| `cargo test -p lakehouse-catalog vended` — all pass unchanged | ✓ |
| Task-5 audit sweep #1 (`fall.?back to the warehouse\|...`) — exactly the 4 predicted correct hits, no others | ✓ |
| Task-5 audit sweep #2 (`S3 URI of the Iceberg warehouse`) — zero hits | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --all-targets — 0 warnings, exit 0
```

### Formatter

```
cargo fmt --check — no diff, exit 0
```

### Build

```
make cross-musl-udf-build — exit 0 (rust:1.94-bookworm container)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-planning | File resolution rejects a loadTable response that carries no table location | `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs` | `absent_table_location_errors_on_both_vended_and_static_paths` | Pass |
| vs-adapter | pushdown-planning-cloud-credentials | The storage backend under vending is selected from the table location's URI scheme (CHANGED clause only) | `crates/lakehouse-catalog/src/vended.rs` | `vended_storage_anchor_is_the_s3_table_location` (unchanged) | Pass |
| datafusion-scan | scan-execution-file-metadata | Relative paths resolve against the table root and absolute paths pass through (retained empty-root clause) | `crates/lakehouse-engine/src/scan/object_store.rs` | `reconstruct_absolute_entry_passes_through`, `reconstruct_relative_entry_normalizes_single_separator` (unchanged) | Pass |
| datafusion-scan | scan-execution-file-metadata | Delete-file relative and absolute paths resolve like data-file paths (retained empty-root clause) | `crates/lakehouse-engine/src/scan/spec.rs` | `legacy_empty_root_treats_paths_as_absolute` (unchanged) | Pass |

## Notes

- The defect was unreachable through any live catalog (a spec-conformant `loadTable` response always carries `location`), so the reproduction is the host unit test constructing a malformed response directly, not a live-stack repro — as scoped in the plan's Requirements § Reproduction gate.
- Code review raised 6 standard findings, all fixed in the same branch before this report: (1) the error message now names the table and correctly describes the key-present-empty-value shape rather than an absent-key shape; (2) the now-stale `table_root` comment parenthetical was corrected to state the guard makes the root non-empty; (3) `resolve_file_list`'s public doc comment now states the path-independent rejection as part of its documented contract; (4) the loopback test fake's `JoinHandle` is now bound and awaited so a fake-server panic surfaces as itself; (5) a dangling plan-directory reference was dropped from a test banner; (6) `docs/catalogs.md`'s field-table row was reworded to state the field's meaning instead of a self-contradicting lexical claim.
- `relativize_path_to_root`'s empty-`table_root` branch and the three `datafusion-scan/scan-execution-file-metadata` empty-table-root clauses are intentionally retained as the wire-format totality property, per the plan's Dead Code Removal section — not touched by this change.
- E2E ran against an already-running Docker stack (Exasol + MinIO + iceberg-rest + Lakekeeper + Keycloak, containers prefixed `lakehouse-engine-rs-2-*`); no contention or flakiness observed — all 224 E2E tests passed on the first run.
