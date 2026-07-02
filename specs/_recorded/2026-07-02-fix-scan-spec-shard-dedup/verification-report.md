# Verification Report: fix-scan-spec-shard-dedup

## Bottom Line

**PASS.** All implementation tasks complete, code review clean (zero must-fix), and every
verification gate green — host unit + integration tests, clippy, fmt, the cross-musl `.so` build,
and the full E2E suite against the live Exasol + MinIO + Iceberg REST stack.

The scan SET UDF is now two-argument (`LAKEHOUSE_SCAN(common VARCHAR(2000000), files
VARCHAR(2000000))`): the shard-invariant common spec is serialized once as a SELECT-list literal and
only each shard's file-URI JSON varies across the `VALUES` rows. The dead `ScanSpec.catalog` field is
removed. Version bumped 0.17.1 → **0.18.0** (minor — breaking UDF ABI change). Closes #25.

## Automated Checks

| Step | Command | Result |
|------|---------|--------|
| Build | `make cross-musl-udf-build` | ✅ exit 0 — `.so` built in `rust:1.92-bookworm` (v0.18.0) |
| Test | `cargo test` | ✅ 328 lib + integration tests, 0 failures (incl. new `scan_two_arg`, 2 passed) |
| Lint | `cargo clippy --all-targets` | ✅ 0 warnings |
| Format | `cargo fmt --check` | ✅ no changes |
| E2E | `make test-e2e` | ✅ 7 `e2e_capability` + 32 `e2e_scan`, 0 failures |

## Scenario Coverage

| Scenario | Test | Status |
|----------|------|--------|
| Scan reconstitutes ScanSpec from common + per-shard args (NEW) | `from_parts_reconstitutes_equal_spec` (spec.rs) | ✅ |
| Fan-out serializes common once, files per shard (CHANGED) | `fan_out_serializes_common_once_files_per_shard` (pushdown.rs) | ✅ |
| Single-shard preserves single-invocation query (CHANGED) | `single_shard_two_arg_common_and_files_once` | ✅ |
| Pushdown carries logical schema in common arg (CHANGED) | `pushdown_carries_logical_schema_in_common_arg` | ✅ |
| Projection pushed into common arg (CHANGED) | `projection_in_common_arg_emits_match` | ✅ |
| Filter pushed into common arg (CHANGED) | `filter_in_common_arg` | ✅ |
| LIMIT pushed into row-scan common arg (CHANGED) | `row_scan_limit_in_common_arg` | ✅ |
| Grouped fan-out via GROUP BY shard_key (CHANGED) | `grouped_fan_out_common_once_files_per_shard` | ✅ |
| LIMIT NOT in per-shard scan for grouped query (CHANGED) | `grouped_common_blob_has_no_limit` | ✅ |
| No catalog block in any scan spec (CHANGED) | `scan_spec_carries_no_catalog_block` | ✅ |
| Malformed either-arg JSON does not leak credentials (NEW) | `malformed_common_or_files_json_does_not_leak_credentials` | ✅ |
| Two-arg scan returns identical rows to pre-split path (CHANGED) | `scan_registers_only_assigned_files_two_arg` (scan_two_arg.rs) + E2E | ✅ |
| NULL in either argument is a user error (NEW) | `two_arg_null_in_either_argument_is_user_error` | ✅ |

All plan scenarios have a corresponding passing test. Deviation: the host two-arg equivalence test
lives in `tests/scan_two_arg.rs` (local Parquet, no S3), complementary to the DB-gated E2E cases in
`e2e_scan_test.rs` — `run_scan`'s S3 path cannot be driven against `file://`, so the test exercises
the real reconstitution seam (`read_scan_spec` → `from_parts_json`) plus the shared downstream.

## Code Review

Clean. Zero must-fix, zero guardrail violations, zero dead code. All five correctness hot-spots
verified sound: arg-order consistency across macro/reader/SQL-builders, structural grouped-LIMIT
exclusion (common blob built with `limit = None`), credential-safe deserialization errors,
`to_common_json` free of both `files` and `catalog`, and per-argument NULL handling. One LOW
nice-to-have (a stale `ctx.get(0)` doc comment on `ScanSpec::from_json`) was fixed.

## Notes

- ADR recorded as **ADR-052** (not the plan's stale "ADR-050" — 050/051 were consumed by the
  merged #36 `fix-grouped-agg-select-order` work).
- The #36 outer merge SELECT ordering / HAVING logic was left untouched; this change is confined to
  the inner fan-out ScanSpec serialization.
