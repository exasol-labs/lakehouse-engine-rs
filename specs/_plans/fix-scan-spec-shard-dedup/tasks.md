# Tasks: fix-scan-spec-shard-dedup

## Phase 2: Implementation (Group A — spec type split, depends: none)
- [x] 1.1 Drop `catalog: CatalogProps` from `ScanSpec` (scan/spec.rs); remove test refs (scan/spec.rs, scan/mod.rs). Keep `CatalogProps` type.
- [x] 1.2 Introduce common/per-shard JSON split: `CommonScanSpec` (invariant fields, no `files`) + `ScanSpec::from_parts(common, files)` + `to_common_json()`/files-json serializer pair; keep credential-safe error redaction. [expert]
- [x] 1.3 Unit tests: common blob round-trips without `files`; `from_parts` reconstitutes spec equal to pre-split; malformed either-arg JSON errors don't leak creds; `catalog` absent from serialized JSON. [expert]

## Phase 2: Implementation (Group B — decision log, depends: none)
- [x] 5.1 Add ADR-052 to `specs/decision-log.md` (two-arg invariant/per-shard split; connect-back rejected refs #32/ADR-048; drop dead `catalog`; field-audit result; no single-arg back-compat). Note: plan.md said "ADR-050" but that number was already taken by `fix-grouped-agg-select-order` (ADR-050/ADR-051); used the next free number, ADR-052.

## Phase 2: Implementation (Group C — scan entry, depends: A)
- [x] 2.1 Change `#[exasol_udf(name="LAKEHOUSE_SCAN", input(spec: String))]` → `input(common: String, files: String)` in lib.rs.
- [x] 2.2 Update `run_scan` (scan/mod.rs): read `ctx.get_string(0)` (common) + `ctx.get_string(1)` (files), merge via `ScanSpec::from_parts`, run unchanged downstream. Preserve NULL-arg handling for both.
- [x] 2.3 Host integration test driving `run_scan` two-arg shape against local Parquet (no S3), asserting identical rows to pre-split single-arg path. [expert]

## Phase 2: Implementation (Group D — adapter fan-out SQL, depends: A)
- [x] 3.1 Rewrite `build_fan_out_inner_with_spec`: emit common blob once as UDF's first SELECT-list literal, only per-shard files JSON in VALUES rows; new shape `SELECT {udf}('<common>', files) EMITS (...) FROM (VALUES ({i},'<files_i>'),...) AS shards(shard_key, files) GROUP BY shard_key`. Drop per-shard spec closure. [expert]
- [x] 3.2 Update single-shard branches of `build_row_scan_sql` and `build_aggregate_scan_sql` to two-arg form (common literal + whole-file-list literal).
- [x] 3.3 Update `build_grouped_aggregate_scan_sql`: build grouped common blob once with `limit = None` (structural LIMIT-exclusion invariant), then emit two-arg fan-out + single-shard forms.
- [x] 3.4 Remove `catalog: catalog.clone()` from the two `ScanSpec` construction sites (pushdown.rs ~1779, ~1825); adjust `handle_pushdown` plumbing.
- [x] 3.5 Update inline SQL-builder unit tests in pushdown.rs: common literal appears exactly once, files per VALUES row, no credential/tuning payload repeats per shard. [expert]

## Phase 2: Implementation (Group E — E2E DDL, depends: C signature)
- [x] 4.1 Update scan SET SCRIPT DDL to two-arg signature in tests/e2e_scan_test.rs (`(common VARCHAR(2000000), files VARCHAR(2000000)) EMITS (...)`) + any direct single-arg invocation.
- [x] 4.2 Update scan SET SCRIPT DDL in tests/e2e_capability_test.rs likewise.

## Phase 4: Code Review
- [x] R.1 Review all changed files.

## Phase 5: Verification
- [x] V.1 Build: `make cross-musl-udf-build` — exit 0, `.so` built (0.18.0)
- [x] V.2 Test: `cargo test` — 328 lib + integration (incl. scan_two_arg), 0 failures
- [x] V.3 Lint: `cargo clippy --all-targets` — 0 warnings
- [x] V.4 Format: `cargo fmt --check` — clean
- [x] V.5 E2E: `make test-e2e` — 7 e2e_capability + 32 e2e_scan, 0 failures (live Exasol + MinIO + Iceberg REST)
