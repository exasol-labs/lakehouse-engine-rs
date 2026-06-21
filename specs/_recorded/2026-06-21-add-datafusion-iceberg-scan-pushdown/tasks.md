# Tasks: add-datafusion-iceberg-scan-pushdown

## Phase 2: Implementation (Group A — Scaffolding)
- [x] 2.A1 Workspace Cargo.toml (edition 2024) + `lakehouse-engine` cdylib crate skeleton; pin arrow/parquet 58, datafusion, iceberg-rust, SDK/macros 0.14.0 (crates.io — published, no path fallback)
- [x] 2.A2 Makefile: `cross-musl-udf-build` (docker rust:1.92-bookworm, `-p lakehouse-engine`, out `target/release/liblakehouse_engine.so`, persistent cargo vol) + gated `test-e2e`, mirror strata-rs
- [x] 2.A3 Docker compose: MinIO + Iceberg REST catalog + Exasol, shared network, BucketFS 2581 / MinIO 9000, mirror strata-rs

## Phase 2: Implementation (Group B+C — crate core) [expert]
- [x] 2.B1 Two `#[exasol_udf]` entry points (adapter via `vs_adapter(fn)` + scan SET UDF) in one crate; one `.so` exports both symbols [expert]
- [x] 2.B2 Scan-spec type (file list, projection, filter, limit, catalog/storage props) + (de)serialization across UDF arg boundary using only SDK `Value` [expert]
- [x] 2.B3 DataFusion scan: SessionContext, object_store→MinIO, register ONLY assigned files, apply projection/filter/limit [expert]
- [x] 2.B4 Arrow RecordBatch → SDK `Value` conversion (full type-mapping table + null + JSON fallback) + batch-by-batch incremental `ctx.emit` (drop each batch) [expert]
- [x] 2.B5 Scan-side error handling: unreadable-file errors without leaking credentials
- [x] 2.C1 `getCapabilities` (projection + filter + LIMIT only; no aggregation/join)
- [x] 2.C2 `createVirtualSchema`: resolve Iceberg schema from REST catalog, map each field via shared type-mapping (incompatible → VARCHAR(2000000), never error)
- [x] 2.C3 `pushdown`: resolve snapshot + file list ONCE, capture projection/filter/limit, build scan-driving SQL invoking scan SET UDF with explicit file list [expert]
- [x] 2.C4 Adapter-side error handling (catalog-unreachable, credential redaction)

## Phase 2: Implementation (Group D — host unit tests)
- [x] 2.D1 Arrow→Value conversion tests (all mappings + null)
- [x] 2.D2 Scan-spec (de)serialization round-trip across Value boundary
- [x] 2.D3 Pushdown SQL generation (projection/filter/limit carried; untranslatable predicate omitted) + capability reporting
- [x] 2.D4 Iceberg-field → Exasol-type schema mapping
- [x] 2.D5 Full type-mapping table — one test per category (numeric/float/string/date-time/in-range Decimal128/out-of-range Decimal128/incompatible families) [expert]

## Phase 2: Implementation (Group E — E2E) [expert]
- [x] 2.E1 E2E helper seeds an Iceberg table into the REST catalog over MinIO
- [x] 2.E2 E2E setup: install Rust SLC 0.14.0 + upload `.so` to BucketFS (22581), set SCRIPT_LANGUAGES, create adapter + scan scripts + virtual schema [expert]
- [x] 2.E3 E2E test: SELECT cols WHERE pred LIMIT n returns correct projected/filtered/capped rows; suite FAILS (not skips) when Exasol unreachable; DSNs include validateservercertificate=0

## Phase 4: Code Review
- [x] 4.1 Review all changed files (guardrails, dead code, credential redaction, type-mapping consistency) — review fixes applied: Makefile whitespace, durable DNS, value-based redaction + documented PoC risk, two missing tests, cleanups

## Phase 5: Verification (live Docker on this machine — CLEAN stack, no manual patches)
- [x] 5.1 Build: `make cross-musl-udf-build` → exit 0, `.so` produced (164 MB)
- [x] 5.2 `nm -D ... | grep __exa_udf_entry_` → both symbols (LAKEHOUSE_SCAN, LAKEHOUSE_ADAPTER)
- [x] 5.3 `cargo test` → 39 passed, 0 failed (incl. +redaction, +build_convention)
- [x] 5.4 `cargo clippy --all-targets --features exasol-e2e` → 0 errors (only pre-existing tests/common nits); `cargo fmt --check` clean
- [x] 5.5 `make test-e2e` against clean-from-scratch stack → 9 passed, 0 failed, NO manual /etc/hosts patch
- [x] 5.6 Scenario coverage: install-slc SCRIPT_LANGUAGES well-formed; gate SELECT returned 5 correct filtered/capped rows

## Phase 6: Verification Report
- [x] 6.1 Write verification-report.md (BLUF) — PASS, reproducible from clean stack
