# Plan: fix-scan-spec-shard-dedup

## Summary

Split the scan SET UDF from one full-`ScanSpec`-per-shard argument into a two-argument
form — one shard-INVARIANT common spec serialized ONCE as a SELECT-list literal, plus a
per-shard file-URI list in the `VALUES` rows — and drop the dead `catalog` field, so the
generated fan-out statement carries credentials/projection/filter/tuning knobs once instead
of up to ~300 times. Closes GitHub issue #25.

## Design

### Context

The adapter serializes the full `ScanSpec` (credentials, projection, filter, aggregates,
group keys, logical schema, EMITS types, all `df_*`/memory tuning knobs) into EVERY shard's
UDF invocation via `build_fan_out_inner_with_spec` — only `files` differs between shards. On
a wide fan-out (G capped at 300) the shard-invariant payload is repeated up to ~300 times in
one generated statement, risking Exasol statement-size limits and multiplying the credential
surface. `ScanSpec` also carries `catalog: CatalogProps`, which the scan UDF never reads
(only tests reference it); all catalog interaction is adapter-side before the UDF runs.

- **Goals** — carry the shard-invariant part exactly once per fan-out; trim `ScanSpec` to
  only fields the scan UDF actually reads; preserve identical scan/aggregate results and the
  grouped-aggregate "LIMIT never in per-shard partial" invariant.
- **Non-Goals** — no change to sharding math (G computation, byte-balancing), pushdown
  translation, aggregate decomposition, memory sizing, or credential resolution semantics; NOT
  reintroducing connect-back for credentials (removed by #32 / ADR-048); no single-argument
  backward-compatibility path (the `.so`, SLC, and adapter deploy together and the DDL is
  recreated per deployment).

### Field Audit

Every `ScanSpec` field vs. actual production scan-UDF reads:

| Field | Read by scan UDF (production)? | Classification | Evidence |
|-------|-------------------------------|----------------|----------|
| `files` | Yes — registers exactly these files | **PER-SHARD** (arg 1) | `scan/mod.rs` file registration |
| `projection` | Yes | INVARIANT (arg 0) | projection DataFrame build |
| `filter` | Yes | INVARIANT (arg 0) | filter applied to scan |
| `limit` | Yes (row-scan); stripped on grouped path | INVARIANT (arg 0) | row-scan LIMIT |
| `aggregates` | Yes — selects partial-agg path | INVARIANT (arg 0) | `run_scan_async` branch |
| `group_keys` | Yes — per-shard GROUP BY | INVARIANT (arg 0) | grouped scan |
| `emit_exa_types` | Yes — output coercion | INVARIANT (arg 0) | emit coercion |
| `logical_schema` | Yes — field-id projection | INVARIANT (arg 0) | logical schema build |
| `storage` | Yes — S3 object store | INVARIANT (arg 0) | object store config |
| `catalog` | **No** — only tests reference it | **DROP** | `scan/spec.rs:318-319`, `scan/mod.rs:1089`; no non-test reader |
| `df_target_partitions` | Yes | INVARIANT (arg 0) | `session_config_for_spec` |
| `df_batch_size` | Yes | INVARIANT (arg 0) | `session_config_for_spec` |
| `df_threads_per_udf` | Yes | INVARIANT (arg 0) | `build_scan_runtime` |
| `memory_pool_fraction` | Yes | INVARIANT (arg 0) | pool sizing |
| `instance_overhead_mb` | Yes | INVARIANT (arg 0) | pool sizing |

Result: `files` is the SOLE per-shard field; `catalog` is dropped; everything else is invariant.

### Decision

#### Architecture

Two-argument SET UDF. The adapter builds the common blob ONCE, and only the per-shard file
list varies across `VALUES` rows.

```
Adapter (once per pushdown)                 Generated fan-out SQL
┌──────────────────────────────┐
│ ScanSpec (no `files`)         │  serialize once → COMMON literal ──┐
│  proj/filter/limit/aggs/...   │                                    │
│  storage creds / tuning knobs │                                    ▼
└──────────────────────────────┘   SELECT {udf}('<COMMON>', files) EMITS (...)
┌──────────────────────────────┐     FROM (VALUES (0,'<files_0>'),(1,'<files_1>'),...)
│ shards: Vec<Vec<String>>      │ ──▶      AS shards(shard_key, files)
│  one file subset per shard    │       GROUP BY shard_key
└──────────────────────────────┘

Scan UDF entry (per invocation)
  ctx.get_string(0) → COMMON JSON ─┐
  ctx.get_string(1) → files JSON ──┴─▶ merge → ScanSpec → existing scan path
```

The JSON split introduces a serialized "common" shape (the current `ScanSpec` minus `files`
and minus `catalog`) and a per-shard files shape (a JSON array of URI strings). `run_scan`
deserializes both and reconstitutes a `ScanSpec` for the rest of the pipeline, which is
unchanged. Only `Value::String` crosses the `.so` boundary — both args are VARCHAR JSON, so
the boundary discipline holds.

Concrete typing approach (implementer's choice, both preserve the invariant): either (a) a
new `CommonScanSpec` struct holding all invariant fields plus a `ScanSpec::from_parts(common,
files)` constructor, or (b) keep `ScanSpec` and add `ScanSpec::to_common_json()` (serializes
with `files` skipped) + `ScanSpec::from_common_and_files(common_json, files_json)`. Approach
(a) is preferred — it makes "files is the only per-shard field" a type-level guarantee and
removes the risk of a stray `files` value leaking into the common literal.

#### Grouped-aggregate LIMIT invariant

Today the grouped path strips `limit` per shard via a closure in
`build_fan_out_inner_with_spec`. With the split, the common blob is shared by ALL shards, so
the adapter builds the grouped common blob with `limit = None` ONCE; the LIMIT-exclusion
invariant then holds for every shard by construction (there is no per-shard spec to strip).
The row-scan path leaves `limit` in the common blob as today.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Serialize-once shared literal | `build_fan_out_inner_with_spec`, `build_row_scan_sql`, `build_aggregate_scan_sql`, `build_grouped_aggregate_scan_sql` | Emit the invariant blob once, not per shard |
| Type-level per-shard isolation | `CommonScanSpec` + `ScanSpec::from_parts` | Make `files` the only per-shard field structurally |
| Two-arg VARCHAR-only boundary | `run_scan` `ctx.get_string(0/1)` | Keep only `Value::String` across `.so` |
| Dead-field removal | drop `ScanSpec.catalog` | Trim payload to what the UDF reads |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Two-argument UDF (common literal + per-shard files) | (a) connect-back to fetch creds; (b) BucketFS-staged spec file; (c) keep single arg | (a) rejected — connect-back removed by #32/ADR-048; (b) adds state/staging to a stateless UDF; (c) is the bug. Two args need no new infra and are supported by `exasol-udf-macros` 0.20.0 |
| Drop `catalog` field entirely | Keep for symmetry / future use | No production reader; keeping it re-introduces dead credential-adjacent payload; YAGNI |
| No single-arg back-compat path | Dual-read `from_json` fallback | `.so`/SLC/adapter deploy together; DDL recreated per deployment; no in-flight spec crosses a version boundary |
| Grouped LIMIT stripped once in the common blob | Keep the per-shard strip closure | The common blob is shared, so a single `limit = None` on it is simpler and structurally guarantees the invariant |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/scan-execution | CHANGED | `datafusion-scan/scan-execution/spec.md` |
| datafusion-scan/scan-execution-memory-and-credentials | CHANGED | `datafusion-scan/scan-execution-memory-and-credentials/spec.md` |
| parallelism/work-unit-sharding | CHANGED | `parallelism/work-unit-sharding/spec.md` |
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| vs-adapter/pushdown-planning-grouped-agg | CHANGED | `vs-adapter/pushdown-planning-grouped-agg/spec.md` |
| vs-adapter/rest-catalog-oauth-auth | CHANGED | `vs-adapter/rest-catalog-oauth-auth/spec.md` |

## Dependencies

- `exasol-udf-macros` 0.20.0 multi-argument `input(a: T, b: T)` support (verified in the
  macro crate's `tests/annotation_typed.rs`).

## Migration

| Current | New |
|---------|-----|
| `LAKEHOUSE_SCAN(spec VARCHAR(2000000))` | `LAKEHOUSE_SCAN(common VARCHAR(2000000), files VARCHAR(2000000))` |
| `ScanSpec { ..., files, catalog }` serialized whole per shard | `CommonScanSpec` (no `files`, no `catalog`) serialized once + files JSON per shard |
| `#[exasol_udf(name="LAKEHOUSE_SCAN", input(spec: String))]` | `input(common: String, files: String)` |

## Implementation Tasks

1. **Spec type split**
   1.1 Drop the `catalog: CatalogProps` field from `ScanSpec` (`scan/spec.rs`); remove its
       test references (`scan/spec.rs`, `scan/mod.rs`). Keep `CatalogProps` itself — it is still
       used adapter-side for catalog interaction.
   1.2 Introduce the common/per-shard JSON split: a `CommonScanSpec` (all invariant fields, no
       `files`) plus `ScanSpec::from_parts(common, files)` and a `to_common_json()` /
       files-json serializer pair. Keep credential-safe error redaction (no raw input echoed).
   1.3 Unit tests: common blob round-trips without `files`; `from_parts` reconstitutes a spec
       equal to the pre-split spec; malformed either-arg JSON errors do not leak credentials;
       `catalog` no longer appears in any serialized JSON. [expert]

2. **Scan UDF entry (two-argument)**
   2.1 Change `#[exasol_udf(name="LAKEHOUSE_SCAN", input(spec: String))]` to
       `input(common: String, files: String)` in `lib.rs`.
   2.2 Update `run_scan` (`scan/mod.rs`) to read `ctx.get_string(0)` (common) and
       `ctx.get_string(1)` (files), merge via `ScanSpec::from_parts`, then run the unchanged
       downstream path. Preserve NULL-argument handling for both arguments.
   2.3 Host integration test driving `run_scan` with the two-argument shape against a local
       Parquet file (no S3), asserting identical rows to the pre-split single-arg path. [expert]

3. **Adapter fan-out SQL (serialize-once)**
   3.1 Rewrite `build_fan_out_inner_with_spec` to emit the common blob once as the UDF's first
       SELECT-list literal and put only the per-shard files JSON in the `VALUES` rows; new shape
       `SELECT {udf}('<common>', files) EMITS (...) FROM (VALUES ({i},'<files_i>'),...) AS
       shards(shard_key, files) GROUP BY shard_key`. Drop the now-unneeded per-shard spec
       closure. [expert]
   3.2 Update the single-shard branches of `build_row_scan_sql` and `build_aggregate_scan_sql`
       to the two-argument form (common literal + whole-file-list literal).
   3.3 Update `build_grouped_aggregate_scan_sql`: build the grouped common blob once with
       `limit = None` (preserving the LIMIT-exclusion invariant structurally), then emit the
       two-argument fan-out and single-shard forms.
   3.4 Remove `catalog: catalog.clone()` from the two `ScanSpec` construction sites
       (`pushdown.rs:1547`, `:1592`) and adjust the `handle_pushdown` plumbing accordingly.
   3.5 Update inline SQL-builder unit tests in `pushdown.rs`: assert the common literal appears
       exactly once, files appear per `VALUES` row, and no credential/tuning payload repeats per
       shard. [expert]

4. **E2E DDL signatures**
   4.1 Update the scan SET SCRIPT DDL to the two-argument signature in `tests/e2e_scan_test.rs`
       (`(common VARCHAR(2000000), files VARCHAR(2000000)) EMITS (...)`) and any direct
       single-arg invocation in that test.
   4.2 Update the scan SET SCRIPT DDL in `tests/e2e_capability_test.rs` likewise.

5. **Decision log**
   5.1 Add ADR-050 to `specs/decision-log.md` (see decision-log.md for content): two-argument
       invariant/per-shard split; connect-back rejected (refs #32/ADR-048); drop dead `catalog`;
       field-audit result; no single-arg back-compat.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 1.3 (spec type split + tests) |
| Group B | 5.1 (decision log — independent doc) |
| Group C | 2.1, 2.2, 2.3 (scan entry — depends on A) |
| Group D | 3.1–3.5 (adapter fan-out — depends on A) |
| Group E | 4.1, 4.2 (E2E DDL — depends on C signature) |

Sequential dependencies:
- Group A → Group C, Group D (both consume the split types)
- Group C → Group E (DDL must match the new `input(...)` arity)
- Group B is independent

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Field | `ScanSpec.catalog` (`scan/spec.rs:185`) | No production scan-UDF reader; catalog interaction is adapter-side |
| Field init | `catalog: catalog.clone()` (`pushdown.rs:1547`, `:1592`) | Field removed |
| Test refs | `scan/spec.rs:280-284`, `:318-319`; `scan/mod.rs:1089` | Reference the removed field |
| Closure | per-shard `spec_to_json` LIMIT-strip closure in grouped path | Replaced by a single `limit = None` on the shared common blob |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Scan registers only its assigned files and returns matching rows (CHANGED, two-arg) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `scan_registers_only_assigned_files_two_arg` |
| Scan reconstitutes the ScanSpec from the common and per-shard arguments (NEW) | Unit | `crates/lakehouse-engine/src/scan/spec.rs` | `from_parts_reconstitutes_equal_spec` |
| Scan-driving query fans the SET UDF across shards via GROUP BY shard_key (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `fan_out_serializes_common_once_files_per_shard` |
| Single node with G collapsing to one preserves the single-invocation query (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `single_shard_two_arg_common_and_files_once` |
| Pushdown resolves the file list once and builds a scan-driving query (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `pushdown_carries_logical_schema_in_common_arg` |
| Projection is pushed into the scan-driving query (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `projection_in_common_arg_emits_match` |
| Filter predicate is pushed into the scan spec (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `filter_in_common_arg` |
| LIMIT is pushed into the scan spec (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `row_scan_limit_in_common_arg` |
| Grouped scan-driving SQL fans out via GROUP BY shard_key over G work units (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_fan_out_common_once_files_per_shard` |
| LIMIT is NOT pushed into per-shard scan for a grouped query (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_common_blob_has_no_limit` |
| Scan reads data files with vended credentials carried in the scan spec (CHANGED) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `scan_reads_with_vended_creds_in_common_arg` |
| Catalog auth props are never placed in any scan spec (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `scan_spec_carries_no_catalog_block` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Two-arg scan UDF + fan-out dedup | `make cross-musl-udf-build && make test-e2e` | E2E passes; the scan SET SCRIPT is created with two VARCHAR args; multi-shard fan-out SQL contains the common literal once and one files literal per `VALUES` row |
| No catalog block / no per-shard credential repetition | `cargo test -p lakehouse-engine fan_out_serializes_common_once_files_per_shard -- --nocapture` | Test passes; generated SQL string shows the credential/tuning payload exactly once |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures (fails, not skips, without a DB) |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt` | No changes |
