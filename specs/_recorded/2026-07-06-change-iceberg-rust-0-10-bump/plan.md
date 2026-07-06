# Plan: change-iceberg-rust-0-10-bump

## Summary

Bump `iceberg` / `iceberg-catalog-rest` / `iceberg-storage-opendal` from 0.9.1 to
0.10.0-rc.2 (git-tag pin), which moves iceberg-rust onto arrow/parquet 58 and collapses the
**entire workspace** (production *and* dev/e2e) onto a single arrow-58 dependency tree — with zero
observable behavior change. Reaching a single tree requires dropping the test-only `tpchgen-arrow`
dependency (it has no arrow-58 release) and building arrow-58 batches from `tpchgen` core instead.
This is a standalone dependency/toolchain bump; wiring the Iceberg positional/equality delete fix
stays scoped to issue #11 as a separate follow-on plan (`Closes #65`, cross-links #11).

## Design

### Context

Today the workspace resolves **two** arrow trees: `arrow 57.3.1` (linked internally by iceberg
0.9.1) and `arrow 58.3.0` (datafusion 54 + `exasol-udf-sdk` 0.20.2). The split is safe only by
discipline — iceberg types never carry Arrow batches across the `.so` boundary; only test fixtures
construct arrow-57 objects directly to feed iceberg's writer. iceberg 0.10.0-rc.2 moves iceberg-rust
onto arrow/parquet **58** (verified against the tag's `Cargo.toml`), so the production split can be
retired. The rustc-1.94 blocker that previously gated this bump is already resolved
(`f3c2d61`; `rust-toolchain.toml` pins 1.94).

- **Goals** —
  - Pin all three iceberg crates to 0.10.0-rc.2 and unify the **entire workspace** (production +
    iceberg-write + test fixtures) on a single arrow/parquet 58 tree — no arrow 57 or 59 anywhere.
  - Make every iceberg-rust call site compile and behave **identically** against 0.10.0-rc.2.
  - Retire the now-obsolete arrow-57 split rationale (Cargo.toml comments, dev-dependency aliases,
    CLAUDE.md/spec prose that names the 57/58 split as a live constraint).
- **Non-Goals** —
  - **No delete-application work.** This plan MUST NOT touch `ArrowReaderBuilder` usage — production
    code never calls it (only `plan_files()`/`FileScanTask` path+size metadata crosses from iceberg
    into DataFusion). The `ArrowReaderBuilder::new(runtime, ...)` signature change is irrelevant here
    and belongs to #11.
  - No arrow major bump beyond 58, no datafusion/SDK change, no new scenarios, no capability changes.

### Decision

Pin the three iceberg crates to the **git tag `v0.10.0-rc.2`** (commit
`be6cc96eaeb1cac4574cabb11ea6e1e92e0aad45`) of `apache/iceberg-rust`. 0.10.0-rc.2 is **not published
to crates.io** (latest crates.io release is 0.9.1), so the interview's `=0.10.0-rc.2` exact-version
form is not usable — a git-tag pin is the only mechanism, and it satisfies the "deliberate, reviewed
bump" intent equally (a tag is immutable and a later RC/GA is a deliberate edit).

#### Architecture

```
Cargo.toml [workspace.dependencies]
  iceberg / iceberg-catalog-rest / iceberg-storage-opendal
    0.9.1 (crates.io, arrow 57)  ──▶  git tag v0.10.0-rc.2 (arrow 58)

Production code (single arrow-58 tree after bump)
  pushdown.rs ─ catalog build, list_tables, load_table, Table.scan().plan_files() ─▶ FileScanTask (path+size)
  iceberg_predicate.rs ─ Predicate/Reference/Datum + spec::Schema
  tables.rs / mod.rs ─ TableIdent/NamespaceIdent
  types/mapping.rs ─ spec::{Type, PrimitiveType} → Arrow/Exasol

Test-only fixtures (e2e, dev-deps) — single arrow-58 tree, no bridge
  seed.rs ─ iceberg writer stack ── fed arrow-58 batches (drop ice_arrow_* / ice_parquet 57 aliases)
  tpch_loader.rs ─ tpchgen CORE rows ─[build arrow-58 batches by hand]─▶ seed helpers
                   (tpchgen-arrow dependency removed entirely)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Git-tag dependency pin | `Cargo.toml` workspace deps | 0.10.0-rc.2 is not on crates.io; a tag is immutable and reviewable |
| API-diff-then-edit (verify each signature before touching code) | Task 1 → Tasks 3–6 | Pre-release RC; renames/added params must be confirmed against tag source, not assumed |
| Build arrow-58 batches from the generator's *core* rows | `tpch_loader.rs` / `seed.rs` | tpchgen-arrow has no arrow-58 release (2.0.2→57, 3.0.0→59); tpchgen **core** has zero deps, so hand-built arrow-58 batches keep the whole dev graph on one arrow tree with no bridge |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Git-tag pin `v0.10.0-rc.2` | `version = "=0.10.0-rc.2"` (crates.io); `rev = <sha>` | rc.2 is not on crates.io so the `=` form cannot resolve; a tag reads cleaner than a bare rev and is immutable in Apache's release process |
| Production/iceberg tree unifies on arrow 58; **do not** bump workspace arrow | Move workspace arrow to 59 to match tpchgen-arrow 3.0.0 | Would drag datafusion 54 + SDK 0.20.2 + iceberg 0.10 (all arrow-58) off their pinned tree — a far larger, out-of-scope change |
| Drop `tpchgen-arrow`; build arrow-58 batches from `tpchgen` core in the test loader | Keep tpchgen-arrow 2.0.2 (arrow 57) + IPC bridge; bump to 3.0.0 (arrow 59) + IPC bridge | Both alternatives leave a second, divergent arrow tree in the dev graph and add an IPC round-trip that does not exist today. tpchgen **core** has zero deps, so hand-built arrow-58 batches reach a genuinely single arrow tree with no bridge — matching the bump's purpose. Cost is bounded test-only code. See decision-log [3]. |
| Do not touch `ArrowReaderBuilder` | Route scans through the reader now | Deletes are #11's scope; production never invokes the reader, so its 0.10 signature change is a non-issue for this bump |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| _(none)_ | UNCHANGED | No spec delta — pure dependency bump, zero observable behavior change |

This plan changes **no** observable behavior, so it authors **no** scenario deltas. The existing
scenarios under `vs-adapter/pushdown-planning*`, `vs-adapter/pushdown-planning-cloud-credentials`,
`vs-adapter/create-virtual-schema*`, `datafusion-scan/type-mapping`, and `packaging/e2e-harness*`
remain the verification oracle and MUST continue to pass **unchanged** against 0.10.0-rc.2. See the
Verification section for the regression mapping.

Doc-consistency note (not a scenario change): the Background prose of
`vs-adapter/pushdown-planning-cloud-credentials` names `iceberg-catalog-rest 0.9.1` when explaining
why the self-issued `loadTable` GET is required. That prose is illustrative, not normative, and the
credential-vending behavior is out of this plan's scope; the version string is intentionally left for
refresh when that feature is next recorded (see decision-log entry [4]). No spec file is edited by
this plan.

## Dependencies

- Git tag `v0.10.0-rc.2` of `github.com/apache/iceberg-rust` (commit `be6cc96eaeb1cac4574cabb11ea6e1e92e0aad45`), covering `iceberg`, `iceberg-catalog-rest`, `iceberg-storage-opendal` (all workspace members of that repo, so one `git`+`tag` triple pins all three).
- `tpchgen` (dev-only, e2e): kept at 2.0.2 (**zero dependencies** — pure row generator, no arrow). `tpchgen-arrow` 2.0.2 (arrow 57) is **removed** — see Task 6 / decision-log [3].
- Unchanged: `arrow`/`parquet` 58, `datafusion` 54, `object_store` 0.13.2, `exasol-udf-sdk`/`-macros` 0.20.2.

## Migration

| Current | New |
|---------|-----|
| `iceberg{,-catalog-rest,-storage-opendal} = { version = "0.9.1" }` (crates.io) | `{ git = "…/iceberg-rust", tag = "v0.10.0-rc.2", … }` |
| Two arrow trees in `Cargo.lock` (57.3.1 + 58.3.0) across production | Single arrow-58 tree across production + iceberg-write |
| `ice_arrow_array` / `ice_arrow_schema` / `ice_parquet` (arrow/parquet 57 dev aliases) | Removed — `seed.rs` uses workspace arrow/parquet 58 |
| `tpchgen-arrow = "2.0.2"` (dev, arrow 57) | Removed — batches built from `tpchgen` core with workspace arrow 58 |
| Two arrow trees in the **dev** lock (57 + 58) | Single arrow-58 tree across the whole workspace (prod + dev) |
| Cargo.toml comments describing the 57/58 split + mandatory StorageFactory | Rewritten/removed to match 0.10 reality (re-verify StorageFactory is still mandatory) |

## Implementation Tasks

1. **API-diff research (do first, no code).** For every call site below, diff the 0.9.1 vs
   0.10.0-rc.2 signature against the **tag source** (`v0.10.0-rc.2`), not from memory. Produce a
   per-site change list the later tasks consume. Sites: `RestCatalogBuilder` + `OpenDalStorageFactory::S3`
   construction, `catalog.list_tables`, `LoadTableResult`/`TableMetadata`, `iceberg::table::Table`,
   `table.scan().with_filter(...).select_all().build()` → `plan_files()` → `FileScanTask`
   (`data_file_path()`, `file_size_in_bytes`), `iceberg::expr::{Predicate, Reference, Datum}` +
   `iceberg::spec::Schema`, `iceberg::{TableIdent, NamespaceIdent}`, `iceberg::spec::{Type, PrimitiveType}`,
   and the full writer stack in `seed.rs` (`IcebergWriter`/`IcebergWriterBuilder`,
   `DataFileWriterBuilder`, `ParquetWriterBuilder`, `RollingFileWriterBuilder`,
   `DefaultFileNameGenerator`, `Transaction`/`ApplyTransactionAction`, `TableCreation`/`TableUpdate`/
   `TableRequirement`, `UnboundPartitionSpec`/`UnboundPartitionField`). Also confirm: (a) whether
   `iceberg-storage-opendal`'s S3 factory API changed, (b) whether `iceberg-catalog-rest` kept the
   same crate name/module split, (c) that `RestCatalog::load_table` still returns only a `Table`
   (drops `config`/`storage_credentials`) — the fact the cloud-credentials self-issued GET relies on. `[expert]`
2. **Update workspace pins.** In `Cargo.toml [workspace.dependencies]`, replace the three iceberg
   crate `version = "0.9.1"` pins with the git-tag triple (`git`, `tag = "v0.10.0-rc.2"`, preserving
   `default-features`/`features` for `iceberg-storage-opendal`). Rewrite/remove the arrow-57 split
   comment block (lines ~31–36) and re-verify + update the mandatory-StorageFactory comment (~39–43).
3. **Fix `adapter/pushdown.rs`** (catalog build, `list_tables`, `load_table`/`LoadTableResult`/
   `TableMetadata`, `Table`, `scan().with_filter().select_all().build()`, `plan_files()`,
   `FileScanTask` accessors) per Task 1's diff. Preserve behavior exactly; no Arrow type crosses this
   boundary. `[expert]`
4. **Fix predicate + schema + identifier + type-mapping call sites** per Task 1's diff:
   `adapter/iceberg_predicate.rs` (`Predicate`/`Reference`/`Datum`/`spec::Schema`),
   `adapter/tables.rs` (`TableIdent`/`NamespaceIdent`), `adapter/mod.rs` (identity resolution),
   `types/mapping.rs` (`spec::{Type, PrimitiveType}` → Arrow/Exasol). `[expert]`
5. **Fix `tests/common/seed.rs`** to feed **arrow-58** into the 0.10 writer stack: drop the
   `ice_arrow_array` / `ice_arrow_schema` / `ice_parquet` (57) imports, switch to workspace
   `arrow`/`parquet` 58, and update the writer-builder API per Task 1's diff
   (`IcebergWriterBuilder`, `DataFileWriterBuilder`, `ParquetWriterBuilder`,
   `RollingFileWriterBuilder`, `DefaultFileNameGenerator`, `Transaction`/`ApplyTransactionAction`,
   `TableCreation`/`TableUpdate`/`TableRequirement`, `UnboundPartitionSpec`). `[expert]`
6. **Rework `tests/tpch_loader.rs` to drop `tpchgen-arrow` and build arrow-58 batches from `tpchgen`
   core.** `tpchgen-arrow` has **no arrow-58 release** (2.0.2→arrow 57, 3.0.0→arrow 59), so instead
   of carrying a second arrow tree + a bridge, remove it entirely. `tpchgen` **core** (2.0.2, zero
   deps, no arrow) provides the row generators; replace the `tpchgen_arrow` `*Arrow`/`RecordBatchIterator`
   emitters (`RegionArrow`, `SupplierArrow`, and the other 6 TPC-H tables the loader uses) with
   hand-built **workspace arrow-58** `RecordBatch`es — one column builder per field, matching each
   table's schema — then hand those straight to the (arrow-58) `seed` helpers. Update
   `arrow_schema_to_iceberg()` / `arrow_to_iceberg_type`'s `DataType` matches to the workspace
   arrow-58 `DataType`. Note: the earlier code relied on tpchgen-arrow's `Utf8View` string columns
   (see `seed.rs` header comment) — pick the arrow-58 string type the 0.10 iceberg writer accepts and
   keep the existing normalization. Result: a single arrow-58 tree, no IPC bridge. `[expert]`
7. **Clean dev-dependencies.** In `crates/lakehouse-engine/Cargo.toml [dev-dependencies]` remove the
   `ice_arrow_array` / `ice_arrow_schema` / `ice_parquet` (57) aliases **and** the `tpchgen-arrow`
   entry; keep `tpchgen` at 2.0.2. Update the stale alias/tpchgen-arrow rationale comments (~lines
   74–87) to state that fixtures now build workspace arrow-58 batches directly.
8. **Host unit tests green.** `cargo test` — all existing unit tests pass unchanged (pushdown ~145,
   iceberg_predicate 21, tables 11, mapping 14).
9. **E2E tests green.** `make test-e2e` — seed/loader-backed e2e suites pass unchanged
   (`e2e_scan_test` 43, `e2e_capability_test` 7, `e2e_count_distinct_test` 6).
10. **Lint + format clean.** `cargo clippy --all-targets` (0 warnings) and `cargo fmt` (no changes).
11. **Verify the arrow tree in `Cargo.lock`.** Confirm the **entire workspace** (production *and*
    dev/e2e) resolves a **single arrow-58 tree** — **no arrow 57 and no arrow 59 anywhere** in the
    lock. With `tpchgen-arrow` removed (Task 6) this should now be a genuinely clean single-tree
    result; if any non-58 arrow remains, trace which crate still pulls it and resolve before closing.
12. **Refresh stale version prose in docs/comments** (not specs): update the iceberg-version
    references in `CLAUDE.md` (§Tech stack / §Data types notes that name the 57/58 split as live) and
    any remaining Cargo.toml comments to the 0.10.0-rc.2 / single-arrow-58 reality. Do **not** edit
    spec files (see Features note + decision-log [4]). Reference `Closes #65` in the implementing
    commit; do **not** reference `Closes #11`.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A (research) | Task 1 |
| Group B (pin) | Task 2 |
| Group C (production fixes) | Task 3, Task 4 |
| Group D (test fixtures) | Task 5, Task 6, Task 7 |
| Group E (verify) | Task 8 → Task 9 → Task 10 → Task 11 → Task 12 |

Sequential dependencies:
- Group A → Group B → Group C (fixes need the diff and the pin resolving)
- Group C → Group D (test fixtures depend on the production crate compiling)
- Group D → Group E (verification runs after all code fixes)
- Within Group E, Task 8 → 9 → 10 → 11 → 12 run in order.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Dev-dependency aliases | `crates/lakehouse-engine/Cargo.toml` — `ice_arrow_array`, `ice_arrow_schema`, `ice_parquet` | arrow-57 tree retired; seed feeds arrow-58 |
| Dev-dependency | `crates/lakehouse-engine/Cargo.toml` — `tpchgen-arrow` | No arrow-58 release; batches now built from `tpchgen` core with workspace arrow-58 |
| Test code | `tests/tpch_loader.rs` — `tpchgen_arrow` `*Arrow`/`RecordBatchIterator` emitter usage | Replaced by hand-built arrow-58 batch construction |
| Comment block | `Cargo.toml` ~lines 31–36 (arrow-57/58 split rationale) | Split no longer exists after 0.10 bump |
| Comment | `crates/lakehouse-engine/Cargo.toml` dev-dep alias rationale (~lines 74–87) | Aliases + tpchgen-arrow removed |
| Doc prose | `CLAUDE.md` references naming the 57/58 split as a live constraint | No longer accurate |

No production functions/modules/tests are removed — behavior and public surface are unchanged.

## Verification

### Scenario Coverage

No new or changed scenarios (pure dependency bump). The regression oracle is the **existing** test
suite, which must remain green after the bump. Each affected iceberg-rust call site is already
covered by an existing test that MUST continue to pass unchanged:

| Behavior guarded (affected call site) | Test Type | Test Location | Test(s) |
|---------------------------------------|-----------|---------------|---------|
| Catalog build, `list_tables`, `load_table`, `plan_files()`, `FileScanTask` path/size | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | existing `#[test]`/`#[tokio::test]` suite (~145, mocked HTTP) |
| `Predicate`/`Reference`/`Datum` + `spec::Schema` translation | Unit | `crates/lakehouse-engine/src/adapter/iceberg_predicate.rs` | existing 21 tests |
| `TableIdent`/`NamespaceIdent` formatting | Unit | `crates/lakehouse-engine/src/adapter/tables.rs` | existing 11 tests |
| `spec::{Type, PrimitiveType}` → Arrow/Exasol mapping | Unit | `crates/lakehouse-engine/src/types/mapping.rs` | existing 14 tests (incl. Arrow `DataType` assertions) |
| End-to-end scan / pushdown against live REST catalog + MinIO + Exasol (writer stack + `plan_files`) | Integration (e2e) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | existing 43 tests |
| Capability negotiation e2e | Integration (e2e) | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | existing 7 tests |
| COUNT DISTINCT pushdown e2e | Integration (e2e) | `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` | existing 6 tests |
| Iceberg writer stack (`seed.rs`) + TPC-H loader (`tpch_loader.rs`) under arrow-58 | Integration (e2e) | `crates/lakehouse-engine/tests/common/seed.rs`, `tests/tpch_loader.rs` | exercised transitively by the e2e suites above |

A green run of the full unit + e2e suite against 0.10.0-rc.2, with no assertion edits, is the proof
that the bump preserves behavior.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Single arrow-58 tree (incl. dev) | `cargo tree -i arrow` (from workspace root, no `-e no-dev`) | Exactly one `arrow v58.x` node; no `v57.x` or `v59.x` in either the dev or non-dev graph |
| Production bump compiles | `cargo build` (host, debug) | Exit 0, links iceberg 0.10.0-rc.2 |
| UDF `.so` builds against 0.10 | `make cross-musl-udf-build` | Exit 0; `.so` produced in the `rust:1.94-bookworm` container |
| No arrow-57/59 in the lock | `grep -A1 '^name = "arrow"' Cargo.lock` | Shows `58.x` only — no `57.x`, no `59.x`, `tpchgen-arrow` absent |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (UDF `.so`) | `make cross-musl-udf-build` | Exit 0 |
| Test (unit) | `cargo test` | 0 failures |
| Test (e2e) | `make test-e2e` | 0 failures (fails, not skips, if no DB) |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
