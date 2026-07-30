# Plan: refactor-storage-backend-enum

## Summary

Give the storage-backend decision one home: wrap `StorageProps` in a `StorageBackend` enum with a single `S3` variant, so every other module asks the enum for what it needs instead of assuming S3. Pure refactor: S3 behavior stays identical, guarded by the existing S3 E2E suite, and the only wire change is a variant tag on the scan spec's `storage` value.

Tracked in issue [#274](https://github.com/exasol-labs/lakehouse-engine-rs/issues/274) — slice B of six (A-F) for Azure Data Lake Storage Gen2 (`abfss://`) support. The implementing commit closes it (`Closes #274`).

## Design

### Context

Four engine-side sites independently know the storage backend is S3: `extract_bucket` derives a bucket from `Url::host_str()`, `build_s3_store` builds the `AmazonS3Builder` store, `register_bucket_store` registers it under `s3://{bucket}`, and `file_resolution.rs` calls `build_s3_file_io`. Two catalog-side sites independently map the same six iceberg S3 config keys: `build_s3_file_io` and `build_rest_catalog`'s inline block. Adding a second backend to that shape means adding a branch at each of the six sites, and one of them cannot take a branch at all — `extract_bucket`'s `url.host()` returns the storage account on an `abfss://` URI, not the container, so it is wrong for Azure rather than merely S3-specific.

- **Goals** — one type whose variant IS the backend; every backend-specific question answered by a method on it; the two identical S3 key mappings collapsed to one; `extract_bucket` and `build_s3_store` deleted; S3 behavior identical under the existing suites.
- **Non-Goals** — the Azure variant (slice C), Azure credential parsing, `abfss://` URI handling, any change to `StorageProps`' fields or serde encoding, any change to file pruning, snapshot reading, delete handling, or type mapping.

### Decision

`StorageBackend` is declared in `lakehouse-catalog` — the crate that produces storage credentials — as `enum StorageBackend { S3(StorageProps) }`, externally tagged with a lowercase variant key. It declares three methods: two `pub` (`secret_values`, `file_io`) and one crate-private (`catalog_storage_props`, whose only consumers are `build_rest_catalog` and `file_io`, both in-crate). DataFusion object-store registration stays engine-side as one backend-dispatching function, because `vs-adapter/catalog-crate-structure` forbids the catalog crate from declaring `object_store` or `datafusion` as a dependency.

#### Architecture

```
crates/lakehouse-catalog/src/storage.rs
  StorageBackend { S3(StorageProps) }          ← the one home for "which backend"
    ├── pub secret_values()          → error redaction (8 engine call sites)
    ├── crate catalog_storage_props()→ the SINGLE six-key S3 config map
    └── pub file_io()                → folds that map into FileIOBuilder
                    │                     │
        session.rs  │                     │  pushdown/file_resolution.rs
   build_rest_catalog                     └─ effective.file_io()
   props.extend(backend.catalog_storage_props())

crates/lakehouse-engine/src/scan/object_store.rs
  register_side_store(ctx, backend, files, table_root, budget, sizes)
      -> Result<Option<Url>, UdfError>   ← Some(url) registered, None already present
    match StorageBackend::S3(props) => derive bucket from first file
                                       build AmazonS3Builder store
                                       wrap in sized-HEAD decorator
                                       register under s3://{bucket}
```

`register_side_store` returns the store key it registered rather than `()`: DataFusion's `ObjectStoreRegistry` exposes only `get_store(url)`, so without an observable return the "already registered, skip" path cannot be asserted and its unit test cannot fail.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Enum variant as the backend discriminant | `lakehouse-catalog/src/storage.rs` | Extend by adding a case, not by editing a dispatch (Open/Closed); slice C adds one arm |
| Wrap, do not absorb, the payload | `StorageBackend::S3(StorageProps)` | Keeps the one type whose serde encoding is pinned field-for-field unedited |
| One key map, two consumers | `catalog_storage_props` feeding `file_io` | Removes the back-door duplication between `build_s3_file_io` and `build_rest_catalog` |
| Boundary-forced second owner | engine-side `register_side_store` | The catalog crate may not name `object_store`; one engine-side match arm replaces four S3-aware sites |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| `StorageBackend::S3(StorageProps)` wrapper | Inline the seven fields onto the variant | Wrapping leaves `StorageProps`, its `Default`, its `secret_values`, and its three unit tests untouched, and keeps every construction literal as written |
| Externally tagged, lowercase (`{"s3":{...}}`) | `#[serde(untagged)]` (byte-identical wire, zero golden churn); internally tagged (`{"backend":"s3",...}`) | Untagged picks a variant by trial deserialization, so slice C's credential choice would depend on which shape parses first and errors degrade to `data did not match any variant` — not a corner to cut on a credentials path. Internally tagged costs the same churn as externally tagged with no gain. Landing the tag in this slice isolates the churn where every non-`storage` byte staying identical is itself the proof |
| Three methods on the enum, one engine-side function | Four methods on the enum; an engine-side extension trait with one impl | `object_store`/`datafusion` are forbidden dependencies of the catalog crate. A one-impl trait is the "interface with one implementation" red flag; a plain function with one match arm is smaller |
| `catalog_storage_props` is the single key map | Keep `build_s3_file_io`'s and `build_rest_catalog`'s mappings separate | The two are provably identical — same six keys, same conditions — so keeping both re-declares one decision twice |
| `resolve_vended_storage` keeps its name, changes its type | New method `StorageBackend::resolve_vended`; leave vending on `StorageProps` and wrap at the call site | Keeping the name preserves the recorded "exactly ONE function owns the whole sequence" contract and the crate's public name set. Wrapping at the call site would put a variant construction back in the planning layer — the knowledge this slice removes |
| `extract_bucket` deleted; `extract_bucket_from_files` demoted to the S3 arm | Keep both and add a backend branch beside them | `extract_bucket`'s `url.host()` is wrong for `abfss://`, not merely S3-specific, so it must not survive as a sibling to a backend-aware path |
| `storage_block` takes `allow_http` | Add a payload mutator so `resolve_connection_config` can keep patching it | Reaching into the payload to finish construction is exactly the knowledge being removed; a parameter is also the smaller diff |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| storage-backend-enum | NEW | `vs-adapter/storage-backend-enum/spec.md` |
| catalog-crate-structure | CHANGED | `vs-adapter/catalog-crate-structure/spec.md` |
| pushdown-planning-cloud-credentials | CHANGED | `vs-adapter/pushdown-planning-cloud-credentials/spec.md` |
| scan-execution-spec-reconstitution | CHANGED | `datafusion-scan/scan-execution-spec-reconstitution/spec.md` |
| scan-execution-memory-and-credentials | CHANGED | `datafusion-scan/scan-execution-memory-and-credentials/spec.md` |
| pushdown-module-structure | CHANGED | `vs-adapter/pushdown-module-structure/spec.md` |
| pushdown-col-types-consolidation | CHANGED | `vs-adapter/pushdown-col-types-consolidation/spec.md` |
| pushdown-joins-module-structure | CHANGED | `vs-adapter/pushdown-joins-module-structure/spec.md` |
| pushdown-catalog-session | CHANGED | `vs-adapter/pushdown-catalog-session/spec.md` |

Five features normatively require the `dispatch_golden` goldens, the join golden-SQL full-string assertions, or the per-shard scan-spec `storage` itself to be byte-identical to the pre-refactor output. This plan edits the `storage` value of five goldens and three golden strings, so **15 clauses** across those five carry the same `storage` carve-out `scan-execution-spec-reconstitution`'s delta already uses: 7 in `pushdown-module-structure` (one Background bullet plus one clause in each of six scenarios), 3 each in `pushdown-col-types-consolidation` and `pushdown-catalog-session`, and 1 each in `pushdown-joins-module-structure` and `catalog-crate-structure`. The gate is narrowed, not retired: an edit to any other byte still fails it.

## Impact

None for operators. No property, no CONNECTION field, no SQL surface, and no query result changes; the S3 read path is behavior-identical.

One internal wire change: the scan-driving SQL literal's `storage` value becomes `{"s3":{...}}` instead of `{...}`. `datafusion-scan/scan-execution-spec-reconstitution` already records that the same `.so` produces and consumes that literal with no cross-version wire-compatibility requirement, so the upload needs no coordinated step. The one caveat: a query planned by the pre-upload `.so` and executed after it fails on the tag, because plan and scan are separate statements.

The `lakehouse-catalog` public surface loses `build_s3_file_io` and gains `StorageBackend` with two `pub` methods (`secret_values`, `file_io`) plus the crate-private `catalog_storage_props`. The crate has no out-of-repo consumer, so this breaks nothing outside the workspace.

## Dependencies

Depends on issue #267 (catalog-crate extraction), merged and current on `main` — this plan targets the post-#267 layout. Blocks slice C (Azure variant addition).

## Migration

| Current | New |
|---------|-----|
| `"storage":{"endpoint":...,"path_style":true}` in the common blob | `"storage":{"s3":{"endpoint":...,"path_style":true}}` — payload bytes unchanged |
| `build_s3_file_io(&storage)` | `backend.file_io()` |
| `build_rest_catalog`'s six inline `props.insert` calls | `props.extend(backend.catalog_storage_props())` |
| `extract_bucket(spec)` + `build_s3_store(..)` + `register_bucket_store(..)` | one `register_side_store(ctx, backend, files, table_root, ..)` per side |
| `storage_block(creds)` then `storage.allow_http = ..` | `storage_block(creds, allow_http)` |
| `register_bucket_store(..) -> Result<(), UdfError>` | `register_side_store(..) -> Result<Option<Url>, UdfError>` |
| `storage: StorageProps` / `-> StorageProps` on the 18 sites below | `StorageBackend` |

The 18 sites, by lane — the seventeen production sites `vs-adapter/storage-backend-enum` scenario 2 enumerates, plus the `minimal_spec` test fixture. Catalog crate: `resolve_vended_storage` (`vended.rs`), `build_rest_catalog` (`session.rs`), `list_namespace_tables` (`namespace.rs:48`), `list_namespace_tables_unsigned` (`namespace.rs:75`). Adapter: `storage_block` (`adapter/connection.rs`), `resolve_connection_config`, `handle_pushdown_request`, `redact_error` (`adapter/mod.rs:363` and neighbours), `handle_pushdown` (`adapter/pushdown/mod.rs:105` — a different function from `handle_pushdown_request`), `build_dispatch_sql` (`adapter/pushdown/mod.rs:290`), `resolve_file_list` (`adapter/pushdown/file_resolution.rs`), `plan_join` (`adapter/pushdown/joins/mod.rs:102`), `resolve_one_join_side` (`adapter/pushdown/joins/planning.rs:330`), the `pub` FIELD `ResolvedJoinSide::effective_storage` (`joins/planning.rs:208`) plus `ResolvedJoinSide::new`'s matching parameter (`joins/planning.rs:224`), consumed at `joins/sql_builders.rs:505`. Scan: `CommonScanSpec.storage` and its `impl Default` initializer (`scan/spec.rs:607`, `:689`), `register_file_list` (`scan/raw_scan.rs:173`, called at `raw_scan.rs:128` and `join_scan.rs:98,109`), `PositionalDeleteScanTable::new` (`scan/positional_deletes.rs:542`), and `minimal_spec` (`scan/test_support.rs:26`).

## Implementation Tasks

1. Declare the enum in the catalog crate
   - [ ] 1.1 Add `crates/lakehouse-catalog/src/storage.rs` declaring `StorageBackend { S3(StorageProps) }` (externally tagged, lowercase variant key; deriving `Debug, Clone, PartialEq, Eq, Serialize, Deserialize`) with `pub secret_values`, crate-private `catalog_storage_props`, and `pub file_io`; move `build_s3_file_io`'s body in as `file_io` built by folding `catalog_storage_props` into `FileIOBuilder`; delete `build_s3_file_io`; update `lib.rs`'s `mod` list and `pub use` set and `iceberg_io.rs`'s module doc, which now owns one primitive; add the `pub use` re-export of `StorageBackend` in `crates/lakehouse-engine/src/scan/spec.rs` so all three Group-B lanes can import it from the path the spec pins [expert]
   - [ ] 1.2 Repoint `build_rest_catalog` (`session.rs`) onto `catalog_storage_props`, deleting its six inline S3 inserts; change `build_rest_catalog`, `list_namespace_tables`, and `list_namespace_tables_unsigned` to take `&StorageBackend`
   - [ ] 1.3 Change `resolve_vended_storage` (`vended.rs`) to take and return `StorageBackend`, keeping `select_credential_source` and `merge_vended_into_storage` bodies verbatim in the S3 arm; add the test-only payload unwrapper and backend fixture to `test_support.rs` so all 10 vended assertions stay verbatim
   - [ ] 1.4 Update `crates/lakehouse-catalog/tests/catalog_public_surface.rs`: import `StorageBackend`, drop `build_s3_file_io` from the reachability list, and extend the existing negative-assertion loop (`catalog_public_surface.rs:52`, today `["pub fn merge_vended_into_storage", "pub fn select_credential_source"]`) to the four-item list `vs-adapter/catalog-crate-structure` pins — adding `pub fn extract_vended_keys` and `pub fn build_s3_file_io`; `select_credential_source` is already asserted and needs no change. Reference `StorageBackend::secret_values` and `StorageBackend::file_io` from the reachability probe — not just the type — so narrowing either method below `pub` is a build failure rather than a silent gap. Do NOT add `catalog_storage_props` to the reachability list: it is crate-private

2. Thread the backend through the adapter
   - [ ] 2.1 Change `storage_block` to `storage_block(creds, allow_http) -> StorageBackend`; drop `resolve_connection_config`'s post-hoc `allow_http` mutation; change `resolve_connection_config`, `handle_pushdown_request`, and `redact_error` to carry `StorageBackend`; update `storage_block_maps_creds_to_storage_props`
   - [ ] 2.2 Change `resolve_file_list` to take `&StorageBackend` and return `StorageBackend`; replace `build_s3_file_io(&effective_storage)` with `effective_storage.file_io()` in `file_resolution.rs`
   - [ ] 2.3 Change `adapter/pushdown/test_support.rs`'s `sample_storage()` to return `StorageBackend`; update `crates/lakehouse-engine/tests/catalog_session_signatures.rs`
   - [ ] 2.4 Thread the backend through the dispatch and join paths: change `handle_pushdown` (`pushdown/mod.rs:105`) and `build_dispatch_sql` (`pushdown/mod.rs:290`) to take `&StorageBackend`; change `plan_join` (`joins/mod.rs:102`) and `resolve_one_join_side` (`joins/planning.rs:330`) likewise; change the `pub` field `ResolvedJoinSide::effective_storage` (`joins/planning.rs:208`) and `ResolvedJoinSide::new`'s matching parameter (`joins/planning.rs:224`) to `StorageBackend`, updating its consumer at `joins/sql_builders.rs:505` and the `sides.dimension.effective_storage` assertion at `joins/planning.rs:684`; wrap the in-`src` `#[cfg(test)]` `StorageProps` construction at `adapter/pushdown/mod.rs:797` (`catalog_auth_secrets_never_in_scan_spec_with_vending`'s `vended_storage`), which the type change compile-forces

3. Make the scan backend-agnostic
   - [ ] 3.1 Change `CommonScanSpec.storage` to `StorageBackend` (`scan/spec.rs:607`) and rewrap the `storage:` initializer in the manual `impl Default for CommonScanSpec` (`scan/spec.rs:689`) — a production `impl`, not `#[cfg(test)]`-gated, and permitted by `vs-adapter/storage-backend-enum` scenario 2 as a construction site that selects nothing; wrap the in-module `#[cfg(test)]` `storage: StorageProps {` construction at `scan/spec.rs:853`, which the type change compile-forces; update the scan-spec JSON test fixtures and the `common_blob_wire_is_byte_stable` golden to the tagged shape; add a wrapper-encoding pin beside the unedited `storage_props_wire_encoding_unchanged`
   - [ ] 3.2 Replace `build_session_context`'s bucket-aware registration with one backend-dispatching `register_side_store` returning `Result<Option<Url>, UdfError>` (`Some(url)` = registered, `None` = key already present); delete `extract_bucket`; delete `build_s3_store` as a named function, its body becoming the S3 arm; demote `extract_bucket_from_files` to the S3 arm's private derivation; preserve the sized-HEAD wrapper, the `s3_max_connections` budget, the `!join.files.is_empty()` dimension-side guard (`object_store.rs:65-67`) VERBATIM, and the dual-bucket skip without the call site comparing buckets; leave `build_spec_size_index` (`object_store.rs:140`) UNEDITED and keep passing its ONE whole-spec result — which indexes `spec.files` AND `join.files` into a single map — to BOTH registration calls, and do NOT derive a per-side size map from the new per-side `files` parameter: the shared-bucket case registers only the fact store, which must answer the dimension files' HEADs from that same map, so narrowing it silently breaks `datafusion-scan/scan-execution-file-metadata`'s no-per-file-HEAD guarantee; convert `register_file_list` (`raw_scan.rs:173`) and `PositionalDeleteScanTable::new` (`positional_deletes.rs:542`) to take `&StorageBackend`, updating the `&spec.common.storage` call sites at `raw_scan.rs:128` and `join_scan.rs:98,109`; wrap `minimal_spec`'s `storage:` initializer (`src/scan/test_support.rs:26`), the fixture every repointed `object_store.rs` unit test uses; leave `register_file_list`'s own `ListingTableUrl::object_store()` store-key derivation (`raw_scan.rs:181-185`) and `validate_uniform_object_store_files` UNEDITED — unifying the two store-key derivations is deferred to slice C (decision-log [9]); repoint `build_s3_store_applies_spec_connection_budget` at the new seam [expert]
   - [ ] 3.3 Update the five `dispatch_golden` `.sql` fixtures that embed a `storage` value and the join golden-SQL full-string assertions in `joins/sql_builders.rs`, changing the `storage` value only

4. Wrap the remaining test fixtures
   - [ ] 4.1 Change these six `tests/` storage helpers to return `StorageBackend`: `local_stack_storage` (`common/e2e_harness.rs:337`), `test_storage` (`scan_plan_shape.rs:606`), `dummy_storage` (`scan_name_mapping.rs:97`), `dummy_storage` (`scan_no_head_test.rs:246`), `dummy_storage` (`scan_positional_deletes.rs:108`), and `storage` (`scan_join_test.rs:166`). Wrap the inline `storage: StorageProps { .. }` spec constructions in `scan_two_arg.rs:141,267`, `scan_batch_loop.rs:152,199`, `scan_agg_projection_pruning.rs:97`, `scan_plan_shape.rs:69,381`, `scan_parquet_pruning.rs:75`, `micro_bench.rs:300`, and `scan_telemetry.rs:136`. `src/scan/test_support.rs::minimal_spec` is covered by task 3.2 and MUST NOT be wrapped twice

5. Gate
   - [ ] 5.1 Run `cargo test`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo fmt --check`
   - [ ] 5.2 Prerequisite: bring up the `spark-iceberg-fixtures` one-shot provisioning job (`docker-compose.yml:89`, `scripts/spark-fixtures/run_fixtures.sh`) and confirm its Iceberg fixtures are present, because `make test-e2e` runs `e2e_int96_timestamp_test` and `e2e_positional_deletes_test` against them and an unprovisioned environment fails with "object not found" rather than skipping — indistinguishable from a refactor regression unless the fixtures are verified first. Then run `make cross-musl-udf-build` and `make test-e2e` against the freshly built `.so`

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1 |
| Group B | 1.2, 1.3, 1.4 \| 2.1, 2.2, 2.3, 2.4 \| 3.1, 3.2 |
| Group C | 3.3, 4.1 |
| Group D | 5.1, 5.2 |

Sequential dependencies:
- Group A → Group B (every other task names `StorageBackend`, and 1.1 also lands the `scan::spec` re-export all three lanes import)
- Group B → Group C (the goldens and fixtures can only be regenerated once the code compiles)
- Group C → Group D
- 5.1 → 5.2 (do not build the `.so` for E2E until the host suites are green)

Group B's three lanes were re-checked file by file against the corrected 18-site list and remain disjoint:
- catalog lane (1.2-1.4): `session.rs`, `namespace.rs`, `vended.rs`, `src/test_support.rs`, `tests/catalog_public_surface.rs`
- adapter lane (2.1-2.4): `adapter/connection.rs`, `adapter/mod.rs`, `pushdown/mod.rs`, `pushdown/file_resolution.rs`, `pushdown/joins/mod.rs`, `pushdown/joins/planning.rs`, `pushdown/joins/sql_builders.rs`, `adapter/pushdown/test_support.rs`, `tests/catalog_session_signatures.rs`
- scan lane (3.1-3.2): `scan/spec.rs`, `scan/object_store.rs`, `scan/raw_scan.rs`, `scan/join_scan.rs`, `scan/positional_deletes.rs`, `src/scan/test_support.rs`

Two overlaps exist and are both ordered, not concurrent: `pushdown/joins/sql_builders.rs` is edited by 2.4 (the `effective_storage` consumer at line 505) and again by 3.3 (the golden strings at 1927, 1965, 2008), which is a later group; and `scan/spec.rs`'s `StorageBackend` re-export moved into 1.1 (Group A) so the adapter and catalog lanes do not wait on the scan lane for their import path.

**Group B's three lanes land as one atomic unit and have NO per-lane verification gate.** `cargo check` is expected to FAIL inside every lane until all three land, because each lane changes signatures the other two call. Neither the failing-test-first cycle nor any lane-local `cargo test` can run mid-group; the first green signal is the whole-workspace build after the last lane merges, and a failure there surfaces as a workspace compile error with no lane-local bisect. Task 3.2 is `[expert]` for exactly this reason: it carries the largest single-checkbox edit set in the group.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `build_s3_file_io`, `crates/lakehouse-catalog/src/iceberg_io.rs` | Replaced by `StorageBackend::file_io`; retaining it would leave two public names for one operation |
| Function | `extract_bucket`, `crates/lakehouse-engine/src/scan/object_store.rs` | Derives the bucket via `Url::host_str()`, which returns the storage account on `abfss://`; unsalvageable for a second backend |
| Function | `build_s3_store`, `crates/lakehouse-engine/src/scan/object_store.rs` | Body becomes the S3 arm of `register_side_store` |
| Function | `register_bucket_store`, `crates/lakehouse-engine/src/scan/object_store.rs` | Folded into `register_side_store`; a bucket-parameterized registrar is the S3 knowledge being removed |
| Statement | six inline `props.insert` S3 keys in `build_rest_catalog`, `crates/lakehouse-catalog/src/session.rs` | Replaced by `catalog_storage_props`, the single key map |
| Statement | `storage.allow_http = ..` in `resolve_connection_config`, `crates/lakehouse-engine/src/adapter/mod.rs` | `storage_block` now takes `allow_http`, so no post-construction patch is needed |

No test is deleted. `build_s3_store_applies_spec_connection_budget` is repointed at the new seam rather than removed.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| One enum names the storage backend and answers every backend-specific question | Unit | `crates/lakehouse-catalog/src/storage.rs` | `catalog_storage_props_carries_the_six_s3_keys_only_when_present` |
| One enum names the storage backend and answers every backend-specific question | Unit | `crates/lakehouse-catalog/src/storage.rs` | `secret_values_matches_the_wrapped_props_values_and_order` |
| One enum names the storage backend and answers every backend-specific question | Unit | `crates/lakehouse-catalog/src/storage.rs` | `file_io_is_built_from_the_same_key_map_as_catalog_storage_props` |
| Every consumer holds a backend and no consumer names one | Integration | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | compile-time reachability probe: `StorageBackend` imported, `StorageBackend::secret_values` and `StorageBackend::file_io` both referenced (so narrowing either below `pub` fails the build), `build_s3_file_io` absent |
| Every consumer holds a backend and no consumer names one | Integration | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `demoted_and_deleted_functions_are_not_declared_public` |
| Every consumer holds a backend and no consumer names one | Integration | `crates/lakehouse-engine/tests/catalog_session_signatures.rs` | `file_resolution_entry_points_take_a_shared_session` |
| Every consumer holds a backend and no consumer names one | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `storage_block_maps_creds_to_storage_props` |
| The scan registers its object store without naming the backend | Unit | `crates/lakehouse-engine/src/scan/object_store.rs` | `build_s3_store_applies_spec_connection_budget` (repointed: a non-default `s3_max_connections` still registers and returns `Some(url)`) |
| The scan registers its object store without naming the backend | Unit | `crates/lakehouse-engine/src/scan/object_store.rs` | `register_side_store_registers_one_store_per_distinct_side` (two distinct `Some(url)`) |
| The scan registers its object store without naming the backend | Unit | `crates/lakehouse-engine/src/scan/object_store.rs` | `join_dimension_side_sharing_the_fact_bucket_is_not_registered_twice` (asserts the second call returns `Ok(None)` — fails if the skip is lost) |
| The scan registers its object store without naming the backend | Unit | `crates/lakehouse-engine/src/scan/object_store.rs` | `join_with_empty_dimension_file_list_registers_only_the_fact_side` |
| The scan registers its object store without naming the backend | Unit | `crates/lakehouse-engine/src/scan/object_store.rs` | `shared_bucket_join_store_answers_both_sides_sizes_from_the_spec` — asserts the registered store's size map contains a DIMENSION-side file key, so deriving `sizes` from the per-side `files` parameter fails it. This is the ONLY test pinning the whole-spec scope of the size index |
| The scan registers its object store without naming the backend | Integration | `crates/lakehouse-engine/tests/scan_no_head_test.rs` | existing sized-HEAD suite, passing with wrapped fixtures. It does NOT exercise the S3 registration path for a join: it builds only `raw_spec(..)` and carries no join spec, so it cannot cover the shared-bucket size-map scope |
| The scan registers its object store without naming the backend | Integration | `crates/lakehouse-engine/tests/scan_join_test.rs` | existing join suite, passing with wrapped fixtures. It does NOT reach S3 registration at all: its fixtures are `file://` only, and `extract_bucket_from_files` errors on a `file://` first entry, so it covers the join SQL and scan shape rather than store registration |
| The scan-spec wire carries the backend as a tagged variant | Unit | `crates/lakehouse-engine/src/scan/spec.rs` | `common_blob_wire_is_byte_stable` |
| The scan-spec wire carries the backend as a tagged variant | Integration | `crates/lakehouse-engine/tests/shared_type_reexports.rs` | `storage_props_wire_encoding_unchanged` (UNEDITED) |
| The scan-spec wire carries the backend as a tagged variant | Integration | `crates/lakehouse-engine/tests/shared_type_reexports.rs` | `storage_backend_wire_encoding_tags_the_s3_payload` |
| The scan-spec wire carries the backend as a tagged variant | Unit | `crates/lakehouse-engine/src/scan/spec.rs` | `from_json_error_never_contains_credentials` (existing malformed-JSON redaction test) |
| S3 behavior is unchanged across the refactor | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | all 10 golden fixtures |
| S3 behavior is unchanged across the refactor | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | join golden-SQL full-string assertions |
| S3 behavior is unchanged across the refactor | Unit | `crates/lakehouse-catalog/src/vended.rs` | all 10 vended resolution tests |
| S3 behavior is unchanged across the refactor | Integration | `crates/lakehouse-engine/tests/` (E2E harness) | `make test-e2e` full S3 suite, against the spark-iceberg-fixtures-provisioned environment |
| Golden fixtures change ONLY in their `storage` value (pushdown-module-structure) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | `group_by_fallback` and `multi_count_distinct_decline` decline-wrapper assertions |
| Golden fixtures change ONLY in their `storage` value (pushdown-col-types-consolidation) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | the leaf, non-`column`, nameless-column, and unresolvable-column guard tests, UNEDITED (they embed no scan spec) |
| Golden strings change ONLY in their `storage` value (pushdown-joins-module-structure) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | the three spec-bearing golden strings, plus `ineligible_join_decline`'s message assertion UNEDITED |
| Golden fixtures change ONLY in their `storage` value (catalog-crate-structure) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | all 10 golden fixtures, verified by the `git diff` command below |
| The per-shard scan-spec storage changes ONLY by its variant tag (pushdown-catalog-session) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | the five spec-bearing goldens, whose per-shard `storage` value is the one this feature's byte-identical clause names; the grant-count and `loadTable`-count assertions are UNEDITED |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| storage-backend-enum | `cargo test -p lakehouse-catalog` | 0 failures; the public-surface probe compiles with `StorageBackend` and without `build_s3_file_io` |
| storage-backend-enum | `git diff -- crates/lakehouse-engine/src/adapter/pushdown/testdata/dispatch_golden/` | Every changed line differs ONLY inside its `storage` value, which reads `"storage":{"s3":{...}}` |
| storage-backend-enum | `rg -n 'StorageBackend::S3' crates/lakehouse-engine/src crates/lakehouse-catalog/src` | Matches only in the five permitted production modules — `storage.rs`, `vended.rs` (S3 arm), `object_store.rs` (registration), `connection.rs` (the single selection-from-input site), and `scan/spec.rs` (the `impl Default` placeholder initializer, which selects nothing) — plus `#[cfg(test)]` code: each crate's `test_support.rs` and the in-module test constructions at `scan/spec.rs:853` and `adapter/pushdown/mod.rs:797`. No other production module names the variant |
| storage-backend-enum | `rg -n 'extract_bucket\b|build_s3_store\b|build_s3_file_io' crates/` | No matches. Plain `|` alternation, not the escaped `\|` literal — `rg`'s Rust regex reads `\|` as a literal pipe, which matches nothing and makes the check vacuous. The `\b` anchors correctly exclude the surviving `extract_bucket_from_files` and `build_s3_store_applies_spec_connection_budget` |
| catalog-crate-structure | `cargo test -p lakehouse-catalog --test catalog_public_surface` | 0 failures |
| scan-execution-spec-reconstitution | `cargo test -p lakehouse-engine common_blob_wire_is_byte_stable storage_props_wire_encoding_unchanged` | 0 failures; `storage_props_wire_encoding_unchanged` passes with no source edit |
| scan-execution-memory-and-credentials | `EXASOL_CONTAINER=lakehouse-engine-rs-2-exasol-1 make test-e2e` | 0 failures against a `.so` built this run |
| pushdown-planning-cloud-credentials | `cargo test -p lakehouse-catalog vended` | 0 failures with every pre-refactor assertion byte-identical |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E fixtures | `docker compose up spark-iceberg-fixtures` (one-shot) then list the int96 and positional-delete tables through the catalog | Both fixture sets present BEFORE `make test-e2e`, so an "object not found" failure is never mistaken for a refactor regression |
| Test (E2E) | `EXASOL_CONTAINER=lakehouse-engine-rs-2-exasol-1 make test-e2e` | 0 failures |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
