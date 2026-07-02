# Verification Report: change-scan-spec-files-payload

## Bottom Line

**PASS.** All implementation tasks (Groups A–E) and the one code-review finding (R.1) are
complete and verified. Build, host test suite (all targets), clippy, format, and the live-DB E2E
suite are all green. The feature is ready to record and ship.

| Check | Command | Result |
|-------|---------|--------|
| Build (cross-musl `.so`) | `make cross-musl-udf-build` | ✅ exit 0 (166 MB `liblakehouse_engine.so`) |
| Host tests (all targets) | `cargo test -p lakehouse-engine --all-targets` | ✅ 340 lib + 13 integration, 0 failures |
| E2E (live Exasol+MinIO+Iceberg REST) | `make test-e2e` | ✅ 7 capability + 33 scan, 0 failures |
| Lint | `cargo clippy -p lakehouse-engine --all-targets` | ✅ 0 warnings |
| Format | `cargo fmt --check` | ✅ no changes |

## What was implemented

The per-shard `ScanSpec.files` payload was reshaped from bare absolute URI strings to compact
`(relative-or-absolute-path, byte-size)` 2-tuples, and the Iceberg table root is now carried ONCE
in the shard-invariant common spec. This shrinks the fan-out SQL (#45) and lets the scan UDF build
each file's `ObjectMeta` from the spec instead of a redundant per-file object-store HEAD (#29).

| Group | Scope | Key outcome |
|-------|-------|-------------|
| A | `scan/spec.rs` | `files: Vec<(String,u64)>`; `table_root` on both specs (`#[serde(default)]`, skip-if-empty); credential-safe redacted deserializers |
| B | `adapter/sharding.rs` | `partition_files_by_bytes -> Vec<Vec<(String,u64)>>`; LPT balancing / 0→1 rule / clamp / disjoint-cover unchanged; true size preserved (0 stays 0) |
| C | `adapter/pushdown.rs` | `resolve_file_list` returns `table_root`; `relativize_shards_to_root` strips at a real segment boundary; retyped SQL builders; root serialized once |
| D | `scan/mod.rs` | `reconstruct_abs_uri` (`://` passthrough, else join on root); `SpecSizedObjectStore` intercepts `get_opts(head:true)` to skip network HEAD; `extract_bucket` handles relative first entry; field-id projection untouched |
| E | `tests/` | New `scan_no_head_test.rs` (proves 0 HEADs forwarded on spec-size path; relative≡absolute rows); migrated existing fixtures; E2E `scan_registers_assigned_files_with_path_size_payload` |

## Scenario coverage (plan Verification § cross-referenced)

| Scenario | Test | Status |
|----------|------|--------|
| Pushdown carries root + sizes in common and shards | `pushdown_carries_table_root_and_sizes_in_common_and_shards` | ✅ |
| Root carried once, under-root paths relative | `table_root_stripped_from_under_root_paths_and_carried_once` | ✅ |
| Path not under root stays absolute | `path_not_under_root_stays_absolute` | ✅ |
| Sibling non-`/`-boundary path stays absolute (R.1 regression) | `sibling_prefix_paths_are_not_relativized` | ✅ |
| Sharding propagates size into shards | `partition_by_bytes_propagates_size_into_shards` | ✅ |
| Fan-out carries root once + `(path,size)` per shard | `fan_out_carries_root_once_and_path_size_tuples_per_shard` | ✅ |
| Reconstitute files tuples + table_root | `from_parts_reconstitutes_files_tuples_and_table_root` | ✅ |
| Legacy empty root ⇒ absolute paths | `legacy_empty_root_treats_paths_as_absolute` | ✅ |
| Malformed JSON never leaks credentials | `malformed_common_or_files_json_does_not_leak_credentials` | ✅ |
| Spec size used, 0 HEADs issued | `scan_uses_spec_size_and_issues_no_head` | ✅ |
| Relative & absolute entries resolve to same files | `relative_and_absolute_entries_resolve_to_same_files` | ✅ |
| Multi-file scan through VS returns correct rows | `scan_registers_assigned_files_with_path_size_payload` (E2E) | ✅ |
| Field-id projection preserved | `field_id_adapter_reads_renamed_column_rows`, `..._divergent_layouts_across_files` | ✅ |

## Code review

One medium-severity correctness defect found and fixed:

- **R.1 — strip/reconstruct non-`/`-boundary asymmetry (FIXED).** `relativize_path_to_root`
  stripped `table_root` on a bare string-prefix match, corrupting sibling paths like
  `{root}-archive/…` on round-trip (violating the plan's `write.data.path`/migrated-layout
  correctness claim). Fixed to strip only at a real path-segment boundary (root ends with `/` or
  remainder begins with `/`); otherwise the path is emitted absolute. Regression test
  `sibling_prefix_paths_are_not_relativized` added (RED before, GREEN after). The `path == root`
  degenerate case (empty-entry smell) is subsumed by the same guard. `reconstruct_abs_uri`
  needed no change — the adapter now only ever emits a true under-root relative path or a full
  `://` absolute path.

Two minor observations were subsumed by the R.1 fix / judged non-defects (weak `>=1` HEAD-count
asserts are backed by an exact `==0` load-bearing assertion).

## Deviations from plan (for the decision log at record time)

- **object_store 0.13.2 `head` is not an `ObjectStore` trait method** — it is the auto-implemented
  `ObjectStoreExt` blanket that dispatches to `get_opts(GetOptions{head:true})`. The plan's
  "override `head()`" is therefore implemented by overriding `get_opts` for the `head:true` case
  (DF54 resolves an exact-file `ListingTableUrl` via `store.head(&prefix)`, uncached). Equivalent
  and correct; suppresses the network HEAD as intended.
- **Added `async-trait` to the crate manifest** — required to implement object_store's
  `#[async_trait] ObjectStore` trait (already resolved transitively at 0.1.x).

## Known pre-existing issue (NOT introduced by this work)

`scan::diagnostics::tests::format_record_contains_required_fields` is an occasional parallel
panic-hook race in the UNCHANGED `scan/diagnostics.rs` (passes single-threaded and in isolation;
passed in the final all-targets run). Out of scope for this plan.
