# Tasks: change-name-mapping-fallback

## Phase 2: Implementation (Group A)
- [x] 1.1 Add `NameMappingEntry { name: String, field_id: i32 }` (serde) to `scan/spec.rs`; add `name_mapping: Vec<NameMappingEntry>` (`#[serde(default, skip_serializing_if = "Vec::is_empty")]`) to `ScanSpec`, `CommonScanSpec`, and `JoinSpec`; copy it in `ScanSpec::to_common`, `ScanSpec::from_parts`, and the join-side builders that already clone `logical_schema`.

## Phase 2: Implementation (Group B, after A)
- [x] 2.1 In `resolve_file_list` (`adapter/pushdown.rs`), read `table.metadata().properties().get(iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING)`; when present, parse via `serde_json::from_str::<iceberg::spec::NameMapping>` and flatten top-level entries to `Vec<NameMappingEntry>` (one per name; skip entries with no `field_id`; do NOT recurse into nested `fields`); on parse failure return a clean, credential-free plan-time `UdfError`. Extend the return tuple and thread the value into the built `ScanSpec` and into `resolve_one_join_side` for the dimension side. [expert]
- [x] 3.1 Thread `name_mapping` from `ScanSpec` through `register_files` → `PositionalDeleteScanTable::new` → `FieldIdExprAdapterFactory` (add a field holding the parsed mapping / a `HashMap<&str,i32>` built once per file open), mirroring how `use_field_id_adapter`/`logical_schema` already flow; do the same for the join dimension side's `register_files` call.
- [x] 5.1 In `rename_physical_to_logical`'s doc comment, replace the claim that drop+rename-into-a-reused-name collisions "belong to the name-mapping work tracked in issue #28" with an accurate note: `schema.name-mapping.default` maps current-state physical names to field-ids and cannot disambiguate a dropped column whose old physical name was later reused, so this collision is a distinct, still-open concern unrelated to (and not resolved by) name-mapping support.

## Phase 2: Implementation (Group C, after B)
- [x] 3.2 Rewire `rename_physical_to_logical` to accept the name-mapping and insert the name-mapping resolution step: for a physical field with NO embedded field-id, look its physical name up in the name-mapping to obtain a field-id, and if that field-id is in `logical_name_by_id` adopt that logical name; otherwise keep the existing physical-name fallback. Embedded field-id resolution stays strictly first. [expert]

## Phase 2: Implementation (Group D, tests, after their code)
- [x] 1.2 Unit test in `scan/spec.rs`: `name_mapping` round-trips through `ScanSpec`/`CommonScanSpec` JSON when populated, is absent from JSON when empty, and a legacy payload lacking the field deserializes to empty (backward-compat), mirroring `logical_schema_round_trips_and_defaults_to_empty`.
- [x] 2.2 Unit test in `adapter/pushdown.rs`: a representative property JSON (multi-name entries + a nested `fields` entry + an entry with no `field-id`) flattens to the expected top-level `{name, field_id}` set with nested/id-less entries excluded; an absent property yields an empty mapping; malformed JSON yields a clean error.
- [x] 3.3 Unit tests in the `field_id_adapter` mod of `scan/mod.rs`: (a) a no-field-id physical field resolves via name-mapping to its logical name; (b) an embedded field-id wins over a conflicting name-mapping entry; (c) name-mapping absent → physical-name identity preserved; (d) name-mapping present but not covering a field → physical-name identity preserved.
- [x] 4.1 Add `tests/scan_name_mapping.rs` (reusing the `scan_no_head_test.rs` harness: local `file://` Parquet via `ArrowWriter`, `run_raw_scan_with_session`, Arrow-batch decode). Write a Parquet file whose column carries NO `PARQUET:field_id` and whose physical name differs from the current logical name; build a `ScanSpec` with `logical_schema` + `name_mapping` mapping the physical name to the field-id; assert the renamed column emits real values (never NULL) under the logical name. Add a companion case with an empty `name_mapping` asserting the physical-name fallback still binds.

## Phase 3: Verification
- [x] 6.1 Run full test suite (`cargo test`) — 562 passed, 0 failed, 2 ignored
- [x] 6.2 Run linter (`cargo clippy --all-targets`) — no issues found
- [x] 6.3 Run formatter check (`cargo fmt --check`) — clean
- [x] 6.4 Build UDF `.so` (`make cross-musl-udf-build`) — 163.1M, exit 0
