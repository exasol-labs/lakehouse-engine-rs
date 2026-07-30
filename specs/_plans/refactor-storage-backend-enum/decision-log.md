# Decision Log: refactor-storage-backend-enum

## Interview

**Q:** `StorageProps` has 7 fields (endpoint, region, access_key, secret_key, session_token, allow_http, path_style), its own `Default` impl, a `secret_values()` method, and 3 dedicated unit tests, plus ~15 call sites across the engine and catalog crates and tests. How should the new `StorageBackend` enum carry the S3 data — wrap the existing `StorageProps` struct as the variant's payload, or inline the 7 fields directly onto the variant?

**A:** Wrap the existing `StorageProps` (recommended) — `StorageBackend::S3(StorageProps)`. The struct itself, its `Default` impl, `secret_values()`, and its 3 unit tests stay completely untouched. Every `StorageProps { .. }` construction literal (in `adapter/connection.rs`'s `storage_block`, `catalog/vended.rs`'s two vending builders, and ~8 test helper functions across engine tests) stays as-is; only the places that hold, pass, or match a storage backend gain one `StorageBackend::S3(..)` wrapping layer. This is the smallest-diff choice and satisfies "move the S3-only build logic into the S3 arm" without touching the struct's internals.

## Design Decisions

### [1] Externally-tagged lowercase serde representation for the storage backend

- **Decision:** `StorageBackend` serializes externally tagged with a lowercase variant key, so the scan spec's `storage` value becomes `{"s3":{...}}` with the payload's own bytes unchanged. The tag lands in this slice, not in slice C.
- **Alternatives:** (a) `#[serde(untagged)]` — keeps the wire byte-identical and spares every golden fixture, the `common_blob_wire_is_byte_stable` string, the five `dispatch_golden` `.sql` files, the join golden-SQL assertions, and ~20 scan-spec JSON fixtures from any edit. (b) Internally tagged (`{"backend":"s3","endpoint":...}`) — costs the same churn as externally tagged.
- **Rationale:** Untagged was the tempting laziest option and was rejected on correctness. An untagged enum selects its variant by trial deserialization, so once slice C adds the Azure variant, which credentials get used depends on which shape happens to parse first, and a genuine field error degrades to `data did not match any variant`. A silent mis-pick on a credentials path is not a corner worth cutting to save fixture churn. Internally tagged buys nothing over externally tagged. Landing the tag here rather than in slice C is deliberate: this slice's entire content is churn, so every non-`storage` byte of every golden staying identical is itself the proof that nothing else moved — whereas in slice C the same churn would be indistinguishable from an Azure bug. The wire is safe to change because `datafusion-scan/scan-execution-spec-reconstitution` already records that the same `.so` produces and consumes the spec within one deploy, with no cross-version compatibility requirement.
- **Promotes to ADR:** yes

### [2] Three methods on the enum plus one engine-side dispatching function

- **Decision:** `StorageBackend` publishes `secret_values`, `catalog_storage_props`, and `file_io`. DataFusion object-store registration stays engine-side as one plain function in `crates/lakehouse-engine/src/scan/object_store.rs` that matches on the backend.
- **Alternatives:** Four methods on the enum, as issue #274's scope list literally reads; or an engine-side extension trait implemented for `StorageBackend`.
- **Rationale:** `vs-adapter/catalog-crate-structure` normatively forbids `lakehouse-catalog` from declaring `object_store` or `datafusion` as a direct dependency, so a `register_object_store` method on a catalog-crate type is not constructible. Issue #274's own scope text states the same split, so the four-method list is shorthand rather than a constraint. An extension trait with one implementation and one method is the "interface with one implementation" red flag from `/speq:design-philosophy`; a free function with one match arm is strictly smaller. The result is two owners of the backend decision rather than one, which the plan and spec state openly as boundary-forced — and the count still drops from four S3-aware engine sites to one.
- **Promotes to ADR:** yes

### [3] `catalog_storage_props` is a new single source for the six S3 config keys

- **Decision:** `catalog_storage_props` returns the six iceberg S3 config keys as one `HashMap<String, String>`; `file_io` folds that same map into `FileIOBuilder`, and `build_rest_catalog` merges it into its props map.
- **Alternatives:** Keep `build_s3_file_io`'s `with_prop` chain and `build_rest_catalog`'s `props.insert` block as separate mappings, moving each into the S3 arm unchanged.
- **Rationale:** The two mappings were read line by line and are provably identical: the same six keys (`S3_ENDPOINT`, `S3_REGION`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_SESSION_TOKEN`, `S3_PATH_STYLE_ACCESS`) under the same conditions — first four only when non-empty, session token only when `Some`, path-style always. Two modules independently encoding one S3 config shape is the back-door duplication `/speq:design-philosophy` names, and this is the slice whose whole purpose is giving that decision one home. A `HashMap` return is order-safe because both consumers perform keyed inserts. This is why `catalog_storage_props` is a genuinely new method rather than a rename of an existing one, and why task 1.1 is tagged `[expert]`.
- **Promotes to ADR:** no

### [4] `resolve_vended_storage` keeps its name and public status, changing only its type

- **Decision:** `resolve_vended_storage` takes and returns `StorageBackend`; `select_credential_source` and `merge_vended_into_storage` keep their bodies verbatim inside the S3 arm.
- **Alternatives:** (a) Replace it with a `StorageBackend::resolve_vended` method. (b) Leave it on `StorageProps` and wrap at the `file_resolution.rs` call site.
- **Rationale:** Vending is not in issue #274's four-method scope list, but the backend must flow through it: `file_resolution.rs` feeds the vended result straight into `file_io()` and into the scan spec, so leaving vending on `StorageProps` would put a `StorageBackend::S3(..)` construction back in the planning layer — precisely the knowledge this slice removes. Keeping the existing name and `pub` status is the smaller move: it preserves `vs-adapter/pushdown-planning-cloud-credentials`' recorded "exactly ONE function owns the whole sequence" contract and leaves the crate's public name set otherwise untouched, so its delta is a type change rather than a surface change. The field-for-field guarantee is preserved by construction, not re-verification, because the merge body is unedited.
- **Promotes to ADR:** no

### [5] `storage_block` takes `allow_http` instead of being patched afterwards

- **Decision:** `storage_block(creds, allow_http) -> StorageBackend`, and `resolve_connection_config` drops its `storage.allow_http = ..` assignment.
- **Alternatives:** Add a payload mutator or a `with_allow_http` builder method so the existing post-construction patch keeps working.
- **Rationale:** Reaching into the variant payload to finish construction is exactly the backend knowledge this slice removes from the adapter, and a mutator would put it on the enum's public surface permanently. Passing the value in is also the smaller diff — one parameter added, one statement deleted.
- **Promotes to ADR:** no

### [6] `extract_bucket` is deleted; `extract_bucket_from_files` survives inside the S3 arm

- **Decision:** `extract_bucket` is deleted outright. `extract_bucket_from_files` keeps its `Url::host_str()` logic but becomes the S3 arm's private derivation, unreachable from the backend-agnostic call site.
- **Alternatives:** Keep `extract_bucket` and add a backend branch beside it; or delete both and inline the host derivation into the S3 arm.
- **Rationale:** Issue #274 requires deleting `extract_bucket` for a reason stronger than tidiness: `url.host()` on an `abfss://` URI returns the storage account, not the container, so the function is wrong for Azure rather than merely S3-specific and must not survive as a sibling to a backend-aware path. `extract_bucket_from_files` carries the same logic but is legitimately correct *within* the S3 arm and is still needed for both a join's fact and dimension sides, so demoting it costs nothing and inlining it twice would duplicate it.
- **Promotes to ADR:** no

### [7] Scope fence: exactly one variant, and no `abfss://` handling

- **Decision:** `StorageBackend` gains only the `S3` variant. No Azure variant, no Azure credential fields, no `abfss://` URI handling, and no change to `ConnectionCreds` parsing.
- **Alternatives:** Add a stub Azure variant now so slice C is a fill-in; or add the URI-scheme detection slice C will need.
- **Rationale:** A stub variant is speculative structure with no caller and would fail this repo's `-D warnings` clippy gate or force placeholder code. It would also make the enum's dispatch observable in a slice whose whole claim is that nothing observable changed, destroying the S3-E2E-as-characterization-gate argument. Issue #274 declares Azure as slice C.
- **Promotes to ADR:** no

### [8] Iceberg specification compliance is recorded as unchanged, not skipped

- **Decision:** The plan records explicitly that no Iceberg-spec-relevant behavior changes, quoting the normative text the touched code implements, rather than skipping the CLAUDE.md compliance check because the change is a refactor.
- **Alternatives:** Note "pure refactor, no Iceberg impact" without citing the specification.
- **Rationale:** CLAUDE.md requires any plan touching scanning, pushdown, or schema/type handling to be checked against the Iceberg specification with the relevant normative section quoted rather than recalled. This plan edits `scan/object_store.rs` and the credential-vending path, so the check applies. The applicable text was read from `apache/iceberg` `open-api/rest-catalog-open-api.yaml` on `main`, not from memory: `StorageCredential.prefix` directs clients to "choose the most specific prefix (by selecting the longest prefix) if several credentials of the same type are available", and `LoadTableResult` states "Credentials for ADLS / GCS / S3 / ... are provided through the `storage-credentials` field. Clients must first check whether the respective credentials exist in the `storage-credentials` field before checking the `config` for credentials." `select_credential_source` and `merge_vended_into_storage` implement both rules and their bodies move into the S3 arm verbatim, so no deviation is introduced and none is fixed. Recording the check with its citation satisfies the rule; asserting no impact without one would not.
- **Promotes to ADR:** no

### [9] Unifying the two store-key derivations is deferred to slice C

- **Decision:** `register_side_store`'s S3 arm keeps deriving its store key from `Url::host_str()`, and `register_file_list` (`raw_scan.rs:181-185`) plus `validate_uniform_object_store_files` keep independently deriving theirs from `ListingTableUrl::parse(..)?.object_store()`. Neither consumes the other's key in this slice.
- **Alternatives:** Make the registration function the single owner of a side's store key and have `register_file_list` consume the `Url` it returns.
- **Rationale:** The two derivations agree for every `s3://` URI, because DataFusion's `get_url_key` reduces both to scheme + authority there. They diverge on `abfss://container@account.dfs.core.windows.net/…`, where the authority is `container@account…` and a registration keyed on a derived container name would not match a lookup keyed on `object_store()`. That divergence is a slice-C failure mode, not an S3 one. Unifying now means threading a key from `build_session_context` into `register_file_list`, whose two other callers are `raw_scan.rs:128` and `join_scan.rs:98,109` — a call-chain reshape across three files, on top of a slice already touching 17 sites, with nothing observable to gate it while only one variant exists. **The risk being accepted:** slice C must unify the derivation as its first move; adding an Azure arm on top of two independent derivations ships a registration/lookup mismatch that no S3 test can catch.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] The migration site list was short by eight production sites

- **Finding:** `plan-reviewer` round 1, `[EFFORT_MISESTIMATION]` BLOCKER. § Migration claimed "9 signatures"; changing `resolve_file_list` and `CommonScanSpec.storage` compile-forces eight further sites that appeared in no task and no table — `handle_pushdown` (a different function from `handle_pushdown_request`), `build_dispatch_sql`, `plan_join`, `resolve_one_join_side`, the `pub` FIELD `ResolvedJoinSide::effective_storage` plus `::new`'s parameter, `register_file_list`, `PositionalDeleteScanTable::new`, and `src/scan/test_support.rs::minimal_spec`. The real count is 17. The `[expert]` tagging and the Group B lane split had both been sized against the short list.
- **Direction change:** § Migration now enumerates all 17 sites with `file:line`, grouped by lane. A new task 2.4 owns the dispatch and join-path conversions; task 3.2 gained `register_file_list`, `PositionalDeleteScanTable::new`, and `minimal_spec`. `vs-adapter/storage-backend-enum` scenario 2's GIVEN was extended to the same 17. § Parallelization was re-derived file by file: the three lanes are still disjoint, and the two overlaps found (`joins/sql_builders.rs` between 2.4 and 3.3; `scan/spec.rs`'s re-export) are now ordered explicitly — the re-export moved into task 1.1 so no Group-B lane waits on another for its import path.
- **Promotes to ADR:** no

### [plan-review] Four live features pinned the goldens this plan edits, and none was amended

- **Finding:** `plan-reviewer` round 1, `[REQUIREMENT_CONFLICT]` BLOCKER. `vs-adapter/catalog-crate-structure:81`, `vs-adapter/pushdown-module-structure:79,162,184`, `vs-adapter/pushdown-col-types-consolidation:42`, and `vs-adapter/pushdown-joins-module-structure:41` each normatively require the exact `dispatch_golden` goldens and join golden-SQL assertions this plan edits to pass UNEDITED. Only `scan-execution-spec-reconstitution` had been amended; `catalog-crate-structure`'s delta Background even claimed "every other scenario of this feature is unchanged" while leaving its own byte-identical-SQL clause intact. Recording the plan as-is would have left the library self-contradictory and silently retired the repo's only cross-refactor behavior-preservation gate.
- **Direction change:** Three new delta files (`vs-adapter/pushdown-module-structure`, `vs-adapter/pushdown-col-types-consolidation`, `vs-adapter/pushdown-joins-module-structure`) and one added scenario in the existing `catalog-crate-structure` delta each carve out the `storage` value exactly as `scan-execution-spec-reconstitution`'s delta does. `catalog-crate-structure`'s Background bullet was corrected from two amended clauses to three amended scenarios. All four features are now § Features rows marked CHANGED, with Scenario Coverage rows. The gate is narrowed, not retired: every carve-out permits an edit to the `storage` value ALONE, so an edit to any other byte still fails.
- **Promotes to ADR:** yes

### [plan-review] The "no module names a variant" clause forbade the plan's own design

- **Finding:** `plan-reviewer` round 1, `[REQUIREMENT_CONFLICT]` BLOCKER. `vs-adapter/storage-backend-enum` scenario 2 line 41 permitted only the enum's methods and the registration function to match a variant or read the payload, which the plan's own tasks violate in three places: `vended.rs`'s S3 arm, `connection.rs::storage_block`'s construction, and each crate's `test_support.rs`. The plan's own verification row expected `StorageBackend::S3` in four files. The clause was unsatisfiable as written, and it named no owner for backend SELECTION — the one question slice C needs answered.
- **Direction change:** The clause now names the four permitted owners exhaustively — the enum's methods, the engine's registration function, `resolve_vended_storage`'s S3 arm (credential overlay onto an already-selected backend, forbidden from changing the variant), and `storage_block` — with `#[cfg(test)]` support modules permitted explicitly. A new clause makes `storage_block` the ONLY place a backend is SELECTED, so slice C's URI-scheme- or property-driven selection has one named home rather than a new decision point.
- **Promotes to ADR:** yes

### [plan-review] The dimension-dedup test could not fail

- **Finding:** `plan-reviewer` round 1, `[AMBIGUOUS_REQUIREMENT]` BLOCKER. Scenario 3's "MUST NOT be registered a second time" was unobservable at the seam the plan specified: `register_side_store` returned `Result<(), UdfError>`, and DataFusion's `ObjectStoreRegistry` exposes only `get_store(url)` — no count, no enumeration. Registering once and registering twice with a fresh identical store are indistinguishable from outside, so `join_dimension_side_sharing_the_fact_bucket_is_not_registered_twice` could only assert that one store resolves, which is true either way. Under the failing-test-first rule the implementer had no way to start.
- **Direction change:** `register_side_store` now returns `Result<Option<Url>, UdfError>` — `Some(url)` for the key it registered, `None` when it skipped an already-registered key — in § Design > Architecture, § Migration, and task 3.2. Scenario 3's dedup clause and its registered-pairs clause are both restated in terms of that return, as an expected sequence per spec shape (`Some`; `Some` then `None`; two distinct `Some`; `Some` with no second call). The named test now fails if the skip is lost.
- **Promotes to ADR:** no

### [plan-review] Task 4.1's fixture enumeration was wrong in three ways

- **Finding:** `plan-reviewer` round 1, `[TRACEABILITY_GAP]` BLOCKER. Task 4.1 is the only owner of test-fixture wrapping and its list was unusable: "eight `tests/` storage helpers" names six functions; `micro_bench.rs:300` and `scan_telemetry.rs:136`'s inline `storage: StorageProps { .. }` constructions were absent entirely; and `src/scan/test_support.rs::minimal_spec` — the fixture the repointed `object_store.rs` tests depend on — was absent from every task.
- **Direction change:** Task 4.1 now names the six helpers with `file:line` (`common/e2e_harness.rs:337`, `scan_plan_shape.rs:606`, `scan_name_mapping.rs:97`, `scan_no_head_test.rs:246`, `scan_positional_deletes.rs:108`, `scan_join_test.rs:166`), lists every inline construction site with its line numbers, and adds `micro_bench.rs` and `scan_telemetry.rs`. `minimal_spec` is owned by task 3.2 (per the `[EFFORT_MISESTIMATION]` fix), and task 4.1 states that explicitly so it is not wrapped twice.
- **Promotes to ADR:** no

### [plan-review] The size index's scope was ambiguous, and no named test pinned it

- **Finding:** `plan-reviewer` round 2, `[AMBIGUOUS_REQUIREMENT]` BLOCKER (`review/round-2.md` lines 22-25). `register_side_store(ctx, backend, files, table_root, budget, sizes)` takes a per-side `files` parameter beside `sizes`, which makes it ambiguous whether `sizes` is per-side or whole-spec. Today `build_spec_size_index` (`object_store.rs:140-150`) indexes `spec.files` AND `join.files` into ONE map and the SAME map reaches both `register_bucket_store` calls — load-bearing, because in the shared-bucket case only the FACT side registers, so the fact store answers the DIMENSION files' HEADs from that map. An implementer deriving `sizes` from `files` — the natural reading once every other parameter is per-side — leaves the dimension HEADs to the network, breaking `datafusion-scan/scan-execution-file-metadata:34,50`'s no-per-file-HEAD guarantee. No named test would catch it: `scan_no_head_test.rs` carries no join spec, and `scan_join_test.rs` is `file://` only so it never reaches S3 registration at all.
- **Direction change:** `vs-adapter/storage-backend-enum` scenario 3 gained a clause requiring the size index handed to EVERY side's registration to be the whole-spec index spanning fact and join dimension files, computed once per `build_session_context` call, with the per-side `files` parameter selecting the store KEY only and forbidden from narrowing the map. Task 3.2 now names `build_spec_size_index` (`object_store.rs:140`) as unedited and its one result as passed to both calls. A new unit test row, `shared_bucket_join_store_answers_both_sides_sizes_from_the_spec`, asserts a dimension-side file key is present in the registered store's size map, and the two integration rows were corrected to state that neither suite exercises the S3 registration path.
- **Promotes to ADR:** no

### [plan-review] Five further live clauses pinned the goldens this plan edits

- **Finding:** `plan-reviewer` round 2, `[REQUIREMENT_CONFLICT]` BLOCKER (`review/round-2.md` lines 34-41). Round 1's carve-out was applied to the four clauses round 1 named, not to the defect class. Five more live clauses across three features still required byte-identical output over the values this plan changes: `pushdown-catalog-session:29` (whose clause literally names "the per-shard scan-spec storage") and `:38`, with NO delta directory and no § Features row at all; `pushdown-module-structure:130`, `:137` (whose two guards' output IS `group_by_fallback.sql` and `multi_count_distinct_decline.sql`), and `:149`; and `pushdown-col-types-consolidation:59` and `:72`. `plan.md`'s "all four clauses are amended" was an undercount.
- **Direction change:** A new `vs-adapter/pushdown-catalog-session` delta amends all THREE of that feature's byte-identical clauses — the two the finding named plus `:46`'s per-table storage-block clause, which carries the same defect and sits in the same file, so amending only the named two would have repeated the round-1 mistake one file later. The `pushdown-module-structure` delta gained the three named scenarios (7 amended clauses total) and the `pushdown-col-types-consolidation` delta the two (3 total). `plan.md` § Features gained the `pushdown-catalog-session` row and a Scenario Coverage row; the clause note now states the real total — 15 clauses across five features, itemized per feature — and both existing deltas' Background counts were corrected.
- **Promotes to ADR:** no

### [plan-review] The exhaustive-owner list forbade its own `Default`-impl carve-out

- **Finding:** `plan-reviewer` round 2, `[REQUIREMENT_CONFLICT]` BLOCKER (`review/round-2.md` lines 43-46). Two round-1 fixes collided. The `[COMPLETENESS_GAP]` fix added `CommonScanSpec`'s `impl Default` (`scan/spec.rs:689`) to task 3.1, but the `[REQUIREMENT_CONFLICT]` fix had made scenario 2's list of variant-naming sites exhaustive at four and made `storage_block` the ONLY selection site. That `impl` is a plain production `impl`, not `#[cfg(test)]`-gated, so it violated both clauses — and falsified `plan.md`'s own `rg` check, whose Expected Output listed neither `scan/spec.rs:689` nor the in-module test construction at `:853`.
- **Direction change:** Scenario 2's list is now exhaustive at FIVE, with `CommonScanSpec`'s `impl Default` described precisely as re-wrapping a placeholder payload without choosing a backend from any input. The sole-selection clause is narrowed to selection FROM INPUT — the only site whose variant depends on the CONNECTION object, deserialized wire bytes, or a URI — so a fixed `S3` placeholder is explicitly not a selection point. The test carve-out widened from "each crate's support module" to any `#[cfg(test)]` module, and the `rg` row's Expected Output now names `scan/spec.rs` for both the `impl Default` initializer and `:853`.
- **Promotes to ADR:** no
