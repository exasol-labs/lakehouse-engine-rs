# Plan: add-delta-scan-execution

## Summary

Delivers the first full round-trip query over a Delta Lake table by routing production pushdown
through the format-reader seam, applying Delta deletion vectors at scan time, and materializing
partition columns from each file's logged partition values. Closes issue #320 and the ADR-4 debt from
PR #340 by collapsing the `resolve_file_list` delegator into the Iceberg reader.

## Design

### Context

Three seams are open, and a Delta query needs all three closed at once.

**Production pushdown never reaches the format-reader seam.** `handle_pushdown` and
`resolve_one_join_side` call the Iceberg-only `resolve_file_list` directly, and `dispatch` refuses
every Unity Catalog pushdown before any of it runs. `format_reader` / `ScanSource` / `ResolvedScan`
exist and are exercised by tests alone.

**The scan refuses Delta deletion vectors.** `applicable_positional_delete` matches all four delete
mechanisms exhaustively; the `DeltaDeletionVector` arm returns a refusal naming issue #320.

**Partition columns NULL-fill.** `ResolvedScan.partition_columns` and `FileEntry.partition_values`
are populated by the Delta reader, dropped at the one `CommonScanSpec` base construction site, and
read by nothing under `scan/`. A partition column is in the table's Arrow schema, is absent from every
Parquet file, and therefore reaches the query as NULL.

- **Goals** — a correct `SELECT` over a Delta table for every pushdown shape (scan, filter, LIMIT,
  ORDER BY, aggregate, grouped aggregate, broadcast join); deletion vectors applied; partition values
  materialized; column mapping verified end to end; one resolution path for both catalog kinds; the
  Iceberg wire encoding and generated SQL byte-identical.
- **Non-Goals** — Delta partition and statistics-based file pruning at plan time (issue #321: this
  plan narrows rows, not files); Delta reader-feature gating and broad Delta type mapping (issue
  #322, which leaves `type_widening`-style tables query-reachable and ungated); Iceberg
  equality-delete and Puffin deletion-vector application, whose refusal arms stay refusing;
  percent-decoding of a Delta `add.path`, which the protocol specifies as *"a URI as specified by
  RFC 2396 ... which needs to be decoded to get the data file path"* and which no seeded fixture
  exercises — task 5.1 verifies it and records a tracked issue if the gap is real.

### Decision

#### Architecture

```
 handle_pushdown ─┬─ single table ──┐
                  └─ plan_join ─────┤  (one leg at a time)
                                    ▼
                        TableScanResolver          built ONCE per request at the
                        ├ Iceberg(CatalogSession)  ONE CatalogKind match site
                        └ Unity(UnityCatalogSession) + load_table
                                    ▼
                     format_reader(ScanSource, ConnectionStorage)
                                    ▼
      ResolvedScan { files, effective_storage, logical_schema, table_root,
                     name_mapping, partition_columns }
                                    ▼
    CommonScanSpec.partition_columns │ JoinSpec.partition_columns │ FileEntry.partition_values
                                    ▼   (format-neutral wire)
                            register_file_list
                                    ▼
                        PositionalDeleteScanTable
      Phase A  Iceberg delete files ┐
               Delta DV sidecars    ├──▶ ONE HashMap<data-file path, RoaringTreemap>
               Delta DV inline      ┘
      Phase B  per PartitionedFile: access plan (deletes) + partition_values (ScalarValue)
      scan()   FileScanConfig { file_schema, table_partition_cols, expr_adapter,
                                projection remapped from declared order }
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| One per-request resolver, one kind match | `TableScanResolver` in the pushdown path | Every request shape and every join leg resolves identically; the kind is matched once, replacing the refusal in the recorded permitted-site list |
| Consumer-defined abstraction over a library | in-memory `StorageHandler` shim feeding `delta_kernel`'s DV decoder | The scan already owns bounded, budgeted object-store I/O; the kernel is used as a pure bytes-to-bitmap function, never as a second execution engine |
| Converge on the shipped pipeline | Delta DV positions merge into the Iceberg delete-position map | A decoded deletion vector and an accumulated positional-delete set are both a bitmap of 0-based row positions; `build_deletes_row_selection` and `build_access_plan` are reused unchanged |
| Reuse the framework's own mechanism | DataFusion `table_partition_cols` + `PartitionedFile.partition_values` | Per-file constants, native predicate pruning, and composition with the access plan and the expr adapter come free; a post-hoc batch rewrite would break filter and GROUP BY pushdown |
| Widen a neutral field, never add a format block | `JoinSpec.partition_columns` | The broadcast side needs the same neutral concept the fact side already carries |

#### Key interfaces

- `TableScanResolver::resolve(&self, table_identifier: &str, filter_json: Option<&Json>) -> Result<ResolvedScan, UdfError>` — the only thing the pushdown pipeline learns about a table. Submodule-private; NOT added to the pushdown façade.
- `delta_kernel::actions::deletion_vector::DeletionVectorDescriptor::read(Arc<dyn StorageHandler>, &Url) -> DeltaResult<RoaringTreemap>` — needs no feature flag beyond those already enabled, and returns the same `roaring` 0.11.4 type the Iceberg path uses (one crate instance resolves workspace-wide).
- `JoinSpec.partition_columns: Vec<String>` — serde-defaulted and omitted when empty, so every Iceberg join spec stays byte-identical.

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Reuse `delta_kernel`'s DV decoder | Hand-decode with `roaring` against the protocol | The kernel validates the version byte, the declared size, the portable magic, and the CRC-32, and handles all three storage types including Z85 inline. Hand-decoding would re-derive protocol details the workspace already depends on, with no upside. |
| Fetch DV bytes on the scan's own async path, decode from memory | Give the kernel a live object-store-backed `StorageHandler` | The kernel's decoder is synchronous and its default handler drives its own background runtime. Pre-fetching keeps every byte on the scan's bounded-concurrency path, keeps one runtime in the UDF, and lets one sidecar body serve many descriptors. |
| Fetch the WHOLE sidecar object, not the descriptor's byte range | Range GET `[offset, offset+4+sizeInBytes+4)` | The decoder reads the container's version byte at file position 0; a range starting at `offset` does not carry it. Delta sidecars are small (the seeded fixture is 45 bytes), and whole-object fetch is what makes the shared-sidecar read-once property possible. |
| DataFusion `table_partition_cols` | Extend the expr adapter's per-file default map with partition literals | `PhysicalExprAdapterFactory` is built once per scan and receives schemas, not file identity, so it cannot carry a per-file constant. The native mechanism is per-file by construction and prunes on partition values. |
| Remap the projection from declared order to `file ++ partition` order | Append partition columns to the output and reorder with a `ProjectionExec` | `FileScanConfig` applies projection indices in the order given, so the remap alone restores declared order with no extra plan node. |
| Recover the Unity table identity by splitting the recorded dotted identifier | Re-encode `TABLE_MAP` with explicit namespace segments | Unity Catalog addresses a table by that same dotted full name and the loader re-joins the segments, so the split round-trips losslessly and cannot resolve a different table. Re-encoding `TABLE_MAP` would change a create-time wire format and force a REFRESH for existing virtual schemas. |
| Keep `positional_deletes.rs` as the delete-application home | Rename it to a format-neutral module name | The module owns the shared `PositionalDeleteScanTable`, which several recorded scenarios name. A rename buys a better name at the cost of churn across the scan façade and those scenarios; the new Delta decode gets its own `scan/deletion_vectors.rs` sibling instead. |
| Delete `resolve_file_list` outright | Keep it `pub` and deprecated | ADR 4 of PR #340 accepted the delegator only until this issue removed its direct callers. Two resolution paths for one format is the leak the seam exists to prevent. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/scan-execution-delta-deletion-vectors | NEW | `specs/_plans/add-delta-scan-execution/datafusion-scan/scan-execution-delta-deletion-vectors/spec.md` |
| datafusion-scan/scan-execution-partition-values | NEW | `specs/_plans/add-delta-scan-execution/datafusion-scan/scan-execution-partition-values/spec.md` |
| datafusion-scan/scan-execution-positional-deletes | CHANGED | `specs/_plans/add-delta-scan-execution/datafusion-scan/scan-execution-positional-deletes/spec.md` |
| vs-adapter/pushdown-format-neutral-resolution | NEW | `specs/_plans/add-delta-scan-execution/vs-adapter/pushdown-format-neutral-resolution/spec.md` |
| vs-adapter/catalog-kind-selection | CHANGED | `specs/_plans/add-delta-scan-execution/vs-adapter/catalog-kind-selection/spec.md` |
| vs-adapter/delta-table-planning | CHANGED | `specs/_plans/add-delta-scan-execution/vs-adapter/delta-table-planning/spec.md` |
| vs-adapter/pushdown-module-structure | CHANGED | `specs/_plans/add-delta-scan-execution/vs-adapter/pushdown-module-structure/spec.md` |
| e2e-harness/unity-catalog-e2e-harness | CHANGED | `specs/_plans/add-delta-scan-execution/e2e-harness/unity-catalog-e2e-harness/spec.md` |

## Impact

A virtual schema created with `CATALOG_KIND = 'UNITY_CATALOG'` becomes queryable. Its pushdown
requests previously failed with *"Unity Catalog scan execution is not yet supported"*; they now plan
and execute, so an operator who created such a schema to test enumeration will start getting rows.

Two boundaries move with it. A Delta table whose schema declares a type this engine does not map
(`byte`, `short`, `binary`, `array`, `map`, `struct`, `variant`) now fails at query time with the
reader's plan-time error rather than at the kind-level refusal — a more specific message for the same
outcome. A Delta table that declares a reader feature this engine does not implement (for example
`typeWidening-preview`) is now query-reachable and ungated: gating is issue #322, and until it lands
such a table's results are not guaranteed correct. That exception is recorded in the
`delta-table-planning` spec rather than left unstated.

No Iceberg behavior changes. The generated SQL, the serialized scan specs, and every Iceberg suite's
assertions stay byte-identical. `resolve_file_list` leaves the crate's public API — a breaking change
for an out-of-tree caller, of which there are none; in-repo callers move to `format_reader`.

## Dependencies

No new Cargo dependency. `delta_kernel` 0.26 and `roaring` 0.11 are already direct dependencies, and
`delta_kernel::actions::deletion_vector` needs no feature flag beyond those already enabled. The E2E
scenarios need the existing `make unity-up` fixture stack (issue #325) and no new fixture.

## Implementation Tasks

### 1. Wire the format-reader seam into production pushdown

- [ ] 1.1 Add `partition_columns: Vec<String>` to `JoinSpec`, serde-defaulted and skipped when empty; assert an Iceberg join spec's serialization is byte-identical
- [ ] 1.2 Build the per-request `TableScanResolver`: one exhaustive `CatalogKind` match, one catalog session per request, Unity `CatalogTableIdent` recovery from the recorded dotted identifier, `format_reader` + `resolve_scan` [expert]
- [ ] 1.3 Route `handle_pushdown`'s single-table path through the resolver; thread the resolved `CatalogKind` from `dispatch` through `handle_pushdown_request`; populate `CommonScanSpec.partition_columns` from the `ResolvedScan`
- [ ] 1.4 Route every join leg through the resolver: replace `JoinSideResolution`'s Iceberg-typed session with the resolver, and populate each side's `partition_columns` (fact side into the common spec, broadcast side into `JoinSpec`)
- [ ] 1.5 Delete the `dispatch` Unity Catalog refusal; replace `unity_kind_pushdown_is_refused_not_iceberg_routed` with a routing assertion; update the construction-site probe's doc comment to name the scan-source construction site instead of the refusal
- [ ] 1.6 Collapse `resolve_file_list` into `IcebergFormatReader::resolve_scan` and delete it; migrate `tests/common/e2e_harness.rs`, `tests/catalog_session_signatures.rs`, `tests/e2e_scan_test.rs`, `file_resolution_tests.rs`, and `format/iceberg_tests.rs` onto the seam; update both surface probes to 25 in-crate and 15 external items
- [ ] 1.7 Pin capability advertisement as catalog-kind-blind with a compile-time signature probe

### 2. Apply Delta deletion vectors at scan time

- [ ] 2.1 Add `scan/deletion_vectors.rs` (+ sibling `deletion_vectors_tests.rs`): resolve a descriptor to its sidecar path or inline payload, serve pre-fetched bytes through a read-only `StorageHandler` shim whose unsupported operations return clean errors and never panic, decode through `DeletionVectorDescriptor::read`, and reject a decoded set whose length disagrees with the declared cardinality [expert]
- [ ] 2.2 Replace `applicable_positional_delete`'s `(path, size)` result with a delete-mechanism classification that keeps both Iceberg refusals; extend Phase A to fetch each unique sidecar once under the shared limiter and merge Delta positions into the one position map; leave Phase B and the row-selection and access-plan builders untouched [expert]
- [ ] 2.3 Integration tests over the vendored `table-with-dv-small` fixture on a local filesystem store: 10 physical rows yield 8, the two deleted values are absent, a mixed Iceberg-and-Delta shard, shared-sidecar read-once, bounded concurrency, and each malformed-container refusal

### 3. Materialize partition values in the scan

- [ ] 3.1 Split the logical schema into file fields and partition fields at registration, keep `TableProvider::schema()` in declared order, attach `table_partition_cols`, and remap the projection indices from declared order to `file ++ partition` order [expert]
- [ ] 3.2 Populate each `PartitionedFile.partition_values` from its `FileEntry`, converting the protocol's string serialization to the column's declared Arrow type, mapping an absent or empty value to NULL, and failing cleanly on a value the type cannot represent [expert]
- [ ] 3.3 Apply the same registration path to both sides of a broadcast join, reading the broadcast side's partition columns from `JoinSpec`
- [ ] 3.4 Integration tests over the vendored `basic_partitioned` fixture: per-file constants across six files, the Hive default-partition file yielding NULL, a filter and a GROUP BY over the partition column, a partition column physically present in the file, and an unpartitioned scan proving no plan-shape change

### 4. End-to-end coverage on the Unity Catalog suite

- [ ] 4.1 Give the suite's virtual schema a CONNECTION carrying the MinIO endpoint and static storage credentials, and confirm the shared harness provisions the scan script
- [ ] 4.2 Add the round-trip scenarios to `tests/e2e_unity_test.rs`: delete-free `multi_part_stats`, deletion-vector `table_with_dv`, column-mapped `cm_id_mode` and `cm_name_mode`, partitioned `basic_partitioned` with filter and GROUP BY, a grouped aggregate, an ORDER BY with LIMIT, a broadcast join whose broadcast side is partitioned with a captured pushdown SQL assertion, and the fail-loud refusals for `unshredded_variant` and `stats_all_types`

### 5. Verification and hygiene

- [ ] 5.1 Verify how a percent-encoded Delta `add.path` flows from log replay to file registration; if it reaches object storage undecoded, open a tracked issue and cite it inline in the `delta-table-planning` spec delta
- [ ] 5.2 Run the Iceberg E2E suites unchanged to prove the rewiring is byte-identical, then the full checklist below

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7 |
| Group B | 2.1, 2.2, 2.3 |
| Group C | 3.1, 3.2, 3.3, 3.4 |
| Group D | 4.1, 4.2 |
| Group E | 5.1, 5.2 |

Sequential dependencies:
- Group A → Group D, Group E
- Group B → Group C (both edit `scan/positional_deletes.rs`; sequencing avoids a conflicting edit)
- Group C → Group D, Group E
- Within Group A: 1.1 → 1.4; 1.2 → 1.3 → 1.4 → 1.5 → 1.6
- Within Group B: 2.1 → 2.2 → 2.3
- Within Group C: 3.1 → 3.2 → 3.3 → 3.4
- Groups A and B run concurrently; they share no file.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs::resolve_file_list` | Body moves into `IcebergFormatReader::resolve_scan`; every caller routes through the seam |
| Re-export | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` `pub use file_resolution::resolve_file_list` | Item deleted; both surface probes drop it |
| Block | `crates/lakehouse-engine/src/adapter/mod.rs:164-168` | The Unity Catalog pushdown refusal is replaced by resolution |
| Struct | `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs::JoinSideResolution` | The per-request resolver carries the session, storage, and credentials it bundled |
| Test | `crates/lakehouse-engine/src/adapter/adapter_tests.rs::unity_kind_pushdown_is_refused_not_iceberg_routed` | Asserts the removed refusal |
| Test | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs::iceberg_reader_returns_empty_partition_columns_and_field_id_bound_logical_fields` | Compares the reader against the deleted function; the comparison target is gone |
| Match arm | `crates/lakehouse-engine/src/scan/positional_deletes.rs::applicable_positional_delete` Delta arm | The refusal is replaced by application |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| DV: A UUID-relative deletion vector removes exactly its flagged rows | Integration | `crates/lakehouse-engine/tests/scan_deletion_vectors.rs` | `uuid_relative_deletion_vector_removes_its_flagged_rows` |
| DV: An inline deletion vector is decoded with no object-store access at all | Integration | `crates/lakehouse-engine/tests/scan_deletion_vectors.rs` | `inline_deletion_vector_decodes_without_object_store_access` |
| DV: An absolute-path deletion vector is read without reconstructing a path | Integration | `crates/lakehouse-engine/tests/scan_deletion_vectors.rs` | `absolute_path_deletion_vector_is_read_verbatim` |
| DV: A deletion-vector file shared by several data files is fetched once per shard | Integration | `crates/lakehouse-engine/tests/scan_deletion_vectors.rs` | `shared_deletion_vector_file_is_fetched_once_per_shard` |
| DV: Concurrent deletion-vector reads stay within the connection budget | Integration | `crates/lakehouse-engine/tests/scan_deletion_vectors.rs` | `deletion_vector_reads_stay_within_the_connection_budget` |
| DV: The decoder is handed bytes and never a live storage client | Unit | `crates/lakehouse-engine/src/scan/deletion_vectors_tests.rs` | `storage_shim_serves_prefetched_bytes_and_refuses_every_other_operation` |
| DV: A deletion vector the scan cannot trust fails loud before any row is emitted | Unit | `crates/lakehouse-engine/src/scan/deletion_vectors_tests.rs` | `untrusted_deletion_vector_containers_fail_loud_without_panicking` |
| DV: Deletion vectors compose with projection, filter, LIMIT, and aggregation | Integration | `crates/lakehouse-engine/tests/scan_deletion_vectors.rs` | `deletion_vectors_compose_with_projection_filter_limit_and_aggregation` |
| DV: A Delta data file carrying no deletion vector scans unchanged | Integration | `crates/lakehouse-engine/tests/scan_deletion_vectors.rs` | `delta_file_without_a_deletion_vector_scans_unchanged` |
| Partition: A partition column absent from the data file is materialized per file | Integration | `crates/lakehouse-engine/tests/scan_partition_values.rs` | `absent_partition_column_is_materialized_per_file` |
| Partition: An absent partition value materializes NULL, never the partition-directory text | Integration | `crates/lakehouse-engine/tests/scan_partition_values.rs` | `absent_and_empty_partition_values_materialize_null` |
| Partition: A partition value is converted to its column's declared type | Integration | `crates/lakehouse-engine/tests/scan_partition_values.rs` | `partition_values_convert_to_their_declared_type_or_fail_cleanly` |
| Partition: The logged partition value wins over a physically present partition column | Integration | `crates/lakehouse-engine/tests/scan_partition_values.rs` | `logged_partition_value_wins_over_a_physical_partition_column` |
| Partition: A materialized partition column is a first-class scan column | Integration | `crates/lakehouse-engine/tests/scan_partition_values.rs` | `materialized_partition_column_serves_projection_filter_and_group_by` |
| Partition: A scan with no partition columns is unchanged | Integration | `crates/lakehouse-engine/tests/scan_partition_values.rs` | `scan_without_partition_columns_is_byte_identical` |
| Partition: Each side of a broadcast join materializes its own partition columns | Integration | `crates/lakehouse-engine/tests/scan_join_test.rs` | `each_join_side_materializes_its_own_partition_columns` |
| Deletes: An unapplicable delete file is rejected with a clean error (read-time backstop) | Unit | `crates/lakehouse-engine/src/scan/positional_deletes_tests.rs` | `only_iceberg_equality_and_puffin_delete_mechanisms_are_refused` |
| Deletes: Both delete mechanisms converge on one position map and one access-plan pipeline | Integration | `crates/lakehouse-engine/tests/scan_deletion_vectors.rs` | `mixed_iceberg_and_delta_shard_shares_one_position_map_and_limiter` |
| Resolution: Every pushdown request shape resolves through the one format-reader seam | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `every_request_shape_resolves_through_the_format_reader_seam` |
| Resolution: One catalog session per request serves every table the request resolves | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `a_two_leg_join_resolves_both_legs_on_one_catalog_session` |
| Resolution: The catalog kind is matched at one added construction site and nowhere else | Unit | `crates/lakehouse-engine/src/adapter/catalog_client_tests.rs` | `catalog_kind_is_matched_only_at_the_construction_site` |
| Resolution: A Unity Catalog table's identity survives the round trip from the involved table | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `unity_table_identity_round_trips_through_the_recorded_identifier` |
| Resolution: Iceberg pushdown output is byte-identical across the rewiring | Integration | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | existing golden suite, run unedited |
| Resolution: The Iceberg file resolver is collapsed into its reader and leaves the façade | Unit | `crates/lakehouse-engine/tests/catalog_session_signatures.rs` | `iceberg_scan_source_carries_a_shared_session` |
| Resolution: Resolved partition columns reach the scan spec for every side | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `resolved_partition_columns_reach_the_common_spec_and_the_join_spec` |
| Resolution: A table the reader cannot plan fails the query loud at plan time | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `every_request_shape_resolves_through_the_format_reader_seam` |
| Resolution: Capability advertisement stays blind to the catalog kind | Unit | `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` | `capabilities_are_assembled_without_the_catalog_kind` |
| Kind: The catalog kind is matched at one construction site and nowhere else | Unit | `crates/lakehouse-engine/src/adapter/catalog_client_tests.rs` | `catalog_kind_is_matched_only_at_the_construction_site` |
| Kind: A pushdown request under the Unity Catalog kind is planned as a Delta scan | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `unity_kind_pushdown_routes_to_the_unity_catalog_loader` |
| Planning: Iceberg planning is byte-identical through the new seam | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `iceberg_reader_owns_resolution_and_keeps_its_encoding` |
| Planning: The Delta reader is reached from production pushdown under the Unity Catalog kind | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `every_request_shape_resolves_through_the_format_reader_seam` |
| Structure: Collapsing the Iceberg file resolver removes exactly one item from the pushdown façade | Unit | `crates/lakehouse-engine/tests/pushdown_public_surface.rs` and `crates/lakehouse-engine/src/adapter/pushdown_surface_probe_tests.rs` | compile-time `use` probes, 15 and 25 items |
| E2E: Create virtual schema over a Unity Catalog namespace lists the fixture tables and columns | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_create_virtual_schema_lists_fixture_tables_and_columns` |
| E2E: The suite's virtual schema carries the storage credentials a UDF-side scan needs | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_delete_free_table_returns_its_rows` |
| E2E: A delete-free Delta table returns its rows end to end | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_delete_free_table_returns_its_rows` |
| E2E: A Delta table with deletion vectors returns only its live rows | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_deletion_vector_table_returns_only_live_rows` |
| E2E: A column-mapped Delta table returns values under its logical column names | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_column_mapped_tables_return_logical_column_values` |
| E2E: A partitioned Delta table returns its partition column values | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_partitioned_table_returns_partition_values` |
| E2E: Join and aggregate pushdown reach a Delta table by the same route as a scan | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_join_and_aggregate_pushdown_return_correct_rows` |
| E2E: A Delta table this engine cannot plan fails the query loud | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_unmappable_table_fails_the_query_loud` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Delta deletion vectors | `make unity-up && make test-e2e-unity 2>&1 \| tee /tmp/unity.log` | `unity_delta_deletion_vector_table_returns_only_live_rows` passes; `COUNT(*)` over `UNITY_DELTA_E2E_VS.TABLE_WITH_DV` reports 8 |
| Delta deletion vectors | `exapump sql "SELECT COUNT(*) FROM UNITY_DELTA_E2E_VS.TABLE_WITH_DV" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` | Returns 8, not the file's 10 physical rows |
| Partition values | `exapump sql "SELECT LETTER, COUNT(*) FROM UNITY_DELTA_E2E_VS.BASIC_PARTITIONED GROUP BY LETTER ORDER BY 1" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` | Four non-null letters plus one NULL group, six rows in total, no `__HIVE_DEFAULT_PARTITION__` value |
| Format-neutral resolution | `exapump sql "SELECT COUNT(*) FROM UNITY_DELTA_E2E_VS.MULTI_PART_STATS" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` | Returns 5 with no *"Unity Catalog scan execution is not yet supported"* error |
| Column mapping | `exapump sql 'SELECT ID, NAME, "VALUE" FROM UNITY_DELTA_E2E_VS.CM_ID_MODE' -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` (`VALUE` is an Exasol reserved word and needs quoting as an identifier) | Real values under the logical column names, never NULL |
| Iceberg byte-identity | `make test-e2e` | Every Iceberg E2E test passes with no assertion edited |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E (Unity/Delta) | `make unity-up && make test-e2e-unity` | 0 failures, no skipped test |
| E2E (Iceberg regression) | `make test-e2e` | 0 failures, no assertion edited |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
