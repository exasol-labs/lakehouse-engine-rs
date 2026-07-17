# Plan: add-vs-refresh

## Summary

Make `ALTER VIRTUAL SCHEMA ... REFRESH` and `ALTER VIRTUAL SCHEMA ... SET` re-read the Iceberg catalog in place by handling the adapter's `refresh` and `setProperties` protocol requests, both reusing the existing `createVirtualSchema` enumeration. Closes #147.

## Design

### Context

The adapter captures table and column metadata once at `CREATE VIRTUAL SCHEMA` and offers no supported way to re-read the catalog in place. `ALTER VIRTUAL SCHEMA x REFRESH` and `ALTER VIRTUAL SCHEMA x SET ...` both fail with `unsupported VS request type: refresh` / `unsupported VS request type: setProperties` (#147), forcing a `DROP ... CASCADE` + `CREATE` that destroys dependent views and grants.

Root cause, found during planning: the dispatch in `crates/lakehouse-engine/src/adapter/mod.rs` (line 139) matches `Some("refreshVirtualSchema")`, but the Exasol protocol request `type` for a refresh is the literal string `refresh` (verified against `virtual-schema-common-java`'s `virtual_schema_api.md`). The `refreshVirtualSchema` arm is dead code that never fires, so every refresh falls through to the `unsupported VS request type` arm. `setProperties` has no arm at all.

- **Goals** — Recognise the real `refresh` and `setProperties` protocol strings; re-run the full `createVirtualSchema` namespace enumeration for each; return responses labelled with the matching `type`; keep the stateless-adapter architecture intact.
- **Non-Goals** — Partial/incremental refresh that lists only `requestedTables` from the catalog (the adapter always enumerates the whole namespace, as `createVirtualSchema` does, and lets Exasol apply the requested subset via the echoed `requestedTables`). No caching, diffing, or metadata persistence. No change to the `createVirtualSchema`, `getCapabilities`, `dropVirtualSchema`, or `pushdown` behaviours. No change to type mapping or field-id handling.

### Decision

Route `refresh` and `setProperties` through the existing `handle_create_virtual_schema` enumeration, generalising only the request-type recognition, the property-merge precedence, the response-`type` label, and the `requestedTables` echo.

#### Architecture

```
dispatch(type)
 ├─ "getCapabilities"     → get_capabilities_response()          (unchanged)
 ├─ "createVirtualSchema" → handle_create_virtual_schema  ┐
 ├─ "refresh"             → handle_create_virtual_schema  ├─ shared enumeration:
 ├─ "setProperties"       → handle_create_virtual_schema  ┘   list tables → resolve
 ├─ "dropVirtualSchema"   → {type: dropVirtualSchema}         schema → type-map →
 └─ "pushdown"            → handle_pushdown_request           build_adapter_notes
                                                              (TABLE_MAP rebuilt)
                          response "type" = request "type"
                          refresh: echo requestedTables if present
                          setProperties: request props override persisted props
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Reuse enumeration path | `handle_create_virtual_schema` serves `refresh`/`setProperties` | Refresh is "re-run create", matching the DROP+CREATE workaround; no reinvented listing/mapping code |
| Response type mirrors request type | response-label helper | Exasol requires the response `type` to equal the request `type` (`refresh`/`setProperties`) |
| Distinct property merge for setProperties | new `merge_set_properties` | `setProperties` requires request props to win and `null` to unset — the inverse of the pushdown-oriented `get_properties` precedence |
| Full rebuild, never diff | `build_adapter_notes` / `TABLE_MAP` | Stateless model: refresh rebuilds `TABLE_MAP` from re-enumeration, preserving unrelated notes |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Include `setProperties` in this plan, not defer | Ship `refresh` only, defer `setProperties` to a follow-up | Both are named in #147's title; the namespace-change case is only reachable via `setProperties`; both reuse the same enumeration, so shipping together avoids a near-duplicate follow-up |
| Always full-namespace enumeration; echo `requestedTables` | Honour `requestedTables` by listing only those tables from the catalog | Full enumeration matches `createVirtualSchema` and the DROP+CREATE workaround; echoing `requestedTables` lets Exasol apply the requested subset, so partial-refresh SQL stays correct without a second listing code path |
| Separate `merge_set_properties` from `get_properties` | Reuse `get_properties` | `get_properties` makes persisted `schemaMetadataInfo.properties` win (correct for pushdown/refresh where the request carries no props); `setProperties` needs the opposite precedence plus `null`-unset, so a shared helper would break one caller |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/refresh-and-set-properties | NEW | `vs-adapter/refresh-and-set-properties/spec.md` |

## Iceberg Spec Compliance

This feature re-enumerates tables and re-resolves schemas, so it touches schema/type handling. It reuses the `createVirtualSchema` enumeration and `datafusion-scan/type-mapping` verbatim and adds no new schema-handling surface. Per the Apache Iceberg table spec, `current-schema-id` is the "ID of the table's current schema" and "points to the schema by ID for use when reading table data", and columns are "selected by field id"; a re-read therefore reflects added, dropped, and renamed columns and type promotion automatically. The known field-id projection exception (`datafusion-scan/scan-execution-field-id-projection`, #27) is unchanged and out of scope.

## Migration

None. No persisted data changes shape. Existing virtual schemas created before this change gain working `REFRESH`/`SET` once the adapter `.so` is rebuilt and reinstalled; the persisted `adapterNotes` from prior `createVirtualSchema` responses are read back unchanged.

## Implementation Tasks

1. Replace the dead `"refreshVirtualSchema"` dispatch arm with `"refresh"` and add a `"setProperties"` arm, both calling `handle_create_virtual_schema` (`crates/lakehouse-engine/src/adapter/mod.rs`).
2. Generalise the response-`type` label in `handle_create_virtual_schema` to mirror the request `type` (`createVirtualSchema` | `refresh` | `setProperties`), replacing the two-way `createVirtualSchema`/`refreshVirtualSchema` branch. [expert]
3. Echo `requestedTables` in the `refresh` response when the request carries it; omit it otherwise.
4. Add `merge_set_properties` with request-props-win precedence and `null`-unset, and use it for the `setProperties` path while leaving `get_properties` for create/refresh/pushdown. [expert]
5. Update doc comments that name `refreshVirtualSchema` to the real protocol strings (`crates/lakehouse-engine/src/lib.rs` line 5; `crates/lakehouse-engine/src/adapter/mod.rs` header and the line-37 comment).
6. Add unit tests: `refresh`/`setProperties` are not rejected as unsupported; response `type` mirrors request `type`; `requestedTables` echo present/absent; `merge_set_properties` new-wins and `null`-unset; `TABLE_MAP` rebuild preserves unrelated notes.
7. Add E2E test file `crates/lakehouse-engine/tests/e2e_refresh_test.rs`: refresh picks up an added table and a column change; `setProperties` re-targets a changed `ICEBERG_NAMESPACE`; catalog-unreachable refresh returns a credential-free error.
8. Create the GitHub issue reference: reference #147 in the implementing commit (`Closes #147`).

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 1, Task 2, Task 3, Task 4 (same file, one coherent edit) |
| Group B | Task 5 (doc comments) |
| Group C | Task 6 (unit tests), Task 7 (E2E tests) |

Sequential dependencies:
- Group A → Group C (tests exercise the new dispatch and merge)
- Group B is independent of A and C

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Match arm | `crates/lakehouse-engine/src/adapter/mod.rs` `dispatch` `Some("refreshVirtualSchema")` | Never matches Exasol's real `refresh` request string; replaced by the `"refresh"` arm |
| Branch | `crates/lakehouse-engine/src/adapter/mod.rs` `handle_create_virtual_schema` `"refreshVirtualSchema"` response label | Replaced by the request-type-mirroring label |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Refresh re-enumerates the namespace and returns a refresh response | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `refresh_dispatched_not_unsupported_and_labels_refresh` |
| Refresh re-enumerates the namespace and returns a refresh response | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_refresh_test.rs` | `refresh_reenumerates_namespace` |
| Refresh reflects table and column structure changes | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_refresh_test.rs` | `refresh_reflects_added_table_and_column_change` |
| Refresh rebuilds the table map and preserves other adapter notes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `refresh_rebuilds_table_map_preserves_notes` |
| Refresh echoes requestedTables when present | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `refresh_echoes_requested_tables_present_and_absent` |
| Set properties overrides persisted properties and re-enumerates | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `merge_set_properties_new_wins_and_null_unsets` |
| Set properties overrides persisted properties and re-enumerates | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_refresh_test.rs` | `set_properties_retargets_namespace` |
| Refresh and set properties redact credentials on catalog failure | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_refresh_test.rs` | `refresh_unreachable_catalog_redacts_credentials` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/refresh-and-set-properties | Add a table to the Glue namespace, then `ALTER VIRTUAL SCHEMA lh REFRESH;` followed by `SELECT * FROM lh.<new_table> LIMIT 1;` | Statement succeeds; the new table is queryable; no `unsupported VS request type` error |
| vs-adapter/refresh-and-set-properties | `ALTER VIRTUAL SCHEMA lh SET ICEBERG_NAMESPACE='<other_ns>';` then `SELECT table_name FROM SYS.EXA_ALL_TABLES WHERE table_schema='LH';` | Tables of the new namespace are listed; statement succeeds |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test (unit) | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt` | No changes |
