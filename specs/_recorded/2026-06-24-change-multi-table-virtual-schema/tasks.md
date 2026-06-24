# Tasks: change-multi-table-virtual-schema

## Phase 2: Implementation (Group A — helpers)
- [x] 2.1 Add `flatten_table_name(configured_ns, ident)` + identifier-string form in a shared adapter helper, with unit tests (single-level, multi-level, casing)
- [x] 2.2 Replace `parse_table_ident`'s `splitn(2, '.')` with multi-segment split building `NamespaceIdent::from_vec` (trailing = table); keep single-level working [expert]

## Phase 2: Implementation (Group B — enumeration)
- [x] 2.3 Add `PROP_ICEBERG_NAMESPACE`, remove `PROP_TABLE`; `resolve_connection_config` no longer requires `TABLE_NAME`
- [x] 2.4 Recursive namespace listing (`list_namespaces(parent)` + `list_tables`) for unsigned REST path → full set of `TableIdent`s [expert]
- [x] 2.5 SigV4/Glue signed listing path for the same enumeration, or resolve documented limitation explicitly [expert]
- [x] 2.6 For each discovered table resolve schema (reuse `resolve_table_schema`) and build one `schemaMetadata.tables` entry

## Phase 2: Implementation (Group C — TABLE_MAP + pushdown derivation)
- [x] 2.7 Build `EXASOL_NAME → iceberg-identifier` map, serialize into `adapterNotes` via `build_adapter_notes` (merge) [expert]
- [x] 2.8 Detect `__` collisions during map construction → clear error naming the colliding Exasol name
- [x] 2.9 Add `adapter_note`-style reader parsing `TABLE_MAP` back into a lookup map at pushdown
- [x] 2.10 Read `involvedTables[0].name` in pushdown, look up in `TABLE_MAP`, set resolved Iceberg identifier on `CatalogProps` [expert]
- [x] 2.11 Clear error when involved virtual table name absent from `TABLE_MAP`
- [x] 2.12 Confirm `resolve_file_list`/`resolve_table_schema` consume derived identifier unchanged

## Phase 2: Implementation (Group D — tests + docs)
- [x] 2.13 Unit tests: flatten round-trip, multi-level parse, collision detection, pushdown lookup (absent → error)
- [x] 2.14 Update E2E (`e2e_scan_test.rs`, `common/seed.rs`): seed second table / child namespace, create VS with `ICEBERG_NAMESPACE`, assert both queryable + two-table Exasol-side join
- [x] 2.15 Move `capabilities.md` to `docs/capabilities.md`, drop the Destination banner

## Phase 4: Code Review
- [x] 4.1 Review all changed files

## Phase 5: Verification
- [x] 5.1 cargo test (host unit)
- [x] 5.2 clippy + fmt
