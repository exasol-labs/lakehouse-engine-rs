# Tasks: add-native-unity-catalog-client

## Phase 2: Implementation

### Group A — trait foundation
- [x] 1.1 Declare `CatalogClient` trait + catalog-neutral types (`CatalogTableIdent`, `CatalogTable`, `CatalogColumn`, `ColumnSourceType`, `CatalogListing`) in a new `lakehouse-catalog` module; boxed-future return, no `async-trait`, `Send + Sync`; no file-planning method [expert]

### Group A-iceberg
- [x] 1.2 Implement `IcebergRestCatalogClient` (`CatalogClient` for the Iceberg REST path): lazy one-session enumeration, empty-batch no-resolution-grant short-circuit (enumeration `RestCatalog` still grants under OAuth2), private per-ident load helper reusing the session, skipped-table routing; demote `list_namespace_tables` to crate-private [expert]

### Group B — Unity client
- [x] 2.1 Add `unity` module: `UnityCatalogSession`, crate-private wire types, base-URL derivation, `GET /catalogs|/schemas|/tables|/tables/{full_name}` with pagination; implement `CatalogClient` (list_tables single sweep, no `omit_columns`, always-empty skipped; load_table single get)
- [x] 2.2 Implement Unity Catalog auth strategy (crate-private): PAT verbatim bearer, Databricks OAuth M2M client_credentials mint/cache/refresh, no-auth mode [expert]
- [x] 2.3 Implement `POST /temporary-table-credentials` + `resolve_uc_vended_storage`; extract shared scheme→variant-kind classification home (classifies scheme only, builds no `StorageBackend`); no selector dispatch wiring [expert]

### Group B-probe
- [x] 2.4 Extend `redaction.rs` so vended `aws_temp_credentials`, `azure_user_delegation_sas`, `gcp_oauth_token` and the OAuth client secret never reach error/SQL/log lines
- [x] 2.5 Update `crates/lakehouse-catalog/tests/catalog_public_surface.rs`: add trait + neutral types + both clients (`Box<dyn CatalogClient>`), assert Unity wire types unreachable, `list_namespace_tables` no longer pub, pin `resolve_uc_vended_storage` arity/return, add every-`StorageBackend`-variant probe; keep demotion assertions

### Group C — engine adapter
- [x] 3.1 Add `CatalogKind` + `resolve_catalog_kind(props)` reading `PROP_CATALOG_KIND`, default `IcebergRest`, reject unrecognized value
- [x] 3.2 Thread `CatalogKind` into `read_connection`/`validate_creds`: `warehouse` required under Iceberg REST only, SigV4 rejected under Unity Catalog, all other Iceberg rules byte-identical [expert]
- [x] 3.4 Map `ColumnSourceType` → Exasol type in `types/mapping.rs` via one exhaustive match (Iceberg → existing; Unity → new crate-internal `unity_type_name_to_exasol`, incompatible/out-of-range → `VARCHAR(2000000)`)
- [x] 3.5 Refuse pushdown under Unity Catalog kind with a clear "scan not yet supported" error; do not route to Iceberg file resolution

### Group C-pipeline
- [x] 3.3 Single construction site (exhaustive `CatalogKind` match → `Box<dyn CatalogClient>`); rewrite `handle_create_virtual_schema` to one listing pipeline over `CatalogListing`; delete `resolve_namespace_virtual_tables` and `resolve_table_schema` (+ `pub use`); update two pushdown façade probes (22→21 in-crate, 12→11 external) and `catalog_session_signatures.rs`; migrate empty-namespace guarantee test; keep Iceberg listing byte-identical incl. full-Unicode fold [expert]

### Group D — E2E
- [x] 4.1 Land #325 harness files on the branch; add `unity-e2e` cargo feature + `test-e2e-unity` Makefile target invoking `make unity-up`
- [x] 4.2 Verify OSS inline-columns precondition: after `make unity-up`, query `GET /tables` for `unity.delta_e2e` and confirm inline `columns[]` by default; escalate if OSS omits them
- [x] 4.3 Add `crates/lakehouse-engine/tests/e2e_unity_test.rs`: create UC virtual schema over `unity.delta_e2e`, assert fixture tables + representative columns listed, fail-not-skip when stack down, no credential leaks

## Phase 4: Review Fixes

### Catalog crate [expert]
- [x] F-C1 Delete unused `list_catalog_names`/`list_schema_names` + orphaned `CatalogsPage`/`CatalogInfo`/`SchemasPage`/`SchemaInfo` wire types + their tests; update `catalog_public_surface.rs` (unspecified surface, pub only to dodge dead-code lint)
- [x] F-C2 Percent-encode each `full_name` path segment in `get_table_info`; add a reserved-char test
- [x] F-C3 Extend `spawn_mock_catalog` to serve the Iceberg namespace-list endpoint; drive public `CatalogClient::list_tables` end-to-end (empty namespace lists nothing at ONE enumeration grant + no resolution-session grant; non-empty enumeration TWO grants; skipped-table)
- [x] F-C4 Fix absent/zero `expires_in` OAuth cache-defeat (error or conservative default TTL) + boundary test

### Engine crate
- [x] F-E1 Re-anchor 5 stale `resolve_table_schema` doc references to `build_listing_virtual_tables` (support.rs, joins/planning.rs, joins/rendering.rs, support_tests.rs, e2e_non_ascii_identifier_test.rs); drop stale `file_resolution.rs:640` cite
- [x] F-E2 Rewrite the `catalog_kind_is_matched_only_at_the_construction_site` doc comment to state only what the probe enforces
- [x] F-E3 Reference-aware rename `iceberg_identifier_string` → `catalog_identifier_string` (+ `iceberg_id` local → `catalog_id`)
- [x] F-E4 `resolve_connection_config` returns the resolved `CatalogKind`; `handle_create_virtual_schema` reuses it instead of re-parsing (leave pushdown-path refusal untouched)

## Phase 5: Verification
- [x] 5.1 Build (`make cross-musl-udf-build`) exit 0
- [x] 5.2 Test (`cargo test`) 0 failures
- [x] 5.3 Lint (`cargo clippy --all-targets`) 0 errors/warnings
- [x] 5.4 Format (`cargo fmt --check`) no changes
- [x] 5.5 Scenario coverage audit against plan
- [x] 5.5a Implement unity_schema_tests.rs (5 scenarios)
- [x] 5.6 Generate verification-report.md
