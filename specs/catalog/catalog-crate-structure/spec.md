# Feature: Catalog Crate Structure

The Iceberg REST catalog access layer lives in the standalone `lakehouse-catalog` crate, so a crate boundary decides what the planning layer in `lakehouse-engine` may reach. The layer covers catalog authentication, the per-query HTTP session, the `loadTable` GET, namespace enumeration, vended-storage resolution, and the credential types and redaction those need.

## Background

* The crate boundary changes code location and visibility only. It changes no query, pushdown, catalog-request, file-pruning, or type-handling behavior.
* The crate lives at `crates/lakehouse-catalog`, a path dependency of `lakehouse-engine`. `make cross-udf-build` still builds `-p lakehouse-engine` and emits one `.so`.
* The crate owns how the engine authenticates to a catalog and its object store, and how credentials are kept out of returned output. The credential types (`ConnectionCreds`, `StorageProps`, `CatalogProps`), the redaction functions (`redact_credentials`, `redact_secret_values`), and AWS SigV4 request signing therefore live beside the session code.
* `StorageProps` is produced by the catalog (a `loadTable` response vends S3 credentials, region, endpoint, and path-style) and consumed by the scan UDF as a `CommonScanSpec` field that crosses the UDF boundary as JSON. It is declared once, in the catalog crate, and re-exported at the path the scan layer imports.
* The Exasol-facing half of credential handling stays in the engine: `read_connection`, `validate_creds`, `parse_creds`, `storage_block`, `catalog_block`, and `REQUIRED_KEY` name the Exasol CONNECTION delivery mechanism, which the catalog crate must not know about.
* Iceberg file planning stays in `lakehouse-engine`. It produces the scan-spec wire types (`FileEntry`, `LogicalField`, `NameMappingEntry`, `DeleteMechanism`) and Arrow type tags, which the catalog crate must not name.
* `Makefile`'s `VS_SRCS` lists its `find` roots by hand and includes `crates/lakehouse-catalog/src`. Without that root the E2E suites would run against a `.so` built before a catalog change.
* The crate-private mode classifier for catalog authentication sits in `creds.rs` beside the `ConnectionCreds` fields it reads. It serves both `auth` and `unity::auth` without widening any visibility.
* `non_empty` has one declaration in the crate, read by `auth`, `unity::auth`, and the classifier.
* `StorageBackend` wraps `StorageProps` without editing the struct, so the payload's serde encoding is unchanged. `StorageBackend` methods cannot register DataFusion object stores, because the crate declares no `object_store` or `datafusion` dependency.
* The history of reviewed additions to the crate's `pub` set is tracked in `catalog/catalog-crate-public-surface-extensions`.


## Scenarios

### Scenario: The catalog access layer lives in a standalone crate the engine depends on one way

* *GIVEN* the catalog authentication, HTTP session, `loadTable`, namespace-enumeration, SigV4-signing, credential-type, and credential-redaction code
* *WHEN* the workspace builds
* *THEN* a crate named `lakehouse-catalog` at `crates/lakehouse-catalog` SHALL own that code, and `lakehouse-engine` SHALL declare it as a path dependency
* *AND* the dependency SHALL point one way only: no `lakehouse-catalog` source file SHALL name `lakehouse_engine`, which the compiler enforces because the reverse edge would be a cycle
* *AND* `lakehouse-catalog`'s manifest MUST NOT declare `arrow`, `parquet`, `datafusion`, `object_store`, `roaring`, `async-trait`, `tracing`, or `exasol-udf-macros` as a direct dependency, so the catalog layer cannot reach the execution engine through its own manifest
* *AND* `lakehouse-catalog` MUST NOT name `FileEntry`, `LogicalField`, `NameMappingEntry`, `DeleteMechanism`, `ScanSpec`, or `CommonScanSpec`, because Iceberg file planning and the scan-spec wire format stay in `lakehouse-engine`
* *AND* `make cross-udf-build` SHALL still build `-p lakehouse-engine` and still emit exactly one `.so` exporting both entry points
* *AND* the `.so` staleness guard SHALL list the crate's sources and manifest: `Makefile`'s `VS_SRCS` SHALL include `crates/lakehouse-catalog/src` among its `find` roots and `crates/lakehouse-catalog/Cargo.toml` plus the root workspace `Cargo.toml` among its files, so that editing a catalog source file rebuilds the `.so`

### Scenario: One crate declares each shared credential type, re-exported at its pre-move engine path

* *GIVEN* `StorageProps`, `CatalogProps`, `ConnectionCreds`, `redact_credentials`, and `redact_secret_values` declared in `lakehouse-catalog`
* *WHEN* the engine names any of them
* *THEN* `lakehouse-catalog` SHALL declare each of those five items EXACTLY ONCE, and no parallel struct, duplicate definition, or field-by-field conversion function SHALL exist on either side of the crate boundary
* *AND* `lakehouse_engine::scan::spec` SHALL re-export `StorageProps`, `CatalogProps`, and `StorageBackend`, `lakehouse_engine::adapter::connection` SHALL re-export `ConnectionCreds`, and `lakehouse_engine::scan::emit` SHALL re-export `redact_credentials` and `redact_secret_values`, each at `pub` visibility, so consumers keep one import path
* *AND* the serde encoding of `StorageProps` MUST stay field-for-field stable, because it is carried into a `CommonScanSpec` field that crosses the UDF boundary as JSON and a renamed or reordered field would break every deployed scan spec
* *AND* `read_connection`, `validate_creds`, `parse_creds`, `storage_block`, `catalog_block`, and `REQUIRED_KEY` SHALL stay in `lakehouse_engine::adapter::connection`, because they interpret the Exasol CONNECTION object and the catalog crate MUST NOT name that delivery mechanism

### Scenario: The crate exposes the concept-level API and hides every mechanism step

* *GIVEN* the catalog code, whose in-crate callers and whose `lakehouse-engine` callers need different subsets of it
* *WHEN* the crate's public surface is declared
* *THEN* the `pub` items of `lakehouse-catalog` SHALL be the concept-level items that `lib.rs` re-exports, and `catalog/catalog-crate-public-surface-extensions` SHALL record each reviewed addition to that set
* *AND* `StorageBackend` SHALL expose NO accessor returning its `StorageProps` payload, so a caller outside the enum's own methods cannot reach the payload to make a backend-specific decision
* *AND* `CatalogSession`'s fields SHALL stay private and `CatalogAuth` SHALL stay crate-private, so the auth strategy never leaks through the public interface
* *AND* these SHALL be crate-private: `StorageBackend::catalog_storage_props`, `CatalogAuth`, `resolve_catalog_auth`, `oauth2_client_credentials_grant`, `authed_get_json`, `resolve_load_table_prefix`, `prefix_from_config`, `build_load_table_url`, `glue_catalog_prefix`, `build_rest_catalog`, `inject_catalog_auth_props`, `non_empty`, `redact_catalog_auth_error`, `sign_request`, `list_namespace_tables`, and the shared catalog-auth mode classifier with the mode type it returns
* *AND* `non_empty` SHALL have exactly ONE declaration in the crate, read by `auth`, by `unity::auth`, and by the mode classifier, and the classifier SHALL sit beside the `ConnectionCreds` fields it reads rather than beside either consumer

### Scenario: Behavior is unchanged across the extraction

* *GIVEN* a pushdown request, a `createVirtualSchema` request, or a catalog request
* *WHEN* the crate layout serves the request
* *THEN* its query, pushdown, catalog-request, file-pruning, and type-handling behavior MUST NOT depend on which crate holds the code, EXCEPT the scan spec's `storage` value
* *AND* the scan spec's `storage` value SHALL be the externally-tagged backend variant of `storage-access/storage-backend-enum`, encoded by `storage-access/scan-spec-credential-reference` as a connection reference carrying no credential or as a sealed envelope over the backend encoding, and the committed `dispatch_golden` goldens and the join golden-SQL assertions SHALL pin that value
* *AND* the catalog request URLs, the header set on each request, and the OAuth2 grant request body MUST be the same on every auth mode for the same query
* *AND* no catalog secret, no live bearer token, no vended STS key in plaintext, and no CONNECTION-supplied storage credential SHALL appear in any returned SQL string or error message, because a vended key travels only inside the AES-GCM-sealed envelope and a CONNECTION credential travels as the CONNECTION name
