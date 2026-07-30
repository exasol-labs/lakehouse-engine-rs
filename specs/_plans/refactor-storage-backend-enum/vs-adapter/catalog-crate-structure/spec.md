# Feature: Catalog Crate Structure

Moves the Iceberg REST catalog access layer — catalog authentication, the per-query HTTP session, the `loadTable` GET, namespace enumeration, vended-storage resolution, and the credential types and redaction those need — out of `lakehouse-engine` into the standalone `lakehouse-catalog` crate, so a crate boundary rather than a module-private visibility rule decides what the planning layer may reach.

## Background

<!-- DELTA:NEW -->
* This delta amends THREE scenarios and supersedes NO Background bullet: the `pub`-set scenario, one clause of the shared-credential-type scenario, and the golden-fixture clause of the "Behavior is unchanged across the extraction" scenario. `vs-adapter/storage-backend-enum` (issue #274) replaces the free function `build_s3_file_io` with a `StorageBackend` method and adds the enum to this crate's public surface, so the enumerated `pub` set and the reachability probe that pins it both move; the same feature re-encodes the scan spec's `storage` value, so the byte-identical-SQL clause needs that value carved out. Every other scenario of this feature is unchanged.
* The "Behavior is unchanged across the extraction" clause naming the `dispatch_golden` goldens and the join golden-SQL assertions as passing UNEDITED is amended, not retired. The carve-out permits an edit to the `storage` value ALONE; every other byte of every golden stays as committed, which is what keeps that clause a working cross-refactor gate rather than a retired one.
* The "MUST NOT declare `object_store` or `datafusion` as a direct dependency" clause is what makes DataFusion object-store registration impossible as a method on `StorageBackend`, so `vs-adapter/storage-backend-enum` keeps that one operation engine-side. That clause is load-bearing for the enum's shape and is NOT amended.
* The "serde encoding of `StorageProps` MUST be unchanged field-for-field" clause stays TRUE and is NOT amended: `StorageBackend::S3` wraps `StorageProps` as its payload without editing the struct, so the payload's encoding is byte-identical and the added variant tag belongs to the enclosing enum. `datafusion-scan/scan-execution-spec-reconstitution` owns the common blob's `storage` field encoding.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: One crate declares each shared credential type, re-exported at its pre-move engine path

* *GIVEN* `StorageProps` and `CatalogProps` declared in `crate::scan::spec`, `ConnectionCreds` declared in `crate::adapter::connection`, and `redact_credentials` / `redact_secret_values` declared in `crate::scan::emit`
* *WHEN* the extraction lands
* *THEN* `lakehouse-catalog` SHALL declare each of those five items EXACTLY ONCE, and no parallel struct, duplicate definition, or field-by-field conversion function SHALL exist on either side of the crate boundary
* *AND* `lakehouse_engine::scan::spec` SHALL re-export `StorageProps` and `CatalogProps`, `lakehouse_engine::adapter::connection` SHALL re-export `ConnectionCreds`, and `lakehouse_engine::scan::emit` SHALL re-export `redact_credentials` and `redact_secret_values`, each at its pre-move path and pre-move `pub` visibility
* *AND* `lakehouse_engine::scan::spec` SHALL likewise re-export `StorageBackend` at `pub` visibility, because the enum declared by `vs-adapter/storage-backend-enum` wraps `StorageProps` and is the type the scan layer's consumers now hold
* *AND* every in-repo consumer MUST compile with NO edit to any `use` path — the 4 `scan/*.rs` runtime modules, the 10 `adapter/**` modules, and the 13 files under `tests/` that name one of these items — so the unedited suites remain the characterization gate
* *AND* the serde encoding of `StorageProps` MUST be unchanged field-for-field, because it is carried into a `CommonScanSpec` field that crosses the UDF boundary as JSON and a renamed or reordered field would break every deployed scan spec
* *AND* `read_connection`, `validate_creds`, `parse_creds`, `storage_block`, `catalog_block`, and `REQUIRED_KEY` SHALL stay in `lakehouse_engine::adapter::connection`, because they interpret the Exasol CONNECTION object and the catalog crate MUST NOT name that delivery mechanism
* *AND* `redact_catalog_error` SHALL NOT survive the extraction in any crate: its `adapter/pushdown/support.rs` declaration SHALL be DELETED and every caller SHALL be repointed at `redact_credentials`, because a function whose whole body is a call to another function with the same argument is the shallow-module red flag `vs-adapter/adapter-module-structure` already records — and re-declaring it on the new crate would restore that alias on a WIDER surface, leaving two of the crate's public names for one function
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The crate exposes the concept-level API and hides every mechanism step

* *GIVEN* the moved catalog code, whose in-crate callers and whose `lakehouse-engine` callers need different subsets of it
* *WHEN* the crate's public surface is declared
* *THEN* exactly these items SHALL be `pub` on `lakehouse-catalog`: the types `CatalogSession`, `ConnectionCreds`, `CatalogProps`, `StorageProps`, `StorageBackend`; the functions `CatalogSession::resolve`, `load_table_any_auth`, `resolve_vended_storage`, `list_namespace_tables`, `parse_table_ident`, `redact_credentials`, `redact_secret_values`; and the methods `StorageBackend::secret_values` and `StorageBackend::file_io`
* *AND* `build_s3_file_io` SHALL NOT be `pub` and SHALL NOT survive in any crate, because `vs-adapter/storage-backend-enum` replaces it with `StorageBackend::file_io` and retaining it would leave two public names for one operation
* *AND* `StorageBackend` SHALL expose NO accessor returning its `StorageProps` payload, so a caller outside the enum's own methods cannot reach the payload to make a backend-specific decision; a payload unwrapper needed by the crate's own tests SHALL be declared in the crate's `#[cfg(test)]` support module and MUST NOT be `pub`
* *AND* `CatalogSession`'s fields SHALL stay private and `CatalogAuth` SHALL stay crate-private, so the auth strategy never leaks through the public interface
* *AND* these SHALL be crate-private: `StorageBackend::catalog_storage_props`, `CatalogAuth`, `resolve_catalog_auth`, `oauth2_client_credentials_grant`, `authed_get_json`, `resolve_load_table_prefix`, `prefix_from_config`, `build_load_table_url`, `glue_catalog_prefix`, `build_rest_catalog`, `inject_catalog_auth_props`, `non_empty`, `redact_catalog_auth_error`, `sign_request`, and every vended-extraction step named by `vs-adapter/pushdown-planning-cloud-credentials`; `catalog_storage_props` is a mechanism step whose only two consumers — `build_rest_catalog` and `file_io` — are both in-crate, so publishing it would widen the surface this scenario exists to narrow
* *AND* an external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs` SHALL name every item of that `pub` set, so narrowing one below `pub` is a build failure rather than a silent gap
* *AND* that probe SHALL additionally assert that the crate's own sources declare NO `pub fn extract_vended_keys`, NO `pub fn merge_vended_into_storage`, NO `pub fn select_credential_source`, and NO `pub fn build_s3_file_io`, so the demotions the extraction and issue #274 perform cannot be silently reversed
* *AND* `build_rest_catalog`, `glue_catalog_prefix`, and `sign_request` SHALL reach crate-private visibility ONLY because `list_namespace_tables` moves with them; leaving namespace enumeration behind would force all three to stay public to serve one engine-side caller
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Behavior is unchanged across the extraction

* *GIVEN* the pre-extraction unit, integration, and E2E suites
* *WHEN* the suites run against the extracted layout
* *THEN* every test MUST pass with no change to any test assertion or expected value, EXCEPT the four edits the redraw itself requires — the two pushdown probe `use` lists, the four external `resolve_file_list` call sites, the tests that named a now-crate-private vended function, and the tests that named a moved `crate::`-relative path — and EXCEPT the scan spec's `storage` value wherever an assertion or committed fixture embeds one
* *AND* the scan-driving SQL generated for a given pushdown request MUST be byte-identical to the pre-extraction output EXCEPT for that `storage` value, which `vs-adapter/storage-backend-enum` re-encodes as an externally-tagged backend variant over a byte-identical payload, proven by the committed `dispatch_golden` goldens and the join golden-SQL full-string assertions passing with the `storage` value as their ONLY edit
* *AND* the catalog request URLs, the header set on each request, and the OAuth2 grant request body MUST be identical to the pre-extraction shapes for the same query on every auth mode
* *AND* no catalog secret, no live bearer token, and no vended STS key SHALL appear in any returned SQL string or error message, on any path the extraction touches
<!-- /DELTA:CHANGED -->
