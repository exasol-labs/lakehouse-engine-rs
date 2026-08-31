# Feature: Catalog Authentication Credentials

Carries the REST-catalog authentication credentials on the resolved CONNECTION, beyond the
static-S3 storage credentials covered by `connection-credentials`. The Virtual Schema can reach
an Iceberg REST catalog in one of three mutually exclusive modes: no catalog authentication, a
static bearer `token`, or an OAuth2 client-credentials exchange (`client_id` + `client_secret`,
with optional `oauth2_server_uri` and `scope`). Catalog authentication is fully orthogonal to S3
storage credentials and to credential vending — an unauthenticated catalog may still vend S3
credentials, and an OAuth-authenticated catalog may be used with static S3 credentials.

## Background

* **This delta is issue #135. It adds ONE scenario, changes no auth mode, no classifier, and no error text.** The three mutually exclusive modes, the SigV4 exclusivity rule, the incomplete-OAuth2 rejection, and the single mode classifier are all UNCHANGED.
* **The recorded sentence "they never cross the UDF boundary" STAYS TRUE, but its reason changes, and leaving that implicit would be the silent gap.** Before this plan the sentence held because the scan UDF never read the CONNECTION at all. After it, the scan UDF reads the SAME CONNECTION password on the vending-disabled path (`vs-adapter/scan-spec-credential-reference`) — a document that CONTAINS `token`, `client_id`, `client_secret`, `oauth2_server_uri`, and `scope`. The sentence now holds because of what the UDF deserializes, not because of what it reads.
* **`parse_creds` populates all seventeen `ConnectionCreds` fields**, the five catalog-auth fields among them, with no `..Default::default()` (`crates/lakehouse-engine/src/adapter/connection.rs:310-339`). Running it inside the UDF would materialize all five on every shard invocation, up to 300 per query, outside the storage-only redaction secret set the scan path builds — so a scan-side error carrying a `client_secret` verbatim would get only the value-blind label pass.
* **The UDF therefore deserializes a nine-field storage-only projection and never constructs a `ConnectionCreds` at all.** The exclusion is structural: the type has no field to hold a catalog-auth value, so no code path can put one there. `vs-adapter/connection-credentials` owns that projection; this feature owns the guarantee it preserves.
* **The guarantee is enforced by a source-level probe, mirroring the one `vs-adapter/catalog-crate-public-surface-extensions` already requires of the vended store-address type.** A prose assertion that a type "carries no catalog secret" is what the probe replaces.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The scan UDF reads the same CONNECTION and cannot construct a catalog-auth field

* *GIVEN* a CONNECTION whose JSON password carries a `token` or `client_id` plus `client_secret`, and optionally `oauth2_server_uri` and `scope`, alongside its storage fields
* *AND* a scan UDF invocation that resolves that same CONNECTION by name under `vs-adapter/scan-spec-credential-reference` because `use_vended_credentials` is false
* *WHEN* the UDF deserializes the returned password to derive its storage backend
* *THEN* the UDF SHALL deserialize ONLY the nine-field storage-credential projection, and MUST NOT construct any value declaring a field spelled `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope`
* *AND* a source-level probe SHALL assert from that projection's own declaration that it names no field with any of those five spellings, so widening it into a second catalog-auth path is a test failure rather than a silent regression
* *AND* the recorded guarantee that the catalog-auth fields never cross the UDF boundary SHALL therefore continue to hold, now because of what the UDF's deserialization target CAN declare rather than because the UDF reads no CONNECTION
* *AND* no `token`, `client_secret`, or bearer value minted from either SHALL appear in any returned SQL string or in any error message the UDF returns
<!-- /DELTA:NEW -->
