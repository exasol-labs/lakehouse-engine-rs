# Feature: Catalog Authentication Credentials

Carries the REST-catalog authentication credentials on the resolved CONNECTION. A CONNECTION uses one of three mutually exclusive modes: no catalog authentication, a static bearer `token`, or an OAuth2 client-credentials exchange (`client_id` and `client_secret`, with optional `oauth2_server_uri` and `scope`).

## Background

* Catalog authentication is independent of S3 storage credentials and of credential vending. An unauthenticated catalog can vend S3 credentials, and an OAuth-authenticated catalog can use static S3 credentials.
* The catalog-auth fields live on the JSON CONNECTION password parsed by `connection-credentials` and never cross the UDF boundary.
* Catalog token or OAuth authentication and AWS SigV4 signing are mutually exclusive. SigV4 signs the `load_table` request with static AWS credentials, and token or OAuth authenticates to the REST catalog itself.
* The validation rules leave exactly three accepted shapes: no auth field, a `token` alone, or a complete `client_id` and `client_secret` pair. A `token` with only one of the pair is rejected by the OAuth2-completeness rule, which is disjoint from the token-plus-pair rule.
* The token-plus-pair rule applies to every catalog kind, because Unity Catalog reuses the same three fields.
* An empty-string field is absent.
* No authentication value (`token`, `client_secret`) appears in any error message, returned SQL, or log line.

## Scenarios

### Scenario: Static bearer token is exposed on the resolved credentials

* *GIVEN* a CONNECTION whose JSON password supplies `warehouse` and a non-empty `token`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL expose the resolved `token` on the credentials
* *AND* the adapter SHALL treat `oauth2_server_uri` and `scope` as not applicable to the token mode
* *AND* the resolved `token` value MUST NOT appear in any error message

### Scenario: OAuth2 client credentials are exposed on the resolved credentials

* *GIVEN* a CONNECTION whose JSON password supplies `warehouse`, a non-empty `client_id`, and a non-empty `client_secret`, and optionally `oauth2_server_uri` and `scope`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL expose the resolved `client_id`, `client_secret`, and the optional `oauth2_server_uri` and `scope` on the credentials
* *AND* the adapter SHALL treat `oauth2_server_uri` and `scope` as optional, leaving them absent when not supplied
* *AND* the resolved `client_secret` value MUST NOT appear in any error message

### Scenario: Incomplete OAuth2 client credentials are rejected naming only the missing field

* *GIVEN* a CONNECTION whose JSON password supplies `warehouse` and `client_id` but omits `client_secret` (or supplies `client_secret` but omits `client_id`)
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error stating that OAuth2 client credentials require both `client_id` and `client_secret` and naming the missing one
* *AND* the error message MUST NOT contain the supplied `client_id` or `client_secret` value

### Scenario: Catalog token/OAuth auth and SigV4 are mutually exclusive

* *GIVEN* a CONNECTION whose JSON password sets `use_sigv4` to true AND also supplies a catalog-auth field (`token`, or `client_id`/`client_secret`)
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error stating that SigV4 signing and catalog token/OAuth authentication cannot both be enabled
* *AND* the error message MUST NOT contain any supplied credential value

### Scenario: A CONNECTION supplying both a static token and OAuth2 client credentials is rejected

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `token` AND a non-empty `client_id` AND a non-empty `client_secret`
* *WHEN* the adapter resolves the connection under `CatalogKind::IcebergRest` or under `CatalogKind::UnityCatalogNative`
* *THEN* the adapter SHALL return an error stating that a static bearer `token` and OAuth2 client credentials cannot both be supplied on one CONNECTION, naming `token` and both `client_id` and `client_secret`
* *AND* the adapter SHALL reject this input identically under BOTH catalog kinds, because the three fields carry the same meaning under each and a per-kind answer is the defect this rule removes
* *AND* the adapter MUST NOT apply a precedence rule between the static token and the client-credentials pair, because an undeclared precedence resolves an ambiguous credentials input silently
* *AND* the adapter SHALL leave every other rule of `connection/connection-credentials` byte-identical, so a CONNECTION supplying a `token` with only ONE of `client_id` and `client_secret` SHALL still be rejected by the OAuth2-completeness rule with its existing message, the two rules are disjoint
* *AND* the error message MUST NOT contain the supplied `token`, `client_id`, or `client_secret` value

### Scenario: One classifier decides the catalog-auth mode and every consumer reads it

* *GIVEN* the three mutually exclusive catalog-auth modes this feature declares, and the three consumers that select behaviour from them, the Iceberg REST auth-strategy resolution, the Iceberg REST catalog-prop injection, and the Unity Catalog auth-strategy resolution
* *WHEN* any consumer decides which mode a resolved credential set describes
* *THEN* exactly ONE function SHALL map `token`, `client_id`, and `client_secret` to one of the three modes, and each consumer SHALL select its behaviour by matching that function's result
* *AND* no consumer SHALL re-derive the mode from the three fields, so the two catalog kinds answer identically BY CONSTRUCTION rather than by coincidence
* *AND* that function MUST NOT encode an order between the `token` and the `client_id`/`client_secret` pair, it SHALL name each accepted shape as a pattern over WHICH fields are present, so a credential set describes exactly one mode or none
* *AND* that function SHALL treat an empty-string field as absent, matching the parsing rule `connection/connection-credentials` already applies to every field
* *AND* every combination the validation rules reject SHALL classify as the NO-AUTH mode, and the function's doc comment SHALL name those rules as its enforcer, so a credential set that reached a consumer without passing validation surfaces as the catalog's own authentication failure rather than as a silently chosen credential the operator never unambiguously supplied
* *AND* no `token`, `client_secret`, or minted bearer value SHALL appear in any error message, returned SQL, or log line on any mode

### Scenario: The scan UDF reads the same CONNECTION and cannot construct a catalog-auth field

* *GIVEN* a CONNECTION whose JSON password carries a `token` or `client_id` plus `client_secret`, and optionally `oauth2_server_uri` and `scope`, alongside its storage fields
* *AND* a scan UDF invocation that resolves that same CONNECTION by name under `storage-access/scan-spec-credential-reference` because `use_vended_credentials` is false
* *WHEN* the UDF deserializes the returned password to derive its storage backend
* *THEN* the UDF SHALL deserialize ONLY the nine-field storage-credential projection, and MUST NOT construct any value declaring a field spelled `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope`
* *AND* a source-level probe SHALL assert from that projection's own declaration that it names no field with any of those five spellings, so widening it into a second catalog-auth path is a test failure rather than a silent regression
* *AND* the guarantee that the catalog-auth fields never cross the UDF boundary SHALL hold because of what the UDF's deserialization target CAN declare rather than because the UDF reads no CONNECTION
* *AND* no `token`, `client_secret`, or bearer value minted from either SHALL appear in any returned SQL string or in any error message the UDF returns
