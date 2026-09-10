# Feature: Catalog Authentication Credentials

Carries the REST-catalog authentication credentials on the resolved CONNECTION, beyond the
static-S3 storage credentials covered by `connection-credentials`. The Virtual Schema can reach
an Iceberg REST catalog in one of three mutually exclusive modes: no catalog authentication, a
static bearer `token`, or an OAuth2 client-credentials exchange (`client_id` + `client_secret`,
with optional `oauth2_server_uri` and `scope`). Catalog authentication is fully orthogonal to S3
storage credentials and to credential vending — an unauthenticated catalog may still vend S3
credentials, and an OAuth-authenticated catalog may be used with static S3 credentials.

## Background

The catalog-auth fields live on the same JSON CONNECTION password parsed by
`connection-credentials` and are exposed on the resolved credentials for the planning layer to
consume; they never cross the UDF boundary. Catalog authentication and AWS SigV4 request signing
are mutually exclusive strategies: SigV4 signs the `load_table` request with static AWS
credentials, whereas catalog token/OAuth authenticates to the REST catalog itself, so enabling
both is a configuration error. Every authentication value (`token`, `client_secret`) MUST NEVER
appear in any error message, returned SQL, or log line.

* **This feature is issue #331. It adds ONE validation rule and ONE structural guarantee, and it changes no accepted CONNECTION and no existing error text.** The rule makes the three mutually exclusive modes this feature already declares ENFORCED rather than assumed. Before it, a CONNECTION supplying a `token` together with a complete `client_id`/`client_secret` pair was accepted and one mode silently won — with OPPOSITE precedence per catalog kind: `resolve_catalog_auth` (`crates/lakehouse-catalog/src/auth.rs`) resolved the pair ahead of the `token`, while `resolve_unity_auth` (`crates/lakehouse-catalog/src/unity/auth.rs`) resolved the `token` ahead of the pair. Nothing declared which answer was correct.
* **The rule fires only when ALL THREE of `token`, `client_id`, and `client_secret` are supplied, and the narrowness is deliberate.** A `token` alongside HALF a pair is already rejected by the OAuth2-completeness rule with its existing message, so the two rules are DISJOINT and every error this feature already produces stays byte-identical. Widening the new rule to "a `token` plus ANY OAuth2 field" would replace a recorded error text for an input that is rejected either way — buying one fewer operator round-trip on a doubly-malformed CONNECTION at the cost of changing a message the spec already pins.
* **The two rules together leave exactly three accepted shapes, which is what turns "mutually exclusive" into a guarantee.** Those shapes are: no auth field; a `token` alone; a complete `client_id` + `client_secret` pair. Every other combination of the three fields is a named user error. That closure is the precondition the shared mode classifier below depends on.
* **The rule is kind-independent and lives in the ONE shared validation path.** A native Unity Catalog reuses these same three fields and adds no CONNECTION field of its own (`vs-adapter/connection-credentials`), so a per-kind copy of the rule would be the same duplication that produced the opposite-precedence defect in the first place.
* **The mode DECISION gets one owner, and that is the fix's other half.** Three functions independently re-derived the mode from the same three fields — `resolve_catalog_auth` and `inject_catalog_auth_props` (`crates/lakehouse-catalog/src/auth.rs`) and `resolve_unity_auth` (`crates/lakehouse-catalog/src/unity/auth.rs`) — which is precisely why two of them could disagree. One decision with three homes is the back-door leakage `/speq:design-philosophy` singles out; rejecting the ambiguous input without removing the duplication would leave the three free to drift apart again.
* **`inject_catalog_auth_props`' doc comment already asserted "Token and client-credentials are mutually exclusive by construction" while its own body encoded a precedence.** The claim becomes TRUE by enforcement rather than being deleted, which is the resolution issue #331 chose over deleting the sentence.
* **AWS SigV4 stays OUTSIDE the mode classification, deliberately.** SigV4 is a catalog-auth strategy this feature already declares mutually exclusive with token/OAuth, but only the Iceberg REST path can carry it: the Unity kind rejects `use_sigv4` outright, and the prop-injection path never receives it. Folding SigV4 into the shared classification would force two of the three consumers to handle a case that cannot reach them. The Iceberg strategy resolution therefore dispatches on `use_sigv4` ahead of the classification — a choice between two upstream-exclusive strategies, NOT a precedence.

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
* *AND* the adapter SHALL leave every other rule of `vs-adapter/connection-credentials` byte-identical, so a CONNECTION supplying a `token` with only ONE of `client_id` and `client_secret` SHALL still be rejected by the OAuth2-completeness rule with its existing message — the two rules are disjoint
* *AND* the error message MUST NOT contain the supplied `token`, `client_id`, or `client_secret` value

### Scenario: One classifier decides the catalog-auth mode and every consumer reads it

* *GIVEN* the three mutually exclusive catalog-auth modes this feature declares, and the three consumers that select behaviour from them — the Iceberg REST auth-strategy resolution, the Iceberg REST catalog-prop injection, and the Unity Catalog auth-strategy resolution
* *WHEN* any consumer decides which mode a resolved credential set describes
* *THEN* exactly ONE function SHALL map `token`, `client_id`, and `client_secret` to one of the three modes, and each consumer SHALL select its behaviour by matching that function's result
* *AND* no consumer SHALL re-derive the mode from the three fields, so the two catalog kinds answer identically BY CONSTRUCTION rather than by coincidence
* *AND* that function MUST NOT encode an order between the `token` and the `client_id`/`client_secret` pair — it SHALL name each accepted shape as a pattern over WHICH fields are present, so a credential set describes exactly one mode or none
* *AND* that function SHALL treat an empty-string field as absent, matching the parsing rule `vs-adapter/connection-credentials` already applies to every field
* *AND* every combination the validation rules reject SHALL classify as the NO-AUTH mode, and the function's doc comment SHALL name those rules as its enforcer — so a credential set that reached a consumer without passing validation surfaces as the catalog's own authentication failure rather than as a silently chosen credential the operator never unambiguously supplied
* *AND* no `token`, `client_secret`, or minted bearer value SHALL appear in any error message, returned SQL, or log line on any mode

### Scenario: The scan UDF reads the same CONNECTION and cannot construct a catalog-auth field

* *GIVEN* a CONNECTION whose JSON password carries a `token` or `client_id` plus `client_secret`, and optionally `oauth2_server_uri` and `scope`, alongside its storage fields
* *AND* a scan UDF invocation that resolves that same CONNECTION by name under `vs-adapter/scan-spec-credential-reference` because `use_vended_credentials` is false
* *WHEN* the UDF deserializes the returned password to derive its storage backend
* *THEN* the UDF SHALL deserialize ONLY the nine-field storage-credential projection, and MUST NOT construct any value declaring a field spelled `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope`
* *AND* a source-level probe SHALL assert from that projection's own declaration that it names no field with any of those five spellings, so widening it into a second catalog-auth path is a test failure rather than a silent regression
* *AND* the recorded guarantee that the catalog-auth fields never cross the UDF boundary SHALL therefore continue to hold, now because of what the UDF's deserialization target CAN declare rather than because the UDF reads no CONNECTION
* *AND* no `token`, `client_secret`, or bearer value minted from either SHALL appear in any returned SQL string or in any error message the UDF returns
