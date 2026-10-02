# Feature: Lakekeeper Permission Client

Asks Lakekeeper, in one call on a query's catalog session, whether one principal is allowed to read each table of a set. The client returns one decision per table, or an error that carries no credential. It serves `vs-adapter/lakekeeper-permission-check` (#415).

## Background

* Endpoint: `POST <management base>/v1/action/batch-check` of Lakekeeper `v0.13.1`. The fixtures in `crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/` pin the request and response shapes (#414).
* Request: `{"checks": [...], "error-on-not-found": false}`. Each check is `{"id", "identity": {"user": <principal>}, "operation": {"table": {"warehouse-id", "namespace", "table", "action": {"action": "read_data"}}}}`. `namespace` holds the catalog identifier's namespace segments, and `table` holds its last segment.
* Response: `{"results": [{"id", "allowed"}]}`. With `error-on-not-found: false`, a missing table answers `allowed: false`, the same answer as a denied table (`missing.json`, `denied.json`).
* Warehouse id: Lakekeeper serves the warehouse id as the `/v1/config` `defaults.prefix`. The Iceberg REST catalog spec defines `prefix` only as "An optional prefix in the path", so this reading is specific to Lakekeeper. The client takes the warehouse id from the session's resolved prefix and accepts only a UUID.
* Checking another identity needs `can_read_assignments` on each checked table, for example `manage_grants` on the warehouse or on the namespace (#414). Without it, Lakekeeper fails the whole batch with 403 `CannotInspectPermissions` and no `results` (`cannot-inspect.json`).
* The client sends the request with the session's HTTP client and resolved catalog auth, and only to the scheme, host, and port of the session's catalog URI.

## Scenarios

### Scenario: The batch-check request carries one identity-bearing read check per distinct table

* *GIVEN* a resolved catalog session whose prefix is a warehouse UUID, a principal, and a list of catalog identifiers that can repeat
* *WHEN* the client checks read access
* *THEN* the client SHALL send exactly one batch-check request with one `read_data` table check per distinct identifier, in first-seen order, each check under a unique id
* *AND* every check SHALL carry the principal as `identity.user`, and the request type SHALL make a check without an identity unrepresentable
* *AND* the request SHALL set `error-on-not-found` to `false`, and SHALL equal the `request` of each fixture once the fixture's placeholders are substituted

### Scenario: Each table's decision is read from the result that carries its check id

* *GIVEN* a batch-check answer with status 200
* *WHEN* the client reads the answer
* *THEN* the client SHALL return one decision per checked table, taken from the result whose id matches that table's check id, so `allowed.json` yields allowed and `denied.json` and `missing.json` yield denied
* *AND* the client SHALL treat as malformed an answer that lacks a result for a check, carries an unknown or a duplicate id, or carries a non-boolean `allowed`

### Scenario: A failed or malformed batch-check is an error that carries no credential

* *GIVEN* a session whose prefix is empty or not a UUID, an identifier with no namespace segment, a management base whose scheme, host, or port differs from the session's catalog URI, an unreachable management API, a non-2xx answer, or a 2xx body that is not the response shape
* *WHEN* the client checks read access
* *THEN* the client SHALL return an error and no decision, so an answer that the client cannot verify never counts as allowed
* *AND* the client SHALL detect a check it cannot build before it sends any HTTP request
* *AND* no error message SHALL contain the CONNECTION's `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope`, or the session's bearer token, even when the answer body echoes them

### Scenario: A caller that cannot inspect permissions gets an error that names the missing privilege

* *GIVEN* a CONNECTION identity without `can_read_assignments` on a checked table, which Lakekeeper answers as `cannot-inspect.json` records
* *WHEN* the client checks read access
* *THEN* the client SHALL return an error that names `CannotInspectPermissions` and states that the CONNECTION's identity needs a grant that includes `can_read_assignments`, such as `manage_grants` on the warehouse or the namespace
* *AND* the error MUST NOT contain a credential value
* *AND* on the live `lakekeeper-e2e` stack, a pushdown through a CONNECTION that authenticates as such an identity SHALL be refused with that error

### Scenario: The Lakekeeper client extends the catalog crate's public surface through a reviewed probe edit

* *GIVEN* the enumerated `pub` surface of `lakehouse-catalog` and its reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`
* *WHEN* the Lakekeeper client is added to the crate
* *THEN* the crate SHALL add exactly three items, each re-exported at the crate root: the management-base type with its resolver, the `batch_check` free function that takes `&CatalogSession`, and the per-table decision type
* *AND* the request and response wire types SHALL stay crate-private, and `CatalogSession` SHALL gain no public method and no public field
* *AND* the probe SHALL be edited to name the three items, and no `lakehouse-catalog` source SHALL name `lakehouse-engine`, an Exasol user, or a virtual-schema property
