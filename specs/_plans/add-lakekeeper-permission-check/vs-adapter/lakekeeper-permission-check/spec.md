# Feature: Lakekeeper Permission Check

Refuses a query over a table that the querying Exasol user holds no grant to read. When a virtual schema sets `PERMISSION_CHECK = 'LAKEKEEPER'`, the adapter asks Lakekeeper once per query and enforces the answer. Without that property the adapter behaves as before and contacts no management API (#415).

## Background

* `PERMISSION_CHECK` is a virtual-schema property. Absent or empty means off. `LAKEKEEPER`, compared case-insensitively, means on.
* With the check on, `USER_MAPPING` is required and `LAKEKEEPER_MANAGEMENT_URL` is optional. With the check off, the adapter ignores both.
* The adapter reads all three only from the virtual schema's properties. The querying user influences the principal only through their own user name.
* `USER_MAPPING` grammar: `<idp>~<subject>`. `<idp>` is non-empty literal text. `<subject>` is literal text around exactly one placeholder: `$1` (the user name unchanged) or `$lower($1)` (the user name in ASCII lowercase). Literal text is printable ASCII without whitespace, `$`, or `~`. Example: `oidc~$lower($1)@corp` maps `ALICE` to `oidc~alice@corp`.
* Accepted user names: `[A-Za-z0-9_]+` under `$1`, and `[A-Z0-9_]+` under `$lower($1)`. The uppercase rule under `$lower($1)` stops a delimited user `"alice"` from taking the principal of `ALICE`.
* The principal is the Lakekeeper user id that every check names. A Lakekeeper grant authorizes a mapped user only when the grant names the template-derived id. The adapter never searches Lakekeeper's users for a match.
* The management API base is the catalog URI with its last path segment `catalog` replaced by `management`: `http://lakekeeper:8181/catalog` gives `http://lakekeeper:8181/management`. `LAKEKEEPER_MANAGEMENT_URL` replaces the derived base for a gateway that rewrites paths. It MUST share the catalog URI's scheme, host, and port, because the check carries the catalog session's bearer token.
* The check is one `batch-check` call per pushdown request (`vs-adapter/lakekeeper-permission-client`) on the request's catalog session (`vs-adapter/pushdown-catalog-session`).
* Coverage: every Iceberg REST pushdown shape. The Unity Catalog and direct-storage kinds have no permission client. `createVirtualSchema`, `refresh`, and `setProperties` run without a check, so the listing shows tables that a querying user holds no grant to read (#416 asserts this limitation end to end).

## Scenarios

### Scenario: Absent PERMISSION_CHECK leaves every request unchanged and contacts no management API

* *GIVEN* a virtual schema whose properties carry no `PERMISSION_CHECK`, with or without `USER_MAPPING` and `LAKEKEEPER_MANAGEMENT_URL`
* *WHEN* the adapter handles a createVirtualSchema, refresh, setProperties, or pushdown request
* *THEN* the adapter MUST NOT send any request to the Lakekeeper management API
* *AND* the catalog requests, the response, the pushdown SQL, and every error message SHALL be byte-identical to the adapter's output before this feature for the same request
* *AND* the adapter MUST NOT validate `USER_MAPPING` or `LAKEKEEPER_MANAGEMENT_URL`, so an existing virtual schema keeps working with no configuration change

### Scenario: Invalid permission properties are rejected before the CONNECTION is read

* *GIVEN* a request whose effective properties set `PERMISSION_CHECK` to a value other than `LAKEKEEPER` in any letter case, or set it to `LAKEKEEPER` with an absent `USER_MAPPING` or one that breaks the grammar in Background
* *WHEN* the adapter handles a createVirtualSchema, refresh, setProperties, or pushdown request
* *THEN* the adapter SHALL return an error before it reads the CONNECTION, and MUST NOT fall back to running with the check off
* *AND* an unrecognized-value error SHALL name the value and the accepted value `LAKEKEEPER`, and SHALL state that an absent `PERMISSION_CHECK` turns the check off
* *AND* a `USER_MAPPING` error SHALL name the rule the template breaks: no `~`, an empty `<idp>`, a placeholder in `<idp>`, zero or several placeholders, an unknown placeholder, or a forbidden literal character
* *AND* no error message SHALL contain a credential value

### Scenario: The management API base is derived from the catalog URI or set on the catalog's origin

* *GIVEN* a virtual schema with the check on and a valid `USER_MAPPING`
* *WHEN* the adapter resolves the management API base for a createVirtualSchema, refresh, setProperties, or pushdown request
* *THEN* the adapter SHALL derive the base as Background states when `LAKEKEEPER_MANAGEMENT_URL` is absent, and SHALL use `LAKEKEEPER_MANAGEMENT_URL` when it is present
* *AND* the adapter SHALL reject a `LAKEKEEPER_MANAGEMENT_URL` that is not an absolute `http` or `https` URL, or whose scheme, host, or port differs from the catalog URI
* *AND* the adapter SHALL reject a catalog URI whose path does not end in the segment `catalog` when no `LAKEKEEPER_MANAGEMENT_URL` is set, with an error that names `LAKEKEEPER_MANAGEMENT_URL`
* *AND* every rejection SHALL happen before any catalog request

### Scenario: The principal is derived from the querying user through USER_MAPPING

* *GIVEN* a virtual schema with `PERMISSION_CHECK = 'LAKEKEEPER'` and `USER_MAPPING = 'oidc~$lower($1)@corp'`
* *WHEN* a pushdown request arrives whose UDF context reports the current user `ALICE` and the scope user `OWNER`
* *THEN* the adapter SHALL read the current user from the UDF context in its dispatch function, and every check of the request SHALL name the principal `oidc~alice@corp`
* *AND* the adapter MUST NOT derive the principal from the scope user, from any other request field, or from a Lakekeeper user search
* *AND* no check SHALL omit the identity, because Lakekeeper answers an identity-less check for the CONNECTION's own identity

### Scenario: A user name the mapping cannot accept is refused before any catalog request

* *GIVEN* a virtual schema with the check on
* *WHEN* a pushdown request arrives whose current user is absent, empty, or outside the characters that Background accepts for the template's placeholder, for example `alice` under `$lower($1)` or a name that holds `@`, `~`, whitespace, or a control character
* *THEN* the adapter SHALL refuse the query with an error stating that the current user cannot be mapped to a Lakekeeper principal
* *AND* the adapter MUST NOT send any catalog request
* *AND* two distinct user names that the template's placeholder accepts SHALL map to two distinct principals, for example `ALICE` and `alice` under `$1`

### Scenario: The check runs once per request at the resolution seam before any table load

* *GIVEN* a virtual schema with the check on over an Iceberg REST catalog, and a pushdown request of any shape: row scan, single-group aggregate, grouped aggregate, COUNT(DISTINCT), top-N, a qualified fallback wrapper, or an inner join, a self-join included
* *WHEN* the adapter plans the request and Lakekeeper allows every checked table
* *THEN* the adapter SHALL send exactly one batch-check after the request's catalog session resolves and before its first `loadTable` GET, with one check per distinct catalog identifier that the request resolves, which is the `TABLE_MAP` identifier its `loadTable` GET addresses, so a table alias or case folding cannot make the check and the load name different tables
* *AND* the pushdown SQL SHALL be byte-identical to the SQL that the same request yields with the check off
* *AND* the resolver SHALL refuse to load any table identifier that the batch-check did not cover, so a planning path added later cannot read an unchecked table

### Scenario: A denied or unverifiable answer refuses the query before any table metadata is loaded

* *GIVEN* a virtual schema with the check on and a pushdown request over one or more tables
* *WHEN* Lakekeeper denies a checked table or reports it missing, or the batch-check fails for any reason that `vs-adapter/lakekeeper-permission-client` lists
* *THEN* the adapter SHALL refuse the whole query, and MUST NOT issue any `loadTable` GET or return any SQL
* *AND* a denial SHALL name the Exasol user, the principal, and every denied table identifier, and SHALL state that only a grant naming that principal authorizes the user
* *AND* a failed batch-check SHALL carry the client's reason, and no message SHALL contain a credential value

### Scenario: PERMISSION_CHECK is rejected under the Unity Catalog and direct-storage kinds

* *GIVEN* a request whose effective properties set `PERMISSION_CHECK = 'LAKEKEEPER'` and set `CATALOG_KIND` to `UNITY_CATALOG` or `DIRECT_STORAGE`
* *WHEN* the adapter handles a createVirtualSchema, refresh, setProperties, or pushdown request
* *THEN* the adapter SHALL return an error stating that `PERMISSION_CHECK = 'LAKEKEEPER'` supports only the Iceberg REST catalog kind, before it reads the CONNECTION
* *AND* the pushdown resolver SHALL refuse a permission-checked request under those kinds before any catalog or storage request, so a caller that skips the property check still cannot plan an unchecked query

### Scenario: Listing requests validate the permission properties and run no check

* *GIVEN* a virtual schema with the check on and valid permission properties over an Iceberg REST catalog
* *WHEN* the adapter handles a createVirtualSchema, refresh, or setProperties request
* *THEN* the adapter SHALL list the namespace exactly as it does with the check off, and MUST NOT send any request to the Lakekeeper management API
* *AND* the listing SHALL include tables that a querying user holds no grant to read, and a query over such a table SHALL be refused at pushdown

### Scenario: Grants on the template-derived principal decide a live Exasol user's query

* *GIVEN* the `lakekeeper-e2e` stack, a virtual schema with the check on and `USER_MAPPING = 'oidc~$lower($1)@lakehouse.test'` over a seeded table, and three Exasol users granted `SELECT` on the virtual schema: one whose template-derived principal holds a Lakekeeper `select` grant on the table, one whose principal holds a grant on another table only, and one whose principal Lakekeeper has never seen
* *WHEN* each user runs the same `SELECT` over the table, the granted user first
* *THEN* the granted user SHALL receive the seeded rows, which proves that the adapter reads the querying user and that a grant created for a template-derived id authorizes that user
* *AND* each of the two other users SHALL receive the denial of "A denied or unverifiable answer refuses the query before any table metadata is loaded", although the granted user ran the byte-identical statement just before
* *AND* the test MUST fail, not skip, when the stack is unavailable
