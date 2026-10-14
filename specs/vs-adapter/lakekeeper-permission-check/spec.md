# Feature: Lakekeeper Permission Check

Refuses a query over a table that the querying Exasol user holds no Lakekeeper grant to read. When a virtual schema sets `PERMISSION_CHECK = 'LAKEKEEPER'`, the adapter maps the querying user to a Lakekeeper principal with the `USER_MAPPING` template and asks Lakekeeper once per query whether that principal can read each table. Without that property the adapter behaves as before and contacts no management API (#415).

## Background

* `PERMISSION_CHECK` is a virtual-schema property. Absent or empty means off. `LAKEKEEPER`, in any letter case, means on. With the check on, `USER_MAPPING` is required. With the check off, `USER_MAPPING` is ignored.
* The check works only for a virtual schema over an Iceberg REST catalog whose URI ends in `/catalog`, and only against a Lakekeeper server. Lakekeeper reports a missing table the same way as a denied one.
* The check covers every Iceberg REST pushdown shape. Creating, refreshing, and altering a virtual schema run no check, so the table listing shows tables that a querying user holds no grant to read (#416 asserts this limitation end to end).
* To check another identity, the CONNECTION's identity needs a Lakekeeper grant that includes `can_read_assignments` on each checked table, for example `manage_grants` on the warehouse or the namespace (#414).
* `USER_MAPPING` is a template. Its one variable, `user`, holds the querying user's name exactly as Exasol reports it, which is uppercase for an undelimited name. The template's output, without leading and trailing whitespace, is the Lakekeeper user id. The adapter assumes no id format.
* A template may use conditions, lookup tables, filters, and tests, and may span several indented lines. It cannot include other templates. A lookup of a missing value fails the query instead of yielding a partial id, and a template that runs too long is refused.
* A mapped id is refused when it is empty or holds a whitespace or control character.
* A template that does not compile is rejected when the virtual schema is created, refreshed, or altered. A template that compiles but fails when it runs, for example through an unknown filter or a missing lookup, refuses each query it is applied to.

## Scenarios

### Scenario: Absent PERMISSION_CHECK leaves every request unchanged and contacts no management API

* *GIVEN* a virtual schema whose properties carry no `PERMISSION_CHECK`, with or without `USER_MAPPING`
* *WHEN* the adapter handles a createVirtualSchema, refresh, setProperties, or pushdown request
* *THEN* the adapter MUST NOT send any request to the Lakekeeper management API
* *AND* the catalog requests, the response, the pushdown SQL, and every error message SHALL be byte-identical to the adapter's output before this feature for the same request
* *AND* an existing virtual schema SHALL keep working with no configuration change, even when its `USER_MAPPING` would not compile

### Scenario: Invalid permission properties are rejected before the catalog is contacted

* *GIVEN* a request whose effective properties set `PERMISSION_CHECK` to a value other than `LAKEKEEPER` in any letter case, or set it to `LAKEKEEPER` with an absent or empty `USER_MAPPING` or one that does not compile, such as `oidc~{{ user|lower @corp.net`
* *WHEN* the adapter handles a createVirtualSchema, refresh, setProperties, or pushdown request
* *THEN* the adapter SHALL return an error without contacting the catalog, and MUST NOT fall back to running with the check off
* *AND* an unrecognized-value error SHALL name the value and the accepted value `LAKEKEEPER`, and SHALL state that an absent `PERMISSION_CHECK` turns the check off
* *AND* a `USER_MAPPING` error SHALL state that the template does not compile and give the line of the mistake
* *AND* no error message SHALL contain a credential value

### Scenario: The check is accepted only for an Iceberg REST catalog URI that ends in /catalog

* *GIVEN* a request whose effective properties set `PERMISSION_CHECK = 'LAKEKEEPER'` with a `USER_MAPPING` that compiles
* *WHEN* the adapter handles a createVirtualSchema, refresh, or setProperties request, or a pushdown request whose current user `USER_MAPPING` maps
* *THEN* under any catalog kind other than Iceberg REST, the adapter SHALL return the error `PERMISSION_CHECK = 'LAKEKEEPER' requires the Iceberg REST catalog kind` without contacting the catalog, also for a virtual schema that carried the property before this feature
* *AND* under Iceberg REST, the adapter SHALL reject a catalog URI that does not end in `/catalog` before any catalog request, with an error that names the URI and states that the check needs a catalog URI ending in `/catalog`
* *AND* a createVirtualSchema, refresh, or setProperties request that passes both checks SHALL list the namespace as it does with the check off, and MUST NOT send any management API request

### Scenario: USER_MAPPING maps the querying user to a principal

* *GIVEN* a virtual schema with the check on
* *WHEN* the current user is `ALICE_COOPER` under `oidc~{{ user|lower|replace("_", ".") }}@corp.net`, or `BOB_EXT` or `ETL_SVC` under a template that spans several indented lines:
  ```
  {% if user == "ETL_SVC" %}
    oidc~6f1c0d2e-58b4-4c39-9a8f-2b1e7d4c0a91
  {% elif user is endingwith("_EXT") %}
    oidc~{{ user[:-4]|lower }}@partner.com
  {% else %}
    oidc~{{ user|lower }}@corp.net
  {% endif %}
  ```
* *THEN* every check of the request SHALL name `oidc~alice.cooper@corp.net`, `oidc~bob@partner.com`, or `oidc~6f1c0d2e-58b4-4c39-9a8f-2b1e7d4c0a91` respectively, so no check runs as the CONNECTION's own identity
* *AND* the adapter SHALL take the principal from `USER_MAPPING` alone, and MUST NOT derive it from the scope user, from another request field, or from a Lakekeeper user search

### Scenario: A user that USER_MAPPING cannot map is refused before any request

* *GIVEN* a virtual schema with the check on and a `USER_MAPPING` that compiles
* *WHEN* a pushdown request arrives whose current user is absent or empty, whose template fails to run, or whose mapped id Background rejects, such as `CAROL` under `{% set ids = {"ALICE": "a.smith"} %}oidc~{{ ids[user] }}@corp.net`, or the delimited user `ALICE SMITH` under `oidc~{{ user }}@corp.net`
* *THEN* the adapter SHALL refuse the query without contacting the catalog or the management API
* *AND* the error SHALL name the user, or state that the request names none, and SHALL state why `USER_MAPPING` cannot map it

### Scenario: The adapter trusts the template author and evaluates no user name as template text

* *GIVEN* a virtual schema with the check on
* *WHEN* `USER_MAPPING` is `oidc~{% if user is startingwith("BI_") %}svc-reporting{% else %}{{ user|lower }}{% endif %}@corp.net` and the current user is `BI_TABLEAU` or `BI_POWERBI`
* *THEN* every check of both requests SHALL name the one principal `oidc~svc-reporting@corp.net`, because the adapter checks neither the uniqueness nor the ownership of a principal
* *AND* under `oidc~{{ user }}@corp.net`, the delimited user `{{7*7}}` SHALL get the principal `oidc~{{7*7}}@corp.net`, and the delimited user `A"B\C` SHALL get the principal `oidc~A"B\C@corp.net` exactly, so no user name changes the template, the request, or another user's principal

### Scenario: One batch-check per query decides every table before any table is read

* *GIVEN* a virtual schema with the check on over a Lakekeeper catalog, and a pushdown request of any shape: row scan, single-group aggregate, grouped aggregate, COUNT(DISTINCT), top-N, a qualified fallback wrapper, or an inner join, a self-join included
* *WHEN* Lakekeeper allows every table that the request reads
* *THEN* the adapter SHALL send exactly one batch-check to the management API for the whole request, with one `read_data` check per distinct table, before it reads any table's metadata
* *AND* the pushdown SQL SHALL be byte-identical to the SQL that the same request yields with the check off
* *AND* the adapter SHALL NOT read any table that the batch-check did not cover, so a query shape added later cannot read an unchecked table

### Scenario: A denied table refuses the whole query

* *GIVEN* a virtual schema with the check on and a pushdown request over one or more tables
* *WHEN* Lakekeeper answers `allowed: false` for a checked table, as it does for a denied or a missing table
* *THEN* the adapter SHALL refuse the whole query, and MUST NOT read any table's metadata or return any SQL
* *AND* the error SHALL name the Exasol user, the principal, and every denied table, MUST NOT name a table that Lakekeeper allows, and SHALL state that only a Lakekeeper grant naming that principal authorizes the user

### Scenario: A failed batch-check refuses the query with an error that names the cause

* *GIVEN* a virtual schema with the check on and a pushdown request
* *WHEN* the batch-check cannot be sent, answers a non-2xx status, or answers a body that is not a batch-check answer
* *THEN* the adapter SHALL refuse the query and MUST NOT read any table's metadata, so an answer that it cannot read never counts as allowed
* *AND* the error SHALL name the batch-check URL, the HTTP status, if any, and the redacted answer body, if any, and, except for a 403 `CannotInspectPermissions`, SHALL state that the check needs a Lakekeeper server as the catalog
* *AND* for a 403 `CannotInspectPermissions` the error SHALL state that the CONNECTION's identity needs a grant that includes `can_read_assignments`, such as `manage_grants` on the warehouse or the namespace
* *AND* no error SHALL contain the CONNECTION's `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope`, or the session's bearer token, even when the answer body echoes them

### Scenario: Grants on the mapped principal decide a live Exasol user's query

* *GIVEN* a virtual schema with the check on over a Lakekeeper-managed table, and three Exasol users that may query the virtual schema: one whose mapped principal holds a Lakekeeper grant on the table, one whose principal holds a grant on another table only, and one whose principal Lakekeeper has never seen
* *WHEN* each user runs the same `SELECT` over the table
* *THEN* the user with the grant SHALL receive the table's rows, and each other user SHALL receive the denial of "A denied table refuses the whole query", although the user with the grant ran the identical statement first
* *AND* the stored `USER_MAPPING` SHALL be exactly the template that the operator set

### Scenario: A table that the user cannot read stays listed and is refused at query time

* *GIVEN* a virtual schema with the check on, and a user that may query it whose mapped principal holds no grant on one of its tables
* *WHEN* the user reads the virtual schema's table and column listing and queries that table, once after the virtual schema is created and once after it is refreshed
* *THEN* the listing SHALL show the user that table and its columns
* *AND* the user's query SHALL receive the denial of "A denied table refuses the whole query"
