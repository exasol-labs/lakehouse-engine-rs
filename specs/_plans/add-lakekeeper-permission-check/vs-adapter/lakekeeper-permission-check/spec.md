# Feature: Lakekeeper Permission Check

Refuses a query over a table that the querying Exasol user holds no Lakekeeper grant to read. When a virtual schema sets `PERMISSION_CHECK = 'LAKEKEEPER'`, the adapter renders the querying user's Lakekeeper principal from the `USER_MAPPING` template and asks Lakekeeper once per query whether that principal can read each table. Without that property the adapter behaves as before and contacts no management API (#415).

## Background

* `PERMISSION_CHECK` is a virtual-schema property. Absent or empty means off. `LAKEKEEPER`, in any letter case, means on. With the check on, `USER_MAPPING` is required. With the check off, the adapter ignores `USER_MAPPING`.
* The check works only under the Iceberg REST catalog kind. The management API URL is the catalog URI with its trailing `/catalog` replaced by `/management`, after one trailing `/` is dropped: `http://lakekeeper:8181/catalog/` gives `http://lakekeeper:8181/management`.
* The check is one `POST <management API URL>/v1/action/batch-check` per query, with one `read_data` check per table. Each check names the principal, and it names the warehouse by the catalog's `/v1/config` prefix, which Lakekeeper sets to the warehouse id. The Iceberg REST catalog spec defines `prefix` only as "An optional prefix in the path", so this reading holds for Lakekeeper only. The #414 fixtures record the request and answer shapes.
* A batch-check answer is a JSON object whose `results` hold exactly one boolean `allowed` for each check id. Lakekeeper answers `allowed: false` for a missing table, the same as for a denied one.
* To check another identity, the caller needs `can_read_assignments` on each checked table, for example through `manage_grants` on the warehouse or the namespace (#414). Without it, Lakekeeper fails the whole check with 403 `CannotInspectPermissions`.
* `USER_MAPPING` is a MiniJinja template (`minijinja` 2.24.0, built-in filters and tests only). Its one variable `user` holds the querying user's name exactly as Exasol reports it, which is uppercase for an undelimited name. The rendered output, with leading and trailing whitespace trimmed, is the principal, the Lakekeeper user id. The adapter assumes no id format such as `<idp>~<subject>`.
* The template engine treats an undefined value as an error, so a missing lookup fails the render instead of yielding a partial principal such as `oidc~@corp.net`. It has no template loader, so `{% include %}`, `{% import %}`, `{% extends %}`, and `{% macro %}` are syntax errors. It escapes no output, and it stops a render after 100,000 fuel units. `trim_blocks` and `lstrip_blocks` are on, so a template over several indented lines renders the selected text plus whitespace that the trim removes.
* A principal is rejected when it is empty or holds a whitespace or control character. The user name is a template value only and is never compiled as template text. The principal enters the batch-check body only as a serialized JSON string.
* createVirtualSchema, refresh, and setProperties compile `USER_MAPPING` and reject a syntax error. They do not render it, so an unknown filter, an undefined value, or exhausted fuel shows only when a query renders the template.
* Coverage: every Iceberg REST pushdown shape. createVirtualSchema, refresh, and setProperties run no check, so the listing shows tables that a querying user holds no grant to read (#416 asserts this limitation end to end).

## Scenarios

### Scenario: Absent PERMISSION_CHECK leaves every request unchanged and contacts no management API

* *GIVEN* a virtual schema whose properties carry no `PERMISSION_CHECK`, with or without `USER_MAPPING`
* *WHEN* the adapter handles a createVirtualSchema, refresh, setProperties, or pushdown request
* *THEN* the adapter MUST NOT send any request to the Lakekeeper management API
* *AND* the catalog requests, the response, the pushdown SQL, and every error message SHALL be byte-identical to the adapter's output before this feature for the same request
* *AND* the adapter MUST NOT compile `USER_MAPPING`, so an existing virtual schema keeps working with no configuration change

### Scenario: Invalid permission properties are rejected before the CONNECTION is read

* *GIVEN* a request whose effective properties set `PERMISSION_CHECK` to a value other than `LAKEKEEPER` in any letter case, or set it to `LAKEKEEPER` with an absent or empty `USER_MAPPING` or one that does not compile, such as `oidc~{{ user|lower @corp.net` or `{% include "ids" %}`
* *WHEN* the adapter handles a createVirtualSchema, refresh, setProperties, or pushdown request
* *THEN* the adapter SHALL return an error before it reads the CONNECTION, and MUST NOT fall back to running with the check off
* *AND* an unrecognized-value error SHALL name the value and the accepted value `LAKEKEEPER`, and SHALL state that an absent `PERMISSION_CHECK` turns the check off
* *AND* a `USER_MAPPING` error SHALL state that the template does not compile and carry the template engine's message with its line number
* *AND* no error message SHALL contain a credential value

### Scenario: The check is accepted only for an Iceberg REST catalog URI that ends in /catalog

* *GIVEN* a request whose effective properties set `PERMISSION_CHECK = 'LAKEKEEPER'` with a `USER_MAPPING` that compiles
* *WHEN* the adapter handles a createVirtualSchema, refresh, or setProperties request, or a pushdown request whose current user `USER_MAPPING` maps
* *THEN* under any catalog kind other than Iceberg REST, the adapter SHALL return the error `PERMISSION_CHECK = 'LAKEKEEPER' requires the Iceberg REST catalog kind` before it reads the CONNECTION, also for a virtual schema that carried the property before this feature
* *AND* under Iceberg REST, the adapter SHALL reject a catalog URI that does not end in `/catalog` before any catalog request, with an error that names the URI and states that the check needs a catalog URI ending in `/catalog`
* *AND* a createVirtualSchema, refresh, or setProperties request that passes both checks SHALL list the namespace as it does with the check off, and MUST NOT send any management API request

### Scenario: USER_MAPPING renders the querying user's principal

* *GIVEN* a virtual schema with the check on, and a pushdown request whose scope user is `OWNER`
* *WHEN* the current user is `ALICE` under `oidc~{{ user|lower }}@corp.net`, `EXA_ALICE` under `oidc~{{ user[4:]|lower }}@corp.net`, or `ALICE_COOPER` under `oidc~{{ user|lower|replace("_", ".") }}@corp.net`
* *THEN* every check of the request SHALL name `oidc~alice@corp.net`, `oidc~alice@corp.net`, or `oidc~alice.cooper@corp.net` respectively, so no check runs as the CONNECTION's own identity
* *AND* `{% if user == "ETL_SVC" %}oidc~6f1c0d2e-58b4-4c39-9a8f-2b1e7d4c0a91{% elif user is endingwith("_EXT") %}oidc~{{ user[:-4]|lower }}@partner.com{% else %}oidc~{{ user|lower|replace("_", ".") }}@corp.net{% endif %}`, also with each tag and each branch on its own indented line, SHALL map `ETL_SVC` to `oidc~6f1c0d2e-58b4-4c39-9a8f-2b1e7d4c0a91`, `BOB_EXT` to `oidc~bob@partner.com`, and `ALICE_COOPER` to `oidc~alice.cooper@corp.net`
* *AND* `{% set ids = {"ETL_SVC": "6f1c0d2e-58b4-4c39-9a8f-2b1e7d4c0a91", "DBT_RUN": "0b7e41aa-1d2c-4e8f-9a33-5c6d7e8f9012"} %}{% if user in ids %}oidc~{{ ids[user] }}{% else %}oidc~{{ user|lower }}@corp.net{% endif %}` SHALL map `DBT_RUN` to `oidc~0b7e41aa-1d2c-4e8f-9a33-5c6d7e8f9012` and `ALICE` to `oidc~alice@corp.net`
* *AND* the adapter SHALL read `USER_MAPPING` only from the virtual schema's properties, and MUST NOT derive the principal from the scope user, from another request field, or from a Lakekeeper user search

### Scenario: A user that USER_MAPPING cannot map is refused before any request

* *GIVEN* a virtual schema with the check on and a `USER_MAPPING` that compiles
* *WHEN* a pushdown request arrives whose current user is absent or empty, whose render fails, or whose rendered principal Background rejects
* *THEN* the adapter SHALL refuse the query with an error that names the user, or states that the request names none, and states why `USER_MAPPING` cannot map it, and MUST NOT read the CONNECTION or send any request
* *AND* the adapter SHALL refuse `CAROL` under `{% set ids = {"ALICE": "a.smith", "BOB": "b.jones"} %}oidc~{{ ids[user] }}@corp.net`, `SYS` under `{% if user not in ["SYS", "ADMIN"] %}oidc~{{ user|lower }}@corp.net{% endif %}`, and the delimited user `alice` under `{% if user == user|upper %}oidc~{{ user|lower }}@corp.net{% endif %}`
* *AND* the adapter SHALL refuse the delimited user `ALICE SMITH` under `oidc~{{ user }}@corp.net`, and every user under a template that names an unknown filter or runs out of fuel

### Scenario: The adapter trusts the template author and evaluates no user name as template text

* *GIVEN* a virtual schema with the check on
* *WHEN* `USER_MAPPING` is `oidc~{% if user is startingwith("BI_") %}svc-reporting{% else %}{{ user|lower }}{% endif %}@corp.net` and the current user is `BI_TABLEAU` or `BI_POWERBI`
* *THEN* every check of both requests SHALL name the one principal `oidc~svc-reporting@corp.net`, because the adapter checks neither the uniqueness nor the ownership of a principal
* *AND* createVirtualSchema, refresh, and setProperties SHALL accept every template that compiles, even one that maps a user to another person's principal
* *AND* under `oidc~{{ user }}@corp.net`, the delimited user `{{7*7}}` SHALL get the principal `oidc~{{7*7}}@corp.net`, because the adapter MUST NOT compile a user name as template text
* *AND* the delimited user `A"B\C` under the same template SHALL get the unescaped principal `oidc~A"B\C@corp.net`, and the batch-check body SHALL carry each principal as a JSON string that decodes to exactly that principal

### Scenario: One batch-check per query decides every table before any table metadata is read

* *GIVEN* a virtual schema with the check on over a Lakekeeper catalog, and a pushdown request of any shape: row scan, single-group aggregate, grouped aggregate, COUNT(DISTINCT), top-N, a qualified fallback wrapper, or an inner join, a self-join included
* *WHEN* the adapter plans the request and Lakekeeper allows every checked table
* *THEN* the adapter SHALL send exactly one batch-check to the management API URL that Background defines, after the catalog's `/v1/config` lookup and before the first `loadTable` GET, with one `read_data` check per distinct table that the request loads
* *AND* each check SHALL name the table by the namespace and table name that its `loadTable` GET addresses, and the warehouse by the `/v1/config` prefix, sent unchanged
* *AND* the pushdown SQL SHALL be byte-identical to the SQL that the same request yields with the check off
* *AND* the adapter SHALL refuse to load any table that the batch-check did not cover, so a query shape added later cannot read an unchecked table

### Scenario: A denied table refuses the whole query

* *GIVEN* a virtual schema with the check on and a pushdown request over one or more tables
* *WHEN* Lakekeeper answers `allowed: false` for a checked table, as it does for a denied or a missing table
* *THEN* the adapter SHALL refuse the whole query, and MUST NOT send any `loadTable` GET or return any SQL
* *AND* the error SHALL name the Exasol user, the principal, and every denied table, and SHALL state that only a Lakekeeper grant naming that principal authorizes the user

### Scenario: A failed batch-check refuses the query with an error that names the cause

* *GIVEN* a virtual schema with the check on and a pushdown request
* *WHEN* the batch-check cannot be sent, answers a non-2xx status, or answers a body that is not a batch-check answer
* *THEN* the adapter SHALL refuse the query and MUST NOT send any `loadTable` GET, so an answer that it cannot read never counts as allowed
* *AND* the error SHALL name the batch-check URL, the HTTP status, if any, and the redacted answer body, if any, so that a 422 names its cause, and for every non-2xx answer other than a 403 `CannotInspectPermissions` and every 2xx body that is not a batch-check answer SHALL state that the check needs a Lakekeeper server as the catalog
* *AND* for a 403 `CannotInspectPermissions` the error SHALL state that the CONNECTION's identity needs a grant that includes `can_read_assignments`, such as `manage_grants` on the warehouse or the namespace, and a pushdown on the live `lakekeeper-e2e` stack through such a CONNECTION SHALL get that error
* *AND* no error SHALL contain the CONNECTION's `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope`, or the session's bearer token, even when the answer body echoes them

### Scenario: Grants on the mapped principal decide a live Exasol user's query

* *GIVEN* the `lakekeeper-e2e` stack, a virtual schema with the check on over a seeded table whose `USER_MAPPING` spans three lines, `{% if user is startingwith("LK_PERM_") %}`, a tab and `oidc~lk.{{ user[8:]|lower }}@lakehouse.test`, and `{% endif %}`, and three Exasol users granted `SELECT` on the virtual schema: one whose mapped principal holds a Lakekeeper `select` grant on the table, one whose principal holds a grant on another table only, and one whose principal Lakekeeper has never seen
* *WHEN* each user runs the same `SELECT` over the table, the granted user first
* *THEN* `EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES` SHALL return the `USER_MAPPING` value byte for byte, and the granted user SHALL receive the seeded rows, which proves that the template reaches the adapter intact, that the adapter reads the querying user, and that a grant on a mapped id authorizes that user
* *AND* each of the two other users SHALL receive the denial of "A denied table refuses the whole query", although the granted user ran the byte-identical statement just before
* *AND* the test MUST fail, not skip, when the stack is unavailable
