# Feature: Lakekeeper Permission Check

Refuses a query over a table that the querying Exasol user holds no Lakekeeper grant to read. When a virtual schema sets `PERMISSION_CHECK = 'LAKEKEEPER'`, the adapter maps the querying user to a Lakekeeper principal and asks Lakekeeper once per query whether that principal can read each table. Without that property the adapter behaves as before and contacts no management API (#415).

## Background

* `PERMISSION_CHECK` is a virtual-schema property. Absent or empty means off. `LAKEKEEPER`, in any letter case, means on. With the check on, `USER_MAPPING` is required. With the check off, the adapter ignores `USER_MAPPING`.
* The check works only under the Iceberg REST catalog kind. The management API URL is the catalog URI with its trailing `/catalog` replaced by `/management`, after one trailing `/` is dropped: `http://lakekeeper:8181/catalog/` gives `http://lakekeeper:8181/management`.
* The check is one `POST <management API URL>/v1/action/batch-check` per query, with one `read_data` check per table. Each check names the principal, and it names the warehouse by the catalog's `/v1/config` prefix, which Lakekeeper sets to the warehouse id. The Iceberg REST catalog spec defines `prefix` only as "An optional prefix in the path", so this reading holds for Lakekeeper only. The #414 fixtures record the request and answer shapes.
* A batch-check answer is a JSON object whose `results` hold exactly one boolean `allowed` for each check id. Lakekeeper answers `allowed: false` for a missing table, the same as for a denied one.
* To check another identity, the caller needs `can_read_assignments` on each checked table, for example through `manage_grants` on the warehouse or the namespace (#414). Without it, Lakekeeper fails the whole check with 403 `CannotInspectPermissions`.
* `USER_MAPPING` is a list of rules separated by `;`. Each rule is `<pattern> -> <template>`, and whitespace around `;` and `->` is ignored. The adapter maps the querying user through the first rule whose pattern matches the user name.
* A pattern is an exact user name, or a name with one `*` that matches one or more characters. Pattern text holds only `A-Z`, `a-z`, `0-9`, `_`, and that `*`. A pattern matches case-sensitively against the user name that Exasol reports, which is uppercase for an undelimited name.
* A template is `<idp>~<subject>` and yields the principal, the Lakekeeper user id. `<idp>` is non-empty literal text. A rule with `*` holds exactly one placeholder `{*}` in `<subject>`, which stands for the part of the user name that `*` matched. A rule with an exact name holds no placeholder. Literal text is printable ASCII without whitespace, `~`, `;`, `{`, or `}`.
* A placeholder applies functions left to right, each after a `|`. `lower` turns ASCII letters into lowercase. `replace(a,b)` turns each character `a` into the character `b`, where `a` and `b` are two different literal-text characters other than `,`, `)`, and `|`. Example: `{*|lower|replace(_,.)}` turns `ALICE_COOPER` into `alice.cooper`.
* Each function refuses an input that it would merge with another input: `lower` refuses an input that holds a lowercase ASCII letter, and `replace(a,b)` refuses an input that holds `b`. A user name that a function refuses, or whose subject holds `~`, whitespace, or a control character, cannot be mapped.
* Two rules clash when they can produce one principal. The adapter decides this from the rules' literal text, without knowing which users exist. A template's text before `{*}` includes `<idp>~`. Two exact rules clash when their principals are equal. An exact rule and a `*` rule clash when the exact principal starts with the `*` rule's text before `{*}` and ends with its text after `{*}`. Two `*` rules clash when the text before `{*}` of one starts with that of the other, and the text after `{*}` of one ends with that of the other.
* createVirtualSchema, refresh, and setProperties catch every clash between two rules. They cannot catch a clash inside one rule, because it depends on which user names exist. The function refusals prevent that clash at query time, whether or not the other user exists. No check can tell whether a principal belongs to the intended person.
* Coverage: every Iceberg REST pushdown shape. createVirtualSchema, refresh, and setProperties run no check, so the listing shows tables that a querying user holds no grant to read (#416 asserts this limitation end to end).

## Scenarios

### Scenario: Absent PERMISSION_CHECK leaves every request unchanged and contacts no management API

* *GIVEN* a virtual schema whose properties carry no `PERMISSION_CHECK`, with or without `USER_MAPPING`
* *WHEN* the adapter handles a createVirtualSchema, refresh, setProperties, or pushdown request
* *THEN* the adapter MUST NOT send any request to the Lakekeeper management API
* *AND* the catalog requests, the response, the pushdown SQL, and every error message SHALL be byte-identical to the adapter's output before this feature for the same request
* *AND* the adapter MUST NOT validate `USER_MAPPING`, so an existing virtual schema keeps working with no configuration change

### Scenario: Invalid permission properties are rejected before the CONNECTION is read

* *GIVEN* a request whose effective properties set `PERMISSION_CHECK` to a value other than `LAKEKEEPER` in any letter case, or set it to `LAKEKEEPER` with an absent `USER_MAPPING` or one that breaks the grammar in Background
* *WHEN* the adapter handles a createVirtualSchema, refresh, setProperties, or pushdown request
* *THEN* the adapter SHALL return an error before it reads the CONNECTION, and MUST NOT fall back to running with the check off
* *AND* an unrecognized-value error SHALL name the value and the accepted value `LAKEKEEPER`, and SHALL state that an absent `PERMISSION_CHECK` turns the check off
* *AND* a `USER_MAPPING` error SHALL quote the rule at fault and state what is wrong with it
* *AND* no error message SHALL contain a credential value

### Scenario: The check is accepted only for an Iceberg REST catalog URI that ends in /catalog

* *GIVEN* a request whose effective properties set `PERMISSION_CHECK = 'LAKEKEEPER'` with a valid `USER_MAPPING`
* *WHEN* the adapter handles a createVirtualSchema, refresh, setProperties, or pushdown request
* *THEN* under any catalog kind other than Iceberg REST, the adapter SHALL return the error `PERMISSION_CHECK = 'LAKEKEEPER' requires the Iceberg REST catalog kind` before it reads the CONNECTION, also for a virtual schema that carried the property before this feature
* *AND* under Iceberg REST, the adapter SHALL reject a catalog URI that does not end in `/catalog` before any catalog request, with an error that names the URI and states that the check needs a catalog URI ending in `/catalog`
* *AND* a createVirtualSchema, refresh, or setProperties request that passes both checks SHALL list the namespace as it does with the check off, and MUST NOT send any management API request

### Scenario: USER_MAPPING maps the querying user through the first rule that matches

* *GIVEN* a virtual schema with the check on and `USER_MAPPING = 'ETL_SVC -> oidc~6f1c0d2e-58b4-4c39-9a8f-2b1e7d4c0a91; *_EXT -> oidc~{*|lower}@partner.com; * -> oidc~{*|lower|replace(_,.)}@corp.net'`
* *WHEN* a pushdown request arrives whose current user is `ALICE_COOPER`, `BOB_EXT`, or `ETL_SVC`, and whose scope user is `OWNER`
* *THEN* every check of the request SHALL name the principal `oidc~alice.cooper@corp.net` for `ALICE_COOPER`, `oidc~bob@partner.com` for `BOB_EXT`, and `oidc~6f1c0d2e-58b4-4c39-9a8f-2b1e7d4c0a91` for `ETL_SVC`, so no check runs as the CONNECTION's own identity
* *AND* the same mapping with the pattern `EXA_*` in place of `*` SHALL map `EXA_ALICE` to `oidc~alice@corp.net`
* *AND* the adapter SHALL read `USER_MAPPING` only from the virtual schema's properties, and MUST NOT derive the principal from the scope user, from another request field, or from a Lakekeeper user search

### Scenario: A user that USER_MAPPING cannot map is refused before any request

* *GIVEN* a virtual schema with the check on
* *WHEN* a pushdown request arrives whose current user is absent or empty, matches no rule, or cannot be mapped as Background defines, for example `ALICE` under `EXA_* -> oidc~{*|lower}@corp.net` alone, the delimited user `alice` under `* -> oidc~{*|lower}@corp.net`, or a delimited user name that holds a space
* *THEN* the adapter SHALL refuse the query with an error that names the user and states that `USER_MAPPING` cannot map it to a Lakekeeper principal
* *AND* the adapter MUST NOT read the CONNECTION or send any request

### Scenario: USER_MAPPING never gives two Exasol users one principal

* *GIVEN* a request whose `USER_MAPPING` holds two rules that clash as Background defines, for example `EXA_* -> oidc~{*|lower}@corp.net; * -> oidc~{*|lower}@corp.net`, under which `EXA_ALICE` and `ALICE` both give `oidc~alice@corp.net`
* *WHEN* the adapter handles a createVirtualSchema, refresh, setProperties, or pushdown request
* *THEN* the adapter SHALL reject the mapping before it reads the CONNECTION, with an error that quotes both rules
* *AND* under a mapping that the adapter accepts, two distinct user names SHALL never get one principal, because a function refuses a name that it would merge with another, for example the delimited user `alice` under `{*|lower}`, which cannot take the principal of `ALICE`
* *AND* the adapter SHALL accept a mapping whose rules do not clash, even when a fixed id or a template names the wrong person's principal, because the adapter cannot tell who owns a principal

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
* *AND* the error SHALL name the batch-check URL and the HTTP status, if any, and for a 404, a 405, or a body that is not a batch-check answer SHALL state that the check needs a Lakekeeper server as the catalog
* *AND* for a 403 `CannotInspectPermissions` the error SHALL state that the CONNECTION's identity needs a grant that includes `can_read_assignments`, such as `manage_grants` on the warehouse or the namespace, and a pushdown on the live `lakekeeper-e2e` stack through such a CONNECTION SHALL get that error
* *AND* no error SHALL contain the CONNECTION's `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope`, or the session's bearer token, even when the answer body echoes them

### Scenario: Grants on the mapped principal decide a live Exasol user's query

* *GIVEN* the `lakekeeper-e2e` stack, a virtual schema with the check on and `USER_MAPPING = 'LK_PERM_* -> oidc~lk.{*|lower}@lakehouse.test'` over a seeded table, and three Exasol users granted `SELECT` on the virtual schema: one whose mapped principal holds a Lakekeeper `select` grant on the table, one whose principal holds a grant on another table only, and one whose principal Lakekeeper has never seen
* *WHEN* each user runs the same `SELECT` over the table, the granted user first
* *THEN* the granted user SHALL receive the seeded rows, which proves that the adapter reads the querying user and that a grant on a mapped id authorizes that user
* *AND* each of the two other users SHALL receive the denial of "A denied table refuses the whole query", although the granted user ran the byte-identical statement just before
* *AND* the test MUST fail, not skip, when the stack is unavailable
