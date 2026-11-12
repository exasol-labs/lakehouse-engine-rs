# Feature: Lakekeeper Permission Check

The adapter refuses a query over a table that the querying Exasol user holds no Lakekeeper grant to read. When a virtual schema sets `PERMISSION_CHECK = 'LAKEKEEPER'`, the adapter maps the querying user to a Lakekeeper principal with the `USER_MAPPING` template and asks Lakekeeper whether that principal can read each table before the statement first reads it. Each pushdown request sends at most one batch-check, for the tables that no earlier pushdown of the statement checked. Without that property the adapter contacts no management API.

<!-- DELTA:CHANGED -->
## Background

* `PERMISSION_CHECK` is a virtual-schema property. Absent or empty means off. `LAKEKEEPER`, in any letter case, means on. With the check on, `USER_MAPPING` is required. With the check off, `USER_MAPPING` is ignored. The check works only for a virtual schema over an Iceberg REST catalog whose URI ends in `/catalog`, and only against a Lakekeeper server. Lakekeeper reports a missing table the same way as a denied one.
* The check covers every Iceberg REST pushdown shape. Creating, refreshing, and altering a virtual schema run no check, so the table listing shows tables that a querying user holds no grant to read.
* To check another identity, the CONNECTION's identity needs a Lakekeeper grant that includes `can_read_assignments` on each checked table, for example `manage_grants` on the warehouse or the namespace.
* `USER_MAPPING` is a template. Its one variable, `user`, holds the querying user's name exactly as Exasol reports it, which is uppercase for an undelimited name. The template's output, without leading and trailing whitespace, is the Lakekeeper user id. The adapter assumes no id format. A template may use conditions, lookup tables, filters, and tests, and may span several indented lines. It cannot include other templates. A lookup of a missing value fails the query instead of yielding a partial id, and a template that runs too long is refused. A mapped id is refused when it is empty or holds a whitespace or control character.
* A template that does not compile is rejected when the virtual schema is created or altered. A template that compiles but fails when it runs, for example through an unknown filter or a missing lookup, refuses each query it is applied to.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Invalid permission properties are rejected before the catalog is contacted

* *GIVEN* a request whose effective properties set `PERMISSION_CHECK` to a value other than `LAKEKEEPER` in any letter case, or set it to `LAKEKEEPER` with an absent or empty `USER_MAPPING` or one that does not compile, such as `oidc~{{ user|lower @corp.net`
* *WHEN* the adapter handles a createVirtualSchema, setProperties, or pushdown request
* *THEN* the adapter SHALL return an error without contacting the catalog, and MUST NOT fall back to running with the check off
* *AND* an unrecognized-value error SHALL name the value and the accepted value `LAKEKEEPER`, and SHALL state that an absent `PERMISSION_CHECK` turns the check off
* *AND* a `USER_MAPPING` error SHALL state that the template does not compile and give the line of the mistake
* *AND* no error message SHALL contain a credential value
* *AND* the adapter MUST NOT run these checks on a refresh request, so a virtual schema persisted with such properties refreshes and its queries fail with these errors
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The check is accepted only for an Iceberg REST catalog URI that ends in /catalog

* *GIVEN* a request whose effective properties set `PERMISSION_CHECK = 'LAKEKEEPER'` with a `USER_MAPPING` that compiles
* *WHEN* the adapter handles a createVirtualSchema or setProperties request, or a pushdown request whose current user `USER_MAPPING` maps
* *THEN* under any catalog kind other than Iceberg REST, the adapter SHALL return the error `PERMISSION_CHECK = 'LAKEKEEPER' requires the Iceberg REST catalog kind` without contacting the catalog, also for a virtual schema that carried the property before this feature
* *AND* under Iceberg REST, the adapter SHALL reject a catalog URI that does not end in `/catalog` before any catalog request, with an error that names the URI and states that the check needs a catalog URI ending in `/catalog`
* *AND* a createVirtualSchema or setProperties request that passes both checks, and every refresh request, SHALL list the namespace as it does with the check off, and MUST NOT send any management API request
* *AND* the adapter MUST NOT run either check on a refresh request
<!-- /DELTA:CHANGED -->
