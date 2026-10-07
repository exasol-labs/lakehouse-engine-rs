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

<!-- DELTA:CHANGED -->
### Scenario: A denied table refuses the whole query

* *GIVEN* a virtual schema with the check on and a pushdown request over one or more tables
* *WHEN* Lakekeeper answers `allowed: false` for a checked table, as it does for a denied or a missing table
* *THEN* the adapter SHALL refuse the whole query, and MUST NOT read any table's metadata or return any SQL
* *AND* the error SHALL name the Exasol user, the principal, and every denied table, MUST NOT name a table that Lakekeeper allows, and SHALL state that only a Lakekeeper grant naming that principal authorizes the user
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A table that the user cannot read stays listed and is refused at query time

* *GIVEN* a virtual schema with the check on, and a user that may query it whose mapped principal holds no grant on one of its tables
* *WHEN* the user reads the virtual schema's table and column listing and queries that table, once after the virtual schema is created and once after it is refreshed
* *THEN* the listing SHALL show the user that table and its columns
* *AND* the user's query SHALL receive the denial of "A denied table refuses the whole query"
<!-- /DELTA:NEW -->
