# Feature: Lakekeeper Permission Check

Refuses a query over a table that the querying Exasol user holds no Lakekeeper grant to read. When a virtual schema sets `PERMISSION_CHECK = 'LAKEKEEPER'`, the adapter maps the querying user to a Lakekeeper principal with the `USER_MAPPING` template and asks Lakekeeper once per query whether that principal can read each table. Without that property the adapter behaves as before and contacts no management API (#415).

<!-- DELTA:CHANGED -->
## Background

* `PERMISSION_CHECK` is a virtual-schema property. Absent or empty means off. `LAKEKEEPER`, in any letter case, means on. With the check on, `USER_MAPPING` is required. With the check off, `USER_MAPPING` is ignored.
* The check works only for a virtual schema over an Iceberg REST catalog whose URI ends in `/catalog`, and only against a Lakekeeper server. Lakekeeper reports a missing table the same way as a denied one.
* The check covers every table of every Iceberg REST pushdown request, `EXPLAIN VIRTUAL` included. Creating, refreshing, and altering a virtual schema run no check, so every user who may query the virtual schema sees each table and its columns in the listing, also a table that the user holds no grant to read.
* The check protects a table only from users who hold no `EXECUTE` on the scan and distributor scripts and no `EXECUTE ANY SCRIPT` (#402).
* To check another identity, the CONNECTION's identity needs a Lakekeeper grant that includes `can_read_assignments` on each checked table, for example `manage_grants` on the warehouse or the namespace (#414).
* `USER_MAPPING` is a template. Its one variable, `user`, holds the querying user's name exactly as Exasol reports it, which is uppercase for an undelimited name. The template's output, without leading and trailing whitespace, is the Lakekeeper user id. The adapter assumes no id format.
* A template may use conditions, lookup tables, filters, and tests, and may span several indented lines. It cannot include other templates. A lookup of a missing value fails the query instead of yielding a partial id, and a template that runs too long is refused.
* A mapped id is refused when it is empty or holds a whitespace or control character.
* A template that does not compile is rejected when the virtual schema is created, refreshed, or altered. A template that compiles but fails when it runs, for example through an unknown filter or a missing lookup, refuses each query it is applied to.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Every single-table pushdown shape returns rows only to a user whose principal holds the grant

* *GIVEN* a virtual schema with the check on over a Lakekeeper-managed table, and two Exasol users that may query it: one whose mapped principal holds a grant on the table, and one whose principal holds none
* *WHEN* each user runs a row scan, a single-group aggregate, a grouped aggregate, a COUNT(DISTINCT), a top-N, a query that the adapter answers through the qualified single-table wrapper, a grouped query that it answers through that wrapper, and a query whose filter excludes every data file
* *THEN* the user with the grant SHALL receive, in every shape, the rows that the same query returns with the check off
* *AND* the other user SHALL receive the denial of "A denied table refuses the whole query" in every shape, the query whose filter excludes every data file included
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Each table of a join is checked and named on its own

* *GIVEN* a virtual schema with the check on over three Lakekeeper-managed tables, a user whose mapped principal holds a grant on every table, and a user whose principal holds a grant on one of the tables only
* *WHEN* each user runs inner joins with a table that the second user cannot read on the left only, on the right only, and on both sides, planned as a broadcast join and as the unaccelerated join wrapper
* *THEN* the user with every grant SHALL receive, for every join, the rows that the same join returns with the check off
* *AND* the second user SHALL receive the denial of "A denied table refuses the whole query" for every join, and the denial SHALL name each table of the join that the user cannot read and no table that the user can read
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A table that the user cannot read stays listed and is refused at query time

* *GIVEN* a virtual schema with the check on, and a user that may query it whose mapped principal holds no grant on one of its tables
* *WHEN* the owner creates the virtual schema and later refreshes it, and after each the user reads the virtual schema's table and column listing and queries that table
* *THEN* the listing SHALL show the user that table with its columns
* *AND* the user's query SHALL receive the denial of "A denied table refuses the whole query"
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A user without the grant can neither obtain nor run the table's plan

* *GIVEN* a virtual schema with the check on over a Lakekeeper-managed table, a user whose mapped principal holds a grant on the table, and a user whose principal holds none, neither user holding `EXECUTE` on the scan or distributor script or `EXECUTE ANY SCRIPT`
* *WHEN* the second user runs `EXPLAIN VIRTUAL` over a query of the table, and then submits as its own statement the pushdown SQL that `EXPLAIN VIRTUAL` returns to the first user for the same query
* *THEN* the second user's `EXPLAIN VIRTUAL` SHALL receive the denial of "A denied table refuses the whole query"
* *AND* Exasol SHALL reject the submitted statement for insufficient privilege to call the script, so the second user receives no row
<!-- /DELTA:NEW -->
