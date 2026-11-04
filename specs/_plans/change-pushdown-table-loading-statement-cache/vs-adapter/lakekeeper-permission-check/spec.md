<!-- DELTA:CHANGED -->
# Feature: Lakekeeper Permission Check

The adapter refuses a query over a table that the querying Exasol user holds no Lakekeeper grant to read. When a virtual schema sets `PERMISSION_CHECK = 'LAKEKEEPER'`, the adapter maps the querying user to a Lakekeeper principal with the `USER_MAPPING` template and asks Lakekeeper whether that principal can read each table before the statement first reads it. Each pushdown request sends at most one batch-check, for the tables that no earlier pushdown of the statement checked. Without that property the adapter contacts no management API.
<!-- /DELTA:CHANGED -->

## Background

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: One batch-check per query decides every table before any table is read

* *GIVEN* a statement whose pushdown requests read tables under the check
* *WHEN* the adapter checks permissions
* *THEN* this scenario SHALL be REMOVED, because a statement with several pushdown requests sends up to one batch-check per request, not one per query
* *AND* it SHALL be REPLACED by "One batch-check per pushdown decides every table that no earlier pushdown of the statement checked" below
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: One batch-check per pushdown decides every table that no earlier pushdown of the statement checked

* *GIVEN* a virtual schema with the check on over a Lakekeeper catalog, and a statement whose pushdown requests have any shape: row scan, single-group aggregate, grouped aggregate, COUNT(DISTINCT), top-N, a qualified fallback wrapper, or an inner join, a self-join included
* *WHEN* Lakekeeper allows every table that the statement reads
* *THEN* each pushdown request SHALL send at most one batch-check to the management API, with one `read_data` check per distinct table of the request that no earlier pushdown of the statement on the virtual schema checked, before it reads the metadata of any of those tables
* *AND* a pushdown request whose tables earlier pushdowns of the statement all checked SHALL send no batch-check
* *AND* the pushdown SQL SHALL be byte-identical to the SQL that the same request yields with the check off
* *AND* the adapter SHALL NOT read any table that no batch-check of the statement covered, so a later pushdown of the statement that reads a further table checks it first
<!-- /DELTA:NEW -->
