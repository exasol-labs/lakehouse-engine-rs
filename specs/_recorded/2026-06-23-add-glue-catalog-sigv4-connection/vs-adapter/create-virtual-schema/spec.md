# Feature: Create Virtual Schema

Lets an Exasol user register an Iceberg table (resolved through an Iceberg REST catalog
over S3-compatible storage, including AWS Glue with SigV4-signed requests) as a queryable
virtual schema, so the table's columns appear to Exasol with correctly mapped SQL types,
and records — in the response — both the cluster's active node count (via `NPROC()`) and a
parallelism factor, so later pushdowns can size the oversubscribed work-unit shard count.

## Background

* Catalog endpoint and storage credentials are supplied through a CONNECTION object named by `CATALOG_CONNECTION`.
* The adapter resolves credentials via `ctx.connection` and never persists catalog metadata between requests.
* Credentials MUST NOT appear in any returned response or error message.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Create virtual schema maps the Iceberg table schema

* *GIVEN* an Iceberg table exists in the REST catalog
* *AND* the catalog endpoint and storage credentials are supplied through the CONNECTION object named by `CATALOG_CONNECTION`
* *WHEN* Exasol sends a `createVirtualSchema` request naming that table
* *THEN* the adapter SHALL resolve credentials via `ctx.connection`, SigV4-sign the catalog requests when enabled, and resolve the table's current Iceberg schema
* *AND* the adapter SHALL return a JSON response describing one virtual table whose columns map each Iceberg field to an Exasol SQL type per the type-mapping table, declaring any Exasol-incompatible type as VARCHAR rather than failing
* *AND* the adapter MUST NOT persist any catalog metadata between requests

<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Create virtual schema fails clearly when the catalog is unreachable

* *GIVEN* the Iceberg REST catalog endpoint resolved from the CONNECTION cannot be reached
* *WHEN* Exasol sends a `createVirtualSchema` request
* *THEN* the adapter SHALL return an error describing that the catalog could not be reached
* *AND* the error message MUST NOT contain storage access keys, secret keys, session tokens, or any SigV4 signing key

<!-- /DELTA:CHANGED -->
