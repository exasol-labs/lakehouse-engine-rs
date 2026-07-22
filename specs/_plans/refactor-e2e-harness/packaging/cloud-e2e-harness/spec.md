# Feature: Cloud E2E Harness (Glue + SigV4)

An opt-in end-to-end smoke and performance test that exercises the full lakehouse
query path against a real Exasol cluster and a real AWS Glue Iceberg REST catalog
loaded with meaningful data, verifying that CONNECTION-object credentials, SigV4
catalog signing, and vended S3 credentials all work against the live cloud stack.
Unlike the local Docker harness (which must FAIL when its stack is down), this cloud
test is opt-in: it SKIPS cleanly when the AWS credentials are not configured, so it is
safe to run in CI or trigger manually without a permanently-attached cloud account.

## Background

The test is gated behind a dedicated cargo feature distinct from `exasol-e2e` (e.g.
`cloud-e2e`), so the local-Docker suite's fail-when-down semantics are never changed.
The cloud test discovers its credentials and endpoints from environment variables. The
Exasol CONNECTION used by the virtual schema is created from those same environment
values. All DSN/connection strings include `validateservercertificate=0`. No credential
value is printed to test output. The same cloud path is driven by the remote bench
harness (`bench/run.sh` with `BENCH_TARGET=remote`), which builds the
`CREATE VIRTUAL SCHEMA` statement against the live cluster from the bench environment.
The cloud suite drives Exasol through the shared `common/exasol_ws::ExaConn` WebSocket
client — the same client the local Docker suite uses — connected in a redacting mode so
credential-bearing SQL never reaches test output.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Cloud suite drives Exasol through the shared redacting WebSocket client

* *GIVEN* the AWS credential and endpoint environment variables are present
* *AND* the cloud E2E suite opens its Exasol WebSocket SQL session
* *WHEN* it issues credential-bearing DDL (a CONNECTION carrying SigV4 or vended keys) that fails
* *THEN* the suite SHALL use the shared `common/exasol_ws::ExaConn` client — the same client the local Docker suite uses — connected in a redacting mode that omits the SQL statement and the Exasol response body from the `execute()` DDL-failure panic; this redaction covers the `execute()` DDL-failure path only, leaving `query_scalar_i64`, `query_row_count`, and the `connect()` auth-failure assertion unredacted for this fold (the cloud suite passes no credential-bearing SQL through them and the login response carries no credential value)
* *AND* that `execute()` failure message MUST NOT print the failing SQL or the Exasol response, so no static, SigV4, or vended credential value embedded in the credential-bearing DDL SHALL appear in the failure output
* *AND* the redacting behaviour MUST be opt-in, so the local Docker suite still includes the failing SQL in its `execute()` failure messages for debuggability
<!-- /DELTA:NEW -->
