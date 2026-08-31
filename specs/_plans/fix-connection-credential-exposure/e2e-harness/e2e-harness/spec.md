# Feature: End-to-End Harness

End-to-end test suite that exercises the full lakehouse VS query path — from Exasol SQL
through the adapter and scan UDF to Iceberg Parquet files in MinIO — verifying
correctness of projection, filter, and Iceberg file-pruning pushdown against a local
Exasol Docker container. The harness installs `LAKEHOUSE_SCAN` as a SCALAR EMIT script
and `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET distributor script. See
`e2e-harness/e2e-harness-grouped-agg` for grouped-aggregate and nested-aggregate E2E
scenarios.

## Background

* **This delta is issue #135. It adds ONE scenario and amends ONE, and it is what makes the credential fix falsifiable.** Every recorded scenario, seed fixture, row-cap rule, fetch-paging rule, and version gate is otherwise unchanged.
* **The suite has never exercised a least-privilege user, which is why the exposure of issue #135 was invisible to it.** Every E2E binary connects as the Exasol `sys` DBA and issues no `GRANT` at all — `crates/lakehouse-engine/tests/common/stack.rs:370` builds a `CREATE OR REPLACE CONNECTION` and nothing grants it, because `sys` needs no grant. So the suite cannot distinguish "the scan resolved the CONNECTION because the grant is right" from "the scan resolved it because the caller is a DBA", and it cannot observe what a `SELECT`-only user reads out of a query plan.
* **`CREATE OR REPLACE CONNECTION` DROPS the script-scoped grant, verified live on Exasol 2025.2.1.** The harness therefore has to issue the grant AFTER its `CREATE OR REPLACE CONNECTION`, not before, or every binary provisions a grant the next connection replacement silently revokes.
* **The new user is the attacker of issue #135, specified by what it must NOT hold.** It holds `SELECT` on the virtual schema and the script-scoped grant that reaches the CONNECTION through `LAKEHOUSE_SCAN`, and it holds NEITHER `ACCESS ON CONNECTION` for that connection NOR the `ACCESS ANY CONNECTION` or `SELECT ANY DICTIONARY` system privileges. Exasol documents the plain `ACCESS` grant as including "passwords/tokens", so a user holding it could read the credential directly and would prove nothing.
* **`EXPLAIN VIRTUAL` alone is an insufficient assertion surface, so the new scenario reads the profiling view too.** `EXA_USER_PROFILE_LAST_DAY` carries a `SQL_TEXT` column and Exasol documents it as "All users have access to the table" for the user's own sessions with profiling enabled.
* **Every absence assertion in this feature carries a POSITIVE CONTROL, because a security test that goes green on an empty surface is worse than no test.** An assertion that a credential value is absent from a text is satisfied by the empty string, by a missing row, and by a fixture that seeded nothing. Each such assertion therefore first proves its surface is populated.
* **The profiling view CAN be made to populate deterministically, and this was verified live on Exasol 2025.2.1 rather than assumed.** With `ALTER SESSION SET PROFILE = 'ON'`, a query carrying a distinctive SQL comment, and a DBA-issued `FLUSH STATISTICS` afterwards, the least-privilege user then selected its own statement's rows from `EXA_USER_PROFILE_LAST_DAY` — three rows, one per execution-graph part (`COMPILE / EXECUTE`, `SCAN`, `GROUP BY`), each carrying the full `SQL_TEXT`. Without the flush the same query returned zero rows, which is precisely the vacuous pass the positive control exists to catch. The same user was refused `EXA_DBA_CONNECTIONS` with SQL state `42500`, so the profiling read needs no dictionary privilege.
* **The harness already has the `EXPLAIN VIRTUAL` seam and it is reused rather than duplicated.** `explain_virtual_sql` (`crates/lakehouse-engine/tests/common/e2e_harness.rs:302-311`) runs `EXPLAIN VIRTUAL` and flattens the result set into one string. No existing test asserts anything about credentials in that output.
* **The assertion is on the CONNECTION's own credential values, not on the absence of the substring `access_key`.** The wire still carries the connection name, and a JSON key spelling is not a secret. Asserting on the seeded MinIO key values is what fails if the credential returns in any encoding.
* **The grant is asserted by its ABSENCE as well as its presence.** A test that only grants and then queries passes equally well against a build that ignores the grant and reads an inline credential. The scenario therefore also requires that revoking the script-scoped grant makes the query fail.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Every E2E binary provisions the scan path from one shared harness definition

* *GIVEN* the `exasol-e2e` test binaries under `crates/lakehouse-engine/tests`, each with its own `OnceLock`-guarded setup
* *AND* a single shared `common/e2e_harness` module defining the SLC install, the `.so` upload, the script creation, and the Virtual Schema creation
* *WHEN* any binary's setup provisions the lakehouse VS scan path
* *THEN* the binary SHALL install `LAKEHOUSE_SCAN`, `LAKEHOUSE_DISTRIBUTE_FILES`, and the adapter script from that shared definition, so the script DDL is byte-identical across every binary
* *AND* the shared definition SHALL issue `GRANT ACCESS ON CONNECTION <connection> FOR SCRIPT <schema>.LAKEHOUSE_SCAN` AFTER the `CREATE OR REPLACE CONNECTION` that provisions the connection, because a connection replacement drops the grant, so every binary provisions the grant `vs-adapter/scan-spec-credential-reference` requires and no binary passes only because its caller is a DBA
* *AND* the per-binary Virtual Schema properties that vary (VS name, Iceberg namespace, catalog CONNECTION name, `PARALLELISM_FACTOR`, `JOIN_BROADCAST_MAX_BYTES`) SHALL be supplied as explicit parameters rather than by re-declaring the provisioning logic
* *AND* an end-to-end query through any binary's Virtual Schema SHALL return results identical to the single-node DataFusion equivalent, and the affected tests MUST fail (not skip) when the Exasol Docker container or MinIO is unavailable
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A least-privilege user queries the virtual schema and recovers no credential from the plan

* *GIVEN* a provisioned virtual schema over a CONNECTION whose static `access_key` and `secret_key` are the seeded MinIO values
* *AND* an Exasol user granted `SELECT` on that virtual schema and `ACCESS ON CONNECTION <connection> FOR SCRIPT <schema>.LAKEHOUSE_SCAN`, holding NEITHER `ACCESS ON CONNECTION` for that connection NOR `ACCESS ANY CONNECTION` NOR `SELECT ANY DICTIONARY`
* *WHEN* that user runs a projection/filter query carrying a distinctive SQL comment over the virtual schema with session profiling enabled, and separately runs `EXPLAIN VIRTUAL` over the same query
* *THEN* the query SHALL return the same rows the DBA-run equivalent returns, so the least-privilege grant set is sufficient to execute the scan
* *AND* the test SHALL first assert POSITIVELY that the `PUSHDOWN_SQL` text `EXPLAIN VIRTUAL` returns is non-empty and names the CONNECTION, and only then assert that it contains neither the `access_key` value nor the `secret_key` value
* *AND* the test SHALL flush statistics from its DBA connection and then assert POSITIVELY that at least one `EXA_USER_PROFILE_LAST_DAY` row for that user's own session matches the distinctive comment the query embedded, failing if none does, and only then assert that no matching row's `SQL_TEXT` contains either credential value
* *AND* the assertion SHALL be made on those two seeded VALUES rather than on the absence of the field-name spellings `access_key` and `secret_key`, because the connection name legitimately remains in the SQL and a JSON key spelling is not a secret
* *AND* after the script-scoped grant is revoked from that user, the same query SHALL fail with the scan-time error naming the connection name and the missing access, so the test distinguishes a build that honours the reference from one that reads an inline credential, and that error MUST NOT contain either credential value
* *AND* the test MUST fail (not skip) when the Exasol Docker container or MinIO is unavailable
<!-- /DELTA:NEW -->
