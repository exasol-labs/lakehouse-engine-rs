# Feature: Scan-Spec Credential Reference

The scan spec carries the Exasol CONNECTION name instead of storage credentials, and the scan UDF resolves the credentials at execution time through `ctx.connection()`. A credential the CONNECTION does not state, such as a vended credential or an assumed AWS role's session credential, travels only inside an AES-GCM-sealed envelope. Vending without key material is refused at plan time.

## Background

* The pushdown response is the leak surface, because `EXPLAIN VIRTUAL` returns it verbatim. Profiling and audit `SQL_TEXT` carry only the user's literal statement.
* A CONNECTION-supplied credential never appears in returned SQL. A vended or assumed-role session credential appears only inside the sealed envelope.
* The sealed envelope defeats plaintext reads, not offline cryptanalysis (`docs/security.md`).
* `ctx.connection()` is engine-local, not a catalog round-trip. The wire wrapper exposes no secret accessor.

## Scenarios

### Scenario: The scan spec references the CONNECTION by name

* *GIVEN* a virtual schema whose CONNECTION neither sets `use_vended_credentials` nor names `aws_assume_role_arn` (`connection/connection-credentials-assume-role`), so its storage credentials are the CONNECTION's own static credentials
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the common scan-spec argument SHALL carry the `CATALOG_CONNECTION` name and `ALLOW_HTTP` and no other storage field, and the SQL MUST NOT contain any credential value in any encoding
* *AND* the UDF SHALL re-derive store addressing from the same CONNECTION read, the reference SHALL be carried ONCE in the shard-invariant argument, and a join SHALL carry one reference PER SIDE

### Scenario: One pure function selects the wire variant and gates the sealed envelope

* *GIVEN* the resolved connection config and the effective storage backend
* *WHEN* any storage-block site builds the scan-spec storage block
* *THEN* ONE function SHALL return REFERENCE when the CONNECTION neither sets `use_vended_credentials` nor names `aws_assume_role_arn` (`connection/connection-credentials-assume-role`), SEALED when it does either and key material is present, or a refusal error when it sets `use_vended_credentials` and no key material is present; every storage-block site SHALL call it, and it MUST NOT return plaintext `Inline`
* *AND* the key-material gate SHALL be exactly ONE predicate reporting TRUE iff at least one of `token`, `client_secret`, `secret_key`, `session_token`, `account_key`, `sas_token` is non-empty; `access_key` alone MUST NOT satisfy it; a CONNECTION that names a role always satisfies it, because it requires `secret_key`
* *AND* the refusal SHALL name both remedies (add auth or disable vending), SHALL return no pushdown SQL, and MUST NOT contain any credential
* *AND* a vended or assumed-role JOIN SHALL seal each side independently (two envelopes, one key)

### Scenario: The scan UDF resolves the referenced CONNECTION

* *GIVEN* a scan invocation with a storage reference or sealed envelope
* *WHEN* the scan UDF starts
* *THEN* the UDF SHALL call `ctx.connection()`, deserialize into a STORAGE-ONLY projection (nine fields), and derive the backend through the SAME selector the adapter uses
* *AND* for a sealed envelope it SHALL derive the key from password BYTES and open via AEAD
* *AND* a failed open SHALL produce a named error with no plaintext, and an unresolvable CONNECTION SHALL produce a named error with no inline fallback

### Scenario: Error redaction reads from resolved credentials, not the wire spec

* *GIVEN* a scan invocation whose storage resolves to a credential-bearing backend
* *WHEN* the scan builds an error message
* *THEN* value-based redaction SHALL take its secret set from RESOLVED backends, and a join's secret set SHALL be the UNION of both sides
* *AND* the wire wrapper MUST NOT expose a secret accessor (enforced at compile time)

### Scenario: The generated SQL is asserted credential-free at every builder path

* *GIVEN* every `RequestShape` variant crossed with join/top-N/COUNT(DISTINCT) sub-paths
* *WHEN* the adapter generates the pushdown SQL under BOTH vending settings
* *THEN* the returned SQL STRING MUST contain no sentinel credential
* *AND* the connection NAME SHALL be positively asserted present (static) or the envelope SHALL unseal correctly (vended)
* *AND* the eighteen credential-bearing golden fixtures SHALL carry the reference encoding, and the six `empty_*` fixtures SHALL stay byte-identical

### Scenario: The scan script needs the owner's CONNECTION grant

* *GIVEN* a deployment with the adapter grant already in place
* *WHEN* a scan runs
* *THEN* the scan script SHALL additionally need `GRANT ACCESS ON CONNECTION ... FOR SCRIPT <schema>.LAKEHOUSE_SCAN TO <owner>` or the same grant `FOR SCHEMA <schema>`, and a missing grant SHALL fail with a named error and no inline fallback
* *AND* that grant SHALL be checked against the virtual schema's OWNER, not the querying user, so a user holding only `CREATE SESSION` and `SELECT` on the virtual schema, and no privilege on the CONNECTION, SHALL receive the owner's rows
* *AND* when the owner's grant is missing, the error a reader receives SHALL name the CONNECTION, both the script-scoped and the schema-scoped grant, and the virtual schema's owner as the grantee, and MUST NOT contain any credential value

### Scenario: A role can own the virtual schema and hold its grants

* *GIVEN* a virtual schema whose owner is a role, and the role holds `EXECUTE` on the adapter, scan, and distributor scripts and the script-scoped CONNECTION grants for the adapter and scan scripts
* *WHEN* a reader that holds only `CREATE SESSION` and `SELECT` on the virtual schema, and is not a member of the role, queries it
* *THEN* the reader SHALL receive the owner's rows
* *AND* when the role's scan grant is revoked, the reader's query SHALL fail with the scan's named error

### Scenario: A reader's own CONNECTION grant does not replace the owner's

* *GIVEN* a virtual schema whose owner lacks the CONNECTION grant of the scan script, or of the adapter script
* *AND* a reader that holds that grant itself, and for the adapter script also `GRANT CONNECTION`
* *WHEN* the reader queries the virtual schema
* *THEN* the query SHALL fail with the named error of the script whose owner grant is missing

### Scenario: A missing adapter grant fails with the adapter's named error

* *GIVEN* a virtual schema whose owner holds the scan script's CONNECTION grant but not the adapter script's
* *WHEN* a reader queries the virtual schema
* *THEN* the query SHALL fail with an error stating `CONNECTION '<name>' could not be resolved`, distinct from the scan's named error, and MUST NOT contain any credential value
* *AND* the error SHALL state that the CONNECTION does not exist or that the virtual schema's OWNER lacks `GRANT ACCESS ON CONNECTION <name> FOR SCRIPT <schema>.LAKEHOUSE_ADAPTER` or the same grant `FOR SCHEMA <schema>`, because Exasol reports both cases the same way

### Scenario: A CONNECTION that does not exist fails with the adapter's named error

* *GIVEN* a `CATALOG_CONNECTION` that names no existing CONNECTION
* *WHEN* a DBA creates a virtual schema with it
* *THEN* the statement SHALL fail with an error stating `CONNECTION '<name>' could not be resolved: it does not exist`

### Scenario: A user owner can hold the CONNECTION grants through a role

* *GIVEN* a virtual schema whose owner is a user that owns the schema of the adapter, scan, and distributor scripts, and holds the script-scoped CONNECTION grants only through a role it is a member of, as the install script's template sets up
* *WHEN* a reader queries the virtual schema
* *THEN* the reader SHALL receive the owner's rows
* *AND* when the role is revoked from the owner, the reader's query SHALL fail with the adapter's named error

### Scenario: Schema-scoped grants survive a re-install, script-scoped grants do not

* *GIVEN* a script schema that holds the adapter, scan, and distributor scripts
* *AND* one virtual schema whose owner holds only `GRANT EXECUTE ON SCHEMA` and `GRANT ACCESS ON CONNECTION ... FOR SCHEMA` on that schema
* *AND* another virtual schema whose owner holds `EXECUTE` on each script and the script-scoped CONNECTION grants
* *WHEN* the scripts are re-created with `CREATE OR REPLACE SCRIPT`, as a re-install does
* *THEN* both virtual schemas SHALL return the owner's rows to a reader before the re-install
* *AND* the schema-scoped grants SHALL remain and the first virtual schema SHALL still return the owner's rows to a reader
* *AND* the script-scoped grants SHALL be gone and a reader's query over the second virtual schema SHALL fail with Exasol's insufficient-privilege error on the adapter script

### Scenario: ALTER CONNECTION rotates the CONNECTION and keeps its grants

* *GIVEN* a virtual schema whose owner holds the script-scoped grants on its CONNECTION
* *WHEN* a DBA changes the CONNECTION's storage secret with `ALTER CONNECTION`
* *THEN* the grants SHALL remain and a reader's next query SHALL use the changed secret, with no restart and no credential value in an error
* *AND* after `ALTER CONNECTION` restores the secret, a reader SHALL receive the owner's rows
* *AND* each shard SHALL read the CONNECTION value current at ITS resolution time, and a sealed envelope under a rotated password SHALL fail AEAD with no plaintext

### Scenario: A missing EXECUTE grant of the owner fails the query with Exasol's privilege error

* *GIVEN* a virtual schema whose owner holds every grant, and whose scan of the queried table spans more than one shard
* *WHEN* the owner's `EXECUTE` on the adapter script is revoked and a reader queries the virtual schema
* *THEN* the query SHALL fail with `insufficient privileges for calling adapter script`
* *AND* when the owner's `EXECUTE` on the scan script or the distributor script is revoked instead, the query SHALL fail with `insufficient privileges for calling script`
* *AND* a query whose scan spans a single shard MUST NOT call the distributor, so it SHALL succeed without the owner's `EXECUTE` on the distributor script

### Scenario: A virtual schema reader can read the pushdown plan but cannot execute it

* *GIVEN* a virtual schema whose owner holds `EXECUTE` on the adapter, scan, and distributor scripts and the script-scoped CONNECTION grant
* *AND* a reader that holds only `CREATE SESSION` and `SELECT` on the virtual schema, and holds no `EXECUTE` privilege on any of the three scripts and no `EXECUTE ANY SCRIPT` privilege, neither directly nor through a role
* *WHEN* the reader takes the scan-driving statement that `EXPLAIN VIRTUAL` returns for a query over the virtual schema and submits it verbatim as its own SQL
* *THEN* Exasol SHALL reject the statement with an insufficient-privilege error on the script call, and that error MUST NOT contain any CONNECTION credential value
* *AND* the reader's ordinary query over the virtual schema SHALL still return the owner's rows, because the denial applies only to a direct script call and not to the scan the virtual schema runs on the reader's behalf
* *AND* the virtual schema's owner SHALL run the same statement, because it holds the script and CONNECTION grants
