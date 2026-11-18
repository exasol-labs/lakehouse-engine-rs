# Feature: EXPORT INTO SCRIPT Catalog and Storage Access

The export reaches the Iceberg REST catalog and the S3-compatible storage that the CONNECTION names, with static or vended storage credentials. It refuses a CONNECTION or a table location it cannot write through before any data is read, and a storage error fails the writer that meets it.

## Background

* Statement and options: export/export-options § Background.
* Storage credentials are the CONNECTION's static storage keys or, when the CONNECTION sets `"use_vended_credentials": true`, the credentials that the catalog vends for the table.
* On a catalog that offers a credentials endpoint (Iceberg REST `loadCredentials`), the export renews vended credentials through it before they expire.
* The export writes only onto S3-compatible storage, through an Iceberg REST catalog that does not need AWS SigV4 signing.

## Scenarios

### Scenario: Static and vended storage credentials both write the table

* *GIVEN* one CONNECTION with static storage keys, and one CONNECTION that sets `"use_vended_credentials": true` and holds no storage keys
* *WHEN* a user exports the same source through each CONNECTION into a new table
* *THEN* both exports SHALL succeed
* *AND* each new table, read through a virtual schema, SHALL hold the source rows

### Scenario: A writer that outlives its vended credentials refreshes them, or fails naming their expiry

* *GIVEN* a CONNECTION that sets `"use_vended_credentials": true`, and vended credentials that expire before the writer finishes
* *WHEN* a user exports a source into a new table, with or without `SINGLE_COMMIT = 'true'`, through a catalog that offers a credentials endpoint
* *THEN* the writer SHALL renew the credentials through that endpoint before they expire and the export SHALL succeed
* *WHEN* the catalog offers no credentials endpoint
* *THEN* the export SHALL fail with an error that names the table and states that its vended credentials expired and that the catalog offers no credentials endpoint
* *AND* the error MUST NOT contain a storage credential

### Scenario: A bad CONNECTION fails before any data is read

* *GIVEN* a source whose rows raise an error when they are read
* *WHEN* a user exports it through a CONNECTION whose catalog address is unreachable or whose catalog credentials are wrong
* *THEN* the export SHALL fail before any source row is read, with an error that names the catalog as the failing side
* *AND* the error MUST NOT contain a secret of the CONNECTION or a storage credential
* *AND* the export SHALL NOT create the table

### Scenario: A wrong storage key fails the writer, naming the storage

* *GIVEN* a CONNECTION whose static storage keys cannot write to the catalog's storage, and a source of 1,000 rows
* *WHEN* a user exports it
* *THEN* the export SHALL fail with an error that names the storage as the failing side and contains no storage credential
* *AND* with `SINGLE_COMMIT = 'true'` the export SHALL NOT create the table, and without it the table SHALL exist and have no snapshots

### Scenario: A CONNECTION the export does not support is refused before any data is read

* *GIVEN* a source whose rows raise an error when they are read
* *WHEN* a user exports it, with or without `SINGLE_COMMIT`, through a CONNECTION with Azure storage credentials, or one that signs catalog requests with AWS SigV4
* *THEN* the export SHALL fail before any source row is read, with an error that names the unsupported storage or signing
* *AND* the export SHALL NOT create the table

### Scenario: A table location outside S3-compatible storage is refused

* *GIVEN* a catalog that answers a table location whose scheme is not `s3` or `s3a`
* *WHEN* a user exports into a new table
* *THEN* the writer SHALL fail with an error that names the table and the scheme
