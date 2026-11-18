# Feature: EXPORT INTO SCRIPT Catalog and Storage Access

The export reaches the Iceberg REST catalog that the CONNECTION names and the table's storage, S3-compatible or Azure Data Lake Storage Gen2, with static or vended storage credentials. It refuses a CONNECTION or a table location it cannot write through before any data is written, and a storage error fails the writer that meets it.

## Background

* Statement and options: export/export-options § Background.
* Storage credentials are the CONNECTION's static storage credentials, S3 keys or an Azure `account_key` or `sas_token`, or, when the CONNECTION sets `"use_vended_credentials": true`, the credentials that the catalog vends for the table, S3 keys or an ADLS SAS. The CONNECTION fields and the choice among vended credentials are the virtual schema's: connection/connection-credentials-azure, storage-access/pushdown-planning-cloud-credentials-vended-storage.
* On a catalog that offers a credentials endpoint (Iceberg REST `loadCredentials`), the export renews vended credentials through it before they expire.
* The export writes onto S3-compatible storage or ADLS Gen2, through an Iceberg REST catalog that does not need AWS SigV4 signing.

## Scenarios

### Scenario: Static and vended storage credentials both write the table

* *GIVEN* one CONNECTION with static storage keys, and one CONNECTION that sets `"use_vended_credentials": true` and holds no storage keys
* *WHEN* a user exports the same source through each CONNECTION into a new table
* *THEN* both exports SHALL succeed
* *AND* each new table, read through a virtual schema, SHALL hold the source rows

### Scenario: A writer that outlives its vended credentials refreshes them, or fails naming their expiry

* *GIVEN* a CONNECTION that sets `"use_vended_credentials": true`, and vended credentials, S3 keys or an ADLS SAS, that expire before the writer finishes
* *WHEN* a user exports a source into a new table, with or without `SINGLE_COMMIT = 'true'`, through a catalog that offers a credentials endpoint
* *THEN* the writer SHALL renew the credentials through that endpoint before they expire, every later storage request of the writer, its commit's manifests included, SHALL use the renewed credentials, and the export SHALL succeed
* *WHEN* the catalog offers no credentials endpoint
* *THEN* the export SHALL fail with an error that names the table and states that its vended credentials expired and that the catalog offers no credentials endpoint
* *AND* the error MUST NOT contain a storage credential
* *WHEN* the catalog states no expiry for the credentials it vends, no `s3.session-token-expires-at-ms` for S3 keys and no `adls.sas-token-expires-at-ms.<host>` for the table location's host for an ADLS SAS
* *THEN* the writer SHALL use the credentials as vended and SHALL NOT renew them
* *WHEN* the catalog states an expiry that is no epoch millisecond
* *THEN* the writer SHALL fail with an error that names the expiry key and contains no storage credential
* *WHEN* the credentials the catalog vends, at table creation or at renewal, lack an S3 access key or secret key, or lack an ADLS SAS for the table location's host
* *THEN* the writer SHALL fail with an error that names the missing key and contains no storage credential

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
* *WHEN* a user exports it, with or without `SINGLE_COMMIT`, through a CONNECTION that signs catalog requests with AWS SigV4
* *THEN* the export SHALL fail before any source row is read, with an error that names the unsupported signing
* *AND* the export SHALL NOT create the table
* *WHEN* a user exports it through a CONNECTION whose credential fields `CREATE VIRTUAL SCHEMA` refuses before it contacts the catalog, such as one with Azure storage fields and `aws_assume_role_arn`
* *THEN* the export SHALL fail before any source row is read, with the error `CREATE VIRTUAL SCHEMA` gives for that CONNECTION
* *AND* the export SHALL NOT create the table

### Scenario: Static and vended credentials write a table on ADLS Gen2

* *GIVEN* an Iceberg REST catalog whose warehouse lies on ADLS Gen2
* *AND* three CONNECTIONs to it: one with `account_name` and `account_key`, one with `account_name` and `sas_token`, and one that sets `"use_vended_credentials": true` and holds no storage credential
* *WHEN* a user exports the same source through each CONNECTION into a new table, with `SINGLE_COMMIT = 'true'` through the vended CONNECTION, and then exports it again into that table
* *THEN* every export SHALL succeed
* *AND* each table, read through a virtual schema, SHALL hold every source row twice

### Scenario: A table location the CONNECTION's storage cannot address is refused

* *GIVEN* a catalog that answers a table location
* *WHEN* a user exports into a new table through a CONNECTION whose storage is S3, by static keys or an IAM role, and the location's scheme is not `s3` or `s3a`
* *OR* through a CONNECTION with static Azure credentials and the scheme is not `abfss`
* *OR* through a CONNECTION that sets `"use_vended_credentials": true` and the scheme is none of `s3`, `s3a`, `abfss`, and `abfs`
* *THEN* the writer SHALL fail with an error that names the table and the scheme, before it writes any data file
* *AND* under vended credentials an `abfs` location SHALL be refused unless `ALLOW_HTTP = 'true'`, with an error that names the plaintext scheme and the export option `ALLOW_HTTP`
