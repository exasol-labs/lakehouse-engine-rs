# Feature: Connection-Object Credential Source: AWS Assumed-Role Session Use

Defines how the adapter applies the session credentials that `vs-adapter/connection-credentials-assume-role` obtains. The session signs SigV4 catalog requests and reads object storage, and it reaches the scan UDF only inside the sealed envelope. Tracked by issue [#139](https://github.com/exasol-labs/lakehouse-engine-rs/issues/139).

## Background

* **The session comes from one STS `AssumeRole` call per request.** `vs-adapter/connection-credentials-assume-role` specifies the CONNECTION fields, the request, the response, and the failure error.
* **A role leaves credential vending unchanged.** The session replaces the CONNECTION's static key pair only where that pair is read.

## Scenarios

### Scenario: One call resolves the AWS identity a request acts as

* *GIVEN* a resolved CONNECTION credential set
* *WHEN* the adapter resolves the request's configuration at either entry point
* *THEN* exactly ONE `lakehouse-catalog` function SHALL return the credential set with `access_key`, `secret_key`, and `session_token` replaced by the session's, and every other field unchanged, `aws_assume_role_arn` and `use_vended_credentials` included
* *AND* the adapter SHALL build the request's static storage backend from that returned set, so the catalog session, the format readers, the direct-storage store, and error redaction read the session without naming the role
* *AND* for a set that names no role, the function SHALL return the set unchanged and SHALL send no network request
* *AND* the acceptance validation and the sealing key SHALL be computed from the CONNECTION as stated, before the replacement, so neither reads a session value

### Scenario: Session credentials sign every SigV4 catalog request

* *GIVEN* a CONNECTION that sets `use_sigv4` to true and names a role, whether or not it sets `use_vended_credentials`
* *WHEN* the adapter issues its SigV4-signed catalog requests, covering namespace enumeration and `loadTable`
* *THEN* each request SHALL be signed with the session `AccessKeyId` and `SecretAccessKey` and SHALL carry the session `SessionToken` as `x-amz-security-token`
* *AND* no catalog request SHALL be signed with the base key pair
* *AND* the signing region SHALL be the one `vs-adapter/connection-credentials-sigv4` resolves, unchanged by the role
* *AND* each catalog request SHALL carry the prefix that `vs-adapter/pushdown-planning-cloud-credentials` § "SigV4/Glue derives the catalogs/{account-id} REST prefix on every catalog request" specifies, unchanged by the role

### Scenario: Session credentials are the storage credential under every catalog kind

* *GIVEN* a CONNECTION that names a role and does not set `use_vended_credentials`, under the Iceberg REST, Unity Catalog, or direct-storage catalog kind
* *WHEN* the adapter reads object storage at plan time, through Iceberg manifests, the Delta log, or a direct-storage listing, and the scan UDF reads data files
* *THEN* every such read SHALL use the session credentials as the S3 access key, secret key, and session token, with `endpoint`, `region`, and `path_style` resolved from the CONNECTION exactly as `storage_block` resolves them for a CONNECTION that names no role and does not vend
* *AND* no object-storage read SHALL use the base key pair

### Scenario: A role leaves credential vending unchanged

* *GIVEN* a CONNECTION that names a role and sets `use_vended_credentials` to true
* *WHEN* the adapter resolves a table's storage
* *THEN* the adapter SHALL resolve that storage through credential vending exactly as for the same CONNECTION without the role: it SHALL send the `X-Iceberg-Access-Delegation` header on `loadTable`, and SHALL request Unity Catalog temporary table credentials for a Delta table
* *AND* no object-storage read SHALL use the session credentials, which sign only the SigV4 catalog requests of § "Session credentials sign every SigV4 catalog request"
* *AND* the `path_style` guard of `vs-adapter/connection-credentials` § "A non-vended CONNECTION configuring a store endpoint without stating path_style is rejected" SHALL skip this CONNECTION, as it skips every CONNECTION that vends

### Scenario: The scan receives the session credentials only inside the sealed envelope

* *GIVEN* a `pushdown` request through a CONNECTION that names a role and does not set `use_vended_credentials`
* *WHEN* the adapter builds the scan-spec storage block
* *THEN* the block SHALL carry the effective backend, session credentials included, ONLY inside the sealed envelope of `vs-adapter/scan-spec-credential-reference`, one envelope per join side, and MUST NOT carry a bare CONNECTION reference
* *AND* the scan UDF SHALL read the session from that envelope and MUST NOT send any STS request, so a query costs one `AssumeRole` call whatever its shard count
* *AND* neither the session credentials, the base `secret_key`, nor the external id SHALL appear in plaintext in the returned SQL
