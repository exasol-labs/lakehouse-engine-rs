# Feature: Assumed-Role Session Use

Defines where the session of an assumed AWS IAM role replaces the CONNECTION's own key pair. The session signs SigV4 catalog requests and native Glue requests, serves as the storage credential when the CONNECTION does not vend, and reaches the scan only inside the sealed envelope. A CONNECTION that names no role reaches the catalog with its own key pair, and a CONNECTION that vends keeps its vended storage credentials. Tracked by issue [#139](https://github.com/exasol-labs/lakehouse-engine-rs/issues/139).

## Background

* **The base identity is the CONNECTION's own key pair.** `access_key`, `secret_key`, and an optional `session_token` sign the request. No ambient AWS credential is read: no environment variable, instance profile, or web-identity token.
* **STS region.** The region is the SigV4 signing region of `connection/connection-credentials-sigv4`, else `us-east-1`. Without `aws_sts_endpoint`, the SDK resolves the regional endpoint. A China-region CONNECTION states `aws_sts_endpoint`, because the AWS STS endpoint table lists China endpoints under `.amazonaws.com.cn`.
* **The session replaces the key pair only where the pair is read.** Those reads are SigV4 catalog signing, the native Glue API requests of the `GLUE` catalog kind, and non-vended object storage.
* **A cross-account Glue `CatalogId` is an untested exception (#TBD).** A `GLUE` CONNECTION whose `warehouse` names another account's `CatalogId` sends that id unchanged, but no test covers it. The supported way to read another account's Glue catalog is a role in that account with `warehouse` omitted, so Glue resolves the role's own account.
* See `connection/connection-credentials-assume-role` for the CONNECTION fields that name the role, their validation, and the `AssumeRole` call.

## Scenarios

### Scenario: The adapter assumes the role once per request and substitutes the session

* *GIVEN* a CONNECTION that names a role and carries the base key pair
* *WHEN* the adapter handles one `createVirtualSchema`, `refresh`, `setProperties`, or `pushdown` request through that CONNECTION
* *THEN* the adapter SHALL send exactly ONE `AssumeRole` call before its first catalog or object-storage request, and SHALL use that session for every table the request resolves, including every side of a join
* *AND* ONE `lakehouse-catalog` function SHALL return the credential set with `access_key`, `secret_key`, and `session_token` replaced by the session's and every other field unchanged, so the catalog session, the format readers, the direct-storage store, and error redaction read the session without naming the role
* *AND* the acceptance validation and the sealing key SHALL be computed from the CONNECTION as stated, before the replacement
* *AND* a CONNECTION that names no role SHALL cause no STS call, and the adapter MUST NOT reuse a session across requests

### Scenario: A catalog denial of a CONNECTION that names no role is a credential-safe error

* *GIVEN* a CONNECTION that sets `use_sigv4` to true, carries the base key pair and `region`, and names no role
* *AND* a catalog that denies the identity of that key pair
* *WHEN* the adapter handles a `createVirtualSchema` request through that CONNECTION
* *THEN* the adapter SHALL fail the request with an error that reports the catalog's access denial
* *AND* the error MUST NOT contain any supplied credential value

### Scenario: Session credentials sign every SigV4 catalog request

* *GIVEN* a CONNECTION that sets `use_sigv4` to true and names a role, whether or not it sets `use_vended_credentials`
* *WHEN* the adapter issues its SigV4-signed catalog requests, covering namespace enumeration and `loadTable`
* *THEN* each request SHALL be signed with the session `AccessKeyId` and `SecretAccessKey` and SHALL carry the session `SessionToken` as `x-amz-security-token`
* *AND* no catalog request SHALL be signed with the base key pair
* *AND* the signing region and the catalog prefix SHALL be those of `connection/connection-credentials-sigv4` and `storage-access/pushdown-planning-cloud-credentials`, unchanged by the role

### Scenario: Session credentials are the storage credential when the CONNECTION does not vend

* *GIVEN* a CONNECTION that names a role and does not set `use_vended_credentials`, under the Iceberg REST, Unity Catalog, Glue, or direct-storage catalog kind
* *WHEN* the adapter reads object storage at plan time and the scan UDF reads data files
* *THEN* every read SHALL use the session credentials as the S3 access key, secret key, and session token, with `endpoint`, `region`, and `path_style` resolved from the CONNECTION as for a CONNECTION that names no role
* *AND* no object-storage read SHALL use the base key pair

### Scenario: Session credentials sign every native Glue request

* *GIVEN* a CONNECTION under the `GLUE` catalog kind that names a role
* *WHEN* the adapter calls `GetTables`, `GetTable`, and `GetPartitions`, and reads an Iceberg metadata file that Glue points to
* *THEN* each Glue request SHALL be signed with the session credentials, and the metadata file SHALL be read with them as the storage credential
* *AND* the Glue signing region SHALL be the region STS was called for, so one region serves both
* *AND* a `GLUE` CONNECTION that omits `warehouse` SHALL send no `CatalogId`, so Glue resolves the role's account

### Scenario: A role leaves credential vending unchanged

* *GIVEN* a CONNECTION that names a role and sets `use_vended_credentials` to true
* *WHEN* the adapter resolves a table's storage
* *THEN* the adapter SHALL resolve that storage through credential vending exactly as without the role, including the `X-Iceberg-Access-Delegation` header on `loadTable` and Unity Catalog temporary table credentials for a Delta table
* *AND* no object-storage read SHALL use the session credentials, which sign only the SigV4 catalog requests
* *AND* the `path_style` guard of `connection/connection-credentials` SHALL skip this CONNECTION, as it skips every CONNECTION that vends

### Scenario: The scan receives the session credentials only inside the sealed envelope

* *GIVEN* a `pushdown` request through a CONNECTION that names a role and does not set `use_vended_credentials`
* *WHEN* the adapter builds the scan-spec storage block
* *THEN* the block SHALL carry the session credentials ONLY inside the sealed envelope of `storage-access/scan-spec-credential-reference`, one envelope per join side, and MUST NOT carry a bare CONNECTION reference
* *AND* the scan UDF SHALL read the session from that envelope and MUST NOT call STS, so a query costs one `AssumeRole` call whatever its shard count
* *AND* neither the session credentials, the base `secret_key`, nor the external id SHALL appear in plaintext in the returned SQL
