# Feature: Connection-Object Credential Source: AWS IAM Role Assumption

Lets a CONNECTION name an AWS IAM role that the engine assumes through AWS STS `AssumeRole`. A base identity that holds only `sts:AssumeRole` permission then reaches Glue and S3 as that role. Tracked by issue [#139](https://github.com/exasol-labs/lakehouse-engine-rs/issues/139).

## Background

* **Three optional CONNECTION password fields.** `aws_assume_role_arn` names the role. `aws_external_id` is the optional `ExternalId` for a role whose trust policy requires one. `aws_sts_endpoint` overrides the STS endpoint. An empty string is absent, per `vs-adapter/connection-credentials`.
* **The base identity is the CONNECTION's own key pair.** `access_key`, `secret_key`, and an optional `session_token` sign the request. No ambient AWS credential is read: no environment variable, instance profile, or web-identity token.
* **The official `aws-sdk-sts` client owns the wire protocol.** It owns request encoding, SigV4 signing, response parsing, endpoint resolution, and retries. The engine owns only the fields above, the fixed session name `lakehouse-engine`, the plaintext-endpoint gate, and the session's use.
* **STS region.** The region is the SigV4 signing region of `vs-adapter/connection-credentials-sigv4`, else `us-east-1`. Without `aws_sts_endpoint`, the SDK resolves the regional endpoint. A China-region CONNECTION states `aws_sts_endpoint`, because the AWS STS endpoint table lists China endpoints under `.amazonaws.com.cn`.
* **The session replaces the key pair only where the pair is read.** Those reads are SigV4 catalog signing, the native Glue API requests of the `GLUE` catalog kind, and non-vended object storage.
* **A cross-account Glue `CatalogId` is an untested exception (#TBD).** A `GLUE` CONNECTION whose `warehouse` names another account's `CatalogId` sends that id unchanged, but no test covers it. The supported way to read another account's Glue catalog is a role in that account with `warehouse` omitted, so Glue resolves the role's own account.

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: A role option is accepted only beside a role

* *GIVEN* a CONNECTION that supplies `aws_external_id` or `aws_sts_endpoint` and omits `aws_assume_role_arn`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming each supplied field and stating that it requires `aws_assume_role_arn`
* *AND* the adapter SHALL accept a CONNECTION that supplies none of the three fields, the role alone, or the role with either option
* *AND* the error MUST NOT contain any supplied credential value
* *AND* the credential set's `Debug` rendering SHALL print `aws_external_id` as redacted
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: A CONNECTION naming a role carries the base identity's key pair

* *GIVEN* a CONNECTION that supplies `aws_assume_role_arn` and omits `access_key`, `secret_key`, or both
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming each omitted field and stating that the role is assumed with the CONNECTION's own `access_key` and `secret_key`
* *AND* the error MUST NOT contain any supplied credential value
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: The adapter assumes the role once per request and substitutes the session

* *GIVEN* a CONNECTION that names a role and carries the base key pair
* *WHEN* the adapter handles one `createVirtualSchema`, `refresh`, `setProperties`, or `pushdown` request through that CONNECTION
* *THEN* the adapter SHALL send exactly ONE `AssumeRole` call before its first catalog or object-storage request, and SHALL use that session for every table the request resolves, including every side of a join
* *AND* ONE `lakehouse-catalog` function SHALL return the credential set with `access_key`, `secret_key`, and `session_token` replaced by the session's and every other field unchanged, so the catalog session, the format readers, the direct-storage store, and error redaction read the session without naming the role
* *AND* the acceptance validation and the sealing key SHALL be computed from the CONNECTION as stated, before the replacement
* *AND* a CONNECTION that names no role SHALL cause no STS call, and the adapter MUST NOT reuse a session across requests
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: The AssumeRole call carries the role, the external id, and the fixed session name

* *GIVEN* a CONNECTION that names a role and carries the base key pair
* *WHEN* the adapter calls `AssumeRole`
* *THEN* the call SHALL pass `RoleArn`, `RoleSessionName=lakehouse-engine`, and `ExternalId` exactly when the CONNECTION states `aws_external_id`
* *AND* the call MUST NOT pass `DurationSeconds`
* *AND* the call SHALL time out after 30 seconds with an error naming the STS endpoint host
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: A plaintext STS endpoint requires ALLOW_HTTP

* *GIVEN* a CONNECTION that names a role and states an `http://` `aws_sts_endpoint`
* *WHEN* the adapter resolves the STS endpoint
* *THEN* the adapter SHALL call the endpoint only when the `ALLOW_HTTP` virtual-schema property is true, because the response carries the session secret
* *AND* otherwise the adapter SHALL return an error naming `aws_sts_endpoint` and `ALLOW_HTTP`
* *AND* an `aws_sts_endpoint` whose scheme is neither `http` nor `https` SHALL be an error naming `aws_sts_endpoint`
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: An incomplete Credentials result is rejected

* *GIVEN* a successful STS response whose `Credentials` are absent, or whose `AccessKeyId`, `SecretAccessKey`, or `SessionToken` is empty
* *WHEN* the adapter reads the response
* *THEN* the adapter SHALL return an error naming the missing element
* *AND* the adapter MUST NOT fall back to the base identity
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: A failed AssumeRole is a credential-safe error

* *GIVEN* an STS error response, an unreachable STS endpoint, or a timed-out call
* *WHEN* the adapter handles the request that needed the session
* *THEN* the adapter SHALL fail that request with an error naming the role ARN and the STS endpoint host, plus the HTTP status, the STS error code, and the STS error message whenever the response carries them
* *AND* the adapter MUST NOT send any catalog or object-storage request with the base identity instead
* *AND* the error MUST NOT contain the base `secret_key`, the base `session_token`, the external id, or any returned credential
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Session credentials sign every SigV4 catalog request

* *GIVEN* a CONNECTION that sets `use_sigv4` to true and names a role, whether or not it sets `use_vended_credentials`
* *WHEN* the adapter issues its SigV4-signed catalog requests, covering namespace enumeration and `loadTable`
* *THEN* each request SHALL be signed with the session `AccessKeyId` and `SecretAccessKey` and SHALL carry the session `SessionToken` as `x-amz-security-token`
* *AND* no catalog request SHALL be signed with the base key pair
* *AND* the signing region and the catalog prefix SHALL be those of `vs-adapter/connection-credentials-sigv4` and `vs-adapter/pushdown-planning-cloud-credentials`, unchanged by the role
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Session credentials are the storage credential when the CONNECTION does not vend

* *GIVEN* a CONNECTION that names a role and does not set `use_vended_credentials`, under the Iceberg REST, Unity Catalog, Glue, or direct-storage catalog kind
* *WHEN* the adapter reads object storage at plan time and the scan UDF reads data files
* *THEN* every read SHALL use the session credentials as the S3 access key, secret key, and session token, with `endpoint`, `region`, and `path_style` resolved from the CONNECTION as for a CONNECTION that names no role
* *AND* no object-storage read SHALL use the base key pair
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: A Glue CONNECTION may name a role

* *GIVEN* a CONNECTION under the `GLUE` catalog kind that supplies `aws_assume_role_arn` beside the base key pair, optionally with `aws_external_id` or `aws_sts_endpoint`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL accept it under the Glue validation of `vs-adapter/catalog-kind-selection`, and SHALL derive its sealing key, so the scan can receive the session
* *AND* under the `GLUE` kind, `aws_external_id` or `aws_sts_endpoint` without `aws_assume_role_arn` SHALL be rejected as for every other kind
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Session credentials sign every native Glue request

* *GIVEN* a CONNECTION under the `GLUE` catalog kind that names a role
* *WHEN* the adapter calls `GetTables`, `GetTable`, and `GetPartitions`, and reads an Iceberg metadata file that Glue points to
* *THEN* each Glue request SHALL be signed with the session credentials, and the metadata file SHALL be read with them as the storage credential
* *AND* the Glue signing region SHALL be the region STS was called for, so one region serves both
* *AND* a `GLUE` CONNECTION that omits `warehouse` SHALL send no `CatalogId`, so Glue resolves the role's account
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: A role leaves credential vending unchanged

* *GIVEN* a CONNECTION that names a role and sets `use_vended_credentials` to true
* *WHEN* the adapter resolves a table's storage
* *THEN* the adapter SHALL resolve that storage through credential vending exactly as without the role, including the `X-Iceberg-Access-Delegation` header on `loadTable` and Unity Catalog temporary table credentials for a Delta table
* *AND* no object-storage read SHALL use the session credentials, which sign only the SigV4 catalog requests
* *AND* the `path_style` guard of `vs-adapter/connection-credentials` SHALL skip this CONNECTION, as it skips every CONNECTION that vends
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: The scan receives the session credentials only inside the sealed envelope

* *GIVEN* a `pushdown` request through a CONNECTION that names a role and does not set `use_vended_credentials`
* *WHEN* the adapter builds the scan-spec storage block
* *THEN* the block SHALL carry the session credentials ONLY inside the sealed envelope of `vs-adapter/scan-spec-credential-reference`, one envelope per join side, and MUST NOT carry a bare CONNECTION reference
* *AND* the scan UDF SHALL read the session from that envelope and MUST NOT call STS, so a query costs one `AssumeRole` call whatever its shard count
* *AND* neither the session credentials, the base `secret_key`, nor the external id SHALL appear in plaintext in the returned SQL
<!-- /DELTA:REMOVED -->
