# Feature: Connection-Object Credential Source: AWS IAM Role Assumption

Lets a CONNECTION name an AWS IAM role that the engine assumes through AWS STS `AssumeRole`. A base identity that holds only `sts:AssumeRole` permission then reaches Glue and S3 as that role. The role's short-lived session credentials replace the CONNECTION's static key pair wherever that pair is read: SigV4 catalog signing, and object storage when the CONNECTION does not vend. A role leaves credential vending unchanged. A role CONNECTION's storage credential reaches the scan UDF only inside the sealed envelope. Tracked by issue [#139](https://github.com/exasol-labs/lakehouse-engine-rs/issues/139).

## Background

* **Three optional CONNECTION password fields.** `aws_assume_role_arn` names the role. `aws_external_id` is the `ExternalId` sent for a role whose trust policy requires one. `aws_sts_endpoint` overrides the STS endpoint. Each is a string, and an empty string is absent, per the parsing rule of `vs-adapter/connection-credentials`.
* **The base identity is the CONNECTION's own key pair.** `access_key`, `secret_key`, and an optional `session_token` sign the `AssumeRole` request. No ambient AWS credential is read: no environment variable, instance profile, or web-identity token. This keeps the explicit-CONNECTION credential model.
* **The STS request shape follows the AWS STS API reference (`API_AssumeRole`).** `RoleArn` is "Required: Yes". `RoleSessionName` is "Required: Yes", with "Minimum length of 2. Maximum length of 64" and pattern `[\w+=,.@-]*`. `ExternalId` is "Required: No", described as "A unique identifier that might be required when you assume a role in another account", with pattern `[\w+=,.@:\/-]*`. `DurationSeconds` defaults to `3600`. The sample response carries `AssumeRoleResponse/AssumeRoleResult/Credentials` with `AccessKeyId`, `SecretAccessKey`, `SessionToken`, and `Expiration`, under namespace `https://sts.amazonaws.com/doc/2011-06-15/`.
* **STS endpoint and signing region.** The STS region is the SigV4 signing region `vs-adapter/connection-credentials-sigv4` resolves: the region a standard AWS Glue endpoint names, else the stated `region`. The endpoint is `aws_sts_endpoint` when stated, else `https://sts.<region>.amazonaws.com` for a resolved region, else the global `https://sts.amazonaws.com`. A request with no resolved region is signed for `us-east-1`. A China-region CONNECTION states `aws_sts_endpoint`, because the AWS STS endpoint table lists China endpoints under `.amazonaws.com.cn`.

## Scenarios

### Scenario: An external id is accepted only beside a role

* *GIVEN* a CONNECTION whose JSON password supplies `aws_external_id` as a non-empty string and omits `aws_assume_role_arn`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming `aws_external_id` and stating that it requires `aws_assume_role_arn`
* *AND* the adapter SHALL accept a CONNECTION that supplies neither field, `aws_assume_role_arn` alone, or both, when it has no other defect
* *AND* the error MUST NOT contain any supplied credential value, including the external id
* *AND* the credential set's `Debug` rendering SHALL print `aws_external_id` as redacted

### Scenario: A CONNECTION naming a role carries the base identity's key pair

* *GIVEN* a CONNECTION that supplies `aws_assume_role_arn` and omits `access_key`, `secret_key`, or both
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming each omitted field and stating that the role is assumed with the CONNECTION's own `access_key` and `secret_key`
* *AND* the error SHALL state that no ambient AWS credential, such as an environment variable or an instance profile, is read
* *AND* the error MUST NOT contain any supplied credential value

### Scenario: An STS endpoint override is accepted only beside a role

* *GIVEN* a CONNECTION that supplies `aws_sts_endpoint` and omits `aws_assume_role_arn`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming `aws_sts_endpoint` and stating that it requires `aws_assume_role_arn`
* *AND* the error MUST NOT contain any supplied credential value

### Scenario: The adapter assumes the role exactly once per request, before any catalog or storage access

* *GIVEN* a CONNECTION that names a role and carries the base key pair
* *WHEN* the adapter handles one `createVirtualSchema`, `refresh`, `setProperties`, or `pushdown` request through that CONNECTION
* *THEN* the adapter SHALL send exactly ONE STS `AssumeRole` request before its first catalog or object-storage request, and SHALL use that one session for every table the request resolves, including every side of a join
* *AND* the adapter MUST NOT reuse a session across two requests, because the adapter keeps no state between requests
* *AND* a CONNECTION that names no role SHALL cause no STS request

### Scenario: The AssumeRole request carries the role, the external id, and the base identity's signature

* *GIVEN* a CONNECTION that names a role and carries the base key pair
* *WHEN* the adapter sends the `AssumeRole` request
* *THEN* the request SHALL be an HTTP `GET` whose query carries `Action=AssumeRole`, `Version=2011-06-15`, `RoleArn`, and `RoleSessionName=lakehouse-engine`, SHALL carry `ExternalId` exactly when the CONNECTION states `aws_external_id`, and MUST NOT carry `DurationSeconds`
* *AND* the request SHALL be SigV4-signed for the service `sts` and the STS region of § Background, with the base `access_key` and `secret_key`, and with the base `session_token` as `x-amz-security-token` when the CONNECTION states one
* *AND* every character the `ExternalId` pattern admits (`+`, `=`, `,`, `.`, `@`, `:`, `/`, `-`, and word characters) SHALL be encoded identically in the sent query and in the signed canonical query, so STS recomputes the same signature

### Scenario: The STS endpoint is resolved from the CONNECTION and gated on plaintext consent

* *GIVEN* a CONNECTION that names a role
* *WHEN* the adapter resolves the STS endpoint
* *THEN* the adapter SHALL use the endpoint and signing region § Background resolves
* *AND* the adapter SHALL send the request to an `http://` `aws_sts_endpoint` only when the `ALLOW_HTTP` virtual-schema property is true, and otherwise SHALL return an error naming `aws_sts_endpoint` and `ALLOW_HTTP`, because the response carries the session secret
* *AND* an `aws_sts_endpoint` whose scheme is neither `http` nor `https` SHALL be an error naming `aws_sts_endpoint`
* *AND* the request SHALL time out after 30 seconds with an error naming the STS endpoint host

### Scenario: The AssumeRole response yields the session credentials

* *GIVEN* an STS response with a 2xx status whose body is an `AssumeRoleResponse` document
* *WHEN* the adapter reads the response
* *THEN* the adapter SHALL take `AccessKeyId`, `SecretAccessKey`, and `SessionToken` from `AssumeRoleResult/Credentials`, each with surrounding whitespace removed and XML entity references decoded
* *AND* an absent or empty one of those three elements SHALL be an error naming the element, and the adapter MUST NOT fall back to the base identity
* *AND* a body that is not a well-formed `AssumeRoleResponse` document SHALL be an error stating so, and MUST NOT contain the body text, because the body can carry a secret

### Scenario: A failed AssumeRole is a clear, credential-safe error

* *GIVEN* an STS response with a non-2xx status, an unreachable STS endpoint, or a timed-out request
* *WHEN* the adapter handles the request that needed the session
* *THEN* the adapter SHALL fail that request with a `UdfError::User` naming the role ARN and the STS endpoint host, plus the HTTP status, the STS `Error/Code`, and the STS `Error/Message` whenever the response carries them
* *AND* the adapter MUST NOT send any catalog or object-storage request with the base identity instead
* *AND* the error MUST NOT contain the base `secret_key`, the base `session_token`, the external id, any returned credential, or the request URL's query string, which carries the external id
* *AND* the error MUST be returned as a `Result` and never raised as a panic, because a panic inside a UDF is an abnormal VM exit

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
