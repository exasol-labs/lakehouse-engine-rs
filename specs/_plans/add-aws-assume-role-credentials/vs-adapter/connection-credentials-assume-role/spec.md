# Feature: Connection-Object Credential Source: AWS IAM Role Assumption

Lets a CONNECTION name an AWS IAM role that the engine assumes through AWS STS `AssumeRole`. A base identity that holds only `sts:AssumeRole` permission then reaches Glue and S3 as that role. The role's short-lived session credentials replace the CONNECTION's static key pair for every AWS request a query makes. They reach the scan UDF only inside the sealed envelope. Tracked by issue [#139](https://github.com/exasol-labs/lakehouse-engine-rs/issues/139).

## Background

* **Three optional CONNECTION password fields.** `aws_assume_role_arn` names the role. `aws_external_id` is the `ExternalId` the role's trust policy requires. `aws_sts_endpoint` overrides the STS endpoint. Each is a string, and an empty string is absent, per the parsing rule of `vs-adapter/connection-credentials`.
* **The pairing rule is a product rule, stricter than AWS.** The AWS STS API reference lists `ExternalId` as "Required: No" and describes it as "A unique identifier that might be required when you assume a role in another account." This engine requires `aws_assume_role_arn` and `aws_external_id` together, as defense in depth against the confused-deputy problem for cross-account roles.
* **The base identity is the CONNECTION's own key pair.** `access_key`, `secret_key`, and an optional `session_token` sign the `AssumeRole` request. No ambient AWS credential is read: no environment variable, instance profile, or web-identity token. This keeps the explicit-CONNECTION credential model.
* **The STS request shape follows the AWS STS API reference (`API_AssumeRole`).** `RoleArn` is "Required: Yes". `RoleSessionName` is "Required: Yes", with "Minimum length of 2. Maximum length of 64" and pattern `[\w+=,.@-]*`. `ExternalId` has pattern `[\w+=,.@:\/-]*`. `DurationSeconds` defaults to `3600`. The sample response carries `AssumeRoleResponse/AssumeRoleResult/Credentials` with `AccessKeyId`, `SecretAccessKey`, `SessionToken`, and `Expiration`, under namespace `https://sts.amazonaws.com/doc/2011-06-15/`.
* **STS endpoint and signing region.** The STS region is the SigV4 signing region `vs-adapter/connection-credentials-sigv4` resolves: the region a standard AWS Glue endpoint names, else the stated `region`. The endpoint is `aws_sts_endpoint` when stated, else `https://sts.<region>.amazonaws.com` for a resolved region, else the global `https://sts.amazonaws.com`. A request with no resolved region is signed for `us-east-1`. A China-region CONNECTION states `aws_sts_endpoint`, because the AWS STS endpoint table lists China endpoints under `.amazonaws.com.cn`.
* **The storage credential source has one precedence.** Three sources exist: the assumed role, credentials the catalog vends, and the CONNECTION's own static credentials. The assumed role wins, then vending, then the CONNECTION. `use_vended_credentials` therefore has no effect on a CONNECTION that names a role.
* **Omitting the access-delegation header is spec-compliant.** The Iceberg REST OpenAPI (`open-api/rest-catalog-open-api.yaml`, main) marks `X-Iceberg-Access-Delegation` `required: false` and describes it as an "Optional signal to the server that the client supports delegated access".

## Scenarios

### Scenario: A role and its external id are supplied together or not at all

* *GIVEN* a CONNECTION whose JSON password supplies exactly one of `aws_assume_role_arn` and `aws_external_id` as a non-empty string
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming the absent field and stating that `aws_assume_role_arn` and `aws_external_id` MUST be supplied together
* *AND* the adapter SHALL accept a CONNECTION that supplies both fields or neither, when it has no other defect
* *AND* the error MUST NOT contain any supplied credential value, including the external id
* *AND* the credential set's `Debug` rendering SHALL print `aws_external_id` as redacted

### Scenario: A CONNECTION naming a role carries the base identity's key pair

* *GIVEN* a CONNECTION that supplies `aws_assume_role_arn` and `aws_external_id` and omits `access_key`, `secret_key`, or both
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

* *GIVEN* a CONNECTION that names a role, its external id, and the base key pair
* *WHEN* the adapter handles one `createVirtualSchema`, `refresh`, `setProperties`, or `pushdown` request through that CONNECTION
* *THEN* the adapter SHALL send exactly ONE STS `AssumeRole` request before its first catalog or object-storage request, and SHALL use that one session for every table the request resolves, including every side of a join
* *AND* the adapter MUST NOT reuse a session across two requests, because the adapter keeps no state between requests
* *AND* a CONNECTION that names no role SHALL cause no STS request

### Scenario: The AssumeRole request carries the role, the external id, and the base identity's signature

* *GIVEN* a CONNECTION that names a role, its external id, and the base key pair
* *WHEN* the adapter sends the `AssumeRole` request
* *THEN* the request SHALL be an HTTP `GET` whose query carries `Action=AssumeRole`, `Version=2011-06-15`, `RoleArn`, `ExternalId`, and `RoleSessionName=lakehouse-engine`, and MUST NOT carry `DurationSeconds`
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
* *THEN* exactly ONE `lakehouse-catalog` function SHALL return the credential set with `access_key`, `secret_key`, and `session_token` replaced by the session's, and every other field unchanged, `aws_assume_role_arn` included
* *AND* the adapter SHALL build the request's static storage backend from that returned set, so the catalog session, the format readers, the direct-storage store, and error redaction read the session without naming the role
* *AND* for a set that names no role, the function SHALL return the set unchanged and SHALL send no network request
* *AND* the acceptance validation and the sealing key SHALL be computed from the CONNECTION as stated, before the replacement, so neither reads a session value

### Scenario: Session credentials sign every SigV4 catalog request

* *GIVEN* a CONNECTION that sets `use_sigv4` to true and names a role
* *WHEN* the adapter issues its SigV4-signed catalog requests, covering namespace enumeration and `loadTable`
* *THEN* each request SHALL be signed with the session `AccessKeyId` and `SecretAccessKey` and SHALL carry the session `SessionToken` as `x-amz-security-token`
* *AND* no catalog request SHALL be signed with the base key pair
* *AND* the signing region SHALL be the one `vs-adapter/connection-credentials-sigv4` resolves, unchanged by the role
* *AND* each catalog request SHALL carry the prefix that `vs-adapter/pushdown-planning-cloud-credentials` § "SigV4/Glue derives the catalogs/{account-id} REST prefix on every catalog request" specifies, unchanged by the role

### Scenario: Session credentials are the storage credential under every catalog kind

* *GIVEN* a CONNECTION that names a role under the Iceberg REST, Unity Catalog, or direct-storage catalog kind
* *WHEN* the adapter reads object storage at plan time, through Iceberg manifests, the Delta log, or a direct-storage listing, and the scan UDF reads data files
* *THEN* every such read SHALL use the session credentials as the S3 access key, secret key, and session token, with `endpoint`, `region`, and `path_style` resolved from the CONNECTION exactly as `storage_block` resolves them for a CONNECTION that names no role and does not vend
* *AND* no object-storage read SHALL use the base key pair

### Scenario: An assumed role wins over vending for storage

* *GIVEN* a CONNECTION that names a role and sets `use_vended_credentials` to true
* *WHEN* the adapter resolves a table's storage
* *THEN* the adapter SHALL accept the CONNECTION, and the table's storage SHALL resolve exactly as § "Session credentials are the storage credential under every catalog kind" requires
* *AND* the adapter MUST NOT send the `X-Iceberg-Access-Delegation` header and MUST NOT request Unity Catalog temporary table credentials, because no vended credential would be read, per § Background
* *AND* the adapter SHALL apply the `path_style` guard of `vs-adapter/connection-credentials` § "A non-vended CONNECTION configuring a store endpoint without stating path_style is rejected" to this CONNECTION, because its storage resolves on the static path

### Scenario: One function owns the storage credential source precedence

* *GIVEN* the three storage credential sources of § Background
* *WHEN* any site decides where a table's storage credential comes from: the Iceberg and Delta readers' vended-versus-static split, the `loadTable` access-delegation header, the Unity Catalog temporary-credentials request, the scan-spec storage wire variant, or the `path_style` guard
* *THEN* exactly ONE `lakehouse-catalog` function over the parsed credential set SHALL return the source: the assumed role when `aws_assume_role_arn` is present, else vending when `use_vended_credentials` is true, else the CONNECTION
* *AND* each site SHALL match that function's result exhaustively and MUST NOT re-read `aws_assume_role_arn` or `use_vended_credentials` itself, so a fourth source fails to compile at every site

### Scenario: The scan receives the session credentials only inside the sealed envelope

* *GIVEN* a `pushdown` request through a CONNECTION that names a role
* *WHEN* the adapter builds the scan-spec storage block
* *THEN* the block SHALL carry the effective backend, session credentials included, ONLY inside the sealed envelope of `vs-adapter/scan-spec-credential-reference`, one envelope per join side, and MUST NOT carry a bare CONNECTION reference
* *AND* the scan UDF SHALL read the session from that envelope and MUST NOT send any STS request, so a query costs one `AssumeRole` call whatever its shard count
* *AND* neither the session credentials, the base `secret_key`, nor the external id SHALL appear in plaintext in the returned SQL
