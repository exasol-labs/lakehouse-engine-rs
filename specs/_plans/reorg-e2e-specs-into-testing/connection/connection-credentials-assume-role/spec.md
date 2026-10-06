# Feature: Connection-Object Credential Source: AWS IAM Role Assumption

Lets a CONNECTION name an AWS IAM role that the engine assumes through AWS STS `AssumeRole`, so a base identity that holds only `sts:AssumeRole` permission reaches Glue and S3 as that role. This feature covers the CONNECTION fields that name the role, how the adapter validates them, and the `AssumeRole` call: its parameters, the plaintext-endpoint gate, the checks on its response, and the error when it fails. Tracked by issue [#139](https://github.com/exasol-labs/lakehouse-engine-rs/issues/139).

## Background

* **Three optional CONNECTION password fields.** `aws_assume_role_arn` names the role. `aws_external_id` is the optional `ExternalId` for a role whose trust policy requires one. `aws_sts_endpoint` overrides the STS endpoint. An empty string is absent, per `connection/connection-credentials`.
* **The base identity is the CONNECTION's own key pair.** `access_key`, `secret_key`, and an optional `session_token` sign the request. No ambient AWS credential is read: no environment variable, instance profile, or web-identity token.
* **The official `aws-sdk-sts` client owns the wire protocol.** It owns request encoding, SigV4 signing, response parsing, endpoint resolution, and retries. The engine owns only the fields above, the fixed session name `lakehouse-engine`, the plaintext-endpoint gate, and the session's use.
* **STS region.** The region is the SigV4 signing region of `connection/connection-credentials-sigv4`, else `us-east-1`. Without `aws_sts_endpoint`, the SDK resolves the regional endpoint. A China-region CONNECTION states `aws_sts_endpoint`, because the AWS STS endpoint table lists China endpoints under `.amazonaws.com.cn`.
* See `connection/connection-credentials-assume-role-session-use` for where the session replaces the base key pair once the role is assumed.

## Scenarios

### Scenario: A role option is accepted only beside a role

* *GIVEN* a CONNECTION that supplies `aws_external_id` or `aws_sts_endpoint` and omits `aws_assume_role_arn`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming each supplied field and stating that it requires `aws_assume_role_arn`
* *AND* the adapter SHALL accept a CONNECTION that supplies none of the three fields, the role alone, or the role with either option
* *AND* the error MUST NOT contain any supplied credential value
* *AND* the credential set's `Debug` rendering SHALL print `aws_external_id` as redacted

### Scenario: A CONNECTION naming a role carries the base identity's key pair

* *GIVEN* a CONNECTION that supplies `aws_assume_role_arn` and omits `access_key`, `secret_key`, or both
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming each omitted field and stating that the role is assumed with the CONNECTION's own `access_key` and `secret_key`
* *AND* the error MUST NOT contain any supplied credential value

### Scenario: The AssumeRole call carries the role, the external id, and the fixed session name

* *GIVEN* a CONNECTION that names a role and carries the base key pair
* *WHEN* the adapter calls `AssumeRole`
* *THEN* the call SHALL pass `RoleArn`, `RoleSessionName=lakehouse-engine`, and `ExternalId` exactly when the CONNECTION states `aws_external_id`
* *AND* the call MUST NOT pass `DurationSeconds`
* *AND* the call SHALL time out after 30 seconds with an error naming the STS endpoint host

### Scenario: A plaintext STS endpoint requires ALLOW_HTTP

* *GIVEN* a CONNECTION that names a role and states an `http://` `aws_sts_endpoint`
* *WHEN* the adapter resolves the STS endpoint
* *THEN* the adapter SHALL call the endpoint only when the `ALLOW_HTTP` virtual-schema property is true, because the response carries the session secret
* *AND* otherwise the adapter SHALL return an error naming `aws_sts_endpoint` and `ALLOW_HTTP`
* *AND* an `aws_sts_endpoint` whose scheme is neither `http` nor `https` SHALL be an error naming `aws_sts_endpoint`

### Scenario: An incomplete Credentials result is rejected

* *GIVEN* a successful STS response whose `Credentials` are absent, or whose `AccessKeyId`, `SecretAccessKey`, or `SessionToken` is empty
* *WHEN* the adapter reads the response
* *THEN* the adapter SHALL return an error naming the missing element
* *AND* the adapter MUST NOT fall back to the base identity

### Scenario: A failed AssumeRole is a credential-safe error

* *GIVEN* an STS error response, an unreachable STS endpoint, or a timed-out call
* *WHEN* the adapter handles the request that needed the session
* *THEN* the adapter SHALL fail that request with an error naming the role ARN and the STS endpoint host, plus the HTTP status, the STS error code, and the STS error message whenever the response carries them
* *AND* the adapter MUST NOT send any catalog or object-storage request with the base identity instead
* *AND* the error MUST NOT contain the base `secret_key`, the base `session_token`, the external id, or any returned credential

### Scenario: A Glue CONNECTION may name a role

* *GIVEN* a CONNECTION under the `GLUE` catalog kind that supplies `aws_assume_role_arn` beside the base key pair, optionally with `aws_external_id` or `aws_sts_endpoint`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL accept it under the Glue validation of `vs-adapter/catalog-kind-selection`, and SHALL derive its sealing key, so the scan can receive the session
* *AND* under the `GLUE` kind, `aws_external_id` or `aws_sts_endpoint` without `aws_assume_role_arn` SHALL be rejected as for every other kind
