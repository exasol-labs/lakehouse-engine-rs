# Feature: Connection-Object Credential Source — AWS SigV4 Catalog-Signing Credentials

Extends `vs-adapter/connection-credentials` with the AWS Glue SigV4 catalog-signing
requirement: `access_key`, `secret_key`, and a signing region on the CONNECTION password
JSON, including the standard-AWS-Glue-endpoint derivation of the signing region.

<!-- DELTA:CHANGED -->
## Background

* `access_key`, `secret_key`, and a signing region are required when `use_sigv4` is true, and stay required whether or not `use_vended_credentials` is true. The key pair signs the catalog `load_table` request, ahead of any credential vending. When the CONNECTION names `aws_assume_role_arn`, the key pair instead signs the STS `AssumeRole` request, and the session it returns signs the catalog requests (`vs-adapter/connection-credentials-assume-role`).
* **The SigV4 signing region is a separate value from the CONNECTION's `region`, and for a standard AWS Glue endpoint the two are independent even when both are present.** For a standard AWS Glue endpoint — an address with the `https` scheme whose host, compared case-insensitively, is exactly `glue.<region>.amazonaws.com`, where `<region>` is a commercial AWS region code (two letters, a hyphen, one or more letters, a hyphen, one or more digits, for example `us-east-1` or `ap-southeast-2`; port and path do not affect the match) — the signing region is ALWAYS the region that address names, even when `region` is also stated and even when the two differ, because a Glue catalog and the S3 buckets of its tables can sit in different regions and `region` places the store, not the signature. For any other address form — AWS GovCloud (US), AWS China, FIPS, dual-stack, VPC interface, a private or proxy host, or any `http` address — the signing region is the stated `region`. Only the SigV4 guard, catalog request signing, and the STS `AssumeRole` request of `vs-adapter/connection-credentials-assume-role` read the signing region; `region` holds exactly what the CONNECTION states and independently places the S3 store.
* `endpoint` stays optional even when `use_sigv4` is true.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: When SigV4 is enabled, access_key, secret_key, and a signing region are required

* *GIVEN* a CONNECTION whose JSON password sets `use_sigv4` to true and supplies `warehouse`
* *AND* the password omits `access_key`, omits `secret_key`, or omits `region` while the CONNECTION address is not a standard AWS Glue endpoint, per § Background
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming each omitted field among `access_key` and `secret_key`, and naming `region` when the CONNECTION supplies no signing region
* *AND* the error SHALL state that the named fields are required when SigV4 signing is enabled
* *AND* when the error names `region`, the error SHALL also state that a CONNECTION whose address is a standard AWS Glue endpoint of the form `https://glue.<region>.amazonaws.com` can omit `region`
* *AND* the adapter SHALL apply this guard even when `use_vended_credentials` is true, because the SigV4 inputs sign the catalog `load_table` request before any vended credential is used
* *AND* `endpoint` SHALL remain optional even when `use_sigv4` is true
* *AND* the error message MUST NOT contain any supplied credential value
