# Feature: Connection-Object Credential Source — AWS SigV4 Catalog-Signing Credentials

Extends `vs-adapter/connection-credentials` with the AWS Glue SigV4 catalog-signing
requirement: `access_key`, `secret_key`, and a signing region on the CONNECTION password
JSON, including the standard-AWS-Glue-endpoint derivation of the signing region.

## Background

* `access_key`, `secret_key`, and a signing region are required when `use_sigv4` is true. These sign the catalog `load_table` request, ahead of any credential vending, and stay required whether or not `use_vended_credentials` is true.
* **The SigV4 signing region is a separate value from the CONNECTION's `region`, and for a standard AWS Glue endpoint the two are independent even when both are present.** For a standard AWS Glue endpoint — an address with the `https` scheme whose host, compared case-insensitively, is exactly `glue.<region>.amazonaws.com`, where `<region>` is a commercial AWS region code (two letters, a hyphen, one or more letters, a hyphen, one or more digits, for example `us-east-1` or `ap-southeast-2`; port and path do not affect the match) — the signing region is ALWAYS the region that address names, even when `region` is also stated and even when the two differ, because a Glue catalog and the S3 buckets of its tables can sit in different regions and `region` places the store, not the signature. For any other address form — AWS GovCloud (US), AWS China, FIPS, dual-stack, VPC interface, a private or proxy host, or any `http` address — the signing region is the stated `region`. Only the SigV4 guard and catalog request signing read the signing region; `region` holds exactly what the CONNECTION states and independently places the S3 store.
* `endpoint` stays optional even when `use_sigv4` is true.

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

### Scenario: A standard AWS Glue endpoint supplies the SigV4 signing region when the CONNECTION omits region

* *GIVEN* a CONNECTION whose address is `https://glue.eu-west-1.amazonaws.com/iceberg`
* *AND* the CONNECTION's JSON password sets `use_sigv4` to true, supplies `warehouse`, `access_key`, and `secret_key`, and omits `region`
* *WHEN* the adapter resolves the connection and issues its SigV4-signed catalog requests
* *THEN* the adapter SHALL accept the CONNECTION without reporting `region` as missing, per the standard-AWS-Glue-endpoint definition in § Background
* *AND* the adapter SHALL sign every SigV4-signed catalog request for the region `eu-west-1`, covering both the namespace-enumeration requests and the `loadTable` request
* *AND* the adapter MUST NOT write the derived signing region into the CONNECTION's `region`, so `ConnectionCreds.region` SHALL stay empty and every other rule that reads it — the store-addressing precedence of `vs-adapter/pushdown-planning-cloud-credentials`, the S3 region `storage_block` builds, and the Azure-and-S3 mixed-fields guard — SHALL see it as unstated

### Scenario: A standard AWS Glue endpoint signs the catalog request even when the CONNECTION states a different region

* *GIVEN* a CONNECTION whose address is `https://glue.eu-west-1.amazonaws.com/iceberg`
* *AND* the CONNECTION's JSON password sets `use_sigv4` to true and supplies `warehouse`, `access_key`, `secret_key`, and a `region` of `us-east-1`
* *WHEN* the adapter resolves the connection and issues its SigV4-signed catalog requests
* *THEN* the adapter SHALL accept the CONNECTION
* *AND* the adapter SHALL sign every SigV4-signed catalog request for `eu-west-1`, the region the address names, and NOT for the stated `us-east-1`
* *AND* `ConnectionCreds.region` SHALL stay `us-east-1` for every other rule that reads it, so the stated `region` places the S3 store in a region DIFFERENT from the one that signs the catalog request — the deployment shape where a Glue catalog and its tables' S3 bucket sit in different AWS regions
