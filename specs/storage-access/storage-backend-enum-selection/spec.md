# Feature: Storage Backend Selection

Decides which storage backend (S3 or ADLS Gen2) a table is read through, and from which input. Without credential vending the CONNECTION selects it. With vending the table location selects it, and the credential comes from the catalog's vended response alone.

## Background

* The static selector reads the CONNECTION credential shape and runs only when vending is disabled. The Iceberg REST and Unity Catalog vended selectors read the table location's URI scheme and run only when vending is enabled. The location's scheme is known only after `loadTable`, which runs once per table and never at `createVirtualSchema`, so one selector cannot serve both modes.
* The rule that a vended selector reads no CONNECTION credential exists to stop a vended credential silently falling back to a static one. It was never about addressing: real Databricks on AWS vends short-lived credentials with no endpoint and no region, so the CONNECTION's `endpoint`, `region`, and `path_style` may still place the store.
* `allow_http` comes from the `ALLOW_HTTP` virtual-schema property, not from the CONNECTION. Deriving it from a vended `s3.endpoint` scheme would let a catalog that vends `http://` move credentials onto plaintext transport without operator consent.
* The ADLS credential has two states, account key and SAS, so "both" and "neither" cannot be represented. `object_store` prefers an access key over a SAS, so a vended backend must never carry an account key.
* See `storage-access/storage-backend-enum` for the backend's variants, its wire encoding, and the scan's object-store registration.

## Scenarios

### Scenario: The CONNECTION selects the backend without vending and the table location selects it with vending

* *GIVEN* a virtual schema whose CONNECTION sets `use_vended_credentials`
* *WHEN* the adapter resolves a table's storage
* *THEN* with `use_vended_credentials` false, the backend SHALL be selected from the CONNECTION credential shape
* *AND* with `use_vended_credentials` true, the backend SHALL be selected from the table location's URI scheme, `s3` and `s3a` selecting S3 and `abfs` and `abfss` selecting ADLS Gen2, for both the Iceberg REST and the Unity Catalog kind
* *AND* a Unity Catalog table for which the catalog reports no storage-credential vending key SHALL fail with a user error rather than fall back to the CONNECTION's credential
* *AND* a join SHALL carry one backend per side, each read through a store built from its own backend, as `pushdown-joins/pushdown-planning-join` and `scan-aggregation/scan-execution-join` require

### Scenario: The vended selectors take a store address that cannot carry a credential

* *GIVEN* a vended resolution under either catalog kind
* *WHEN* the selector builds the backend
* *THEN* an access key, a secret key, a session token, an Azure account key, and a SAS token SHALL come from the vended response alone, and MUST NEVER be read from the CONNECTION
* *AND* the selector SHALL take the CONNECTION's `endpoint`, `region`, and `path_style` as a store address that declares no credential field
* *AND* a CONNECTION address field that is set SHALL win over the vended one, and an empty endpoint and region SHALL be accepted, because Databricks vends neither
* *AND* `path_style` SHALL resolve from the CONNECTION, else from the vended response, else to true exactly when an endpoint resolved

### Scenario: Plaintext transport needs the operator's consent on every vended path

* *GIVEN* a vended resolution under either catalog kind with the `ALLOW_HTTP` property false
* *WHEN* the resolved S3 endpoint has the `http` scheme, whether the catalog vended it or the CONNECTION configured it, or the table location has the `abfs` scheme
* *THEN* the resolution SHALL fail with a user error that names the location and the `ALLOW_HTTP` property
* *AND* both catalog kinds SHALL apply the same consent and store-address rules, so no kind accepts an input the other refuses

### Scenario: The vended selector reaches the SAS state without widening the enum

* *GIVEN* an ADLS Gen2 table location and a vended SAS token
* *WHEN* the vended selector builds the ADLS backend
* *THEN* it SHALL build the SAS state and MUST NOT build the account-key state, so `object_store`'s access-key-beats-SAS precedence stays unreachable
* *AND* the account name SHALL be the first label of the location's host, compared byte for byte, and a host with no leading label SHALL fail with a user error
* *AND* the backend's secret values SHALL return the vended SAS, so the error redaction covers it exactly as it covers a static SAS
