# Feature: Connection-Object Credential Source (Direct-Storage Kind Parameterization)

Extends `vs-adapter/connection-credentials` with the `CatalogKind::DirectStorage` validation branch.
The CONNECTION address is an object-storage base path rather than a catalog URI. The password
carries storage credentials only. Every catalog-authentication field is refused at
`CREATE VIRTUAL SCHEMA` rather than accepted and ignored.

## Background

* Validation is already parameterized by the resolved `CatalogKind` and takes it as an explicit
  parameter. This feature adds the third arm and changes no rule of the other two.
* Direct storage reaches NO catalog service, so credential vending is permanently unreachable for
  this kind rather than merely unused. The vended selectors
  (`vs-adapter/pushdown-planning-cloud-credentials`) are driven by a `loadTable` response. This
  kind issues none.
* The CONNECTION address carries a URI SCHEME for this kind. The other two kinds' addresses do not
  usefully carry one. An Iceberg REST address is an HTTP endpoint. A Unity Catalog address is a
  workspace URL. The scheme is therefore an input validation can read.
* `vs-adapter/storage-backend-enum` keeps the storage-backend decision single-homed. It enumerates
  the production modules permitted to name a variant. This feature adds NO module to that list: the
  scheme agreement is answered by a method ON the enum, whose own methods are already permitted.
* `vs-adapter/connection-credentials-azure` owns the Azure credential SHAPE rule (`account_name`
  present with exactly one of `account_key` and `sas_token`). This feature reads the backend that
  rule already selects and does not restate it.

## Scenarios

### Scenario: A direct-storage CONNECTION carries storage credentials and a storage base path

* *GIVEN* a `createVirtualSchema` or `pushdown` request resolving `CatalogKind::DirectStorage`, whose CONNECTION address is an object-storage base path and whose JSON password carries only storage fields
* *WHEN* the adapter resolves and validates the connection under the direct-storage kind
* *THEN* the adapter SHALL accept the connection without reporting `warehouse` as a missing field, because this kind reaches no catalog and a warehouse identifier routes no request
* *AND* the adapter SHALL read the address as the storage BASE PATH rather than as a catalog URI, and an EMPTY address SHALL be rejected with an error naming the CONNECTION and stating that a storage base path is expected, so the message does not tell an operator to supply a catalog URI this kind never contacts
* *AND* every other recorded credential rule SHALL apply unchanged: the malformed-password rejection, the Azure-versus-S3 mixed-fields rejection, the Azure credential-shape rule, and the `endpoint`-without-`path_style` rejection
* *AND* the ONE shared storage-credential projection and its ONE backend selector SHALL serve this kind unchanged, so the adapter and the scan UDF derive a field-for-field equal backend from this CONNECTION exactly as they do for the other two kinds
* *AND* no supplied credential value SHALL appear in any error message, returned SQL, or log line

### Scenario: Catalog-authentication and vending fields are rejected, not ignored

* *GIVEN* a `createVirtualSchema` request resolving `CatalogKind::DirectStorage` whose CONNECTION JSON password supplies one or more of `warehouse`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `use_sigv4`, and `use_vended_credentials`
* *WHEN* the adapter validates the connection under the direct-storage kind
* *THEN* the adapter SHALL return an error NAMING every supplied field from that set and stating that the direct-storage kind reaches no catalog service, so an operator learns the field is meaningless here rather than silently unused
* *AND* the adapter MUST NOT accept and ignore any of those fields, because a CONNECTION carrying `use_vended_credentials` expresses an intent this kind cannot satisfy, and reading it as satisfied would read object storage under a credential the operator did not select
* *AND* a `use_sigv4` or `use_vended_credentials` set explicitly to FALSE SHALL be accepted, because false states the behaviour this kind already has
* *AND* this rejection SHALL leave the Unity Catalog kind's treatment of an unread `warehouse` unchanged, so no other kind gains or loses a rejection
* *AND* the error message MUST NOT contain any supplied credential value

### Scenario: The address scheme must agree with the credential shape

* *GIVEN* a `createVirtualSchema` request resolving `CatalogKind::DirectStorage` whose CONNECTION address carries an `s3://` scheme and whose password supplies `account_name` with an `account_key`, and a second request whose address carries an `abfss://` scheme and whose password supplies `access_key` and `secret_key`
* *WHEN* the adapter validates each connection under the direct-storage kind
* *THEN* the adapter SHALL reject BOTH with an error naming the address scheme and the credential shape the password describes, and stating that the two disagree
* *AND* the rejection SHALL happen at `CREATE VIRTUAL SCHEMA`, before any object-store request, so a mismatched pair fails at configuration time rather than at the first scan
* *AND* the agreement SHALL be decided by a method ON the storage-backend enum that answers whether a backend addresses a given URI scheme, with NO catch-all arm, so a third storage backend is a build failure at that method rather than a scheme silently accepted by every backend
* *AND* validation MUST NOT match on a storage-backend variant itself, so the enumerated list of production modules permitted to name a variant is UNCHANGED
* *AND* the accepted schemes SHALL be `s3` and `s3a` for the S3 backend and `abfss` for the Azure backend, and any other scheme SHALL be rejected with an error naming the rejected scheme and the accepted ones
* *AND* a plaintext `abfs` address SHALL be rejected naming `abfss` as the secure spelling, because the plaintext consent gate the recorded rules define lives on the vended path this kind never reaches
* *AND* the error message MUST NOT contain any supplied credential value

### Scenario: Credential vending is unreachable under the direct-storage kind

* *GIVEN* a virtual schema resolving `CatalogKind::DirectStorage` and a query over one of its tables
* *WHEN* the adapter resolves the effective scan storage for that table
* *THEN* the effective storage SHALL be the CONNECTION's static backend, and the adapter MUST NOT call either vended-storage selector, request temporary credentials, or read a vending key
* *AND* the shard-invariant common spec SHALL carry a REFERENCE to the CONNECTION that supplied the backend, per `vs-adapter/scan-spec-credential-reference`, so the scan UDF resolves the credential itself and the generated SQL carries none
* *AND* the permanent unreachability of the vended path for this kind SHALL be recorded as a property of the kind rather than as a per-CONNECTION choice, because the other two kinds select vending per CONNECTION and a reader would otherwise expect the same switch here
