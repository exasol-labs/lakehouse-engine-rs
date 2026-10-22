# Feature: Catalog Kind Selection

The adapter reads the `CATALOG_KIND` virtual-schema property to choose the catalog kind: Iceberg REST, native Unity Catalog, direct storage, or native AWS Glue. It then builds the matching catalog client, and every createVirtualSchema listing runs through the shared `CatalogClient` trait. An absent property selects Iceberg REST.

## Background

* `CATALOG_KIND` is a plain VS property of the createVirtualSchema request, not a field in the CONNECTION password JSON. Its value is compared case-insensitively.
* The accepted values are `UNITY_CATALOG`, `DIRECT_STORAGE`, and `GLUE`. `ICEBERG_REST` is not accepted, because Iceberg REST is selected by leaving the property absent.

## Scenarios

### Scenario: Absent CATALOG_KIND resolves the Iceberg REST catalog kind

* *GIVEN* a createVirtualSchema or pushdown request whose plain VS properties do not include `CATALOG_KIND`
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL resolve `CatalogKind::IcebergRest` and construct the Iceberg REST catalog client, and MUST NOT read `CATALOG_KIND` from the CONNECTION password JSON
* *AND* enumerating a namespace that contains no table SHALL build NO resolution-phase `CatalogSession` and perform NO resolution-phase OAuth2 grant. The namespace-enumeration `RestCatalog` keeps its OWN grant, so an empty namespace costs exactly ONE grant under the OAuth2 client-credentials mode and ZERO under the no-auth and static-token modes (see `catalog/pushdown-catalog-session`). The enumeration grant is unavoidable, because only a catalog call can show that the namespace is empty. A virtual schema over an empty namespace whose credentials would fail a grant therefore succeeds ONLY in the no-auth and static-token modes, and under OAuth2 it can fail on the enumeration grant. Enumerating a namespace with at least one table SHALL build EXACTLY ONE resolution `CatalogSession` for the whole enumeration, so the resolution phase runs at most one OAuth2 grant and one `/v1/config` lookup
* *AND* the Iceberg REST scan and pushdown path SHALL resolve files through the request's catalog session and the Iceberg-native table metadata, and SHALL NOT go through the `CatalogClient` trait

### Scenario: CATALOG_KIND naming Unity Catalog resolves the native Unity Catalog kind

* *GIVEN* a createVirtualSchema request whose plain VS properties include `CATALOG_KIND` set to `UNITY_CATALOG` in any letter case
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL resolve `CatalogKind::UnityCatalogNative`
* *AND* the adapter SHALL construct the native Unity Catalog client rather than the Iceberg REST client, and SHALL then run the SAME listing pipeline it runs for the Iceberg REST kind
* *AND* the adapter SHALL compare the property value case-insensitively, so `unity_catalog`, `Unity_Catalog`, and `UNITY_CATALOG` all resolve the same kind

### Scenario: Unity Catalog validation does not require a warehouse and rejects SigV4

* *GIVEN* a createVirtualSchema request resolving `CatalogKind::UnityCatalogNative` whose CONNECTION JSON password omits `warehouse`
* *WHEN* the adapter resolves and validates the connection under the Unity Catalog kind
* *THEN* the adapter SHALL accept the connection without reporting `warehouse` as a missing field, because a Unity Catalog is addressed by `catalog.schema.table` rather than by an Iceberg warehouse identifier
* *AND* the adapter SHALL reject a Unity Catalog CONNECTION that sets `use_sigv4` to true with an error stating that AWS SigV4 signing is not a Unity Catalog authentication mode, because the native Unity Catalog API authenticates with a bearer token or Databricks OAuth rather than a signed AWS request
* *AND* the error message MUST NOT contain any supplied credential value

### Scenario: Iceberg REST validation is unchanged under the default catalog kind

* *GIVEN* a createVirtualSchema request resolving `CatalogKind::IcebergRest` through an absent `CATALOG_KIND`, whose CONNECTION JSON password omits `warehouse`
* *WHEN* the adapter resolves and validates the connection under the Iceberg REST kind
* *THEN* the adapter SHALL return the same missing-`warehouse` error it returned before this feature, so the Iceberg REST credential contract is unchanged
* *AND* every Iceberg REST validation rule, the Azure/S3 mutual exclusion, the Azure-shape rules, the SigV4-versus-catalog-auth exclusion, the SigV4 required-fields rule, and the OAuth2 completeness rule, SHALL apply exactly as before

### Scenario: An unrecognized CATALOG_KIND value is rejected with a clear error

* *GIVEN* a createVirtualSchema request whose plain VS properties include `CATALOG_KIND` set to a value that names none of the Unity Catalog kind, the direct-storage kind, and the Glue kind
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL return an error naming the unrecognized value and every accepted choice: absent for Iceberg REST, `UNITY_CATALOG`, `DIRECT_STORAGE`, and `GLUE`
* *AND* the error SHALL state that the Iceberg REST kind is selected by leaving `CATALOG_KIND` absent, so an operator who reads the error learns why the literal `ICEBERG_REST` is rejected rather than retrying it
* *AND* the adapter SHALL resolve the kind BEFORE it reads the CONNECTION, on createVirtualSchema and pushdown requests alike, so an unrecognized value fails without a connect-back round trip
* *AND* the adapter MUST NOT fall back to a default catalog kind, because silently defaulting an unrecognized kind would resolve a misconfigured virtual schema against the wrong catalog
* *AND* the error message MUST NOT contain any credential value

### Scenario: A pushdown request under the Unity Catalog kind is planned as a Delta scan

* *GIVEN* a pushdown request whose virtual schema was created with `CATALOG_KIND` set to `UNITY_CATALOG`, over a seeded Delta table
* *WHEN* the adapter handles the pushdown request
* *THEN* the adapter SHALL resolve the request through the Unity Catalog scan source and the Delta format reader, and SHALL return a scan-driving SQL response, SUPERSEDING the recorded refusal that Unity Catalog scan execution is not yet supported
* *AND* the adapter MUST NOT resolve the request through the Iceberg REST file-resolution path, because a Unity Catalog table is a Delta table the Iceberg path cannot read
* *AND* a pushdown request whose Delta table cannot be planned SHALL fail with the reader's own plan-time error rather than a kind-level refusal, so an unreadable table and an unsupported catalog kind are distinguishable
* *AND* no error message on any of these paths SHALL contain a credential value

### Scenario: CATALOG_KIND naming direct storage resolves the direct-storage kind

* *GIVEN* a createVirtualSchema or pushdown request whose plain VS properties include `CATALOG_KIND` set to `DIRECT_STORAGE` in any letter case
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL resolve `CatalogKind::DirectStorage`
* *AND* the adapter SHALL construct the direct-storage catalog client rather than the Iceberg REST or the native Unity Catalog client, and SHALL then run the SAME listing pipeline it runs for the other kinds
* *AND* the adapter SHALL compare the property value case-insensitively, so `direct_storage`, `Direct_Storage`, and `DIRECT_STORAGE` all resolve the same kind
* *AND* the resolution SHALL contact no catalog service, because this kind has none: the client reads the object store named by the CONNECTION address directly

### Scenario: CATALOG_KIND naming Glue resolves the native Glue kind

* *GIVEN* a createVirtualSchema or pushdown request whose plain VS properties include `CATALOG_KIND` set to `GLUE` in any letter case
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL resolve `CatalogKind::Glue`
* *AND* the adapter SHALL construct the Glue catalog client and SHALL run the SAME listing pipeline it runs for the other kinds

### Scenario: Glue validation implies SigV4 and rejects catalog-auth and vending fields

* *GIVEN* CONNECTIONs under the Glue kind: one supplying only `region`, `access_key`, and `secret_key`, one that also supplies `warehouse`, one setting `use_sigv4` to false, one setting `use_vended_credentials` to true, one supplying `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope`, and one omitting `secret_key`
* *WHEN* the adapter validates each connection
* *THEN* the adapter SHALL accept the first two, treating SigV4 signing as enabled and `warehouse` as the optional Glue `CatalogId`
* *AND* the adapter SHALL reject `use_sigv4` false with an error stating that the Glue kind always signs with AWS SigV4
* *AND* the adapter SHALL reject `use_vended_credentials` true with an error stating that the Glue kind has no native credential vending
* *AND* the adapter SHALL reject every supplied token or OAuth2 field with one error naming each supplied field
* *AND* the SigV4 required-fields rule of `connection/connection-credentials-sigv4` SHALL apply, so the error names the missing `secret_key`, and a standard `https://glue.<region>.amazonaws.com` address supplies the signing region
* *AND* `region`, `access_key`, `secret_key`, `session_token`, `endpoint`, and `path_style` SHALL keep their meaning, so the same keys sign the Glue and the S3 requests, unless the CONNECTION names a role, in which case the role's session replaces those keys for both, per `connection/connection-credentials-assume-role-session-use`
* *AND* no error message SHALL contain a credential value
