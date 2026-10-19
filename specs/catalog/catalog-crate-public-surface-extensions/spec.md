# Feature: Catalog Crate Public Surface Extensions

Each addition to or demotion from the `pub` set of `lakehouse-catalog` beyond the base structure is an explicit reviewed edit to the crate's reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`. This feature lists those edits. `catalog/catalog-crate-structure` owns the base `pub` set.

## Background

* The reachability probe fails to compile if an enumerated item is narrowed below `pub`.
* The Unity Parquet table-planning addition is recorded in `unity-catalog/catalog-crate-public-surface-extensions-unity-parquet`.
* The crate keeps the one-way dependency on the engine. It names no `lakehouse-engine` symbol and no Exasol CONNECTION or virtual-schema-property delivery mechanism. Its manifest declares no `iceberg`, `datafusion`, `arrow`, `parquet`, `object_store`, `delta_kernel`, or `delta_kernel_default_engine`.
* The Delta reader lives in `lakehouse-engine`. Only the table-format tag crosses the crate boundary, and the reader reaches the crate through `load_table`, the public temporary-credentials request, and the public vended selector.
* `SkipReason` is a neutral value shared by both catalog clients. The Iceberg REST client sets `NotLoadableIcebergTable`, the Unity Catalog client sets `NotDeltaBaseTable`, and the shared listing pipeline does not branch on catalog kind.
* The Unity-wire fields `data_source_format` and `table_id` stay crate-private. Only neutral projections cross the boundary.
* The store-address type carries `endpoint`, `region`, and `path_style` and no credential field. Its field list is the whitelist of CONNECTION fields permitted to cross into vended resolution, which keeps a vended credential from falling back to a static one.
* The vended policy and construction steps stay crate-private: the neutral vended S3 value shape, the scheme and storage-host derivations, the ADLS account-name derivation, both plaintext consent gates, and the per-variant construction functions.

## Scenarios

### Scenario: One shared catalog-client trait and its neutral types become the crate's operation surface

* *GIVEN* the recorded clause enumerating "exactly these items SHALL be `pub`" on `lakehouse-catalog`, and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the shared catalog-client abstraction lands, so the engine adapter runs one listing pipeline for every catalog kind
* *THEN* the crate SHALL declare a trait-object-usable `CatalogClient` trait as the ONLY operation surface the engine's createVirtualSchema path uses to enumerate a namespace or load one table's metadata, and the recorded `pub` enumeration SHALL be SUPERSEDED to admit that trait, the two client types the engine constructs, and the catalog-neutral metadata types the trait returns — a table identifier carrying its namespace as SEGMENTS (never a pre-joined dotted string, because the engine's flattening and `TABLE_MAP` construction consume segments and re-splitting would introduce a separator ambiguity neither catalog guarantees against) plus its name; a table-metadata type carrying that identifier, its table type, its optional storage location, and its ordered columns; a column type carrying its name and a type descriptor; and a listing type carrying the resolved tables plus a skipped set whose each element pairs an identifier the catalog did not admit as a listable table with a neutral skip reason
* *AND* the trait SHALL carry NO file-planning, scan, or data-file method in this plan, and its two listing operations SHALL be shaped so that adding one later is an ADDITIVE change that reshapes neither of them
* *AND* the neutral column's type descriptor SHALL be SOURCE-TAGGED — carrying the Iceberg type for an Iceberg REST table and the Unity Catalog type name for a Unity Catalog table — and MUST NOT be pre-mapped to an Exasol type inside this crate, because `lakehouse-catalog` MUST NOT name the Exasol delivery mechanism or the engine's type-mapping home, and because discarding the source type here would destroy the fidelity the deferred Delta type work (#322) reads
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to name the trait and every neutral type, and to assert that each client type is usable as a boxed `CatalogClient`, so narrowing one below `pub` or breaking trait-object compatibility is a build failure rather than a silent gap

### Scenario: Namespace enumeration is demoted once the Iceberg client is its only caller

* *GIVEN* the recorded clause naming `list_namespace_tables` among the items that SHALL be `pub` on `lakehouse-catalog`, and its recorded rationale that it reached crate-private-adjacent visibility only to serve one engine-side caller
* *WHEN* the Iceberg REST catalog client implements `CatalogClient` and calls `list_namespace_tables` from inside the crate, leaving the engine with no direct call site
* *THEN* `list_namespace_tables` SHALL be DEMOTED to crate-private and removed from the crate's `pub` re-exports, and the recorded `pub` enumeration SHALL be SUPERSEDED accordingly, because a `pub` function whose only caller is in-crate widens the surface this feature exists to narrow
* *AND* the reachability probe SHALL drop it from the enumerated `pub` set and SHALL assert the crate declares no `pub fn list_namespace_tables`, joining the existing demotion assertions for the vended-mechanism functions, which SHALL remain intact and unweakened
* *AND* the demotion SHALL change no behavior: the same namespaces are enumerated, the same identifiers are returned in the same order, and the same errors are produced under both the signed and unsigned paths

### Scenario: The native Unity Catalog client extends the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the native Unity Catalog client and its vended-credentials selector land in the crate
* *THEN* the crate SHALL add to its public surface exactly the concept-level Unity Catalog items the engine adapter consumes — the `UnityCatalogSession` type and its constructor, the temporary-credentials type `resolve_uc_vended_storage` consumes, and the `resolve_uc_vended_storage` function — each re-exported at the crate root, and it SHALL likewise expose the Iceberg REST catalog client type and its constructor, which are the two types the engine's single construction site builds; both client types SHALL implement `CatalogClient` and the engine adapter SHALL reach every enumeration and table-load operation THROUGH that trait, so neither type needs an inherent enumeration or table-load method on the public surface
* *AND* the Unity Catalog WIRE types (the deserialized `GET /tables` table and column shapes), the authentication strategy, the session's catalog and schema enumeration, and every request-construction step SHALL stay crate-private — so making the session reachable exposes neither auth internals nor a Unity-specific shape, mirroring how `CatalogSession`'s fields stay private; publishing the wire shape would let a consumer branch on which catalog it is talking to, which is the branch this trait exists to remove
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to name every added Unity Catalog public item, and its existing demotion assertions that the crate declares no `pub fn` for the demoted vended-mechanism functions SHALL remain intact and unweakened
* *AND* the one-way dependency SHALL hold: no `lakehouse-catalog` source file SHALL name `lakehouse-engine`, and neither the `CatalogClient` trait, its neutral types, nor the Unity Catalog client SHALL name the Exasol CONNECTION or virtual-schema-property delivery mechanism, which stays in the engine adapter

### Scenario: The Delta-base skip reason extends the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the Delta-base listing filter's neutral skip-reason model lands, so the shared listing pipeline warns per excluded entry without branching on catalog kind
* *THEN* the crate SHALL add to its public surface exactly two items — the `SkipReason` enum (`NotLoadableIcebergTable`; `NotDeltaBaseTable` carrying the disqualifying `table_type` or `data_source_format` as neutral detail) and the `SkippedTable` type pairing a `CatalogTableIdent` with a `SkipReason` — each re-exported at the crate root, and the recorded `pub` enumeration SHALL be SUPERSEDED to admit them
* *AND* the neutral `CatalogListing.skipped` field SHALL change from `Vec<CatalogTableIdent>` to `Vec<SkippedTable>`, so every skipped entry carries the reason it was not admitted; the Iceberg REST client SHALL set `NotLoadableIcebergTable` and the Unity Catalog client SHALL set `NotDeltaBaseTable`, and the shared listing pipeline SHALL NOT branch on catalog kind
* *AND* `SkipReason` SHALL be a neutral value that names NO `CatalogKind`, NO Exasol CONNECTION or virtual-schema-property delivery mechanism, and NO `lakehouse-engine` symbol, and the Unity-wire `data_source_format` field SHALL stay crate-private and MUST NOT appear on `SkipReason` or any other neutral type — only its rendered value travels inside the `NotDeltaBaseTable` detail, so the one-way dependency holds
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to name `SkipReason` and `SkippedTable` and to construct `CatalogListing.skipped` with a `SkippedTable` entry, so narrowing either below `pub` is a build failure rather than a silent gap

### Scenario: The vended store-address type extends the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the credentials/addressing split gives both vended selectors a CONNECTION-configured store-address parameter
* *THEN* the crate SHALL add to its public surface exactly ONE type — a store-address value declaring EXACTLY the CONNECTION's `endpoint`, `region`, and `path_style` — plus its `Default`, one reading accessor per field, and exactly ONE conversion from `ConnectionCreds`, re-exported at the crate root — SUPERSEDING the recorded clause naming EXACTLY `endpoint` and `region` (#130)
* *AND* the type SHALL declare NO credential field, and the reachability probe SHALL assert from that type's own source that its declaration names no field spelled `access_key`, `secret_key`, `session_token`, `token`, `account_key`, `sas_token`, or `password`
* *AND* the conversion from `ConnectionCreds` SHALL be the ONE place that decides which CONNECTION fields are permitted to cross into vended resolution — enforced by the type's own field privacy per `storage-access/storage-backend-enum-selection`
* *AND* every shared vended policy and construction step SHALL stay crate-private and MUST NOT be re-exported: the neutral vended S3 value shape, the URI-scheme and storage-host derivations, the ADLS account-name derivation, the two plaintext-transport consent gates, and the per-variant construction functions
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to name the added items, and its existing demotion assertions that the crate declares no `pub fn` for the demoted vended-mechanism functions SHALL remain intact and unweakened

### Scenario: The neutral table's format tag and vending key extend the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability
  probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any
  enumerated item is narrowed below `pub`
* *WHEN* the neutral table gains the table-FORMAT tag and the credential-vending key that the
  engine-side Delta format reader consumes
* *THEN* the crate SHALL add to its public surface exactly ONE type — a closed table-format enum with
  one variant per format the engine can plan (Iceberg, Delta) — re-exported at the crate root, and the
  recorded `pub` enumeration SHALL be SUPERSEDED to admit it
* *AND* the recorded clause enumerating the neutral table-metadata type as "carrying that identifier,
  its table type, its optional storage location, and its ordered columns" SHALL be SUPERSEDED to add
  its table format and its OPTIONAL credential-vending key, because that enumeration is the contract
  a consumer reads and an incomplete one is indistinguishable from a forbidden field
* *AND* the vending key SHALL be a plain optional string documented as OPAQUE, and SHALL NOT be a new
  public type, because a newtype would put a Unity Catalog concept on the crate's enumerated surface
  while buying no invariant the neutral table's own privacy does not already give
* *AND* the Iceberg REST catalog client SHALL set the Iceberg format tag and an ABSENT vending key on
  every neutral table it returns, because it vends storage credentials inline with the table's own
  metadata and needs no per-table scope — so neither field forks the listing pipeline
* *AND* the raw Unity Catalog `data_source_format` and `table_id` wire fields SHALL stay crate-private
  and MUST NOT appear in any neutral type, so only their neutral projections cross the boundary
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to
  name the added format enum and to construct the neutral table with both added fields, so narrowing
  the enum below `pub` or dropping a field is a build failure rather than a silent gap
* *AND* the existing demotion assertions of that probe — that the crate declares no `pub fn` for the
  demoted vended-mechanism functions and no `pub fn list_namespace_tables` — SHALL remain intact and
  unweakened
* *AND* the one-way dependency SHALL hold: no `lakehouse-catalog` source file SHALL name
  `lakehouse_engine`, the crate's manifest MUST NOT declare `delta_kernel` or
  `delta_kernel_default_engine`, and neither added field SHALL name the Exasol CONNECTION or
  virtual-schema-property delivery mechanism

### Scenario: The storage-credential projection extends the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the scan UDF becomes a second reader of a CONNECTION password under `storage-access/scan-spec-credential-reference`
* *THEN* the crate SHALL add to its public surface exactly ONE type — a storage-credential projection declaring EXACTLY `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `account_name`, `account_key`, and `sas_token` — plus its deserialization from a JSON credential document and its ONE backend selector, re-exported at the crate root, and the recorded `pub` enumeration SHALL be SUPERSEDED to admit them
* *AND* the type SHALL declare NO catalog-authentication field, and the probe SHALL assert from that type's own source that its declaration names no field spelled `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `warehouse`, `use_sigv4`, or `use_vended_credentials` — the same source-level form the probe already applies to the vended store-address type
* *AND* exactly ONE conversion from `ConnectionCreds` SHALL be declared beside the type, so the adapter reaches the selector through the projection and no call site builds the projection field-by-field
* *AND* the backend selector SHALL be the ONLY declaration of the CONNECTION-to-backend selection rule in either crate, so `storage_block` retains no selection logic of its own while keeping its adapter-module home
* *AND* the probe SHALL be edited — an explicit reviewed change to the probe file — to name the added items, and its existing demotion assertions that the crate declares no `pub fn` for the demoted vended-mechanism functions and no `pub fn list_namespace_tables` SHALL remain intact and unweakened
* *AND* the one-way dependency SHALL hold: no `lakehouse-catalog` source file SHALL name `lakehouse_engine`, and the added type SHALL name no Exasol CONNECTION or virtual-schema-property delivery mechanism

### Scenario: The Parquet table format, the Parquet column source, and the no-data-file skip reason extend the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog`, its recorded closed table-format enum carrying one variant per format the engine can plan, its recorded neutral column source type, its recorded neutral skip reason, and the external-vantage reachability probe that fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the engine gains a reader that plans a plain directory of Parquet files and an enumeration that reports a directory holding no data file
* *THEN* the table-format enum SHALL gain a THIRD variant naming the Parquet format, SUPERSEDING the recorded two-variant enumeration, and the neutral skip reason SHALL gain ONE variant naming the absence of a data file, SUPERSEDING the recorded two-variant enumeration of that enum
* *AND* the neutral column source type SHALL gain a THIRD variant carrying the column's logical Arrow type as a TAG STRING of the engine's scan-spec tag vocabulary, and it MUST NOT carry an Arrow type value, because the manifest prohibition `catalog/catalog-crate-structure` records forbids this crate from declaring `arrow`
* *AND* the added skip reason SHALL carry NO detail payload, because the identifier it is paired with already names the directory and no further catalog-specific string exists for a directory that simply holds no data file
* *AND* the engine-side tag RENDERER and the engine-side tag PARSER SHALL be the ONLY producer and the ONLY consumer of that variant's string, so no third site spells or interprets the format, because this crate cannot check a string whose vocabulary lives in `lakehouse-engine` and two independent spellings would drift with nothing enforcing agreement
* *AND* a test SHALL assert that EVERY tag the direct-storage client emits parses back to the Arrow type it was rendered from, so the unenforceable cross-crate agreement is held by a round-trip assertion rather than by convention
* *AND* neither addition SHALL name a `CatalogKind`, an Exasol CONNECTION or virtual-schema-property delivery mechanism, or any `lakehouse-engine` symbol, so both stay neutral values of this crate
* *AND* the crate SHALL gain NO source file, NO manifest dependency, and NO further public item for the direct-storage kind, because the client that produces these values is declared in `lakehouse-engine` and reaches this crate only through the already-declared `CatalogClient` trait and its neutral types
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to CONSTRUCT and OBSERVE the three added variants from its external vantage, exactly as it already constructs the recorded ones, so narrowing any of the three enums or removing a variant fails to COMPILE rather than passing silently
* *AND* that edit MUST NOT add a source-TEXT assertion over any enum's variant list, because a structural invariant enforced by matching production source text is not one this project accepts; the probe's value here is that it compiles against the crate from outside it
* *AND* the probe's existing demotion assertions — that the crate declares no `pub fn` for the demoted vended-mechanism functions and no `pub fn list_namespace_tables` — SHALL remain intact and unweakened
* *AND* the table-format enum SHALL NOT gain an exhaustive production match as part of this change, and the ONE existing guard — the Delta reader-selection arm's equality check, which refuses a table whose reported format is not Delta — SHALL be recorded as the guard that a Parquet-tagged table is not misrouted, because a reader expecting a compile error would otherwise read its absence as an oversight

### Scenario: The SigV4 signing-region resolver is a public method of the shared credential type

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *AND* three readers that need the region a SigV4-signed catalog request is signed for: the adapter's SigV4 credential guard in `lakehouse-engine`, the crate's signed namespace enumeration, and the crate's `CatalogSession` catalog requests
* *WHEN* the crate's public surface is declared
* *THEN* `ConnectionCreds` SHALL declare exactly ONE additional `pub` method, which returns that signing region from the credential set and a catalog URI, and returns nothing when neither the stated `region` nor the URI supplies one
* *AND* the crate SHALL add no other item to its public surface for the signing region
* *AND* the standard AWS Glue endpoint host rule and the signing-region precedence of `connection/connection-credentials` SHALL each have exactly ONE declaration, beside the crate's SigV4 request signing, so the adapter guard and the two signing paths cannot disagree about whether a signing region exists
* *AND* every step behind that method, including the host-shape parser, SHALL stay crate-private
* *AND* each of the two signing paths SHALL resolve its region through that method once per session or enumeration, and SHALL refuse to sign, returning an error that names `region` and contains no credential value, when the method returns nothing
* *AND* the method SHALL name no Exasol CONNECTION or virtual-schema-property delivery mechanism, and no `lakehouse-catalog` source file SHALL name `lakehouse_engine`
* *AND* the probe SHALL call the method, and its existing demotion assertions SHALL remain intact and unweakened

### Scenario: The AWS identity resolver extends the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`
* *WHEN* the adapter gains AWS IAM role assumption under `connection/connection-credentials-assume-role`
* *THEN* the crate SHALL add ONE `pub` async function that resolves the AWS identity a request acts as, re-exported at the crate root
* *AND* `ConnectionCreds` SHALL gain the `aws_assume_role_arn`, `aws_external_id`, and `aws_sts_endpoint` fields and ONE `assume_role_arn()` accessor, which is the sole definition of whether a set names a role
* *AND* the crate SHALL gain ONE manifest dependency, the official `aws-sdk-sts` client
* *AND* the endpoint consent gate, the SDK client construction, and the error redaction SHALL stay crate-private
