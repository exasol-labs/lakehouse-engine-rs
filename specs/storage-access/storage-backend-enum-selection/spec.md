# Feature: Storage Backend Selection

Every consumer carries a `StorageBackend` without naming a variant. The feature defines which sites select a backend from input and which modules may name a variant. It covers the static selector `storage_block`, the vended selectors `resolve_vended_storage` and `resolve_uc_vended_storage`, the shared home of vended policy and construction, the source probes that guard these rules, and the store-address parameter that keeps CONNECTION credentials out of vended resolution.

## Background

* `StorageBackend::S3` wraps `StorageProps`, which is unchanged. Inlining its fields onto the variant would edit the type whose serde encoding `catalog/catalog-crate-structure` pins field-for-field.
* `catalog_storage_props` is the single source of the iceberg S3 config keys. `file_io` folds its map into the `FileIOBuilder`, and `build_rest_catalog` merges it into its props map, so neither declares a key of its own.
* `select_credential_source` and `merge_vended_into_storage` implement the Iceberg REST rules that `storage-credentials` is read before `config` and that the longest `prefix` wins.
* `storage_block` selects a backend from the CONNECTION credential shape and runs only when vending is disabled. `resolve_vended_storage` selects from the `loadTable` response's table location and runs only when vending is enabled. Each catalog kind's format reader holds the single `use_vended_credentials` branch that chooses between the static backend and its vended selector. Neither selector reads the other's input.
* One selector cannot serve both modes, because the URI scheme is known only after `loadTable`, which runs once per table and never on the `createVirtualSchema` path. A vended arm that mutates the payload `storage_block` returned would reach into a payload another selector built.
* `resolve_vended_storage` takes no `StorageBackend`, so the vended selector cannot consult the static one.
* `allow_http` arrives from the `ALLOW_HTTP` virtual-schema property and is consumed only by the S3 payload. The Azure backend has no HTTP-scheme knob. Deriving it from the vended `s3.endpoint` scheme would let a catalog that vends `http://` put credentials in the clear without operator control. `allow_http` is not a CONNECTION value, so it does not weaken the disjointness rule.
* `resolve_uc_vended_storage` selects a backend from a Unity Catalog temporary-table-credentials response, an input disjoint from the other two. The Unity Catalog table-storage resolution calls it when the CONNECTION enables vending.
* The vended ADLS arm assembles the SAS state and never the account-key state, so `AdlsCred`'s two-state invariant holds and `object_store`'s access-key-beats-SAS precedence stays unreachable. A vended SAS is a new input to the existing state, not a new state.
* A catalog kind may fork on how a value is read off the wire. It must not fork on what makes the value acceptable. Consent gates, address rules, and target-variant decisions live in one shared home in `storage.rs` that both kinds call, because duplicated copies had drifted: the Unity ADLS arm took no `allow_http` and accepted `abfs://` silently.
* Moving construction into the shared home shrinks the list of modules permitted to name a variant, because neither vended selector names one.
* Each vended selector reads its own wire shape and reduces to three steps: classify the location's scheme, read its credential family, and hand neutral values to the shared construction function.
* Real Databricks AWS vends short-lived credentials with no endpoint and no region, so a store address may legitimately be empty. Each vended selector takes the CONNECTION's `endpoint`, `region`, and `path_style` as a store-address parameter that carries no credential field. The rule exists to stop a vended credential falling back to a static one and was never about addressing.
* `StaticStoreAddress` carries `path_style: Option<bool>`, because the resolution chain branches on whether the CONNECTION stated a value. Its fields are non-`pub` and read through accessors.
* The callers of `resolve_uc_vended_storage` are the Unity Catalog table-storage resolution (`adapter/pushdown/format/unity_table_storage.rs`), its unit tests, `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, and the crate-root re-exports.
* See `storage-access/storage-backend-enum` for the enum's variants and methods, its wire encoding, and the scan's object-store registration.

## Scenarios

### Scenario: Every consumer holds a backend and no consumer names one

* *GIVEN* every production site that carries storage config between the CONNECTION object and the scan spec, in `lakehouse-catalog`: the vended selector, the REST-catalog builder, and the namespace listing; in the adapter: CONNECTION parsing, the pushdown request handlers, redaction, dispatch-SQL building, file resolution, and join planning, which resolves one effective storage per join side; and in the scan: `CommonScanSpec.storage` and its `impl Default` initializer, the file-list registration, and the Parquet scan table
* *WHEN* a backend travels from the CONNECTION object to the scan spec
* *THEN* each of those SHALL hold, accept, or return `StorageBackend` in place of `StorageProps`, so the backend travels as one value from the CONNECTION object to the scan spec, and no site in that chain SHALL unwrap the payload to hand a bare `StorageProps` onwards
* *AND* the ONLY modules permitted to match on a `StorageBackend` variant or read its S3 payload SHALL be these four, exhaustively: the enum's own module (`storage.rs`), which also holds the shared vended construction functions; the CONNECTION-parsing constructor `StorageCreds::backend` (`creds.rs` in `lakehouse-catalog`), which `storage_block` in `connection.rs` calls and which constructs the backend from the CONNECTION credential shape; the engine's single object-store registration function (`scan/object_store.rs`); and `CommonScanSpec`'s manual `impl Default` (`scan/spec.rs`), which re-wraps a placeholder payload without choosing a backend from any input. A test-only payload unwrapper or variant construction MAY appear in ANY `#[cfg(test)]` module of either crate. No other production module SHALL name a variant, with the source-level probes covering the shared home and both vended selectors as the compensating gate
* *AND* there SHALL be EXACTLY THREE sites that SELECT a backend FROM INPUT, one per credential source, reading DISJOINT inputs: `StorageCreds::backend`, whose variant depends on the CONNECTION credential shape and which serves only when `use_vended_credentials` is false; `resolve_vended_storage`, whose variant depends on the Iceberg REST table location's URI scheme; and `resolve_uc_vended_storage`, whose variant depends on the Unity Catalog table location's URI scheme. A `Default` impl that always re-wraps a fixed `S3` placeholder is NOT a selection, because its variant depends on no input
* *AND* each catalog kind SHALL have exactly ONE site that chooses between the static backend and its vended selector: the `use_vended_credentials` branch of the Iceberg format reader, and the same branch in the Unity Catalog table-storage resolution, which fails a table with no vending key rather than falling back to the static credential
* *AND* `resolve_vended_storage` MUST NOT take a `StorageBackend`, a `ConnectionCreds`, or any other value carrying a CONNECTION credential field as a parameter, so credential disjointness is enforced by its signature rather than by inspection of its body; its CONNECTION store-address parameter carries no credential field, and its resolved `ALLOW_HTTP` parameter is a virtual-schema property and not a CONNECTION value, so it does not breach that disjointness
* *AND* because the vended selectors match on a URI SCHEME rather than on a `StorageBackend`, adding a third variant SHALL NOT break their build, so source-level probes in `crates/lakehouse-catalog/tests/catalog_public_surface.rs` SHALL extract the variant lists from the enums' own source (see "The repointed source probe binds the shared home, both selectors, and both enums") as the compensating gate for the compile-time failure those sites cannot provide; a HARDCODED variant list SHALL NOT satisfy this clause, because it keeps passing after a third variant is added
* *AND* because the selected variant is PER TABLE LOCATION, a join SHALL carry one backend PER SIDE rather than collapsing onto one whole-spec value, and each side SHALL be read through a store built from its own backend, as `pushdown-joins/pushdown-planning-join` and `scan-aggregation/scan-execution-join` require, so no consumer holds a backend that names one side while another side's files are read through it
* *AND* `storage_block` SHALL take the resolved `allow_http` value as a parameter and return a fully-formed `StorageBackend`, and `resolve_connection_config` SHALL NOT mutate `allow_http` on the returned value afterwards, because reaching into the payload to finish construction is exactly the knowledge this feature removes
* *AND* `build_rest_catalog` SHALL merge `catalog_storage_props`' entries into its props map and MUST NOT insert an S3 config key of its own
* *AND* `resolve_vended_storage` SHALL keep its name and its `pub` status while returning `Result<StorageBackend, UdfError>`, and it SHALL remain the crate's ONLY vended entry point with every mechanism step crate-private
* *AND* `lakehouse_engine::scan::spec` SHALL re-export `StorageBackend` beside its existing `StorageProps` re-export, at `pub` visibility, so the scan layer's consumers reach it on the path they already import

### Scenario: The vended selector reaches the SAS state without widening the enum

* *GIVEN* the ADLS variant, which carries an account name and a two-state credential whose account-key and SAS states make "both" and "neither" unrepresentable
* *WHEN* the vended selector resolves an ADLS backend from a `loadTable` response
* *THEN* it SHALL construct the SAS state and MUST NOT construct the account-key state, so `object_store`'s access-key-beats-SAS precedence in `build()` stays unreachable
* *AND* `AdlsCred`, `StorageProps`, the enum's variant set, and all three of `secret_values`, `catalog_storage_props`, and `file_io` SHALL NOT gain a state or arm for a vended SAS, because a vended SAS is a new INPUT to the existing SAS state rather than a new state
* *AND* the wire encoding of both variants MUST be the same for a vended and a static backend, and the eighteen credential-bearing golden SQL fixtures carry it inside the `storage` wrapper of `storage-access/scan-spec-credential-reference`, while the six `empty_*` fixtures carry no `storage` value
* *AND* `secret_values` SHALL return the vended SAS as this backend's single secret value, so the value-based redaction `resolve_file_list` already applies covers a vended SAS exactly as it covers a static one

### Scenario: A Unity Catalog vended selector is admitted as a third backend-selection site

* *GIVEN* the Unity Catalog vended path, which resolves a `StorageBackend` from a Unity Catalog temporary-table-credentials response
* *WHEN* the Unity Catalog table-storage resolution runs under `use_vended_credentials`
* *THEN* a THIRD backend-selection site, `resolve_uc_vended_storage`, SHALL select the backend from a DISJOINT input, the Unity Catalog temporary-table-credentials response shape, distinct from both the CONNECTION credential shape and the Iceberg REST `loadTable` response
* *AND* `resolve_uc_vended_storage` SHALL select the backend variant from the table location's URI scheme ALONE and MUST NOT take a `StorageBackend`, a `ConnectionCreds`, or any other value carrying a CONNECTION credential field as a parameter, so the three selectors' credential disjointness is enforced by its signature; its CONNECTION store-address parameter carries no credential field (see "The vended selectors take a store address that cannot carry a credential")
* *AND* the Unity Catalog vended path SHALL terminate in a `StorageBackend` value that the Unity Catalog table-storage resolution returns as the effective storage of Delta and Parquet scans
* *AND* the modules permitted to name a variant SHALL stay the four listed in "Every consumer holds a backend and no consumer names one", because `resolve_uc_vended_storage` constructs its backend through the shared construction functions of `storage.rs` and names no variant itself

### Scenario: The three-selector dispatch stays single-homed and probe-guarded

* *GIVEN* the three backend-selection sites of "Every consumer holds a backend and no consumer names one" and the clause that each catalog kind has exactly ONE vending-versus-static branch
* *WHEN* `resolve_vended_storage` and `resolve_uc_vended_storage` classify a table location's URI scheme
* *THEN* the scheme-to-variant-KIND classification SHALL have ONE home shared by `resolve_vended_storage` and `resolve_uc_vended_storage`, mapping `s3`/`s3a` to the S3 variant kind and `abfs`/`abfss` to the ADLS variant kind in exactly one place rather than duplicated across the two vended selectors
* *AND* each vended selector SHALL read its OWN credential family, `resolve_vended_storage` from the Iceberg REST vended response and `resolve_uc_vended_storage` from the Unity Catalog temporary-table-credentials response, and hand neutral values to the shared construction functions in `storage.rs`, so the classification has one home and no `StorageBackend` is built from a duplicated copy of it
* *AND* the dispatch between the static backend and a vended selector SHALL live in each catalog kind's reader, so no site outside a reader chooses between them
* *AND* the source-level probes described in "The repointed source probe binds the shared home, both selectors, and both enums" SHALL cover BOTH vended selectors, and a hardcoded variant list SHALL NOT satisfy any of them

### Scenario: Vended policy and construction move into the enum's own module

* *GIVEN* the shared scheme-to-variant-kind classification of "The three-selector dispatch stays single-homed and probe-guarded", which each vended selector calls
* *AND* the two policy rules that must not diverge between the selectors: the `abfs://` plaintext consent gate and the S3 store-address check
* *WHEN* vended policy and construction have one shared home in `storage.rs`, beside `classify_vended_scheme`
* *THEN* the shared home SHALL own the neutral vended S3 value shape, the URI-scheme and storage-host derivations, the ADLS account-name derivation, both plaintext-transport consent gates, the S3 store-address rule, and EXACTLY ONE construction function per `StorageBackend` variant
* *AND* neither vended selector SHALL name a `StorageBackend` variant, so the modules permitted to match on a variant remain the four of "Every consumer holds a backend and no consumer names one"
* *AND* each vended selector SHALL keep its OWN wire extraction and SHALL reduce to three steps: classify the location's scheme, read its own credential family off its own wire shape, hand the resulting neutral values to the shared construction function, `resolve_vended_storage` reading a flat `HashMap<String, String>` through `select_credential_source`, `resolve_uc_vended_storage` reading the typed temporary-table-credentials response
* *AND* a catalog kind MAY fork on HOW a value is read off the wire and MUST NOT fork on WHAT makes the resulting value acceptable, so every consent gate, every store-address rule, and every target-variant decision SHALL have exactly ONE home that both kinds call, and a second per-kind copy of any of them SHALL NOT be added
* *AND* the count of sites that SELECT a backend from input SHALL be THREE and each catalog kind SHALL have ONE site that chooses between static and vended storage, because the shared home changes which module NAMES a variant and not which module CHOOSES a backend
* *AND* the shared construction functions SHALL be crate-private and MUST NOT join the crate's public surface, because they are mechanism steps of the one published vended entry point per catalog kind

### Scenario: The repointed source probe binds the shared home, both selectors, and both enums

* *GIVEN* the shared construction home in `storage.rs`, which constructs every `StorageBackend` variant, and the two vended selectors, `vended.rs` and `unity/vended.rs`, which name no variant
* *WHEN* the source-level probes of `crates/lakehouse-catalog/tests/catalog_public_surface.rs` run
* *THEN* the probes SHALL bind the vended policy at THREE levels, so a third variant fails at the first level it reaches instead of passing silently
* *AND* ONE probe SHALL assert that every variant name extracted from `enum StorageBackend`'s own source appears as a `StorageBackend::<Variant>` construction in the shared home's PRODUCTION source, which is non-vacuous because the enum's own method bodies spell their arms `Self::<Variant>` and only a construction expression spells the qualified form
* *AND* a per-selector probe SHALL assert that every variant name extracted from `enum VendedBackendKind`'s own source appears in EACH vended selector's production source, so a third vended kind still forces BOTH selectors to dispatch it
* *AND* a probe SHALL assert that the extracted variant-name SET of `VendedBackendKind` EQUALS that of `StorageBackend`, so adding a third `StorageBackend` variant fails that probe until a third kind exists, which fails the per-selector probes until both selectors dispatch it, which fails the construction probe until the shared home builds it
* *AND* every one of those probes SHALL extract its variant list from the enum's own source through ONE shared extractor parameterised by enum name, and a HARDCODED variant list SHALL NOT satisfy any of them
* *AND* every one of those probes SHALL read PRODUCTION source only, so text from the first `#[cfg(test)]` onward MUST NOT satisfy it

### Scenario: The vended selectors take a store address that cannot carry a credential

* *GIVEN* the rule that neither vended selector takes a `StorageBackend`, a `ConnectionCreds`, or any other CONNECTION-derived credential value as a parameter
* *AND* real Databricks AWS vends short-lived credentials with NO endpoint and its credential type carries no region field at all, so a legal Databricks `s3://` table has an empty store address, which the selectors MUST accept
* *WHEN* credentials are split from addressing
* *THEN* each vended selector SHALL take one additional parameter carrying the CONNECTION's configured store `endpoint`, `region`, and `path_style`, and carrying NOTHING else
* *AND* the credential half of the disjointness rule SHALL be UNCHANGED: an access key, a secret key, a session token, an Azure account key, and a SAS token SHALL come from the vended response ALONE on every vended path, and MUST NEVER be read from the CONNECTION under vending
* *AND* because the signature does not enforce that half, it SHALL be enforced MECHANICALLY rather than by prose: the new parameter's type SHALL declare EXACTLY the three addressing fields, and a source-level probe SHALL assert that its declaration names no field spelled `access_key`, `secret_key`, `session_token`, `token`, `account_key`, `sas_token`, or `password`
* *AND* the added field SHALL be an OPTION of boolean, because the resolution chain branches on whether the CONNECTION stated a value
* *AND* the type SHALL have EXACTLY ONE construction from the CONNECTION, declared beside it, so which CONNECTION fields are permitted to cross into vended resolution is one decision in one place rather than a struct literal repeated per call site
* *AND* that ONE-construction rule SHALL be enforced BY THE TYPE rather than by prose or by review: EVERY addressing field SHALL be declared private, reads SHALL go through accessor methods, and a source-level probe SHALL assert that the declaration keeps EVERY field private
* *AND* that probe SHALL read the field list out of the type's own declaration rather than holding a hardcoded list, so a field added later is covered without editing the probe
* *AND* passing a `ConnectionCreds`, a `StorageBackend`, or any other type carrying a credential field to either vended selector SHALL remain FORBIDDEN, so the rule is narrowed to addressing and not dropped
* *AND* the disjointness rule's PURPOSE SHALL be restated in the spec rather than left to be inferred: it exists to stop a vended credential silently falling back to a static one, and it was never about addressing
