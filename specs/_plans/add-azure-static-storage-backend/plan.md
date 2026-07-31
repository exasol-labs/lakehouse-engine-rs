# Plan: add-azure-static-storage-backend

## Summary

Add a second `StorageBackend` variant, `Adls`, with an `AdlsCred` credential enum, and wire it end to end so the engine reads `abfss://` Iceberg tables with static Azure credentials (a shared account key or an inline SAS). S3 behaviour is unchanged; real-cloud verification lands in slice E (#277) and vended SAS in slice D (#276).

## Design

### Context

Slice B (#274) landed a one-variant `StorageBackend` enum and moved every backend-specific decision behind it, but with one variant no dispatch is observable. This slice is the first with user-visible Azure capability, so it is also the first that can falsify slice B's design. Three forces shape it.

**The credential shape is the only backend signal available at parse time.** There is no `backend` field in the CONNECTION JSON and adding one would be a second source of truth free to disagree with the credentials supplied. The scheme in the table location is the other candidate signal, but it is not known until `loadTable` returns — that is slice D's seam, not this one's.

**DataFusion's object-store registry key and the Azure object store disagree about scope.** `get_url_key` is `scheme://` + `Position::BeforeHost..Position::AfterPort` (`datafusion-execution-54.1.0/src/object_store.rs:268-274`), which EXCLUDES userinfo. On `abfss://<container>@<account>.dfs.core.windows.net/…` the container IS the userinfo, so the key names the storage ACCOUNT while `MicrosoftAzureBuilder::with_url` builds a store scoped to ONE CONTAINER (`object_store-0.13.2/src/azure/builder.rs:662-682`). Two containers of one account collapse onto one store, and a broadcast join's dimension side would be read out of the fact side's container with no error.

**Slice B deferred a duplication to exactly this slice.** `vs-adapter/storage-backend-enum` records that `extract_bucket_from_files` (`Url::host_str()`) and `validate_uniform_object_store_files` (`ListingTableUrl::object_store()`) derive the same notion two ways, and that "unifying the two into one owner is deferred to the slice that adds the second backend". A second backend without that unification would make it three.

- **Goals** — read `abfss://` Iceberg with a static account key or inline SAS; select the backend at one site; keep every Azure secret out of every error, SQL string, and log line; leave the S3 path byte-identical; close the container-collision hole loudly.
- **Non-Goals** — vended Azure SAS (#276); real-cloud E2E and `make test-e2e-azure` (#277, #278); Azurite-emulator endpoint override and `allow_http` for Azure; AAD / client-credentials / workload-identity Azure auth; renaming the `s3_max_connections` wire field, which is already read backend-agnostically as a connection budget and whose rename is pure wire churn; a `use_sigv4`-plus-Azure guard, which the existing SigV4 and mixed-fields rules already reject; and any widening of `MicrosoftAzureBuilder`'s four accepted host suffixes, so a sovereign-cloud, private-endpoint, or custom-domain Azure account is out of reach in this slice and fails loud at `build()` with a redacted `UrlNotRecognised`.

### Decision

#### Architecture

```
CONNECTION JSON
      │  parse_creds        reads account_name / account_key / sas_token (nonempty_str)
      ▼
ConnectionCreds ──► validate_creds   [ambiguity rule] [Azure required-field rule] [existing rules]
      │
      ▼  storage_block       THE ONE BACKEND SELECTION SITE
StorageBackend ─┬─ S3(StorageProps)                    (unchanged)
                └─ Adls { account_name, cred: AdlsCred{ AccountKey | Sas } }
                          │
    lakehouse-catalog ────┼── secret_values()          → the one Azure secret, or none
                          ├── catalog_storage_props()  → adls.account-name / -key | -sas-token
                          ├── file_io()                → OpenDalStorageFactory::Azdls (unit)
                          └── resolve_vended_storage() → Adls arm: passthrough (#276)
                          │
    lakehouse-engine  ────┴── register_side_store()    → MicrosoftAzureBuilder::with_url(...)

                    side_store_url(side)  ── ONE derivation ── scheme://userinfo@host:port
                       │                                        (== ListingTableUrl::object_store())
                       ├─► S3 arm      : host as bucket name
                       ├─► Azure arm   : whole URL into with_url
                       └─► validate_sides_share_one_store(spec)   registry key vs store URL
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Make illegal states unrepresentable | `AdlsCred` | Two credentials or none is a shape the type cannot hold, so "exactly one" is enforced once at the boundary instead of at every use site |
| One derivation, many readers | `side_store_url` | Discharges slice B's deferral: the store key and the uniformity check agree by construction, not by inspection |
| Loud precondition over silent collapse | `validate_sides_share_one_store` | The registry-key asymmetry cannot be fixed (DataFusion owns the key formula), so the only safe reading is to reject the input that would misread |
| Exhaustive match, no `_` arm | every backend `match` | A third backend becomes a build failure at each site that must decide, not a silent fall-through |
| Tracked exception with an issue cite | `resolve_vended_storage` Adls arm | A named, spec-recorded gap citing #276 rather than an undocumented behaviour difference |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Any of the three Azure fields triggers the Azure backend | Trigger on `account_name` alone | Triggering on `account_name` alone makes a CONNECTION that supplies `account_key` and forgets `account_name` fall back to S3 with the key silently ignored. Any-of-three turns that input into a named-field error |
| Reject a CONNECTION mixing Azure and S3 fields | Undeclared precedence (Azure wins / S3 wins); accept and ignore the unused set | The user's Q3 principle: a credentials path must not resolve ambiguity silently. This is the one rule not named by #275, and it is the answer to the open Q2 sub-question |
| `Adls { account_name, cred }` struct variant | `Adls(AdlsProps)` wrapper, mirroring `S3(StorageProps)` | `S3` wraps because `StorageProps` pre-exists with a pinned serde contract. No Azure equivalent exists, so a wrapper would be a type introduced for symmetry alone. Slice D can extract one if it needs a merge target |
| Manual redacting `Debug` on `AdlsCred` | Derive `Debug`, matching `StorageProps`' plaintext `secret_key` | Six lines on a security path. Matching the existing leak would add a NEW leak because an old one exists; this instead leaves issue #135 strictly less to fix |
| Whole-spec precondition, checked once | Per-arm cross-side check inside `register_side_store`; comparing the registered store's `Display` string | The arm sees one side; the collision is a property of the pair. Reading a container back out of a registered `Arc<dyn ObjectStore>` needs string-matching a `Display` impl — fragile. A backend-agnostic whole-spec check states the actual invariant |
| Delete `extract_bucket_from_files`, unify on `side_store_url` | Add a parallel `extract_azure_target_from_files` | Slice B explicitly deferred this unification to this slice. A parallel Azure derivation would make three owners of one notion. The `scheme://userinfo@host:port` slice returns a `Url` byte-identical to today's for every `s3://` input, so every pinned S3 assertion survives unedited |
| `resolve_vended_storage` Adls arm is a passthrough | Reject Azure + `use_vended_credentials`; do the scheme switch now | User answer A4: select at `parse_creds`/`storage_block` and touch nothing at `resolve_vended_storage`. A rejection would break an Azure CONNECTION carrying a leftover flag for no correctness gain |
| Leave `validate_uniform_object_store_files` unedited | Extend it to compare userinfo | It already compares `ListingTableUrl::object_store()`, which INCLUDES userinfo (`datafusion-datasource-54.1.0/src/url.rs:323-327`). The intra-side case is already covered; only the across-side case was missing. Pin it with a test instead of editing it |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/connection-credentials | CHANGED | `vs-adapter/connection-credentials/spec.md` |
| vs-adapter/storage-backend-enum | CHANGED | `vs-adapter/storage-backend-enum/spec.md` |
| vs-adapter/pushdown-planning-cloud-credentials | CHANGED | `vs-adapter/pushdown-planning-cloud-credentials/spec.md` |

## Impact

Operators gain a new CONNECTION credential shape: `account_name` plus exactly one of `account_key` and `sas_token` makes a virtual schema read `abfss://` Iceberg tables. Existing S3 CONNECTIONs are unaffected — same parsing, same validation, same backend, same generated SQL, same wire bytes.

Two inputs that were previously accepted are now errors, both of them previously-silent misconfigurations: a CONNECTION mixing Azure and static S3 credential fields, and a scan spec whose two join sides live in different containers of one Azure storage account. Neither shape can occur on an S3-only deployment.

One existing input changes behavior, and it changes from broken to working. A file list whose paths use the `s3a://` scheme — documented as valid at `crates/lakehouse-engine/src/scan/spec.rs:445` — is registered under `s3a://<bucket>` instead of the `s3://<bucket>` the deleted derivation produced. That old key was one DataFusion never looked up, so an `s3a://` scan fails today with "No suitable object store found for s3a://…". No working scan changes; a broken one starts working. Every committed fixture uses `s3://` and is unaffected.

Three limits ship with the slice. Two are recorded as tracked exceptions in the specs: an Azure CONNECTION with `use_vended_credentials` reads with its static credentials (#276), and there is no automated Azure verification against real cloud storage (#277). The third is an upstream boundary: `object_store` accepts only four Azure host suffixes, so a sovereign-cloud, private-endpoint, or custom-domain account cannot be reached and fails loud at store construction.

## Dependencies

- Slice A (#273) — `object_store`'s `azure` feature and `iceberg-storage-opendal`'s `opendal-azdls` feature, both already enabled. No manifest change in this slice.
- Slice B (#274) — the `StorageBackend` enum and the `Option<Url>` registration contract, present in the working tree on `feat/refactor-storage-backend-enum`. Plan against the working tree, not `main`.
- Blocks slice D (#276) and slice E (#277).

## Implementation Tasks

1. **Catalog crate — the backend type**
   1. Add `AdlsCred` (two states, manual redacting `Debug`, derived `Clone`/`PartialEq`/`Eq`/`Serialize`/`Deserialize`) and the `StorageBackend::Adls { account_name, cred }` variant in `crates/lakehouse-catalog/src/storage.rs`; add the Azure arm of `secret_values`, `catalog_storage_props` (via `iceberg::io::ADLS_*` constants), and `file_io` (`OpenDalStorageFactory::Azdls`, a unit variant), with no `_` arm anywhere.
   2. Add `account_name`, `account_key`, and `sas_token` to `ConnectionCreds` in `crates/lakehouse-catalog/src/creds.rs`, and extend the manual redacting `Debug` impl so both secrets are masked and `account_name` stays visible.
   3. Add the compile-forced `Adls` passthrough arm to `resolve_vended_storage` in `crates/lakehouse-catalog/src/vended.rs`, documented as deferred to #276.
   4. Complete the five compile-forced S3 destructuring sites listed in § Forced Edits table A. Each gains a panicking `Adls` arm (or `let … else`) naming the site as S3-only; no assertion changes and no `_` catch-all is introduced. Together with task 1.7 this restores `cargo test --workspace` to a clean build; neither task alone does.
   5. Extend `redact_credentials` in `crates/lakehouse-catalog/src/redaction.rs` with the Azure labels its AWS-only pattern list lacks — `account_key`, `sas_token`, `adls.account-key`, `adls.sas-token`, `azure_storage_access_key`, `azure_storage_sas_key`, and `sig=` (the SAS signature parameter, the secret-bearing part of a SAS URL) — with a unit test beside `redact_credentials_strips_vended_sts_keys`.
   6. Route the nine credential-exposed error sites in `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs` through value-based redaction before the label heuristic, matching `redact_error` (`crates/lakehouse-engine/src/adapter/mod.rs:1012-1020`). Sites `:261` and `:277` read `effective_storage`, already in scope; sites `:433`, `:442`, `:454`, `:463`, `:526`, `:533` sit in `ensure_supported_delete_mechanisms` and `plan_files_from_table`, which take no backend and gain a `secrets: &[String]` parameter. Site `:520` (`failed to build Iceberg scan`) currently applies NO redaction at all and gains both layers. Covered by `manifest_read_errors_redact_the_literal_azure_secret_values`, which asserts a literal account key and a literal SAS embedded in a manifest-read error are both gone. **[expert]**
   7. Complete the seven compile-forced `ConnectionCreds` literal sites listed in § Forced Edits table B. Each supplies the three new fields as absent, changing no existing field value and no existing assertion; `creds.rs:238` additionally asserts that the two new secret fields are redacted in `Debug`, per that row's resolution.

2. **Adapter — parse, validate, select**
   1. Read `account_name`, `account_key`, and `sas_token` in `parse_creds` (`crates/lakehouse-engine/src/adapter/connection.rs`) through the existing `nonempty_str` helper.
   2. Add the two Azure rules to `validate_creds`: the Azure-versus-S3 ambiguity rule and the `account_name`-plus-exactly-one-credential rule, both placed ahead of the existing SigV4 and OAuth2 rules, both naming field names only. Update the function's documented rule-precedence comment. **[expert]**
   3. Add the Azure branch to `storage_block`, keeping the function TOTAL — no panic, no `Result` — so a `ConnectionCreds` that bypassed `validate_creds` resolves deterministically to S3 rather than aborting the UDF VM.

3. **Scan — one store derivation, one precondition, one Azure arm**
   1. Replace `extract_bucket_from_files` with the backend-agnostic `side_store_url`, returning the `scheme://userinfo@host:port` slice of the side's first reconstructed file URI as a `Url`; repoint the S3 arm to read the bucket name out of it and repoint the two tests that call the deleted function. The `Url` returned for every `s3://` input must equal today's, so every pinned S3 assertion passes unedited. **[expert]**
   2. Add `validate_sides_share_one_store(spec)` beside `build_spec_size_index` and call it once from `build_session_context` before any registration: group each non-empty side by DataFusion's registry key and error when two sides sharing a key need different store URLs. Backend-agnostic, no variant match. **[expert]**
   3. Add the Azure arm of `register_side_store`: `with_url` on the derived URL, the shared `ClientOptions` connection budget, `with_access_key` or the SAS config key per the `AdlsCred` state, the same two-step secret redaction on a builder error, the sized HEAD decorator, and the `Some(url)` / `None` registration contract.

4. **Tests**
   1. Credential-shape unit tests in `crates/lakehouse-engine/src/adapter/connection.rs`: account-key shape, SAS shape, both-present, neither-present, missing `account_name`, mixed Azure-plus-S3, and the no-storage-fields S3 default under `use_vended_credentials`. Assert no supplied value appears in any error.
   2. `abfss://` path unit tests: relativize-then-reconstruct round trip in `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs`, and a size-index test in `crates/lakehouse-engine/src/scan/object_store.rs` asserting the indexed `Path` excludes the container.
   3. Registration unit tests in `crates/lakehouse-engine/src/scan/object_store.rs`: an Azure side registers and returns the container-qualified URL; a second side in the same container returns `None`; a spec whose two sides use different containers of one account is a `UdfError::User`; two sides in different accounts both register; an `s3a://` file list yields the key `s3a://<bucket>`, equal to what `ListingTableUrl::object_store()` reports for the same list, so registration and lookup agree; an `abfss://` host outside the four accepted suffixes surfaces `UrlNotRecognised` with no credential value in the message.
   4. Backend-type unit tests in `crates/lakehouse-catalog/src/storage.rs`: the Azure `catalog_storage_props` map for each credential state and nothing else, `file_io` configured from exactly that map, `secret_values` for each state and for an empty value, the `Debug` redaction, the tagged wire round trip, and the extended variant-key decode test.
   5. Run the full S3 regression gate and record it: workspace tests, clippy, fmt, the container build, and the S3 E2E suite.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 1.5 |
| Group A.2 | 1.4, 1.7 |
| Group B | 1.3, 2.1, 2.2, 2.3, 1.6 |
| Group C | 3.1, 3.2 |
| Group D | 3.3 |
| Group E | 4.1, 4.2, 4.3, 4.4 |
| Group F | 4.5 |

Sequential dependencies:
- Group A → Group A.2 (1.4's five sites only stop compiling once 1.1 lands the second variant; 1.7's seven sites only stop compiling once 1.2 lands the three new fields. 1.4 and 1.7 touch disjoint sites and run in parallel, but nothing else compiles until BOTH are done)
- Group A.2 → Group B (1.3 needs the variant; 2.x needs both the variant and the new creds fields)
- 1.5 is independent of the variant (a pure pattern-list edit) and 1.6 depends only on 1.5
- Group A → Group C (3.1 and 3.2 are backend-agnostic but compile against the two-variant enum)
- Group B + Group C → Group D (3.3 needs `side_store_url` and the `AdlsCred` states)
- Group D → Group E → Group F

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `extract_bucket_from_files` in `crates/lakehouse-engine/src/scan/object_store.rs` | Replaced by the backend-agnostic `side_store_url`; discharges the unification slice B deferred to this slice |

## Forced Edits

This slice widens TWO types, and each widening breaks its own set of sites that must be completed
before the workspace builds. Every site below is test or test-support code, so
`vs-adapter/storage-backend-enum`'s characterization clause admits them as bounded edit classes and
admits nothing else. Both diffs are pre-declared here so they are reviewed rather than improvised.

### A. The second `StorageBackend` variant — five sites, task 1.4

Adding the variant turns four irrefutable `let` bindings and one single-arm `match` into compile
errors (E0005 / E0004). Admitted as CLASS TWO.

| Site | Shape today | Resolution |
|------|-------------|------------|
| `crates/lakehouse-engine/src/adapter/connection.rs:485` | `let StorageBackend::S3(storage) = storage_block(&resolved.creds, false);` | `let … else { panic!("S3 creds must select the S3 backend") }` — keeps the test's S3-selection assertion |
| `crates/lakehouse-engine/src/scan/spec.rs:877` | `let StorageBackend::S3(props) = storage;` in helper `s3_props` | `let … else { panic!("s3_props is S3-only") }` — helper stays an S3 unwrapper |
| `crates/lakehouse-engine/src/scan/spec.rs:963` | `let StorageBackend::S3(props) = &mut spec.common.storage;` | `let … else { panic!("fixture is S3-only") }` |
| `crates/lakehouse-catalog/src/test_support.rs:64` | `match backend { StorageBackend::S3(props) => props, }` in `s3_payload` | Add `StorageBackend::Adls { .. } => panic!("s3_payload is S3-only")` |
| `crates/lakehouse-engine/tests/e2e_int96_timestamp_test.rs:136` | `let StorageBackend::S3(storage) = local_stack_storage();` | `let … else { panic!("LocalStack fixture is S3-only") }` |

The two `extract_bucket_from_files` call sites at `crates/lakehouse-engine/src/scan/object_store.rs:770`
and `:777` are CLASS ONE edits, owned by task 3.1 and already accounted for above.

Two matches are NOT listed here because adding their Azure arm IS the feature, not a forced edit:
`crates/lakehouse-engine/src/scan/object_store.rs:134` (task 3.3) and
`crates/lakehouse-catalog/src/vended.rs:37` (task 1.3).

### B. The three new `ConnectionCreds` fields — seven sites, task 1.7

`ConnectionCreds` derives only `Clone`, has no `Default`, and is not `#[non_exhaustive]`
(`crates/lakehouse-catalog/src/creds.rs:25-52`), so every struct literal must name every field.
Task 1.2 adds `account_name`, `account_key`, and `sas_token`, turning SEVEN literals into E0063
missing-field errors. Each site supplies the three new fields as absent (`None`, the shape every
other optional field uses) and changes no existing field value and no existing assertion. Admitted
as CLASS THREE.

| Site | Shape today | Resolution |
|------|-------------|------------|
| `crates/lakehouse-catalog/src/test_support.rs:14` | `base_creds()` — all 14 fields named | Add the three fields as `None` |
| `crates/lakehouse-catalog/src/test_support.rs:84` | `creds_no_auth()` — all 14 fields named | Add the three fields as `None` |
| `crates/lakehouse-catalog/src/creds.rs:238` | `debug_redacts_every_secret_bearing_field` — all 14 fields named | Supply `account_name: Some("acct")`, `account_key: Some("static-account-key")`, `sas_token: Some("sv=…&sig=static-sas-signature")`, and ADD both secret literals to the existing loop over redacted values. This is the one site that gains assertions; it is the test that owns the `Debug` contract task 1.2 extends, so leaving the two new secret fields unasserted would ship an untested redaction |
| `crates/lakehouse-engine/src/adapter/mod.rs:2838` | `create_virtual_schema_over_empty_namespace_contacts_no_catalog_session` | Add the three fields as `None` |
| `crates/lakehouse-engine/src/adapter/pushdown/mod.rs:2354` | `malformed_table_ident_fails_before_any_catalog_contact` | Add the three fields as `None` |
| `crates/lakehouse-engine/tests/shared_type_reexports.rs:50` | Re-export identity check | Add the three fields as `None` |
| `crates/lakehouse-engine/tests/common/e2e_harness.rs:318` | `local_stack_creds()` | Add the three fields as `None` |

The eighth literal, `parse_creds` (`crates/lakehouse-engine/src/adapter/connection.rs:146`), is NOT
a forced edit: it is production code and reading the three fields there IS task 2.1.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Azure account-key credentials select the ADLS storage backend | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `account_key_creds_select_the_adls_backend` |
| Azure inline-SAS credentials select the ADLS storage backend | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `sas_token_creds_select_the_adls_backend` |
| An Azure CONNECTION without exactly one account name and one credential is rejected | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `azure_creds_require_account_name_and_exactly_one_credential` |
| A CONNECTION mixing Azure and static S3 credential fields is rejected | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `mixed_azure_and_s3_credential_fields_are_rejected` |
| Optional credential fields default sensibly | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `absent_optional_fields_default_and_still_select_s3` |
| One enum names the storage backend and answers every backend-specific question | Unit | `crates/lakehouse-catalog/src/storage.rs` | `adls_catalog_storage_props_emit_the_account_and_one_credential_key`, `adls_file_io_is_configured_from_exactly_the_catalog_storage_props`, `adls_secret_values_are_the_one_credential_and_omit_an_empty_one`, `adls_cred_is_redacted_in_debug_output` |
| The scan registers its object store without naming the backend | Unit | `crates/lakehouse-engine/src/scan/object_store.rs` | `side_store_url_returns_the_same_url_for_s3_as_the_deleted_bucket_derivation`, `side_store_url_preserves_the_s3a_scheme_so_the_key_matches_the_lookup`, `register_side_store_registers_an_adls_store_under_the_container_qualified_url`, `register_side_store_skips_a_second_side_in_the_same_container`, `register_side_store_registers_both_sides_in_different_accounts`, `register_side_store_surfaces_an_unrecognised_azure_host_redacted`, `validate_sides_share_one_store_rejects_two_containers_in_one_account`, `validate_sides_share_one_store_accepts_every_s3_spec_shape`, `spec_size_index_keys_an_abfss_file_without_its_container` |
| Adding the Azure backend leaves the S3 path unchanged (Azure-secret redaction clause) | Unit | `crates/lakehouse-catalog/src/redaction.rs` | `redact_credentials_strips_azure_account_key_and_sas_labels` |
| Adding the Azure backend leaves the S3 path unchanged (Azure-secret redaction clause) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs` | `manifest_read_errors_redact_the_literal_azure_secret_values` |
| The scan-spec wire carries the backend as a tagged variant | Unit | `crates/lakehouse-catalog/src/storage.rs` | `adls_serializes_under_a_lowercase_externally_tagged_variant_key`, `adls_round_trips_through_its_tagged_encoding`, `only_matching_lowercase_variant_keys_decode` |
| Adding the Azure backend leaves the S3 path unchanged | Integration | `crates/lakehouse-engine/tests/` (whole suite; edited only at the sites named in § Forced Edits) + `make test-e2e` | `cargo test --workspace` and the S3 E2E suite, both run as the characterization gate with no S3 assertion changed. Three files in `tests/` are forced edits: `shared_type_reexports.rs`, `common/e2e_harness.rs`, and `e2e_int96_timestamp_test.rs` |
| One concept-level call resolves the effective scan storage from a loadTable response | Unit | `crates/lakehouse-catalog/src/vended.rs` | `resolve_vended_storage_returns_an_adls_backend_unchanged` |
| Iceberg path resolution holds for `abfss://` (Background clause) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs` | `abfss_paths_relativize_and_reconstruct_losslessly` |

### Manual Testing

No Azure endpoint is reachable from this environment — real-cloud verification is slice E (#277). Each command below runs against the built software and produces an observable result.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/connection-credentials | `cargo test -p lakehouse-engine adapter::connection::tests` | 0 failures; the Azure shape tests named above appear in the output as `ok` |
| vs-adapter/storage-backend-enum (type) | `cargo test -p lakehouse-catalog storage::tests` | 0 failures; the `adls_*` tests appear as `ok` |
| vs-adapter/storage-backend-enum (scan) | `cargo test -p lakehouse-engine scan::object_store::tests` | 0 failures; `validate_sides_share_one_store_rejects_two_containers_in_one_account` appears as `ok` |
| vs-adapter/pushdown-planning-cloud-credentials | `cargo test -p lakehouse-catalog vended::tests` | 0 failures; the S3 vended assertions pass unedited and the Adls passthrough test appears as `ok` |
| S3 unchanged (all three features) | `make cross-musl-udf-build && make test-e2e 2>&1 \| tee /tmp/e2e.log; echo "exit=$?"` | `exit=0`; the S3 E2E suite runs against a freshly built `.so` and reports 0 failures. A missing Exasol container must FAIL the run, never skip it |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (UDF `.so`) | `make cross-musl-udf-build` | Exit 0 |
| Test (unit) | `cargo test --workspace` | 0 failures |
| Test (E2E, S3 characterization gate) | `EXASOL_CONTAINER=lakehouse-engine-rs-2-exasol-1 make test-e2e` | 0 failures |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | 0 warnings |
| Format | `cargo fmt --check` | No changes |
