# Code Review Findings: add-azure-static-storage-backend

## Summary
- Files reviewed: 15
- Total findings: 6 (standard: 5, expert: 1)

Verified clean, no finding raised:
- **Exhaustive-match discipline holds.** No `_` arm exists over `StorageBackend` or `AdlsCred`
  anywhere in the workspace (`storage.rs:86-99`, `:110-142`, `:152-157`; `vended.rs:41-51`;
  `object_store.rs:138-227`; `test_support.rs:66-69`). The two tuple matches in
  `connection.rs:279-283` and `:70-74` (of the diff) spell every case explicitly rather than
  using `_`. A third backend is a compile error at every deciding site.
- **Redaction ordering is internally consistent within this diff.** All nine `file_resolution.rs`
  sites (`:266, :282, :455, :464, :476, :485, :547, :555, :562`) route through `redact_error_text`,
  and both `object_store.rs` arms compose value-then-label. `unsupported_delete_error`
  (`file_resolution.rs:419`) stays label-only correctly — it builds its message from a mechanism
  name and table name, never from an external error string. `connection.rs`'s two new validation
  errors name field names only, never values, and the tests assert that.
- **No unredacted Azure secret escape found in the changed files.** `side_store_url`'s two error
  messages carry only a scheme/authority slice; `validate_sides_share_one_store`'s carries two
  store URLs; `object_store-0.13.2`'s Azure builder errors (`UrlNotRecognised`, `DecodeSasKey`,
  `MissingSasComponent` at `src/azure/builder.rs:76-86`) echo no credential value.
- `cargo test --workspace --lib` (873 tests), `cargo clippy --workspace --all-targets -D warnings`,
  and `cargo fmt --check` are all green on the working tree.

## Standard fixes

### crates/lakehouse-catalog/src/storage.rs

#### [INFORMATION_LEAKAGE] The nested `AdlsCred` opts out of the wire-casing convention `StorageBackend` owns
- Location: lines 33-41 (`AdlsCred` declaration), 55-59 (the convention doc), 303-304 and 437 (tests pinning the result)
- Issue: `StorageBackend`'s doc comment states the wire contract — "Externally tagged (serde's
  default) with a lowercase variant key" — and `#[serde(rename_all = "lowercase")]` at line 67
  enforces it, with `only_matching_lowercase_variant_keys_decode` (line ~297) dedicated to proving
  a wrong-case key is rejected. The `AdlsCred` this diff nests inside that payload carries no
  `rename_all`, so the emitted object is
  `{"adls":{"account_name":"…","cred":{"AccountKey":"…"}}}` — one wire object with two casing
  conventions. `adls_serializes_under_a_lowercase_externally_tagged_variant_key` (line 426) now
  pins the PascalCase inner key as the permanent contract despite its own name claiming the
  opposite, so the one casing decision now has two owners and the inconsistency is locked in. The
  fix is free today (producer and consumer are the same `.so`, deployed together) and a wire break
  later.
- Fix: In crates/lakehouse-catalog/src/storage.rs add `#[serde(rename_all = "snake_case")]` to the
  `AdlsCred` enum declaration at line 35 so its variants encode as `account_key` and `sas`, matching
  the `account_key`/`sas_token` vocabulary the CONNECTION and the `adls.*` iceberg config keys
  already use. Update the two pinned literals to the new casing: line 437 becomes
  `"cred": {"account_key": "azure-static-key-secret"}`, and the two rejection payloads at lines
  303-304 keep `{"AccountKey":""}` (they must still fail to decode — add a fourth payload
  `r#"{"adls":{"account_name":"","cred":{"AccountKey":""}}}"#` so the wrong-case INNER key is
  proven rejected too, which is the case the test currently does not cover).

#### [MISSING_DESIGN_INTENT] `Adls::account_name`'s doc does not say which consumer reads it, and the scan path ignores it
- Location: line 73-74 (`/// The storage account name.` / `account_name: String`)
- Issue: `account_name` has exactly one consumer — `catalog_storage_props` (line 130-133), which
  feeds the iceberg `FileIO` manifest-read path. The DataFusion scan path does NOT read it: the
  Azure arm of `register_side_store` destructures it away (`StorageBackend::Adls { cred, .. }`,
  `object_store.rs:196`) because `MicrosoftAzureBuilder::with_url` derives the account from the file
  URI's host instead. So the two storage stacks resolve "which account" from two different sources,
  and a CONNECTION whose `account_name` disagrees with the table location's account host is accepted
  at plan time and only fails later at object-store auth. The one-line field doc states none of
  this, so the next reader has no way to know the field is not authoritative for the scan.
- Fix: In crates/lakehouse-catalog/src/storage.rs replace the `account_name` field doc at line 73
  with a comment stating that it configures the iceberg `FileIO` manifest-read path via
  `catalog_storage_props`, and that the DataFusion scan path does not read it — it derives the
  account from the host of the side's own file URIs (`MicrosoftAzureBuilder::with_url`), so an
  `account_name` that disagrees with the table location surfaces as an object-store auth failure
  rather than a plan-time error.

### crates/lakehouse-engine/src/scan/object_store.rs

#### [VAGUE_TEST_NAME] The "container-qualified" registration test asserts something DataFusion's registry key cannot express
- Location: lines 779-800 (`register_side_store_registers_an_adls_store_under_the_container_qualified_url`)
- Issue: the test's name and docstring claim the Azure store is registered "under the
  container-qualified URL", but `get_store` keys through `get_url_key`
  (`datafusion-execution-54.1.0/src/object_store.rs:266-274`), which is
  `scheme://` + `Position::BeforeHost..Position::AfterPort` — userinfo, i.e. the container, is
  dropped. The `get_store(&expected).is_ok()` assertion therefore succeeds for ANY container of
  `acct.dfs.core.windows.net`, so it does not test what its name says. Worse, the name asserts the
  exact opposite of the asymmetry `validate_sides_share_one_store` exists to compensate for, so a
  reader who trusts it concludes the collision guard is unnecessary. Only the `Some(expected)`
  return-value assertion is container-sensitive.
- Fix: In crates/lakehouse-engine/src/scan/object_store.rs rename the test at line 783 to
  `register_side_store_returns_the_container_qualified_url_but_the_registry_key_drops_the_container`
  and rewrite its docstring to state that `side_store_url`'s derived URL (the return value) carries
  the container while DataFusion's registry key does not, citing
  `datafusion-execution-54.1.0/src/object_store.rs:266-274`. Add a third assertion proving the
  asymmetry directly: after registering, assert
  `ctx.runtime_env().object_store_registry.get_store(&Url::parse("abfss://other@acct.dfs.core.windows.net").unwrap()).is_ok()`
  — a DIFFERENT container of the same account resolves to the same store — with a message naming
  this as the hazard `validate_sides_share_one_store` rejects.

#### [OUTDATED_COMMENT] "An empty side registers no store" is false for the fact side
- Location: lines 450-451, inside `validate_sides_share_one_store`
- Issue: the comment justifying `.filter(|(files, _)| !files.is_empty())` claims "An empty side
  registers no store (see `build_session_context`)". That holds only for the dimension side, which
  `build_session_context:86-88` guards with `&& !join.files.is_empty()`. The fact side is registered
  unconditionally at `build_session_context:71-78`, so an empty `spec.files` does not "register no
  store" — it reaches `side_store_url` and fails with "scan spec has no files". The filter silently
  moves that failure from the precondition to the registration call, and the comment asserts a
  guarantee `build_session_context` does not make.
- Fix: In crates/lakehouse-engine/src/scan/object_store.rs rewrite the comment at lines 450-451 to
  state that only the DIMENSION side is skipped when empty (`build_session_context:86-88`), so it
  can neither collide nor be derived from, and that an empty FACT side is still registered
  unconditionally and fails inside `side_store_url` with "scan spec has no files" — the filter does
  not change that outcome.

### crates/lakehouse-engine/tests/shared_type_reexports.rs

#### [MISSING_BOUNDARY_TEST] The new public `AdlsCred` re-export is pinned by neither surface probe
- Location: line 17-19 (the `use` list); companion gap at crates/lakehouse-catalog/tests/catalog_public_surface.rs lines 20-24
- Issue: this diff adds two new public re-export paths for `AdlsCred` —
  `lakehouse_catalog::AdlsCred` (`lib.rs:24`) and `lakehouse_engine::scan::spec::AdlsCred`
  (`spec.rs:234`). Both probe files exist precisely to turn a narrowed or removed re-export into a
  build failure: `catalog_public_surface.rs`'s module doc says "If any of the items below is
  narrowed below `pub` or its re-export is removed, this file fails to compile", and
  `shared_type_reexports.rs`'s says the proof that a re-exported path names the catalog crate's own
  type "is that this file compiles at all". Neither was extended, so `AdlsCred` — half of the
  credential type this slice ships — can be silently demoted or its re-export dropped with no
  failure, which is exactly the gap these two files were written to close. The diff edited both
  files (for the forced `ConnectionCreds` field additions) without extending either contract.
- Fix: In crates/lakehouse-engine/tests/shared_type_reexports.rs add `AdlsCred` to the
  `use lakehouse_engine::scan::spec::{…}` import at line 18, add a
  `fn accepts_catalog_crate_adls_cred(_cred: lakehouse_catalog::AdlsCred) {}` probe beside the three
  existing ones, and call it inside `reexported_paths_resolve_to_the_catalog_crate_types` with an
  `AdlsCred::AccountKey("k".into())` built via the engine's re-exported path. Separately, in
  crates/lakehouse-catalog/tests/catalog_public_surface.rs add `AdlsCred` to the
  `use lakehouse_catalog::{…}` list at lines 21-24.

## Expert fixes

### crates/lakehouse-catalog/src/redaction.rs

#### [INFORMATION_LEAKAGE] The value-then-label redaction ORDER has no owner, and this diff adds two more copies of it
- Location: redaction.rs lines 8-88 (the two primitives, no composition); new copies at crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs:394-404 (`redact_error_text`) and crates/lakehouse-engine/src/scan/object_store.rs:215-223 (Azure arm, inline)
- Issue: `redact_credentials` and `redact_secret_values` are both public and both live in this
  module, but the ORDER they must be composed in does not. That order is security-load-bearing —
  `redact_error_text`'s own doc comment (file_resolution.rs:396-401) explains why: an Azure SAS
  carries its own `sig=` label, so a label-first pass rewrites the middle of the token and leaves
  the value pass unable to match the literal. The composition is now open-coded in SIX independent
  places: `scan/emit.rs:213`, `scan/emit.rs:228`, `adapter/mod.rs:1015-1016`,
  `scan/positional_deletes.rs:208-209`, `scan/object_store.rs:184-187` (S3, pre-existing), plus the
  two this diff adds. This is not a theoretical risk: `lakehouse-catalog/src/auth.rs:91` and `:150`
  already compose it INVERTED, proving the order can be and has been gotten backwards when each
  caller re-derives it. Adding a named helper (`redact_error_text`) and then bypassing it in the
  very next file of the same diff is the worst of both — the decision now has a name AND six
  owners. Failure mode is a passing test over wrong behavior: a site with the arguments reversed
  still compiles, still redacts something, and still passes any test that only checks the account
  key (whose literal has no embedded label).
- Fix: In crates/lakehouse-catalog/src/redaction.rs add
  `pub fn redact_error_text(msg: &str, secrets: &[&str]) -> String` implemented as
  `redact_credentials(&redact_secret_values(msg, secrets))`, carrying the ordering rationale from
  file_resolution.rs:396-401 in its doc comment (value pass FIRST, because a SAS embeds its own
  `sig=` label and a label-first pass defeats the literal match). Add a unit test beside
  `redact_credentials_strips_azure_account_key_and_sas_labels` that pins the order by asserting
  BOTH that a full SAS literal is gone and that the inverted composition
  `redact_secret_values(&redact_credentials(raw), &[sas])` still leaks the SAS's `sp=` permission
  field — so a future reordering fails the test rather than passing it. Export it from
  crates/lakehouse-catalog/src/lib.rs line 22 alongside the two primitives. Then remove the private
  `redact_error_text` at crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs:394-404 and
  import the catalog one instead (the nine call sites are unchanged), and replace both inline
  compositions in crates/lakehouse-engine/src/scan/object_store.rs — the S3 arm at lines 181-189 and
  the Azure arm at lines 215-223 — with a single `redact_error_text(&e.to_string(), &secrets)` call
  each. Leave crates/lakehouse-engine/src/scan/emit.rs, crates/lakehouse-engine/src/adapter/mod.rs,
  and crates/lakehouse-engine/src/scan/positional_deletes.rs untouched: they are outside this plan's
  changed-files list and are recorded below as follow-up.

## Out-of-scope findings

Not part of this plan's changed-files list. Do NOT fix these here — they are recorded so the
orchestrator can open follow-up issues.

#### [CONFIRMED] `auth.rs` composes credential redaction in the inverted order — at TWO sites, not one
- Location: crates/lakehouse-catalog/src/auth.rs:91 (`redact_catalog_auth_error`) and crates/lakehouse-catalog/src/auth.rs:150 (the OAuth2 token-exchange closure)
- Confirmed by reading the file: both are `redact_secret_values(&redact_credentials(msg), …)` —
  label pass FIRST, value pass SECOND — the exact inversion of the order this diff's
  `redact_error_text` establishes and documents as load-bearing. The brief named `:91`; `:150` is a
  second, previously-unnamed instance of the same defect and should be included in the follow-up.
  Live, pre-existing, and related to open issue #135. Note the practical blast radius is narrower
  than it looks: `redact_catalog_auth_error` strips only CATALOG auth secrets (token,
  client_secret, client_id, oauth2_server_uri, scope), none of which embed their own label, so no
  Azure storage secret reaches it today — but it is the same latent defect and the same root cause
  as the Expert finding above (no single owner for the ordering), so fixing it becomes a one-line
  change once `lakehouse_catalog::redact_error_text` exists.

#### [INFORMATION_LEAKAGE] `build_rest_catalog` hardcodes the S3 OpenDAL factory, duplicating the decision `StorageBackend::file_io` now owns
- Location: crates/lakehouse-catalog/src/session.rs:51-53
- `build_rest_catalog` feeds `storage.catalog_storage_props()` (which, after this diff, can be the
  `adls.*` key set) into a `RestCatalogBuilder` whose storage factory is unconditionally
  `OpenDalStorageFactory::S3 { customized_credential_load: None }`. The backend-to-factory mapping
  is `StorageBackend::file_io`'s decision (storage.rs:152-157, extended by this diff with the
  `Azdls` arm), so it now has two owners and the `session.rs` one is wrong for an Azure CONNECTION.
  It is latent rather than active: the only caller is namespace enumeration
  (`namespace.rs:87-88`), which issues `list_namespaces`/`list_tables` over REST and never reads a
  data or manifest file through the factory. A follow-up should collapse the second owner into
  `file_io`'s.

#### [SCOPE NOTE] Issue #135 now covers a full account-level Azure credential, raising its severity
- Location: crates/lakehouse-engine/src/scan/spec.rs:644 (`CommonScanSpec::to_json`), embedded into pushed SQL via crates/lakehouse-engine/src/adapter/pushdown/support.rs:2281
- The `StorageBackend` — and therefore, after this diff, the Azure account key or SAS — is
  serialized into the common scan-spec JSON that is embedded verbatim in the generated pushdown
  SQL; `CommonScanSpec::from_json`'s own comment at spec.rs:652 states "Do not echo `s` — it
  contains credentials". This is issue #135's known, by-design behavior (the UDF needs the
  credentials), not a defect this diff introduced, and the plan's "leaves issue #135 strictly less
  to fix" framing is accurate for the `Debug`/error surfaces. Worth flagging only because the plan's
  goal line reads "keep every Azure secret out of every error, SQL string, and log line": the
  SQL-string half is not achieved, and an Azure shared account key is a permanent, account-wide,
  unscoped credential — strictly more damaging on exposure than the scoped, expiring STS token
  #135 was filed against. The follow-up should note the severity change.
