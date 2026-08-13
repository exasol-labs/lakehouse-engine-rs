# Code Review Findings: fix-vended-storage-shared-policy

## Summary
- Files reviewed: 13
- Total findings: 11 (standard: 10, expert: 1)

Verified clean, no finding raised: both consent gates are reachable from BOTH selectors
(`storage::adls_backend`'s `abfs://` gate and `storage::s3_backend`'s plaintext-endpoint gate are
the only construction paths, and each selector's own suite exercises both); the address rule is
CONNECTION-wins per field and is NOT inverted (`resolved_address_field` returns the connection value
first, proven by `store_address_resolves_endpoint_and_region_independently_with_the_connection_winning`
and by `vended_addressing_prefers_the_connection_endpoint_and_region` at the production call site);
the Iceberg ADLS error-precedence reorder breaks no asserted scenario (the `abfs` block of
`unsatisfied_vended_request_errors_without_static_fallback` vends a SATISFIABLE SAS, so the gate is
still what refuses it, and the `vended_adls_account_name_requires_a_labelled_host` fixture repair is
honest — it vends a SAS keyed by each unlabelled host so the account-name refusal remains the
refusal under test); the `variant_selected_from_location_scheme` `allow_http: true` relaxation does
not strip the only abfs-gate coverage (`abfs_location_requires_allow_http_on_the_unity_path` and
`adls_backend_gates_abfs_on_allow_http_and_never_gates_abfss` both assert it);
`effective_storage_from_loopback_catalog`'s extraction preserved
`resolve_file_list_against_locationless_catalog`'s behaviour and assertions verbatim; `declares()` is
a correctness fix, not a weakening, and `s3_backend_from_vended`/`adls_backend_from_vended` are both
still pinned in `demoted_and_deleted_functions_are_not_declared_public`; `cargo fmt --check`,
`cargo clippy --workspace --all-targets -- -D warnings`, `cargo test -p lakehouse-catalog`, and
`cargo test -p lakehouse-engine --lib file_resolution` are all clean, and no changed file carries a
formatting-only or out-of-scope diff.

## Standard fixes

### crates/lakehouse-catalog/src/unity/vended.rs

#### [OUTDATED_COMMENT] Module doc and selector doc still describe the pre-split forked design
- Location: lines 4-13 and line 108
- Issue: the module doc states that this selector "shares only the scheme-to-variant-kind
  classification with the Iceberg vended selector", that "each selector constructs its OWN
  `StorageBackend` variant from its own credential family", and that this satisfies "the probe's
  requirement that every variant name appear in this selector's own source". All three are false
  after this change: the selectors now share the whole policy and both construction functions,
  neither names a `StorageBackend` variant (verified — `each_vended_selector_dispatches_every_vended_backend_kind`
  asserts `VendedBackendKind::*` dispatch instead, and `shared_vended_home_constructs_every_storage_backend_variant`
  asserts construction happens in `storage.rs`). Separately, the `resolve_uc_vended_storage` doc at
  line 108 says "its signature carries no `warehouse`, `region`, or existing `StorageBackend`" and
  is then contradicted two sentences later by "only the store address — `endpoint`/`region` — may
  cross over from the CONNECTION". This is the exact class of stale-doc drift the plan was written
  to close.
- Fix: In crates/lakehouse-catalog/src/unity/vended.rs, rewrite the module doc (lines 4-13) to state
  that this module reads the Unity Catalog temporary-table-credentials wire shape ONLY, reduces it
  to the neutral `VendedS3`/SAS values, and hands them to the shared policy and construction in
  `storage`, which is what applies the consent gates and builds the `StorageBackend` variant; drop
  the "constructs its OWN variant" and "every variant name appears in this selector's own source"
  claims and replace them with the `VendedBackendKind`-dispatch requirement the current probe
  enforces. In the `resolve_uc_vended_storage` doc, change "carries no `warehouse`, `region`, or
  existing `StorageBackend`" to "carries no `warehouse`, no credential, and no existing
  `StorageBackend`" so it no longer contradicts the store-address sentence that follows it.

### crates/lakehouse-catalog/src/unity/vended_tests.rs

#### [OUTDATED_COMMENT] Assertion messages claim a CONNECTION-value guarantee the test cannot observe
- Location: lines 90 and 92
- Issue: `s3_vended_response_terminates_in_s3_backend` asserts `props.endpoint.is_empty()` with the
  message "no endpoint vended -> empty, no CONNECTION endpoint read" and `props.region.is_empty()`
  with "no CONNECTION region read". The test passes `&StaticStoreAddress::default()`, so there is no
  CONNECTION endpoint or region to read — the message claims evidence the test does not produce.
  Worse, under the new contract a non-empty CONNECTION `endpoint`/`region` IS read and WINS, so the
  message now asserts the opposite of the shipped behaviour and would mislead the next reader into
  believing a regression is covered here.
- Fix: In crates/lakehouse-catalog/src/unity/vended_tests.rs, change the two assertion messages in
  `s3_vended_response_terminates_in_s3_backend` (lines 90 and 92) to state what the fixture actually
  establishes — that with the CONNECTION address unset and no endpoint or region vended, both
  resolve empty — and remove the "no CONNECTION endpoint read" / "no CONNECTION region read" claims.

### crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs

#### [OUTDATED_COMMENT] The characterization gate's rationale states the inverse of the new address rule
- Location: doc lines 348-353, assertion message lines 370-372
- Issue: `lakekeeper_vended_creds_projection_filter`'s doc says "scheme-driven resolution builds the
  backend from the `loadTable` response ALONE, so a static `endpoint`, `region`, or key pair would
  be a live credential that is never read", and its assertion message repeats "a static endpoint,
  region, or key pair would be an unread credential rather than a fallback". After this change a
  static `endpoint`/`region` IS read and WINS over the vended value (`storage::resolved_address_field`),
  so both texts are now wrong for two of the three fields they name. The empty-shape assertion
  itself remains correct and MUST stay — it is what keeps this E2E suite honest evidence that the
  rows came through vending rather than through a CONNECTION address — but its stated reason is
  inverted, which is how a future reader talks themselves into relaxing it.
- Fix: In crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs, keep the
  `endpoint`/`region`/`access_key`/`secret_key` emptiness assertion in
  `lakekeeper_vended_creds_projection_filter` exactly as it is, and rewrite its doc comment (lines
  348-353) and its assertion message (lines 370-372) to say that a static key pair would be an
  unread credential, while a static `endpoint` or `region` would OVERRIDE the vended store address —
  so this fixture must carry neither, or the row set below would no longer prove the scan reached
  the store through the vended resolution alone.

### crates/lakehouse-engine/tests/cloud_e2e_test.rs

#### [VAGUE_TEST_NAME] Test name still claims a store-address assertion that was deleted
- Location: line 701
- Issue: `cloud_glue_vends_s3_key_pair_and_store_address` no longer asserts anything about a store
  address — the `region_vended || endpoint_vended` assertion was correctly removed and both keys are
  now only reported through `println!`. The name still promises the deleted assertion, so the suite
  advertises Glue store-address evidence it does not produce. (Note: the plan's § Test Disposition
  attributes this amendment to the sibling `cloud_scan_reads_with_vended_credentials`; the amendment
  landed in the right test — this one — but the name was not brought along.)
- Fix: In crates/lakehouse-engine/tests/cloud_e2e_test.rs, rename
  `cloud_glue_vends_s3_key_pair_and_store_address` to
  `cloud_glue_vends_the_s3_key_pair_for_the_table_location`, and update the literal test name
  embedded in its `println!` report prefix (line 769) to the new name so the report line stays
  greppable by test name.

### crates/lakehouse-catalog/tests/catalog_public_surface.rs

#### [VAGUE_TEST_NAME] Arity-pin test name asserts the opposite of what the signature now takes
- Location: line 345
- Issue: `resolve_uc_vended_storage_signature_takes_no_connection_value` now calls the selector with
  `&StaticStoreAddress::default()`, and the whole point of the amended pin is that the signature DOES
  take one CONNECTION-derived value — narrowed to a type that cannot carry a credential. The test's
  own doc comment says so explicitly ("The one CONNECTION-derived value it does take is a store
  ADDRESS"), directly contradicting its name. A name that states the inverse of the contract is worse
  than a vague one: the next reader greps for it to prove no CONNECTION value crosses over and
  concludes the wrong thing.
- Fix: In crates/lakehouse-catalog/tests/catalog_public_surface.rs, rename
  `resolve_uc_vended_storage_signature_takes_no_connection_value` to
  `resolve_uc_vended_storage_signature_takes_only_a_credential_free_store_address`, leaving its body,
  doc comment, and assertions unchanged.

### crates/lakehouse-catalog/src/vended.rs

#### [MISSING_DESIGN_INTENT] The public entry point no longer documents its `anchor` contract
- Location: lines 25-32
- Issue: the rewritten `resolve_vended_storage` doc dropped the one caller contract a caller can
  actually get wrong, previously stated as: "`anchor` must be the table's own location — also what
  `storage_credentials[*].prefix` matches against — so a catalog URI is rejected as an unsupported
  scheme rather than silently selecting the flat `config` map." Nothing states it now: the private
  `select_credential_source` doc explains the prefix rule but is invisible to an external caller, and
  the parameter is a bare `anchor: &str`. Passing the catalog REST URI or the `warehouse` instead of
  the table location is a silent wrong-credential-source selection, not a compile error.
- Fix: In crates/lakehouse-catalog/src/vended.rs, add the dropped `anchor` contract sentence back to
  `resolve_vended_storage`'s doc comment, stating that `anchor` must be the table's OWN location —
  the same value `storage_credentials[*].prefix` is matched against — so that a catalog URI is
  refused as an unsupported scheme rather than silently falling through to the flat `config` map.

### crates/lakehouse-catalog/src/storage.rs

#### [MISSING_DESIGN_INTENT] The reason the ADLS account name must not be case-folded was lost in the split
- Location: lines 228-239
- Issue: the deleted `vended.rs::adls_backend_from_vended` doc carried the rationale for a non-obvious
  constraint: "`account_name` is derived from the host VERBATIM: the guard it feeds compares it
  byte-exactly against the account parsed out of each file URI
  (`iceberg-storage-opendal-0.10.0/src/azdls.rs:165`), so a case-folded account name would fire the
  guard on the very locations it was derived from." The shared `adls_account_name` preserves that
  behaviour (it slices the host without folding) but records no reason for it, and its neighbour
  `scheme_of` two screens above DOES lowercase — so the next reader has an explicit case-folding
  precedent in the same module and no note telling them why it must not be applied here.
- Fix: In crates/lakehouse-catalog/src/storage.rs, extend `adls_account_name`'s doc comment with the
  verbatim-derivation rationale: the label is read from the host byte-exactly because the downstream
  `adls.account-name` wrong-account guard compares it byte-for-byte against the account parsed out of
  each file URI (`iceberg-storage-opendal-0.10.0/src/azdls.rs:165`), so case-folding it would fire the
  guard on the very locations it was derived from.

#### [MISSING_DESIGN_INTENT] Module doc does not state the module's new second responsibility
- Location: lines 1-14
- Issue: `storage.rs` gained the entire shared vended policy — both consent gates, the CONNECTION-wins
  address rule, the `path_style` derivation, `VendedS3`, `StaticStoreAddress`, and both construction
  functions — but its module doc still describes only `StorageBackend`, the iceberg-config-key
  ownership, and redaction. The plan's headline decision (this module is the shared home BECAUSE the
  enum's own module already owns which module may name a variant, shrinking that list from six to
  four) is recorded nowhere in the module itself, so a reader opening the file sees vended policy
  functions with no statement of why they live beside the enum.
- Fix: In crates/lakehouse-catalog/src/storage.rs, add a paragraph to the module doc stating that this
  module is also the single home for vended-storage POLICY and CONSTRUCTION — the `abfs://` and
  plaintext-endpoint consent gates, the CONNECTION-wins store-address rule, and the two
  `StorageBackend` constructions both catalog kinds share — and that it lives here because the enum's
  own module already owns which module may name a variant, so the vended selectors fork only on how a
  value is read off the wire, never on what makes it acceptable.

### crates/lakehouse-catalog/src/storage_tests.rs

#### [MISSING_BOUNDARY_TEST] The plaintext gate's case-insensitive scheme match is untested
- Location: line 497
- Issue: `s3_backend`'s consent gate matches the resolved endpoint's scheme with
  `eq_ignore_ascii_case("http")`, but no test in the workspace exercises a case-variant spelling —
  `grep -rn "HTTP://\|Http://" crates/lakehouse-catalog/src/` returns nothing, and
  `s3_backend_gates_a_plaintext_endpoint_the_connection_supplied` uses only lowercase
  `http://minio:9000`. Deleting `_ignore_ascii_case` from the comparison keeps every test green while
  letting an `HTTP://minio:9000` endpoint through the gate the whole change exists to close. The
  location-scheme side already has this coverage (`S3://`, `ABFSS://` in `vended_tests.rs`); the
  endpoint side does not.
- Fix: In crates/lakehouse-catalog/src/storage_tests.rs, extend
  `s3_backend_gates_a_plaintext_endpoint_the_connection_supplied` to drive the refusal over a
  case-variant endpoint spelling as well (loop the CONNECTION endpoint over `http://minio:9000` and
  `HTTP://minio:9000`), asserting the same refusal for both so the gate's case-insensitive scheme
  match is pinned.

#### [DUPLICATE_TEST] StaticStoreAddress construction is asserted identically in two places
- Location: line 378
- Issue: `static_store_address_defaults_to_empty_and_takes_the_connections_endpoint_and_region`
  asserts exactly what `catalog_public_surface.rs`'s
  `static_store_address_is_reachable_and_declares_no_credential_field` (lines 571-582) already
  asserts — `Default` yields two empty strings, `From<&ConnectionCreds>` yields the CONNECTION's
  `endpoint` and `region`, read back through the same two public accessors. The external probe is
  strictly stronger: it makes the same assertions AND proves the type is reachable from outside the
  crate AND checks the declaration carries no credential field. The plan's task 2.4 did not ask for
  this internal copy (it lists the address-precedence matrix, the `path_style` matrix, the two gates,
  and the both-empty success case), so it is an unplanned second copy of one behaviour.
- Fix: In crates/lakehouse-catalog/src/storage_tests.rs, delete
  `static_store_address_defaults_to_empty_and_takes_the_connections_endpoint_and_region` together
  with the now-unused `base_creds` import if nothing else in the file uses it, leaving
  `catalog_public_surface.rs`'s `static_store_address_is_reachable_and_declares_no_credential_field`
  as the single home for that assertion; run `cargo test -p lakehouse-catalog` and
  `cargo clippy --workspace --all-targets -- -D warnings` afterwards.

## Expert fixes

### crates/lakehouse-catalog/src/storage.rs

#### [DEAD_FLEXIBILITY] `adls_account_name` takes a second parameter that must agree with the first
- Location: line 228
- Issue: `adls_account_name(location: &str, host: &'a str)` requires `host` to be exactly
  `location_host(location)` — its own doc says so ("`host` — [`location_host`]'s result for
  `location`") — but nothing enforces it, and the parameter is never varied independently at any
  production call site (`adls_backend` is the only one, and it computes `location_host(location)` on
  the line above). This is precisely the "undocumented ordering contract between calls" shape the
  guardrails ban: a second call that requires a first should be made unreachable without it. A caller
  that pairs a location with a different host gets a refusal naming a host the location does not have.
  The plan's § Key interfaces specified the single-parameter form
  (`adls_account_name(&str) -> Result<&str, UdfError>`); the implementation deviated. Collapsing it is
  also visibility-correct: `adls_account_name` has no caller outside `storage.rs`, so it needs no
  `pub(crate)`.
- Fix: In crates/lakehouse-catalog/src/storage.rs, change `adls_account_name` to
  `fn adls_account_name(location: &str) -> Result<&str, UdfError>` (private, not `pub(crate)`),
  deriving the host internally via `location_host(location)` and keeping the refusal text byte-identical
  — it must still name both the location and the derived host. Update `adls_backend` to call
  `adls_account_name(location)?` and drop its now-unused `host` local. Update the two callers in
  `crates/lakehouse-catalog/src/storage_tests.rs`
  (`adls_account_name_reads_the_hosts_leading_label` and
  `adls_account_name_errs_when_the_host_has_no_leading_label`) to pass the location alone, dropping the
  paired-host tuples from the loop but keeping both unlabelled-host locations
  (`abfss://mycontainer@/db/t` and `abfss://.dfs.core.windows.net/db/t`) and every existing assertion,
  including the one that the shared refusal names neither catalog kind. Leave the
  `pub fn adls_account_name` entry in `shared_vended_policy_steps_are_not_public` in place. Verify with
  `cargo test -p lakehouse-catalog` and `cargo clippy --workspace --all-targets -- -D warnings`.
