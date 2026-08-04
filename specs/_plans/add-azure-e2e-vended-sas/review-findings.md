# Code Review Findings: add-azure-e2e-vended-sas

## Summary
- Files reviewed: 3
- Total findings: 4 (standard: 2, expert: 2)

Verification run during review (evidence, not a finding): `cargo fmt --all -- --check` exit 0;
`cargo clippy --all-targets --features azure-e2e -- -D warnings` clean; `cargo test --features
azure-e2e --test e2e_azure_test adls_` 3 passed; `cargo test --features azure-e2e --test
e2e_azure_test lakekeeper_connection_password_vended` 1 passed.

Judgement on the two deviations the implementer flagged:

1. **Extending the adapter-script provenance check to loop over both VSs — faithful in substance,
   but its placement violates a normative clause.** The Background delta does state provisioning
   reuses the shared definition "including between the two credential arms", and the recorded
   scenario `Azure binary provisions the scan path from the shared harness definition` is
   arm-agnostic, so checking both VSs is a correct generalization. What is not correct is running
   the *vended* VS's provenance assertion after the static arm's assertions — see the
   `[OUTDATED_COMMENT]` expert finding on assertion order.
2. **Splitting the shared-harness script-DDL check out as its own numbered group — faithful, no
   ordering violation on its own.** The cross-arm comparison remains the last group, and the
   static arm's own groups stay contiguous ahead of it. The group only becomes a problem in
   combination with deviation 1.

Everything else the brief listed as an invariant checks out: no `AdlsWarehouseProfile::new`
reference survives anywhere under `crates/` (grep: only `static_creds` / `vended` call sites); no
backend-specific vended CONNECTION helper was added and the shared `lakekeeper_connection_password`
vended branch still returns `base` unchanged; `seed.rs` changed only the `SeedStorage::Adls`
comment; `_container` is the last-declared `AzureFixture` field and the fixture is a test-local;
provisioning order is container → vended → static; the Keycloak token is minted inside the shared
`seed_arm` closure so freshness is structural; every `exa_conn`/DDL call is outside `rt.block_on`;
`abfss_prefix()` stayed on `AzureFixture`; the per-arm seeded-path assertion replaced the
container-level one and the sibling-prefix assertion is not vacuous (it fires if both arms ever
carry the same warehouse name).

## Standard fixes

### crates/lakehouse-engine/tests/common/lakekeeper.rs

#### [OUTDATED_COMMENT] `post_warehouse`'s doc states two guarantees the function does not give, one of which this change disproved
- Location: lines 402-410
- Issue: two claims in the doc are now wrong. (a) "fail loudly unless it exists afterwards" — the
  function never reads back existence; it inspects the POST status only, which is exactly the gap
  the new `create_warehouse_and_confirm` in `e2e_azure_test.rs:331` was added to close. (b) "Each
  harness warehouse has a unique key-prefix, so an overlap can only mean this same warehouse
  already exists" — that inference was written when at most one ADLS warehouse existed per
  container. This change puts two ADLS warehouses on one `filesystem` for the first time, and the
  plan (`plan.md` task 4.4) explicitly names "the ADLS overlap check is coarser than exact prefix
  comparison" as a live possibility. A reader of `post_warehouse` alone concludes no caller-side
  readback is needed, while one caller now spends a management-API round-trip per create precisely
  because it is.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper.rs, rewrite `post_warehouse`'s doc
  comment so its first sentence says the function fails loudly on any status other than 2xx, 409,
  or an already-exists 400 — dropping the false "unless it exists afterwards" — and replace the
  sentence "Each harness warehouse has a unique key-prefix, so an overlap can only mean this same
  warehouse already exists." with wording that states the overlap-400 to already-exists mapping is
  an unverified inference for warehouses sharing one bucket or filesystem, that a create Lakekeeper
  actually rejected is therefore reported here as success, and that callers needing certainty must
  read the warehouse back through `lakekeeper_warehouse_storage_profile` as the ADLS suite's
  `create_warehouse_and_confirm` does.

### crates/lakehouse-engine/tests/e2e_azure_test.rs

#### [OUTDATED_COMMENT] `create_warehouse_and_confirm`'s assertion cannot report the failure its message describes
- Location: lines 321-342
- Issue: the doc comment and the `assert_eq!` message both describe the swallowed-overlap case — a
  second storage profile Lakekeeper rejected with a 400 that `post_warehouse` reports as success.
  In that exact case there is no warehouse of that name, so `lakekeeper_warehouse_storage_profile`
  (`common/lakekeeper.rs:576-583`) panics first with "Lakekeeper list-warehouse GET to {url}
  reported no warehouse named '{warehouse_name}' with a storage profile", and the carefully worded
  overlap message here never prints. The `assert_eq!` on `key-prefix` can only fire in a different,
  unmentioned case: Lakekeeper registering the warehouse under a prefix other than the one
  requested. The readback still does its job — it moves the failure from a later opaque seed error
  to this step — but the code and the comment describe different mechanisms.
- Fix: In crates/lakehouse-engine/tests/e2e_azure_test.rs, rewrite `create_warehouse_and_confirm`'s
  doc comment to state both mechanisms separately: that a create rejected for a storage-profile
  overlap leaves no warehouse of this name, so the readback surfaces it here as
  `lakekeeper_warehouse_storage_profile`'s own "no warehouse named" panic instead of as a seed
  error several steps later; and that the `key-prefix` equality assertion covers the distinct case
  of Lakekeeper registering the warehouse under a prefix other than the one requested. Reword the
  `assert_eq!` message accordingly so it describes the mismatched-prefix case it can actually
  report, not the overlap case it cannot.

## Expert fixes

### crates/lakehouse-engine/tests/e2e_azure_test.rs

#### [IMPLEMENTATION_COUPLED_TEST] The vended CONNECTION-shape group asserts a re-derived password, not the one that was installed
- Location: line 498 (assertion group 1), against lines 267-272 (provisioning)
- Issue: group 1 calls `lakekeeper_connection_password(&fixture.vended_arm.warehouse, true)` a
  second time and asserts on that fresh return value. `provision()` passed a separately-constructed
  call of the same expression to `create_virtual_schema_with_password`, and the fixture keeps no
  record of it. The assertion therefore proves a property of a pure helper — already pinned by
  `common/lakekeeper.rs::tests::lakekeeper_connection_password_vended_omits_static_s3`, which
  asserts exactly `account_name: None`, `account_key: None`, empty `endpoint`/`region`/`access_key`/
  `secret_key` — and proves nothing about `AZ_VENDED_CATALOG_CREDS` as actually created. The
  masking hole this opens is the specific one the whole arm exists to prevent: if `provision()`
  ever passed `lakekeeper_adls_connection_password(...)` (in scope at line 54) for `VS_VENDED`,
  group 1 would still pass, and so would groups 2, 3, 4 and 7 — the scan would simply read the
  container with the account key, and the suite would report a green vended-SAS proof over a
  static-credential CONNECTION. The spec delta's clause is about the installed artefact: "the test
  SHALL assert that the vended CONNECTION carries no `account_name` key and no `account_key` key at
  all".
- Fix: In crates/lakehouse-engine/tests/e2e_azure_test.rs, in `AzureFixture::provision`, bind the
  vended password to a local (`let vended_password = lakekeeper_connection_password(&vended_warehouse, true);`)
  before line 267, pass `&vended_password` to `create_virtual_schema_with_password`, and move that
  same value into a new `AzureFixture` field `vended_password: CatalogConnectionPassword`
  (`CatalogConnectionPassword` is already imported from `common::stack`; it derives only `Default`,
  so it carries no `Debug` leak risk) declared after `static_arm` and before `_container` so
  `_container` stays last. Document the field as holding the exact password used to create
  `CONN_VENDED`, so the assertion group checks the installed CONNECTION rather than a
  re-derivation. Then change assertion group 1 to drop the second
  `lakekeeper_connection_password(...)` call and assert against `fixture.vended_password` instead,
  keeping the three existing assertions unchanged in content.

#### [OUTDATED_COMMENT] Two doc comments state a vended-before-static assertion invariant the test body violates
- Location: doc claims at lines 35-41 and 481-488; violating code at lines 595-615 (group 6),
  which runs after group 5 (lines 541-573)
- Issue: the module doc says "every vended-arm assertion except the closing cross-arm comparison
  runs BEFORE the static arm's" and the test doc repeats it as "**The assertion order below is
  normative, not style.**" The spec delta makes it normative in the same words: "every assertion
  specific to the vended arm except the cross-arm row comparison SHALL run BEFORE the static arm's
  assertions". Group 6's `for arm in [&fixture.vended_arm, &fixture.static_arm]` loop asserts that
  `AZ_VENDED_LAKEHOUSE` was created USING the shared adapter script — an assertion specific to the
  vended arm — and it runs after the whole of group 5, which is the static arm's storage-profile,
  seeded-path, and projection/filter/LIMIT assertions. Extending that loop to both arms was the
  right call; placing it after the static arm's block is what breaks the invariant. The plan's
  mandated relative order is not otherwise disturbed: the static arm's groups stay contiguous and
  the cross-arm comparison stays last.
- Fix: In crates/lakehouse-engine/tests/e2e_azure_test.rs, in
  `azure_static_and_vended_creds_end_to_end`, move the entire shared-harness provenance block
  (both the `for script in [ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME]` loop and the `for arm in
  [&fixture.vended_arm, &fixture.static_arm]` loop, currently numbered comment group 6 at lines
  575-615) so it runs immediately after the vended scan group (`assert_projection_filter_limit`
  for `vended_arm`, line 538) and before the static arm's block that begins with
  `assert_vs_exists(&mut conn, &fixture.static_arm)`. Renumber the group comments to 1-4 vended,
  5 shared-harness provenance (both arms), 6 static arm, 7 cross-arm comparison, and update the
  cross-arm group's comment so its back-reference names the renumbered vended and static
  seeded-path groups instead of "group 3 and group 5". Leave the two doc comments' ordering claims
  as written — the move is what makes them true again. Verify afterwards that no assertion naming
  `fixture.vended_arm` or `VS_VENDED` remains anywhere below the first `fixture.static_arm`
  assertion except the closing cross-arm comparison.
