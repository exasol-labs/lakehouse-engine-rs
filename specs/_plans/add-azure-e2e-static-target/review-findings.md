# Code Review Findings: add-azure-e2e-static-target

## Summary
- Files reviewed: 14
- Total findings: 8 (standard: 7, expert: 1)

Verified locally: `cargo test --features azure-e2e --test e2e_azure_test -- azure_offline_`
→ `8 passed; 0 failed; 0 ignored; 15 filtered out`. The `AzureContainer` own-thread /
own-runtime teardown design, the account-key-vs-service-principal credential
segregation, the `post_warehouse` extraction, the `SeedStorage` dispatch, and the
`e2e-azure` job's absence from `release`'s `needs` were all checked and are correct as
built — no findings against them. `.gitignore`'s pre-existing `test.env` block is
correct and does not shadow `test.env.example` (an exact-name gitignore pattern);
`Cargo.lock` adds 13 dev-only packages with no second HTTP or TLS stack.

## Standard fixes

### test.env.example

#### [OUTDATED_COMMENT] Operator instructions point at a directory that `/speq:record` removes
- Location: line 2-3 (`# ... See specs/_plans/add-azure-e2e-static-target` / `# for details.`)
- Issue: the committed, operator-facing example file directs the reader to
  `specs/_plans/add-azure-e2e-static-target`, which is the in-flight plan directory.
  `/speq:record` merges the delta into the permanent library and archives the plan, so
  this path stops resolving the moment this slice is recorded — the header comment then
  points a new operator at nothing.
- Fix: In test.env.example, replace the `See specs/_plans/add-azure-e2e-static-target /
  for details.` reference with `See specs/e2e-harness/azure-e2e-harness/spec.md for
  details.` (the path the delta merges to), keeping the rest of the header unchanged.

### crates/lakehouse-engine/tests/common/lakekeeper.rs

#### [OUTDATED_COMMENT] "Unit tests" section banner no longer precedes the unit tests
- Location: lines 532-534 (banner) and 536-582 (`lakekeeper_warehouse_storage_profile`)
- Issue: the banner `// Unit tests — the pure CONNECTION-password builder (no live stack
  required).` is immediately followed by a second banner and then
  `lakekeeper_warehouse_storage_profile`, a live blocking-HTTP helper that issues a
  bearer-authenticated `GET {management_base}/warehouse`. `#[cfg(test)] mod tests` does
  not start until line 584. The banner now labels the opposite of what follows it — a
  reader scanning for the no-live-stack region lands on the one new function that
  requires a running Lakekeeper.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper.rs, move the
  `// Lakekeeper management API — read back a warehouse's storage profile.` banner and
  the whole `lakekeeper_warehouse_storage_profile` function to directly above the
  `// Unit tests — the pure CONNECTION-password builder` banner, so that banner again
  sits immediately before `#[cfg(test)] mod tests`.

### .github/workflows/ci.yml

#### [OUTDATED_COMMENT] Offline-check count guard floors at 4 while 8 tests match, and its comment says "four"
- Location: lines 218-244 (`Azure offline checks (no Azure account, no Docker stack)` step)
- Issue: the step comment states "All four are pure file/string assertions" and the guard
  is `if [ "${PASSED:-0}" -lt 4 ]`, but eight tests carry the `azure_offline_` prefix —
  verified: `cargo test --features azure-e2e --test e2e_azure_test -- azure_offline_`
  reports `running 8 tests` / `8 passed`. Four of the eight also are not file/string
  assertions (`azure_offline_container_name_is_azure_and_lakekeeper_legal`,
  `azure_offline_missing_credential_variable_fails_loud`,
  `azure_offline_present_credential_variable_is_read_without_surrounding_whitespace`,
  `azure_offline_failure_without_a_service_response_carries_no_error_code`). With a floor
  of 4 against a real count of 8, half the offline suite can be renamed, deleted, or
  lose its prefix and the step still exits green — which is precisely the "green and
  proves nothing, only invisible instead of red" outcome the guard was written to
  prevent.
- Fix: In .github/workflows/ci.yml's `Azure offline checks` step, raise the guard floor
  from `4` to `8` (both the `[ "${PASSED:-0}" -lt 8 ]` test and the `expected at least 4`
  text in the `::error::` message), and rewrite the step comment so it states the real
  count and composition: eight offline checks spanning container-name legality, the
  credential-variable readers, Azure error classification, the ADLS warehouse/CONNECTION
  shapes, gitignore hygiene, and the Make target's shape — and that adding an offline
  check requires bumping this number, which is the guard working rather than friction to
  route around.

### crates/lakehouse-engine/tests/common/azure.rs

#### [UNUSED_FUNCTION] Three credential readers are `pub` with no caller outside the module
- Location: lines 40, 46, 52 (`tenant_id`, `client_id`, `client_secret`)
- Issue: `tenant_id()`, `client_id()`, and `client_secret()` are `pub`, but the only
  caller of any of them is `ContainerAccess::from_environment` at line 150-157 inside
  this same file — verified by grepping every `azure::` reference under
  `crates/lakehouse-engine/tests/`, which reaches only `account_name`, `account_key`,
  `per_run_container_name`, `container_exists`, and `AzureContainer`. `mod.rs`'s
  crate-level `#![allow(dead_code)]` suppresses the warning that would otherwise say so.
  This is not cosmetic: the module's own doc comment (lines 4-9) and `client_secret`'s
  (lines 49-51) state that the Entra ID triple must never reach the data path, yet a
  `pub fn client_secret()` is exactly the reachability that makes that mistake a
  one-liner from any test in the binary.
- Fix: In crates/lakehouse-engine/tests/common/azure.rs, remove `pub` from
  `tenant_id`, `client_id`, and `client_secret` so the container-lifecycle triple is
  reachable only through `ContainerAccess`; leave `account_name` and `account_key` public
  (both are called from e2e_azure_test.rs) and leave every doc comment intact.

#### [MAGIC_NUMBER] Minimum container-name length is a bare literal beside a named maximum
- Location: line 366 (`(3..=MAX_CONTAINER_NAME_LEN).contains(&name.len())`)
- Issue: `assert_legal_container_name` bounds the name length with a named
  `MAX_CONTAINER_NAME_LEN` but a bare `3` for the lower bound, even though both halves
  come from the same rule (Azure blob container and Lakekeeper ADLS filesystem names are
  3 to 63 characters). The two ends of one constraint are expressed two different ways.
- Fix: In crates/lakehouse-engine/tests/common/azure.rs, add a module-scope
  `const MIN_CONTAINER_NAME_LEN: usize = 3;` next to `MAX_CONTAINER_NAME_LEN` with a doc
  comment naming it as Azure's and Lakekeeper's shared lower bound, then use it in
  `assert_legal_container_name`'s range check and in that assertion's message in place of
  the literal `3`.

### crates/lakehouse-engine/tests/e2e_azure_test.rs

#### [UNREACHABLE_CODE] Second disjunct of the script-DDL assertion never decides anything
- Location: lines 355-358 (`body.contains("liblakehouse_engine.so") || body.contains("udf")`)
- Issue: `create_schema_and_scripts` builds both script bodies from
  `%udf_object {SO_UDF_OBJECT_PATH}` where
  `SO_UDF_OBJECT_PATH = "buckets/bfsdefault/default/udf/liblakehouse_engine.so"`
  (crates/lakehouse-engine/tests/common/e2e_harness.rs:47, 146-155), so the first
  disjunct holds for every body this suite can ever observe and the `|| body.contains("udf")`
  arm can never be the reason the assertion passes. Worse, that arm is what the assertion
  degrades to if the shared definition ever stops referencing the `.so` — `"udf"` is a
  substring of the `%udf_object` directive itself, so the assertion would still pass over
  the exact regression it is meant to catch. The spec scenario requires the DDL be
  "byte-identical to every other E2E binary"; a substring-or-substring test is weaker
  than the available check.
- Fix: In crates/lakehouse-engine/tests/e2e_azure_test.rs, add `SO_UDF_OBJECT_PATH` to
  the `common::e2e_harness` import list, then replace the assertion condition
  `body.contains("liblakehouse_engine.so") || body.contains("udf")` with
  `body.contains(SO_UDF_OBJECT_PATH)` and update the message to name
  `{SO_UDF_OBJECT_PATH}` as the required BucketFS reference.

#### [SHRINKABLE] Fourth copy of the panic-payload downcast dance
- Location: lines 500-506 (`e2e_azure_test.rs`), lines 355-362
  (`common/azure.rs::panic_message`)
- Issue: the `downcast_ref::<&str>()` / `downcast_ref::<String>()` extraction of a
  `Box<dyn Any + Send>` panic payload now exists in four places under
  `crates/lakehouse-engine/tests/` — this plan added the third and fourth
  (`e2e_azure_test.rs` and `common/azure.rs`) on top of the pre-existing copies in
  `e2e_lakekeeper_test.rs` and `cloud_e2e_test.rs`. Rule of Three is well past, and every
  copy is load-bearing for a credential-redaction assertion, so a divergence between them
  is a silently weakened assertion rather than a compile error.
- Fix: In crates/lakehouse-engine/tests/common/stack.rs (the only `common` module admitted
  under every E2E feature gate), add
  `pub fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> Option<String>`
  returning the `&str` or `String` payload as an owned `String`, with a doc comment
  stating that libtest panics carry one of exactly those two payload types. Then route the
  three copies in changed files through it: `common/azure.rs`'s `panic_message` helper,
  `e2e_azure_test.rs`'s `azure_credentials_never_appear_in_output` (lines 500-506), and
  `e2e_lakekeeper_test.rs`'s equivalent block. Leave `cloud_e2e_test.rs` untouched — it is
  outside this plan's changed-file set.

## Expert fixes

### crates/lakehouse-engine/tests/common/azure.rs

#### [UNTESTED_ERROR_PATH] Both spec-mandated Azure error-code arms of the container guard are uncovered
- Location: line 198 (`(Some(StorageErrorCode::ContainerNotFound), _) => Ok(())`) and
  line 267 (`(Some(StorageErrorCode::ContainerAlreadyExists), _) => bail!(…)`)
- Issue: the container-guard spec scenario states two behaviors as explicit AND clauses —
  "a name collision at create time SHALL fail the run" and "a container already absent at
  delete time SHALL be treated as deleted" — and neither arm has a test.
  `azure_offline_failure_without_a_service_response_carries_no_error_code` covers only the
  `Err(original)` branch of `azure_failure` (no service response → no code); it never
  drives a real `StorageErrorCode` through either match. The plan's verification table
  maps both clauses to `azure_container_guard_deletes_on_panic`, which exercises the
  happy-path delete and asserts nothing about either code. The `ContainerNotFound` arm is
  the load-bearing one: a mis-mapped code there makes teardown print `LEAKED Azure
  container …` for a container it did in fact delete, which trains the reader to ignore
  the one line that reports a real orphan. Both arms are also unreachable from any offline
  test as written, because the decision is fused into the `match` on
  `azure_failure`'s tuple and the tuple can only be produced from a live
  `azure_core::Error` carrying a raw service response.
- Fix: In crates/lakehouse-engine/tests/common/azure.rs, split the code-to-decision step
  out of the two matches without changing any observable behavior: add
  `fn delete_reached_desired_state(code: Option<&StorageErrorCode>) -> bool` returning
  true only for `Some(StorageErrorCode::ContainerNotFound)`, and
  `fn is_name_collision(code: Option<&StorageErrorCode>) -> bool` returning true only for
  `Some(StorageErrorCode::ContainerAlreadyExists)`; give each a doc comment naming the
  spec clause it implements. Rewrite `ContainerAccess::delete` and
  `AzureContainer::create` to call them on `azure_failure`'s `Option<StorageErrorCode>`
  half, preserving the existing bail messages verbatim and preserving the current
  behavior that any other code — including `UnknownValue` and a `None` from a failure
  that reached no service — falls through to the `bail!` arm. Then add one
  `azure_offline_`-prefixed unit test in `azure_error_classification_tests` asserting, for
  each predicate, the matching code → true, the sibling code → false,
  `Some(StorageErrorCode::UnknownValue("SomethingNew".to_string()))` → false, and `None`
  → false, with messages naming the spec clause. Do NOT touch `Drop`, the teardown
  thread, the per-use client construction in `blob_container_client`, or the
  `ContainerAccess` field set — that design is deliberate and load-bearing. Re-verify with
  `cargo test --features azure-e2e --test e2e_azure_test -- azure_offline_` (the count
  must rise, so bump the CI floor in the same pass as the ci.yml finding) and
  `cargo clippy --all-targets --features exasol-e2e,lakekeeper-e2e,cloud-e2e,azure-e2e`.
