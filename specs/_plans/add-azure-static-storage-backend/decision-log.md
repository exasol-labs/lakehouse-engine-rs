# Decision Log: add-azure-static-storage-backend

## Interview

**Q1:** How should the container/store-key collision be handled? DataFusion's `DefaultObjectStoreRegistry::get_url_key` (`datafusion-execution-54.1.0/src/object_store.rs:268`) is `format!("{}://{}", url.scheme(), &url[Position::BeforeHost..Position::AfterPort])`, which EXCLUDES userinfo. For `abfss://container@account.dfs.core.windows.net/path` the registry key is therefore `abfss://account.dfs.core.windows.net`, dropping the container. Two containers in one storage account collide onto one registered store; a join's dimension side would silently read through the fact side's container store.

**A1:** Guard and error loudly. The Azure arm derives the container from the side's first file, verifies every data file and every associated delete file in that side resolves to the same container, and errors if a second side registering into the same account has a different container. Effectively one container per storage account per query; a mixed-container case is a clear `UdfError::User`, never a silent misread.

**Q2:** What triggers the Azure variant in `parse_creds`, and what happens if S3 and Azure fields are both present?

**A2:** The user did not answer directly and asked back: "What if there is none of that, because we use vended credentials? then we don't know the backend until we get the first location from a table. How do we solve that?" — answered in Q4/A4 and Q5/A5. The both-present sub-question was left to planning, subject to the Q3 principle that a credentials path must not resolve ambiguity silently, and to the Q4 constraint that selection happens from credential shape alone.

**Q3:** What if both `account_key` and `sas_token` are supplied?

**A3:** Reject both-present. `validate_creds` requires `account_name` plus EXACTLY ONE of `account_key` and `sas_token`. Both present, or neither present when the backend is Azure, is an error naming only field names — never values. This matches the enum: `AdlsCred` has no "both" state.

**Q4:** For the vended-only case the authoritative signal is the table location scheme, known at `resolve_vended_storage(&result, storage, anchor)` (`crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs:249`), which already receives `result.metadata.location()` and already returns a fresh `StorageBackend`. Three options were offered: (a) static picks and `validate_creds` rejects Azure-static-creds-plus-`use_vended_credentials` citing #276; (b) static picks and the new Adls arm of `resolve_vended_storage` is a documented no-op passthrough; (c) do the scheme switch now.

**A4:** "select at parse_creds from credential shape — and touch nothing at resolve_vended_storage."

Applied interpretation: backend selection lives in `parse_creds`/`storage_block` and nowhere else in this slice. No location-scheme switch in `resolve_vended_storage`. No `validate_creds` guard rejecting Azure plus `use_vended_credentials`. The only change permitted in `resolve_vended_storage` is the compile-forced `Adls` match arm, made a minimal passthrough returning the caller's backend unchanged and recorded in the spec as a tracked exception citing #276.

**Q5:** Is the existing `StorageBackend::S3(StorageProps::default())` fallback for a CONNECTION with `use_vended_credentials: true` and no static storage fields acceptable as slice C behaviour?

**A5:** Yes, unchanged. S3 remains the no-static-fields default. Existing vended-S3 behaviour is preserved bit for bit; the empty base carries no credentials anyway. Slice D corrects the variant from the location scheme after `loadTable`. No `Unknown`/deferred third state in this slice.

**Q6:** What is the scope boundary?

**A6:** Issue text only. Exactly what #275 lists: the variant and `AdlsCred`, `parse_creds`/`validate_creds`, the `register_side_store` Azure arm, the `file_io()`/`catalog_storage_props()`/`secret_values()` Azure arms, plus the unit tests the issue names. Explicitly out: E2E and real-cloud verification (#277), vended SAS (#276), Azurite-emulator endpoint override and `allow_http` for Azure, and any extra serde golden beyond what the above requires.

## Design Decisions

### [1] Backend selection triggers on ANY Azure field, not on `account_name` alone

- **Decision:** `storage_block` selects the ADLS variant when any of `account_name`, `account_key`, or `sas_token` is present. `validate_creds` then enforces `account_name` plus exactly one credential.
- **Alternatives:** Trigger on `account_name` alone (the field that is always required). Trigger on a credential alone. Trigger on an explicit `backend` field.
- **Rationale:** Triggering on `account_name` alone makes a CONNECTION that supplies `account_key` and forgets `account_name` fall back to S3 with the key silently ignored — the exact silent misconfiguration A3's rule exists to prevent. Any-of-three converts that input into a named-field error. An explicit `backend` field was rejected by the issue itself: the credential shape plus the URI scheme already carry the information, and a second source of truth is free to disagree with the credentials actually supplied.
- **Promotes to ADR:** no

### [2] A CONNECTION mixing Azure and static S3 credential fields is rejected

- **Decision:** `validate_creds` errors when any of `account_name`/`account_key`/`sas_token` is supplied together with any of `endpoint`/`region`/`access_key`/`secret_key`/`session_token`. The error names the supplied field names on both sides and no values.
- **Alternatives:** Declare a precedence (Azure wins, or S3 wins). Accept and silently ignore the unused set. Say nothing and let the any-of-three rule pick Azure.
- **Rationale:** This is the Q2 sub-question the user left open, and the Q3 principle settles it: a credentials path must not resolve an ambiguous input silently. An undeclared precedence is exactly that. It is the one rule #275's text does not name, and the plan says so rather than presenting it as issue scope. It can never fire on an S3-only deployment, so no existing CONNECTION is affected.
- **Promotes to ADR:** yes

### [3] `AdlsCred` makes "exactly one credential" unrepresentable rather than merely validated

- **Decision:** `AdlsCred` has an account-key state and a SAS state and no third state; the enum, not a runtime check at each use site, is what guarantees exactly one credential reaches the object-store builder and the `FileIO` props map.
- **Alternatives:** Two `Option<String>` fields on the variant, validated at the boundary.
- **Rationale:** `object_store`'s `build()` silently prefers an access key over a SAS when both are set (`object_store-0.13.2/src/azure/builder.rs:990` vs `:1021`). With two `Option`s that precedence is reachable and would resolve a contradictory credential set silently. With the enum it is unreachable by construction, and the `validate_creds` rule is then the single place the contradiction is reported.
- **Promotes to ADR:** yes

### [4] `Adls { account_name, cred }` is a struct variant, not a wrapper struct

- **Decision:** The variant carries its two fields inline rather than wrapping a new `AdlsProps` struct.
- **Alternatives:** `Adls(AdlsProps)`, mirroring `S3(StorageProps)`.
- **Rationale:** `S3` wraps because `StorageProps` pre-exists and `vs-adapter/catalog-crate-structure` pins its serde encoding field for field; the wrapper protects that contract. There is no Azure equivalent to protect, so a wrapper struct would be a type introduced for symmetry alone. Slice D can extract one if it needs a merge target for vended values.
- **Promotes to ADR:** no

### [5] `AdlsCred` implements a manual redacting `Debug`

- **Decision:** `Debug` masks the wrapped secret for both states. `account_name` stays visible.
- **Alternatives:** Derive `Debug`, matching `StorageProps`, whose `secret_key` prints in the clear today.
- **Rationale:** Six lines on a security path. Matching the existing behaviour would add a NEW leak on the grounds that an old one exists; issue #135 (credentials in cleartext in query plans) then has strictly less to fix rather than strictly more. #275 explicitly says the security path "lands IN this slice, never later". The asymmetry with `StorageProps` is deliberate and named in the spec rather than left to be discovered.
- **Promotes to ADR:** yes

### [6] The container collision is closed by a backend-agnostic whole-spec precondition, not by an Azure arm check

- **Decision:** `validate_sides_share_one_store(spec)` runs once in `build_session_context` before any registration. Per non-empty side it compares DataFusion's registry key (scheme + `host:port`) against the store URL the side needs (scheme + userinfo + `host:port`), and errors when two sides share the former and differ in the latter. It matches on no storage-backend variant.
- **Alternatives:** Check inside the Azure arm of `register_side_store` against the already-registered store (requires reading a container back out of an `Arc<dyn ObjectStore>` by string-matching its `Display` impl). Carry the other sides on `StoreRegistration`. Compare the two returned `Url`s at the call site.
- **Rationale:** The arm sees one side; the collision is a property of the pair, so the check has to sit where both sides are visible. Recovering the container from a registered store means string-matching a `Display` impl through the `SpecSizedObjectStore` wrapper — fragile and clever in the bad way. Comparing at the call site would make `build_session_context` name a container, which `vs-adapter/storage-backend-enum` forbids. Stating the invariant as a property of DataFusion's registry-key formula rather than of Azure keeps it true for any future backend whose store scope is finer than its key, and it can never fire for S3, whose URIs carry no userinfo.
- **Promotes to ADR:** yes

### [7] `validate_uniform_object_store_files` is NOT edited; the intra-side case was already covered

- **Decision:** Leave the function unchanged and pin its `abfss://` behaviour with a test.
- **Alternatives:** Extend its comparison key with the URI userinfo, as A1's phrasing implies.
- **Rationale:** The premise that it misses the container is wrong. `ListingTableUrl::object_store()` slices `Position::BeforeScheme..Position::BeforePath` (`datafusion-datasource-54.1.0/src/url.rs:323-327`), which INCLUDES userinfo — unlike `get_url_key`, which excludes it. The function already compares that per data file and per associated delete file, so a file list mixing containers within one side is already rejected. Only the across-side case was missing, and decision [6] closes it. Editing a correct function to add a comparison it already performs would be churn on the exact code slice B declared unedited.
- **Promotes to ADR:** no

### [8] `extract_bucket_from_files` is deleted and both arms derive from one `side_store_url`

- **Decision:** One private derivation returns the `scheme://userinfo@host:port` slice of the side's first reconstructed file URI as a `Url`. The S3 arm reads the host out of it as the bucket name; the Azure arm passes the whole URL to `MicrosoftAzureBuilder::with_url`.
- **Alternatives:** Add a parallel `extract_azure_target_from_files` beside the S3 one. Leave the unification to a later slice.
- **Rationale:** `vs-adapter/storage-backend-enum` records this unification as "deferred to the slice that adds the second backend rather than done here" — this slice. A parallel Azure derivation would make three independent owners of one notion. The chosen slice is exactly `ListingTableUrl::object_store()`'s, so the store key and the uniformity check agree by construction; and for every `s3://` input it yields a `Url` equal to today's `Url::parse(&format!("s3://{bucket}"))`, so every pinned S3 assertion on the returned value passes unedited.
- **Promotes to ADR:** no

### [9] `storage_block` stays total: no panic, no `Result`

- **Decision:** The Azure branch requires both an account name and a resolvable `AdlsCred`; when either is absent the function falls through to the S3 branch rather than panicking on an unreachable state.
- **Alternatives:** `unreachable!()` on the both-absent case, justified by `validate_creds` running first. Change the return type to `Result`.
- **Rationale:** `read_connection` always calls `validate_creds` before returning, so the fall-through is unreachable in production. But a panic inside a UDF is an abnormal VM exit, and CLAUDE.md records that one such exit makes the engine SIGKILL every sibling VM of the statement part — a cluster-wide failure from a defensive assertion. A deterministic fall-through costs nothing and cannot do that. Changing the signature to `Result` would push a new error path through the one caller for a state that cannot occur.
- **Promotes to ADR:** yes

### [10] `resolve_vended_storage`'s Adls arm is a documented passthrough, recorded as a tracked exception

- **Decision:** The arm returns the caller's backend unchanged, reads neither `storage_credentials` nor `config`, and is recorded in `vs-adapter/pushdown-planning-cloud-credentials` as a tracked exception citing #276. It is written as an explicit `Adls` arm, never a catch-all `_`.
- **Alternatives:** Reject Azure plus `use_vended_credentials` in `validate_creds` (option a). Do the location-scheme switch now (option c).
- **Rationale:** A4 chose option (b) directly. Rejecting would break an Azure CONNECTION carrying a `use_vended_credentials` flag left over from an S3 deployment, for no correctness gain — the static credentials it also supplies are valid. CLAUDE.md requires a deviation to be a named, issue-cited exception in the spec rather than a silent gap, so the clause names #276 inline. The explicit arm rather than `_` keeps a third backend a build failure here instead of a silent second deferral.
- **Promotes to ADR:** no

### [11] The `s3_max_connections` wire field is not renamed

- **Decision:** The Azure arm reads the existing `s3_max_connections` field through the existing `StoreRegistration::connection_budget` and `client_options_for` seam. The wire field keeps its name.
- **Alternatives:** Rename it to a backend-neutral `max_connections` in this slice.
- **Rationale:** The value is already consumed backend-agnostically — `StoreRegistration` calls it `connection_budget` and `client_options_for` sets `pool_max_idle_per_host`, which `object_store` exposes identically on both builders. Renaming the serialized field would churn every committed golden SQL fixture and every scan-spec JSON fixture for a cosmetic gain, and `datafusion-scan/scan-execution-spec-reconstitution` uses those goldens' unchanged remainder as its proof that nothing but the intended value moved. Named as a non-goal in plan.md so it is a deferral rather than an oversight.
- **Promotes to ADR:** no

### [12] `allow_http` and an Azure endpoint override are excluded, and not half-wired

- **Decision:** The Azure variant carries no HTTP-scheme or endpoint knob at all. `storage_block`'s `allow_http` parameter is consumed only by the S3 branch.
- **Alternatives:** Carry `allow_http` on the Azure variant and pass it to `with_allow_http` now, so an Azurite emulator works later with one more change.
- **Rationale:** A6 puts Azurite out of scope. Carrying a flag that no arm reads would be a field whose only effect is to look supported; carrying one that IS read without the matching endpoint override would produce a plain-HTTP store pointed at a public Azure endpoint. The `with_client_options` ordering hazard that governs a later `with_allow_http` is recorded in the spec's Background so the slice that adds it does not have to rediscover it.
- **Promotes to ADR:** no

### [13] `datafusion-scan/scan-execution-memory-and-credentials` gets no delta

- **Decision:** Three features change; the scan-execution credentials feature does not.
- **Alternatives:** Add a Background-only delta reflecting that the registered store may now be an ADLS store.
- **Rationale:** Its two credential-passthrough scenarios are explicitly scoped to vended S3 credentials and remain accurate. Its Background already assigns "how that store is derived and registered without the scan path naming a backend" to `vs-adapter/storage-backend-enum`, which is where this slice's store-derivation and container-collision clauses land. A delta that reworded one adjective would add a fourth file and a fourth merge without changing what any scenario requires. Recorded here so the omission is a decision rather than a gap.
- **Promotes to ADR:** no

### [14] Iceberg spec compliance: no deviation, and the scheme-agnostic path rules are pinned rather than changed

- **Decision:** `relativize_path_to_root` and `reconstruct_abs_uri` are left unedited and covered by an `abfss://` round-trip test.
- **Alternatives:** Add scheme-aware handling for `abfss://` userinfo in the relativize/reconstruct pair.
- **Rationale:** `apache/iceberg` `format/spec.md` defines data-file field `100 file_path` as "Full URI for the file with FS scheme" (required in v1, v2, and v3), and states the resolution rule as "If the path starts with a URI scheme, it is absolute and is used without modification. If the path does not start with a URI scheme, the resolved path is the table location followed by the relative path joined by the URI separator character `/`." Both are scheme-agnostic, and the existing pair implements exactly them — an `abfss://` table root and an `abfss://` file path differ only after the container, which the segment-boundary prefix rule already handles. The spec mandates no scheme-specific reader behaviour beyond that: it "only requires that file systems support the following operations: In-place write, Seekable reads, [and] Deletes". So there is no deviation to fix and none introduced, and the correct action is a test that pins the property rather than code that restates it.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] Six compile-forced S3 destructuring sites were unlisted

- **Finding:** `plan-reviewer` (round 1, `[EFFORT_MISESTIMATION]`) flagged that adding the second variant breaks the build at five sites beyond the two `extract_bucket_from_files` call sites the plan accounted for, that the plan named none of them, and that each needs a decision the plan had not made.
- **Verified:** Confirmed. All five cited `file:line` sites exist and destructure irrefutably: `adapter/connection.rs:485`, `scan/spec.rs:877` (`s3_props`), `scan/spec.rs:963`, `lakehouse-catalog/src/test_support.rs:64` (`s3_payload`), `tests/e2e_int96_timestamp_test.rs:136`. A sweep of every `StorageBackend::S3` occurrence in `crates/` found no sixth irrefutable site; the remaining hits are constructor calls, which stay valid, plus the two match arms that ARE the feature (`scan/object_store.rs:134`, `vended.rs:37`).
- **Direction change:** Added `plan.md` § Forced Edits — a five-row table naming each site, its shape today, and its resolution — plus task 1.4 owning them and a Group A.2 that serializes them after 1.1. One thing the finding's `Fix:` line did not name but its premortem did: all five are TEST code, so `vs-adapter/storage-backend-enum`'s characterization clause forbade the very edit the build requires. That clause now admits exactly two edit classes, and CLASS TWO is bounded to these five sites, panicking arms only, no assertion change, no `_` catch-all.
- **Promotes to ADR:** no

### [plan-review] The "neither special-cased nor broken" endpoint claim was false

- **Finding:** `plan-reviewer` (round 1, `[UNSTATED_ASSUMPTION]`) flagged that `storage-backend-enum/spec.md`'s verbatim-host clause asserted a non-`core.windows.net` Azure endpoint is unbroken, and that a test written from that clause would fail.
- **Verified:** Confirmed against the resolved dependency. `object_store-0.13.2/src/azure/builder.rs:660-682`: with a non-empty userinfo, `parse_url` splits the host once on `.` and matches exactly four suffixes — `dfs.core.windows.net`, `blob.core.windows.net`, `dfs.fabric.microsoft.com`, `blob.fabric.microsoft.com` — returning `Error::UrlNotRecognised` otherwise. One restriction the finding did not name also holds: the `validate` closure at `:655-658` rejects an account segment containing a further `.`.
- **Direction change:** Deleted the false claim. The clause now states the real reason for verbatim pass-through (reconstruction would discard the host the file URI names) and a second clause pins the restriction and its redacted `UrlNotRecognised` outcome. Added a Background bullet naming the four suffixes and the account-segment rule, a Non-Goal in `plan.md`, an Impact line, and the test `register_side_store_surfaces_an_unrecognised_azure_host_redacted`. Chose the finding's scenario-clause branch over its tracking-issue branch: this is an upstream capability boundary, not an Iceberg-spec deviation, so CLAUDE.md's issue-cite rule does not apply.
- **Promotes to ADR:** no

### [plan-review] `redact_credentials` is AWS-label-only, so Azure secrets leak on the manifest path

- **Finding:** `plan-reviewer` (round 1, `[NFR_IGNORED]`) flagged that three spec clauses forbid an Azure secret in any error, that `redact_credentials`' pattern list contains no Azure label, and that no task made the clauses true — leaving the `file_resolution.rs` sites, which redact by label alone, as the leak path.
- **Verified:** Confirmed on both halves. `crates/lakehouse-catalog/src/redaction.rs:30-48` lists only AWS/S3/SigV4 labels. All eight cited `file_resolution.rs` sites call `redact_credentials(&e.to_string())` with no value-based pass. Found one site the finding missed: `:520` (`failed to build Iceberg scan: {e}`) applies NO redaction at all. Also established a constraint the `Fix:` line assumed away — `effective_storage` is in scope at `:261` and `:277`, but `:433`-`:533` sit in `ensure_supported_delete_mechanisms` and `plan_files_from_table`, which take no backend and must gain a parameter.
- **Direction change:** Added task 1.5 (extend `redact_credentials` with the Azure labels plus `sig=`, the secret-bearing SAS parameter) and task 1.6 (route all nine sites through `redact_secret_values` first, threading a `secrets` parameter into the two helper functions, and give `:520` both layers). Added a spec clause stating that BOTH layers are required and why the object-store arm's redaction alone is insufficient, and two rows to § Scenario Coverage. Task 1.5 is a pure pattern-list edit independent of the variant, so it sits in Group A; 1.6 depends only on 1.5.
- **Promotes to ADR:** no

### [plan-review] The `s3a://` store-key change was unspecified — but it is a fix, not a regression

- **Finding:** `plan-reviewer` (round 1, `[COMPLETENESS_GAP]`) flagged that the equivalence clause was scoped to `s3://` only, that `s3a://` is documented and reachable, and that the key silently changes from `s3://bucket` to `s3a://bucket` on the path the plan promises is unchanged.
- **Verified:** The gap is real and the key change is real — but the finding's characterization of it as a silent regression is wrong, and tracing the lookup side disproves it. Registration used `format!("s3://{bucket}")` (`scan/object_store.rs:136`), rewriting the scheme. The lookup never did: `register_file_list` resolves the store via `ListingTableUrl::parse(&first_abs).object_store()` (`raw_scan.rs:194-196`), which preserves `s3a`, and `DefaultObjectStoreRegistry::get_store` is an exact `HashMap` hit on `scheme://host:port` with no fallback (`datafusion-execution-54.1.0/src/object_store.rs:255-274`). The two keys therefore disagree today, so an `s3a://` file list already fails with "No suitable object store found for s3a://…". `side_store_url` makes them agree.
- **Direction change:** Restated the clause over both schemes with the key each yields, added a clause recording the change as a deliberate latent-defect fix with the evidence, added a Background bullet, an `plan.md` § Impact paragraph, and the test `side_store_url_preserves_the_s3a_scheme_so_the_key_matches_the_lookup`. Did NOT declare a behavioral regression in § Impact as the `Fix:` line's conditional suggested, because no working `s3a://` scan exists to regress.
- **Promotes to ADR:** no

### [plan-review] The `ConnectionCreds` widening breaks seven unnamed struct-literal sites

- **Finding:** `plan-reviewer` (round 2, `[EFFORT_MISESTIMATION]`) flagged that round 1's blocker was treated as a fact about `StorageBackend` when it is a fact about every type this plan widens. Task 1.2 adds three `ConnectionCreds` fields, breaking seven struct literals the plan never names and no task owns — and falsifying § Verification's claim that `crates/lakehouse-engine/tests/` runs as a "(whole suite, unedited)" gate.
- **Verified:** Confirmed on every point. `ConnectionCreds` is `#[derive(Clone)]` only, has no `Default`, and is not `#[non_exhaustive]` (`crates/lakehouse-catalog/src/creds.rs:25-52`). A sweep of every `ConnectionCreds {` literal in `crates/` returns exactly eight; all eight name all fourteen fields explicitly and NONE uses `..` functional-update syntax, so all eight are E0063 errors after task 1.2. Seven are test or test-support code (`test_support.rs:14`, `test_support.rs:84`, `creds.rs:238`, `adapter/mod.rs:2838`, `adapter/pushdown/mod.rs:2354`, `tests/shared_type_reexports.rs:50`, `tests/common/e2e_harness.rs:318`); the eighth, `parse_creds` (`adapter/connection.rs:146`), is production code already owned by task 2.1. Two of the seven live under `crates/lakehouse-engine/tests/`, so the "unedited" claim was false as written.
- **Direction change:** § Forced Edits is now two tables under one preamble covering both widenings — table A the five `StorageBackend` sites (task 1.4), table B the seven `ConnectionCreds` sites with each site's resolution (new task 1.7). Task 1.7 joins 1.4 in Group A.2; the sequential-dependency line now states both are prerequisites for a clean build and that they touch disjoint sites. Task 1.4's "restores the build" claim is corrected to "together with 1.7". The § Scenario Coverage integration row now reads "edited only at the sites named in § Forced Edits" and names the three affected `tests/` files. Chose the finding's CLASS THREE branch over widening CLASS TWO to twelve sites: CLASS TWO's mechanism clause mandates a panicking arm, which does not describe a struct-literal completion, so merging the two would have produced a fresh contradiction. On the `creds.rs:238` question the `Fix:` line asked to settle — the test DOES gain assertions, because it is the test that owns the `Debug` contract task 1.2 extends and leaving the two new secret fields unasserted would ship an untested redaction; CLASS THREE admits that one addition explicitly and nothing else.
- **Promotes to ADR:** no

### [plan-review] Clause `:88` cancelled the two-class permission round 1 added

- **Finding:** `plan-reviewer` (round 2, `[REQUIREMENT_CONFLICT]`) flagged that "neither class SHALL touch a test that asserts S3 behavior itself" forbids exactly the edits CLASS ONE and CLASS TWO permit, voiding round-1 blocker 1's fix; and that the decode-test extension clause `:76` mandates an edit that fits no class, so `:85` declares it invalidates the gate.
- **Verified:** Confirmed, and it is the sharper of the two readings. CLASS ONE's sites sit in `extract_bucket_handles_relative_and_absolute_first_entry` (`crates/lakehouse-engine/src/scan/object_store.rs:766-780`), whose entire body asserts S3 bucket derivation (`"warehouse"`, `"legacy-bucket"`) — and the CLASS ONE clause itself REQUIRES the repointed test to assert the same bucket value, i.e. mandates the touch the next clause bans. CLASS TWO's `connection.rs:485` site is inside `storage_block_maps_creds_to_storage_props`, which asserts six fields of the S3 payload. The `:76` extension targets `only_the_lowercase_s3_variant_key_decodes` (`crates/lakehouse-catalog/src/storage.rs:239-250`), a rename plus a new case — neither a repointing nor a pattern completion.
- **Direction change:** The clause now bans the OUTCOME rather than the file touch: "NO class SHALL change, weaken, or delete an assertion about S3 behavior", plus a clause requiring every existing S3 assertion to stay byte-identical. Added CLASS FOUR admitting the decode-test extension, bounded to keeping every existing payload case, adding only rejection cases, and changing no existing assertion, with a rename permitted. The wire scenario's `untagged` clause now cites CLASS FOUR by name and CLASS FOUR cites the wire scenario by name, so neither can be edited without the other's contradiction becoming visible.
- **Promotes to ADR:** no
