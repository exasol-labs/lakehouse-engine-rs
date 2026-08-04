# Decision Log: fix-absent-table-location-error-consistency

Tracking: GitHub issue #296 (pre-existing — the implementing commit carries `Closes #296`).

## Interview

**Q:** In `file_resolution.rs` the absent-location error currently lives INSIDE the `if creds.use_vended_credentials` branch. On the non-vended path an absent location is still tolerated silently (it just yields an empty `table_root`). How strict should the non-vended path be?

**A:** Hoist: error unconditionally. Move the absent-location check above the vended/non-vended split so every path hard-errors. Matches the issue's "consistent across vended, non-vended, join, createVirtualSchema" criterion and the Iceberg spec (location is required in v1/v2/v3). `relativize_path_to_root`'s defensive empty-root handling is left untouched. Accepted shape:

```rust
let table_location = result.metadata.location();
if table_location.is_empty() {
    return Err(UdfError::User("...no table location...".into()));
}
let effective_storage = if creds.use_vended_credentials {
    resolve_vended_storage(&result, table_location, allow_http)?
} else {
    storage.clone()
};
```

**Q:** There is currently NO automated test asserting the absent-location error — the only coverage is a live-credential assertion in `cloud_e2e_test.rs` that runs solely when cloud env vars are set. What coverage should the plan require?

**A:** Host unit test, both paths. A host `cargo test` unit test that drives the absent-location branch with a synthetic loadTable response (no network, no DB), covering both vended and non-vended. Runs on every `cargo test`, so the guarantee cannot silently regress. Accepted shape:

```rust
#[tokio::test]
async fn absent_table_location_errors_on_both_paths() {
    // synthetic loadTable response, location = ""
    // assert UdfError::User for use_vended_credentials = true
    // assert UdfError::User for use_vended_credentials = false
}
```

## Design Decisions

### [1] An absent table location is a hard error on every path, and the REST `warehouse` is never a storage anchor

- **Decision:** Reject a `loadTable` response carrying an empty table metadata `location` with a `UdfError::User`, from one check that runs before the vended/static storage split. No CONNECTION-derived value — `warehouse`, `endpoint`, or any other — may be substituted for it, with or without vended credentials.
- **Alternatives:** Keep the check vended-only (the shipped state after commit `6d08c8a`), leaving the non-vended path to resolve an empty `table_root` silently. Rejected: the Apache Iceberg table spec marks `location` `_required_` in the v1, v2, and v3 columns of `format/spec.md`'s Table Metadata field table, so an absent location is a malformed response independently of how credentials are obtained. Warning-and-continue was not considered: an empty root silently changes the wire encoding of every file path.
- **Rationale:** The `warehouse` and a table `location` live in different namespaces. `warehouse` builds the `loadTable` URL prefix only — a bare AWS account id under Glue, a warehouse name or per-warehouse UUID under Lakekeeper — and denotes no object store. Treating it as an anchor is the misconception this plan exists to purge; the non-SigV4 no-override prefix fallback is already the empty string rather than the warehouse, for the same reason.
- **Promotes to ADR:** yes

### [2] The guard sits at the resolve-once seam, not at the catalog-load seam

- **Decision:** Site the check in `resolve_file_list` (`crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs`), immediately after reading `result.metadata.location()`.
- **Alternatives:** Site it in `load_table_any_auth` (`crates/lakehouse-catalog/src/session.rs`), where every `loadTable` response enters the system. That is the more appealing deep-module answer — the catalog crate would own "what a well-formed `loadTable` response is", the rule would be path-independent by construction, and it would be testable with the loopback-fake pattern already present in that crate's own unit tests. Rejected on blast radius: `resolve_table_schema` also calls `load_table_any_auth` and reads only `current_schema()`, so the guard would fail an entire `createVirtualSchema` over a field that path never uses. The interview (A1) also fixed this placement.
- **Rationale:** The resolve-once seam is the narrowest site through which every path that actually depends on a table location passes exactly once. `resolve_one_join_side` overrides only `table` and delegates here, so every join side is covered without a second check, and the `createVirtualSchema` path stays untouched.
- **Promotes to ADR:** yes

### [3] No helper is extracted to make the guard unit-testable; the test drives the real entry point over a loopback catalog fake

- **Decision:** Add no function. Test `resolve_file_list` itself from `file_resolution.rs`'s own `mod tests`, serving a synthetic `loadTable` JSON body from a `tokio::net::TcpListener` bound on `127.0.0.1:0` — the pattern `crates/lakehouse-catalog/src/session.rs` tests already use — with `use_sigv4 = true` so each call issues exactly one HTTP request.
- **Alternatives:** (a) Extract a pure `require_table_location(&str) -> Result<&str, UdfError>`. Rejected twice over: a one-line `is_empty` guard behind its own function is classitis, and the test could not cover "both paths" — after the hoist there is exactly one path through the guard, so a passing helper test would keep passing if the guard were moved back inside the vended arm. (b) Extract `resolve_effective_storage(result, static_storage, use_vended, allow_http)` into `lakehouse-catalog`. Rejected: it reintroduces precisely the storage-backend parameter and the do-the-work/return-the-input boolean that `vs-adapter/pushdown-planning-cloud-credentials` forbids on the vended entry point, defeating the signature-level guarantee that no CONNECTION storage value can reach vended resolution. (c) Add an HTTP mock crate (`wiremock`/`httpmock`). Rejected: a new dev-dependency for what the repo already does in 20 lines.
- **Rationale:** Testing the real production function is the only way the test proves what the interview asked for — that both values of `use_vended_credentials` reach the same error. Serving JSON over HTTP also avoids naming `iceberg_catalog_rest::LoadTableResult` in engine production code, which would pull the REST crate across the boundary the `lakehouse-catalog` split maintains.
- **Promotes to ADR:** yes

### [4] The error text becomes path-independent

- **Decision:** Reword the message so it names the absent table `location` and the `warehouse` as a non-substitute, and drop "so the storage backend cannot be resolved".
- **Alternatives:** Move the existing string unchanged. Rejected: on the non-vended path the static storage backend resolves normally, so the shipped wording would misdirect an operator to their credentials when the actual fault is a malformed catalog response.
- **Rationale:** A hoisted check needs a message true on every path it now guards.
- **Promotes to ADR:** no

### [5] One spec owner for the rule: `vs-adapter/pushdown-planning`

- **Decision:** `vs-adapter/pushdown-planning` gains the absent-location scenario and the owning Background bullets. `vs-adapter/pushdown-planning-cloud-credentials` replaces its absent-location clause with a reference and records that the rule moved.
- **Alternatives:** Leave the rule where it is; or state it in both features. Rejected: a rule that holds with vending disabled cannot be owned by a vended feature — that mis-framing is what let the non-vended gap survive commit `6d08c8a`. Two copies of one normative clause drift.
- **Rationale:** One normative clause, one home, cross-references elsewhere.
- **Promotes to ADR:** no

### [6] The audit is recorded as a finding, not as ADR churn

- **Decision:** Record the repo-wide audit result in `plan.md` § Audit Findings and change no ADR.
- **Alternatives:** Amend ADRs 053, 006, or 001 for clarity. Rejected: all three are already accurate — ADR 053 has zero `warehouse` occurrences, ADR 006 treats it as a bare account id / routing prefix, ADR 001 L1218 already says the fallback is an EMPTY prefix "rather than the warehouse". Editing correct records to look responsive is churn.
- **Rationale:** No ADR ratifies the misconception, so nothing needs correcting; naming the surfaces as audited-clean stops the next reader re-auditing them.
- **Promotes to ADR:** no

### [7] The reproduction gate is the host unit test, not a Docker Exasol repro

- **Decision:** Satisfy CLAUDE.md's reproduce-before-fix rule with the host unit test's synthetic malformed response; use `make test-e2e` as the no-regression gate only.
- **Alternatives:** Reproduce against the Docker Exasol container per the standing rule. Not possible: no supported Iceberg catalog can be configured to omit a required `location`, so the branch is unreachable from any live stack.
- **Rationale:** Naming the exemption and its reason is required — a silently skipped verification gate is what the rule exists to prevent.
- **Promotes to ADR:** no

### [8] The guard owns the EMPTY-location wire shape; an omitted `location` key stays owned by deserialization

- **Decision:** Scope the hoisted guard to `"location": ""` (key present, value empty) and document — rather than re-route — the omitted-key shape, which `iceberg-0.10.0` already rejects at deserialization. Record both shapes and their owners in `plan.md` § Requirements and in the `vs-adapter/pushdown-planning` delta Background. Add no production code on the omitted-key path.
- **Alternatives:** Extend the plan to make the omitted-key message name the `location` field (`plan-reviewer` round 1, option (a)). Rejected on three grounds. First, it needs the raw response body inspected in `load_table_any_auth`, the only site that sees it — and that function also serves `resolve_table_schema`, so the change lands on the `createVirtualSchema` path this plan lists as an explicit Non-Goal and that decision [2] declined to touch. Second, the body carries vended `storage-credentials`, so a message quoting it would violate the delta's "MUST NOT contain any credential value" clause; a safe version needs a presence check rather than an echo, which is a second guard at the seam decision [2] rejected. Third — decisive — issue #296 does not ask for the omitted-key wire shape, which it never raises; its acceptance criteria target the `warehouse` substitution. The issue defines "absent" operationally as the `table_s3_location.is_empty()` condition its own quoted offending code branches on.
- **Rationale:** Both wire shapes already return a `UdfError::User`, neither can substitute the `warehouse`, and neither panics — so every acceptance criterion of #296 is met either way. What separates them is message specificity on a shape no supported catalog emits. Naming that difference in the spec costs nothing and keeps the plan's blast radius at one `if`. Widening the plan to improve a diagnostic would buy a worse boundary. Precision here also removes the real defect the review found. The delta previously said "absent" where the mechanism tests "empty", which read as a claim the guard covers a shape it never sees.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] The plan conflated an absent `location` key with an empty `location` value

- **Finding:** `plan-reviewer` (round 1, `[UNSTATED_ASSUMPTION]`, BLOCKER) found the plan treating "absent `location`" and "empty `location`" as one case. They are two wire shapes with two error paths. The EMPTY shape reaches the hoisted guard. The OMITTED-key shape fails deserialization in `authed_get_json` first, because `iceberg-0.10.0` declares `location: String` non-`Option` with no `#[serde(default)]` on all three metadata variants. Sources: `src/spec/table_metadata.rs:810` and `:855` for those variants, `:783-788` for the `#[serde(untagged)] enum TableMetadataEnum` they deserialize through. The delta's Background said "absent" while its scenario GIVEN said "EMPTY".
- **Direction change:** Took the reviewer's option (b). Added a `plan.md` § Requirements row ("Two wire shapes, two owners") naming both shapes and their owners with the source citations. Added a `vs-adapter/pushdown-planning` delta Background bullet stating that only the empty shape reaches the guard and why the omitted-key shape is rejected earlier. Added a closing scenario clause requiring the omitted-key shape to be rejected as a `UdfError::User` without substituting the `warehouse`, and naming the diagnostic-specificity difference rather than leaving it unstated. Task 1's harness notes now require the key-present-but-empty body and explain that an omitted key would assert the wrong error and pass before task 2, voiding the failing-test-first gate. Verified the deserialization claim at source, including the `#[serde(try_from = "TableMetadataEnum")]` route at `:63-64`. Choice justified in decision [8].
- **Promotes to ADR:** no

### [plan-review] The empty-root property was attributed to the wrong spec owner

- **Finding:** `plan-reviewer` (round 1, `[REQUIREMENT_CONFLICT]`, BLOCKER) found the delta citing `vs-adapter/pushdown-planning-file-encoding` twice as owner of "empty root ⇒ every path stays absolute". That feature contains no empty-root text; `datafusion-scan/scan-execution-file-metadata` owns the property in three `SHALL` clauses (`spec.md:19`, `:43`, `:66`), and the plan never mentioned it — so three normative clauses describing an empty root as a live case went unaddressed. § Dead Code Removal also cited `pushdown/mod.rs:246-250` as the rejoin site, which is the VS-side `relativize_shards_to_root` call that strips the root.
- **Direction change:** Repointed both delta references to `datafusion-scan/scan-execution-file-metadata` and added a new delta for that feature recording that the adapter can no longer emit an empty table root, so its empty-root clauses are retained as a wire-format totality property rather than a reachable path. That delta reproduces the two owning scenarios byte-identically (verified by diff against the recorded spec) and changes no scenario wording. Corrected § Dead Code Removal to `reconstruct_abs_uri` (`crates/lakehouse-engine/src/scan/object_store.rs:250`) and noted what `pushdown/mod.rs:250` actually does. Added the feature to § Features, § Scenario Coverage, and § Manual Testing.
- **Promotes to ADR:** no

### [plan-review] Both verification sweeps could never pass, because the plan's own artifacts match them

- **Finding:** `plan-reviewer` (round 1, `[AMBIGUOUS_REQUIREMENT]`, BLOCKER) found task 5's and § Manual Testing's pass criteria unachievable. The plan quotes the offending strings in order to describe them, so `specs/_plans/**` matches every sweep regex: the second sweep's "MUST return nothing" was false while `plan.md` itself carried two hits, and the first sweep's expected hit set omitted the four its own artifacts produce.
- **Direction change:** Added a `grep -vE '/target/|/\.git/|specs/_plans/'` stage to both sweeps in task 5 and § Manual Testing, with the reason stated inline. Restated the first sweep's expected set as exactly four hits — `specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md:66` and `:153`, `docs/catalogs.md:156`, `crates/lakehouse-catalog/src/session.rs:279` — each annotated with why it is correct text. Ran both sweeps against the current tree to confirm the filtered output matches that set exactly. Also added `.git/` to the exclusion, which the original omitted.
- **Promotes to ADR:** no

### [plan-review] No task removed the in-code comment asserting the vended-only framing

- **Finding:** `plan-reviewer` (round 1, `[TRACEABILITY_GAP]`, BLOCKER) found that no task rewrites the `resolve_file_list` doc comment, whose final clause reads "so an absent location is its own error on the vended branch below" (`file_resolution.rs:247`). After task 2 that clause is false, and it restates the exact vended-only framing decision [5] blames for the original gap surviving commit `6d08c8a`. No task-5 regex matches it, so it would ship.
- **Direction change:** Extended task 2 to rewrite the comment at `file_resolution.rs:243-247` so it states the guard runs above the vended/static split on every path, while explicitly keeping the comment's correct substance (the anchor is the table's own location; `storage_credentials[*].prefix` is matched against it; neither the catalog REST URI nor the `warehouse` can stand in). Added a § Dead Code Removal row for the false clause beside the existing error-string row.
- **Promotes to ADR:** no

### [plan-review] The test harness relied on tokio features the crate never declares

- **Finding:** `plan-reviewer` (round 1, `[HIDDEN_DEPENDENCY]`, ADVISORY — applied by orchestrator decision as a latent build break) found that task 1's harness needs tokio's `net` and `io-util` features, which `lakehouse-engine` declares nowhere: workspace tokio is `["rt", "macros"]` (`Cargo.toml:65`) and `[dev-dependencies]` adds `["rt-multi-thread", "macros", "time"]` (`crates/lakehouse-engine/Cargo.toml:71`). Both features arrive today only through Cargo feature unification via `reqwest`/`hyper-util`, so the test would compile by accident.
- **Direction change:** Added a task-1 harness note requiring `"net"` and `"io-util"` on the `[dev-dependencies] tokio` entry, citing the current feature lists and the `iceberg-storage-opendal` comment in the same block that already rejects depending on another crate's feature choice.
- **Promotes to ADR:** no

### [plan-review] The `scan-execution-file-metadata` delta reproduced six recorded Background bullets

- **Finding:** `plan-reviewer` (round 2, BLOCKER) found the `datafusion-scan/scan-execution-file-metadata` delta reproducing all six recorded Background bullets verbatim beside its one genuinely new bullet. This repo's merge convention APPENDS delta Background bullets without deduplication. A recorded bullet is removed only when a delta bullet explicitly says `SUPERSEDES` and quotes it. Evidence: `specs/vs-adapter/pushdown-planning-like-type-coercion/spec.md` accumulated its Background across successive plans, while that feature's deltas in `specs/_recorded/2026-07-30-fix-declined-filter-self-apply/` and `specs/_recorded/2026-07-31-fix-join-filter-type-rewrites/` carry only 5 and 2 NEW bullets. Recording this plan as written would therefore write six normative rules into the library file twice, and the next plan editing one copy would leave the other stale.
- **Direction change:** Deleted Background bullets 2 through 7 of the delta — every bullet reproduced from `specs/datafusion-scan/scan-execution-file-metadata/spec.md:10-25` — keeping only the new empty-table-root-retention bullet. That bullet now cites the retained recorded rule by its opening phrase ("When the common spec carries an empty table root") instead of reproducing its text. The feature-description paragraph and the two `DELTA:CHANGED` scenarios are unchanged: scenario markers merge by name, so their byte-identical reproduction is intended. Reconciled `plan.md` § Features, which claimed the delta "adds one Background bullet" while the file added seven.
- **Promotes to ADR:** no

### [plan-review] Three advisories: a miscounted clause label, two misaddressed citations, four over-cap sentences

- **Finding:** `plan-reviewer` (round 2) raised three ADVISORY defects. First, four artifacts instructed the reader to retain "three empty-table-root `SHALL` clauses", but only two of the three named items carry an RFC-2119 keyword. The third is the descriptive Background bullet at `specs/datafusion-scan/scan-execution-file-metadata/spec.md:19-20`, so a reader counting `SHALL` clauses finds two and cannot tell whether the third was lost. Second, task 1's harness notes carried two wrong addresses, which mislead the implementer of an `[expert]` task whose harness notes are their only guidance: `session.rs:395-457` for the SigV4 short-circuit, which is actually the test `sigv4_resolve_prefix_derives_catalogs_segment`, and "line 822" for `file_resolution.rs`'s `mod tests`. Both underlying claims were true; only the addresses were wrong. Third, `decision-log.md` carried four sentences over the 25-word cap in governed prose — in decision [8]'s § Alternatives and § Rationale, and in the first § Review Findings entry's Finding and Direction-change paragraphs.
- **Direction change:** Replaced the label with "three empty-table-root clauses — two normative `SHALL` clauses and one descriptive Background bullet" at all five sites carrying it: `plan.md` § Features and § Dead Code Removal, `vs-adapter/pushdown-planning/spec.md`'s empty-root Background bullet and its scenario's retention clause, and the `datafusion-scan/scan-execution-file-metadata` delta's surviving bullet. `vs-adapter/pushdown-planning-cloud-credentials/spec.md` does not carry the phrase. Repointed the SigV4 short-circuit citation to `session.rs:148-151`, quoting the `if let CatalogAuth::Sigv4 = auth` block and naming the paired test at `session.rs:407` as its proof; § Patterns' separate citation of that test as a loopback-fake precedent is correct and unchanged. Widened the `mod tests` citation to `file_resolution.rs:822-823`, because `#[cfg(test)]` sits at `:822` and `mod tests {` at `:823`, so the two-line construct the plan names spans both lines. Split the four over-cap sentences and changed no claim.
- **Promotes to ADR:** no
