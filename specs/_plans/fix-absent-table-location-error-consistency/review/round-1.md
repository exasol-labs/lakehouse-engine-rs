# Plan Review Findings: fix-absent-table-location-error-consistency (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 10 (Blockers: 4, Advisory: 6)
- Intent Fidelity blockers: 0

## Premortem

Three ways this plan fails after it ships:

1. **The issue's own case is not covered.** An operator hits a `loadTable` body that OMITS the
   `location` key. iceberg-rust 0.10 declares `location: String` (non-`Option`, no
   `serde(default)`) on all three metadata variants behind an `#[serde(untagged)]` enum, so the
   body fails deserialization inside `load_table_any_auth` and never reaches the hoisted guard.
   The operator sees `failed to parse catalog response: …` instead of the clear error issue #296
   demands. The plan's test sets `"location": ""` and cannot detect this.
2. **The wrong spec owner is cited.** The delta twice points readers at
   `vs-adapter/pushdown-planning-file-encoding` for the empty-root property. That feature never
   mentions an empty root; `datafusion-scan/scan-execution-file-metadata` owns it and keeps three
   `SHALL` clauses describing an empty root as a live case. The plan built to fix mis-owned text
   ships mis-owned text.
3. **The vended-only framing survives in the changed function.** `file_resolution.rs:243-248`
   ends "so an absent location is its own error on the vended branch below". No task touches it,
   and the plan's own sweep regexes do not match it. Decision [5] names this exact framing as the
   cause of the gap being fixed.

## Intent Fidelity

Certified on the two interview answers: the plan implements A1's accepted shape verbatim (guard
above the split, `relativize_path_to_root` untouched — plan.md:103, decision-log.md:9-21), and the
error reword is authorized by A1's own snippet, which shows a new `"...no table location..."`
string rather than the shipped one, so it is not scope creep.

#### [INTENT_DRIFT] ADVISORY
- Location: decision-log.md § Design Decisions [3]; plan.md § Implementation Tasks task 1
- Issue: A2 accepted "a synthetic loadTable response (no network, no DB)" and REJECTED "unit test
  + mock HTTP catalog". The plan builds a hand-rolled mock HTTP catalog: a real
  `tokio::net::TcpListener`, a real `reqwest` client, a real HTTP/1.1 parse (plan.md:96-102).
  Decision [3] enumerates three alternatives and omits the one that answers its own objection —
  split `resolve_file_list` at the load seam (`load_table_any_auth` → a private
  `resolve_from_loaded(result, storage, creds, allow_http, filter_json)`) and inject a synthetic
  `LoadTableResult`. That split contains the vended/static branch, so it DOES fail if the guard is
  moved back inside the vended arm — the exact regression decision [3] says a pure
  `require_table_location` helper cannot catch. Task-1 note 3's stated blocker ("naming the type in
  production code would pull the REST crate across the crate boundary") does not apply:
  `iceberg-catalog-rest` is already a `[dev-dependencies]` entry of `lakehouse-engine`, so
  `#[cfg(test)] mod tests` may name `LoadTableResult` with no production-code reference.
- Fix: In decision-log.md decision [3] § Alternatives, add the load-seam-split alternative
  (`load_table_any_auth` + a private `resolve_from_loaded(...)` taking the `LoadTableResult`), state
  why it loses to a loopback HTTP fake, and correct task-1 note 3 in plan.md:101 to stop citing the
  crate boundary — `iceberg-catalog-rest` is already a dev-dependency reachable from
  `#[cfg(test)] mod tests`.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Implementation Tasks tasks 1-2; `vs-adapter/pushdown-planning/spec.md`
  Background bullet 2 and the DELTA:NEW scenario
- Issue: The plan treats "absent `location`" and "empty `location`" as one case. They are two wire
  shapes with two different error paths, and only one reaches the guard. `iceberg-0.10.0`
  `src/spec/table_metadata.rs:810` (`TableMetadataV2V3Shared.location: String`) and `:855`
  (`TableMetadataV1.location: String`) declare the field non-`Option` with no `#[serde(default)]`,
  inside `#[serde(untagged)] enum TableMetadataEnum` (`:783-788`). A body that omits the key
  therefore fails deserialization in `authed_get_json`'s `response.json::<T>()`
  (`crates/lakehouse-catalog/src/iceberg_io.rs:89-94`) and surfaces
  `UdfError::User("failed to parse catalog response: <untagged-enum mismatch>")` — before
  `resolve_file_list` reads any location, naming neither the absent `location` nor the `warehouse`
  non-substitution. Only `"location": ""` reaches the hoisted guard, and task-1 note 4
  (plan.md:102) relies on exactly that shape. Issue #296's first acceptance criterion — "an absent
  `location` returns a clear `UdfError::User`" — is therefore unmet and untested, while the delta
  scenario's GIVEN says "EMPTY" and its THEN plus Background say "absent" / "carries no
  `location`".
- Fix: Add a plan.md § Requirements row naming both wire shapes and their owners — key present but
  empty → the hoisted guard in `resolve_file_list`; key omitted → the `load_table_any_auth`
  deserialization error, because `iceberg-0.10.0` declares `location: String` non-`Option` on all
  three metadata variants. Then either (a) extend task 1's test with a third call whose metadata
  JSON omits the `location` key, asserting a `UdfError::User` whose message names the table
  location, and add a task making that message name it; or (b) restate every "absent" in the
  `vs-adapter/pushdown-planning` delta as "empty" and add a Background bullet stating that an
  omitted key is rejected earlier, at deserialization, with the citation above.

#### [HIDDEN_DEPENDENCY] ADVISORY
- Location: plan.md § Implementation Tasks task 1, harness notes
- Issue: `tokio::net::TcpListener` and `tokio::io::{AsyncReadExt, AsyncWriteExt}` need tokio's
  `net` and `io-util` features. `lakehouse-engine` declares neither: workspace tokio is
  `features = ["rt", "macros"]` (Cargo.toml:65), `[dependencies]` adds `rt-multi-thread` and
  `sync`, `[dev-dependencies]` adds `macros` and `time`. Today `net` arrives only through Cargo
  feature unification via `reqwest`/`hyper-util` — the precise "silently depend on another crate's
  feature choice" that the same `[dev-dependencies]` block rejects in its
  `iceberg-storage-opendal` comment.
- Fix: Add to plan.md task 1's harness notes: extend `crates/lakehouse-engine/Cargo.toml`
  `[dev-dependencies] tokio` features with `"net"` and `"io-util"`, so the test does not rely on
  unification through `reqwest`.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `specs/_plans/fix-absent-table-location-error-consistency/vs-adapter/pushdown-planning/spec.md`
  Background final bullet and the DELTA:NEW scenario's last clause; plan.md § Dead Code Removal
- Issue: Both places cite `vs-adapter/pushdown-planning-file-encoding` as the owner of the
  "empty root ⇒ every path stays absolute" property. That feature's spec contains no empty-root
  text at all — its three scenarios cover only "root is an actual prefix" and "path not under the
  root". The property is owned by `specs/datafusion-scan/scan-execution-file-metadata/spec.md:19`,
  `:43`, and `:66` ("when the common spec carries an empty table root, the UDF SHALL treat every
  entry as absolute and join none of them"), which the plan never mentions — so three normative
  clauses describing an empty table root as a live case go unaddressed by a plan that makes that
  state unreachable from a `loadTable` response. plan.md § Dead Code Removal compounds it: it cites
  "rejoined by the scan UDF at `pushdown/mod.rs:246-250`", but that site is the VS-side
  `relativize_shards_to_root` call (stripping, not rejoining); the scan-side rejoin is
  `reconstruct_abs_uri` at `crates/lakehouse-engine/src/scan/object_store.rs:250`.
- Fix: In the `vs-adapter/pushdown-planning` delta, repoint both empty-root references from
  `vs-adapter/pushdown-planning-file-encoding` to `datafusion-scan/scan-execution-file-metadata`,
  and add a `datafusion-scan/scan-execution-file-metadata` DELTA:CHANGED Background bullet
  recording that the VS can no longer emit an empty table root, so its empty-root clauses are
  retained as a wire-format totality property rather than a reachable path. In plan.md § Dead Code
  Removal, replace `pushdown/mod.rs:246-250` with
  `crates/lakehouse-engine/src/scan/object_store.rs:250` (`reconstruct_abs_uri`).

#### [AMBIGUOUS_REQUIREMENT] BLOCKER
- Location: plan.md § Implementation Tasks task 5; plan.md § Verification / Manual Testing row 2
- Issue: Task 5's stated pass criterion is false, so the plan's final gate cannot pass as written.
  `grep -rn "S3 URI of the Iceberg warehouse" . | grep -v '/target/'` run against the current tree
  returns `crates/lakehouse-engine/tests/cloud_e2e_test.rs:10`, `:794`, AND
  `specs/_plans/fix-absent-table-location-error-consistency/plan.md:104` and `:106`. Task 3 removes
  only the first two, so "The second MUST return nothing after task 3" and the Manual Testing row's
  "no output" are both unachievable. The first sweep has the same defect: its enumerated expected
  set (two cloud-credentials bullets, `docs/catalogs.md:156`, `session.rs:279`) omits the four
  hits the plan's own artifacts produce — plan.md:75, :78, :105, :106 and the
  `pushdown-planning-cloud-credentials` delta's Background bullet 1.
- Fix: In plan.md task 5 and the § Manual Testing row, add `--glob '!specs/_plans/**'` (or a
  `grep -v 'specs/_plans/'` stage) to both sweeps, and restate the expected hit set for the first
  sweep as exactly: `specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md:66` and `:153`,
  `docs/catalogs.md:156`, `crates/lakehouse-catalog/src/session.rs:279`.

#### [COMPLETENESS_GAP] ADVISORY
- Location: `vs-adapter/pushdown-planning/spec.md` Background bullet 2 vs the DELTA:NEW scenario
- Issue: The Background correctly quotes `format/spec.md` making `location` `_optional_` in v4 and
  requiring the location to be supplied out of band, then the scenario states the rejection
  unconditionally with no format-version bound. Read as written, the delta requires rejecting a
  spec-conformant v4 table. The reason it is harmless today is unstated: `iceberg-0.10.0`
  deserializes only `V3 | V2 | V1` (`src/spec/table_metadata.rs:783-788`), so no v4 metadata can
  reach the guard.
- Fix: In the `vs-adapter/pushdown-planning` delta, scope the scenario's GIVEN to v1/v2/v3 table
  metadata and add a Background sentence stating that `iceberg-0.10.0` accepts only format versions
  1-3 (`src/spec/table_metadata.rs:783-788`), so a v4 response cannot reach the guard, and that a
  v4-capable iceberg-rust upgrade must revisit this rule.

## Task Breakdown

#### [TRACEABILITY_GAP] BLOCKER
- Location: plan.md § Implementation Tasks task 2 and § Dead Code Removal;
  `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs:243-248`
- Issue: The delta makes `vs-adapter/pushdown-planning` the single owner of a path-independent
  rule, but no task removes the in-code text asserting the opposite. The doc comment immediately
  above the edited lines ends: "the REST `warehouse` is a routing identifier — so an absent
  location is its own error **on the vended branch below**". After task 2 that clause is false, and
  it restates precisely the vended-only framing decision [5] blames for the original gap surviving
  commit `6d08c8a`. Neither task-5 regex matches it, so it ships. § Dead Code Removal applies the
  right reasoning ("the old wording must not survive alongside it") to the error string alone.
- Fix: Extend plan.md task 2 to rewrite the `resolve_file_list` doc comment at
  `file_resolution.rs:243-248` so it states the guard runs above the vended/static split on every
  path, and add a § Dead Code Removal row for the clause "so an absent location is its own error on
  the vended branch below" beside the existing error-string row.

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md § Verification / Scenario Coverage; `vs-adapter/pushdown-planning/spec.md`
  DELTA:NEW scenario clauses 6 and 8
- Issue: Two normative clauses have no verification row. "that error MUST NOT contain any
  credential value" and "every join side SHALL inherit this rejection through the same
  file-resolution call" are both `MUST`/`SHALL`, and § Scenario Coverage maps one unit test that
  varies only `use_vended_credentials`. The join claim is structurally sound — every side runs
  through `joins/mod.rs:139` → `resolve_one_join_side` → `resolve_file_list`, verified — but the
  plan states it nowhere as the evidence for the clause.
- Fix: Add a § Verification note stating the evidence for each clause without a test: the
  no-credential-value clause holds because the message is a static literal carrying no interpolated
  value, and the join clause holds because `joins/mod.rs:139` resolves every side through
  `resolve_one_join_side`, whose sole action is delegation to `resolve_file_list`
  (`joins/planning.rs:407`).

## Design Depth

Certified: the change introduces no module, interface, or boundary, so the Quick Diagnostic table
is legitimately skipped. Guard placement verified as the narrowest single seam —
`resolve_file_list` has exactly two production callers (`pushdown/mod.rs:221`,
`joins/planning.rs:407`), and `resolve_table_schema` (`adapter/mod.rs:339`) is the only
`createVirtualSchema` entry, so decision [2]'s blast-radius argument against siting the guard in
`load_table_any_auth` holds. Decision [6]'s no-ADR-churn call is consistent with the audit.

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: `vs-adapter/pushdown-planning/spec.md` Background bullet 2; recorded
  `specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md:67`
- Issue: The new Iceberg-spec-grounding bullet is a second near-verbatim copy of one that already
  exists. The recorded cloud-credentials bullet already quotes the same Table Metadata field table
  (`_required_` in v1/v2/v3), the same v4 `_optional_` / "Must be an absolute path when present",
  and the same Table Location Specification passage. Decision [5] rejects stating a rule in two
  features because "two copies of one normative clause drift" — the plan then creates exactly that
  for the citation, and its cloud-credentials delta supersedes only the absent-location bullet, not
  the grounding one. (The quote itself is accurate: verified against `format/spec.md` lines 1146,
  1174, and 227 of the fetched file.)
- Fix: In the `vs-adapter/pushdown-planning` delta, either replace the quoted grounding bullet with
  a citation to `vs-adapter/pushdown-planning-cloud-credentials`' existing grounding bullet, or add
  a `SUPERSEDES` line to the cloud-credentials delta moving that bullet's quote to the new owner —
  so one feature holds the `format/spec.md` citation.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Summary; plan.md § Design / Decision; plan.md § Requirements row 3 vs
  decision-log.md decision [7]
- Issue: Three guardrail misses. The Summary's first sentence runs 45 words against the 25-word
  cap. § Design / Decision opens with review-process meta-commentary ("Per
  `/speq:design-philosophy`, the Quick Diagnostic table applies to a change that introduces a new
  module … so the table is deliberately skipped rather than silently passed") instead of design
  content. § Requirements row 3 and decision [7] state the same reproduction-gate exemption
  near-verbatim, including both "no supported catalog can be configured to omit" sentences.
- Fix: Split plan.md § Summary's first sentence into two, each under 25 words. Delete the
  Quick-Diagnostic meta-sentence from § Design / Decision, keeping only "Add no module, no
  interface, and no boundary." Reduce § Requirements row 3 to "Reproduction gate: exempted — see
  decision-log.md [7]" and keep the reasoning only in decision [7].
