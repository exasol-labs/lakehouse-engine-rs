# Decision Log: change-unity-listing-delta-base-filter

## Interview

**Q1:** Where should the Delta/base-table filter live, given the recorded invariant that the shared listing pipeline is "structurally incapable of branching on catalog kind"?
**A1:** Inside the Unity client. `UnityCatalogSession::list_tables` deserializes `data_source_format` and excludes non-Delta / non-base entries before returning neutral tables. `data_source_format` stays Unity-wire-private. The shared listing pipeline (`build_listing_virtual_tables`) stays kind-agnostic and untouched. The Iceberg path is unaffected.

**Q2:** Should excluded tables be surfaced or silently omitted? (The only warning channel at createVirtualSchema time is `udf_log!(ctx, warn, …)` → the UDF script-output log stream, keyed off the skipped set; it is not SQL-client-visible.)
**A2:** Warn line per excluded table. The Unity client puts each excluded identifier into `CatalogListing.skipped`; the handler logs one `warn` line per skip. The message must use Unity-appropriate wording (for example "not a Delta base table", ideally naming the reason: `table_type=VIEW` or `data_source_format=CSV`), not the current Iceberg wording.

## Design Decisions

### [1] Delta-base filter lives inside the Unity Catalog client

- **Decision:** `UnityCatalogSession::list_tables` deserializes `data_source_format` and admits an entry as a neutral table iff its `table_type` is `MANAGED`/`EXTERNAL` (a neutral `Table`) AND its `data_source_format` is `DELTA`; every other entry is routed into `CatalogListing.skipped`. `data_source_format` is a crate-private wire field on `TableInfo` and never enters a neutral type. `build_listing_virtual_tables` stays kind-agnostic and untouched.
- **Alternatives:** Filter in the shared listing pipeline (would force a `CatalogKind` branch there, breaking the kind-agnostic invariant); expose `data_source_format` on the neutral `CatalogTable` (would leak a Unity wire concept into the kind-free neutral type and into the Iceberg path).
- **Rationale:** The Delta/base decision needs a Unity-specific wire field; making it inside the client keeps the one shared decision (how a listed entry becomes a virtual table) owned by exactly one place per kind and keeps the neutral type and the pipeline kind-free. Matches interview A1.
- **Promotes to ADR:** yes

### [2] Carry the skip reason as neutral data; the adapter renders it per reason, not per catalog kind

- **Decision:** Change `CatalogListing.skipped` from `Vec<CatalogTableIdent>` to `Vec<SkippedTable>`, where `SkippedTable { ident, reason: SkipReason }` and `SkipReason` is `NotLoadableIcebergTable | NotDeltaBaseTable { detail: String }`. The client that decides to skip sets the reason. The adapter's existing warn loop matches `reason` (neutral data) to render one `warn` line per entry: `NotLoadableIcebergTable` reproduces the legacy Iceberg line byte-for-byte; `NotDeltaBaseTable { detail }` renders a Unity line naming the excluded identifier and the disqualifying `table_type=…` or `data_source_format=…`.
- **Alternatives:** (a) Generalize the shared warn message to be kind-neutral — rejected: it would lose the specific per-entry reason AND change the Iceberg warning text, violating the byte-identical guarantee in `vs-adapter/catalog-kind-selection`. (c) Keep `skipped: Vec<CatalogTableIdent>` and branch the warn loop on the resolved `CatalogKind` — rejected: it reintroduces a second `CatalogKind`-matching site (the recorded invariant allows only the construction site), and it re-derives client knowledge ("Unity skips ⇒ not a Delta base table") in the adapter, a back-door leak; and the bare identifier list cannot carry the per-entry reason interview A2 asks for.
- **Rationale:** Only this option satisfies all three hard constraints at once: the Iceberg skipped-table warning stays byte-identical, the Unity warning names the specific reason, and no new `CatalogKind` branch is added. Matching `SkipReason` is matching neutral data carried on the entry — not matching `CatalogKind` — so the single-kind-match invariant holds. This is the skipped-warn-message resolution the planning brief asked to record as an ADR.
- **Message wording is intentionally co-owned, not adapter-owned.** `NotDeltaBaseTable { detail }` carries a pre-formatted disqualifier fragment (`table_type=<raw>` or `data_source_format=<raw>`) that the client authors; the adapter owns the log channel and the surrounding sentence. This is a deliberate trade-off, not leakage. A fully-structured discriminator (e.g. `field: DisqualifyingField, value: String`) would move all wording adapter-side but at a cost the two competing recorded invariants reject: it either adds a third type to the deliberately-minimal `lakehouse-catalog` public surface (`vs-adapter/catalog-crate-structure`), or it names `data_source_format` as a matchable field on a public neutral type, which decision [1] and `vs-adapter/unity-catalog-client` forbid (`data_source_format` stays a crate-private wire concept). The opaque `detail` string keeps `data_source_format` off the neutral surface while the adapter still owns the sentence structure. The single-owner claim therefore applies to the skip DECISION (client-owned); message wording is co-owned by design.
- **Promotes to ADR:** yes

### [3] Spec-impact analysis: no delta for the behavioral Iceberg features; a delta IS required for catalog-crate-structure

- **Decision:** Considered set — `vs-adapter/create-virtual-schema`, `vs-adapter/catalog-kind-selection`, `vs-adapter/pushdown-catalog-session`, and `vs-adapter/catalog-crate-structure`. Author no delta for the first three; author a CHANGED delta for `vs-adapter/catalog-crate-structure`. The skipped-warn behavior is owned by `vs-adapter/create-virtual-schema` (scenarios "One non-Iceberg table … is skipped" and "A namespace whose every table is non-Iceberg …") and its byte-identical guarantee by `vs-adapter/catalog-kind-selection` line 16 and `vs-adapter/pushdown-catalog-session` line 74; all three remain true after this change. `vs-adapter/catalog-crate-structure` is different: it normatively ENUMERATES the `lakehouse-catalog` `pub` set ("exactly these items SHALL be `pub`") and pins it with the reachability probe, and its "One shared catalog-client trait …" scenario describes `CatalogListing.skipped` as carrying "the identifiers the catalog reported as not loadable" — both statements go stale when Task 1 adds `SkipReason`/`SkippedTable` and reshapes `CatalogListing.skipped`.
- **Alternatives:** Add an Iceberg-side behavioral delta reflecting the internal reason-carrying refactor (rejected — those specs pin behavior, not implementation); author no `catalog-crate-structure` delta (rejected — it would leave the permanent `pub`-set enumeration and its probe contradicting the code after `/speq:record`, the exact silent gap the probe discipline exists to prevent, and the prior Unity-client surface extension was itself recorded as a delta).
- **Rationale:** The three behavioral specs pin behavior, not implementation. The Iceberg path still skips only HTTP-404 tables, still routes them into `CatalogListing.skipped`, still writes one `warn` line per skip naming the identifier and the reason, and its rendered warning bytes are unchanged (decision [2] reproduces the legacy line exactly). The change to those three is internal plumbing, so no delta is warranted. `catalog-crate-structure`, by contrast, records the crate's structural public surface as normative text and enforces it with a compile-time probe; extending that surface without a delta is a recorded-spec/code contradiction, so it gets a CHANGED delta (`vs-adapter/catalog-crate-structure/spec.md`).
- **Promotes to ADR:** no

### [4] Case-sensitive comparison against the uppercase Unity Catalog vocabulary

- **Decision:** Compare `table_type` and `data_source_format` case-sensitively against the uppercase tokens `MANAGED`, `EXTERNAL`, `VIEW`, and `DELTA`, matching the existing `neutral_table_type` match and the case Unity Catalog emits.
- **Alternatives:** Case-insensitive comparison.
- **Rationale:** `SPIKE_UC_CLIENT.md` recorded these fields uppercase from live Databricks (`table_type=MANAGED`, `data_source_format=DELTA`); the existing `neutral_table_type` already matches `MANAGED`/`EXTERNAL`/`VIEW` case-sensitively. Matching the emitted case keeps one vocabulary and avoids inventing tolerance the wire never exercises.
- **Promotes to ADR:** no

### [5] Shallow clones are included by the base-table rule; the wire shape is a tracked assumption

- **Decision:** Add no shallow-clone-specific handling. A shallow clone is included iff Unity Catalog reports it as `MANAGED`/`EXTERNAL` + `DELTA`, which the base-table rule already covers. Record the wire-shape claim as an explicit assumption with a tracked follow-up rather than a silent claim.
- **Alternatives:** Special-case shallow clones; assert the wire shape as verified fact.
- **Rationale:** The spike verified live that base tables report `MANAGED`/`EXTERNAL` + `DELTA`, but did not exercise a shallow clone specifically. The inclusion is definitional given that wire shape; the only unverified part is that a real shallow clone presents it, which the project verification discipline requires be tracked, not assumed.
- **Promotes to ADR:** no

### [6] Unit + integration coverage for the exclusion; no new OSS E2E fixture

- **Decision:** Cover the exclusion with mock-server client unit tests and mock-UC engine integration tests. Do not add a VIEW / non-Delta OSS fixture to the `#325` harness in this plan.
- **Alternatives:** Seed a VIEW and a non-Delta table into the OSS UC fixture for an E2E exclusion assertion.
- **Rationale:** The exclusion decision is made entirely from `GET /tables` JSON fields; the mock-server and mock-UC tests drive the exact JSON-to-decision path a live UC would. The wire vocabulary (`VIEW`, non-`DELTA` formats) is already spike-verified against live Databricks. Adding an OSS fixture is harness work with no additional decision-logic coverage; broadening the fixture matrix belongs to #323 if wanted.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] BLOCKER: catalog client test fixtures omit `data_source_format`

- **Finding:** [COMPLETENESS_GAP] The new filter serde-defaults a missing `data_source_format` to `None` and excludes the entry. Several `list_tables`-driven fixtures in `crates/lakehouse-catalog/src/unity/client_tests.rs` set a `MANAGED` `table_type` but no `data_source_format`, so the new filter routes them to `skipped` and their assertions go red — with no task fixing them. `follows_pagination_across_pages` asserts `vec!["t1","t2"]` while its `t1`/`t2` page bodies carry no `data_source_format`. The plan handled this trap for the engine `table_entry` fixture (Task 6) but not the catalog-crate side.
- **Direction change:** Task 5 now instructs the implementer to add `"data_source_format":"DELTA"` to both `MANAGED` page bodies of `follows_pagination_across_pages` and to audit every other `list_tables`-driven fixture, naming each required edit. Audit result recorded in Task 5: only `follows_pagination_across_pages` needs the fixture edit; `lists_tables_in_catalog_schema` (`orders` already `DELTA`, `orders_summary` VIEW intentionally skipped), `request_failure_is_credential_safe_error` (500 error path), and `identical_request_shape_oss_and_databricks` (`{"tables":[]}`) need none; `single_table_body` drives `load_table`, which the filter does not touch.
- **Promotes to ADR:** no

### [plan-review] BLOCKER: missing catalog-crate-structure delta for the extended public surface

- **Finding:** [REQUIREMENT_CONFLICT] Task 1 adds `pub SkipReason` and `pub SkippedTable` and changes `CatalogListing.skipped` from `Vec<CatalogTableIdent>` to `Vec<SkippedTable>`; Task 7 edits the reachability probe. The recorded `vs-adapter/catalog-crate-structure` feature normatively enumerates the crate's `pub` set and pins it with that probe, and its "One shared catalog-client trait …" scenario describes `CatalogListing.skipped` as carrying "the identifiers the catalog reported as not loadable". Extending the surface with no delta would leave the permanent spec contradicting the code after `/speq:record` — the prior Unity-client surface extension was itself recorded as a delta.
- **Direction change:** Authored `specs/_plans/change-unity-listing-delta-base-filter/vs-adapter/catalog-crate-structure/spec.md` (CHANGED): a DELTA:CHANGED on "One shared catalog-client trait …" reshapes the listing-type clause to a skipped set pairing an identifier with a neutral reason, and a DELTA:NEW scenario records `SkipReason` + `SkippedTable` joining the `pub` surface, `CatalogListing.skipped` becoming `Vec<SkippedTable>`, and the probe edit. Added the feature to plan.md § Features and updated decision [3] to include `catalog-crate-structure` in its considered set.
- **Promotes to ADR:** no

### [plan-review] ADVISORY acted on: OSS `data_source_format` presence is unverified

- **Finding:** [UNSTATED_ASSUMPTION] The filter turns on the `GET /tables` list response carrying `data_source_format="DELTA"` (uppercase), spike-verified only against live Databricks. `SPIKE_UC_CLIENT.md` does not confirm the OSS #325 harness list endpoint emits it. If OSS omits or lower-cases it, all-Delta fixtures are excluded — `make test-e2e-unity` yields an empty schema and OSS deployments silently list zero tables, a regression from #318.
- **Direction change:** Added a Dependencies precondition to confirm the OSS `GET /tables` list response carries `data_source_format=DELTA` for the vendored fixtures before the exclusion assertions are trusted, tracked in #323 as an explicit assumption at decision [5]'s rigor if it cannot be verified pre-implement. Recorded the expected value and the empty-schema-means-divergence signal in the Manual Testing row for `make test-e2e-unity`. Case-sensitivity is a conscious choice per decision [4]; the precondition is what catches an OSS casing or absence divergence.
- **Promotes to ADR:** no

### [plan-review] ADVISORY acted on: shallow-clone follow-up was untracked

- **Finding:** [UNSTATED_ASSUMPTION] Decision [5] records the shallow-clone wire-shape claim as an assumption but its follow-up was only "see decision log" — not a tracked issue that resurfaces.
- **Direction change:** Replaced "see decision log" in plan.md § Dependencies with a concrete tracked follow-up citing #323 (E2E hardening / broaden Delta fixture matrix) inline, matching the project `(#n)` tracking pattern. `#325` is the closed fixture-harness spike, so #323 is the live/OSS verification home.
- **Promotes to ADR:** no

### [plan-review] ADVISORY acted on: Task 7 grouped files with differing dependencies

- **Finding:** [TASK_GRANULARITY] Task 7 bundled four test files in Group 2 (parallel with Task 4), but its `adapter_tests.rs` portion asserts on `build_listing_virtual_tables`'s return shape, which Task 4 changes — so it cannot compile until Task 4 lands.
- **Direction change:** Split Task 7 per producing task. Task 7 now holds the catalog-crate fixtures (`catalog_public_surface.rs`, `crates/lakehouse-catalog/src/client_tests.rs`), following Tasks 1 and 2; new Task 8 holds the engine-side fixtures (`adapter_tests.rs`, `catalog_client_tests.rs`), following Task 4. Both moved to Group 3, after the production code they assert on.
- **Promotes to ADR:** no

### [plan-review] ADVISORY acted on (declined structural change): skip-reason detail is a pre-formatted fragment

- **Finding:** [INFORMATION_LEAKAGE] Task 3 has the client pre-format `NotDeltaBaseTable { detail }` as `table_type=<raw>` / `data_source_format=<raw>`, a fragment of the warn sentence, weakening decision [2]'s "adapter owns the sentence" claim.
- **Direction change:** Declined the structural discriminator (`field: DisqualifyingField, value: String`). A discriminator would either add a third type to the deliberately-minimal `lakehouse-catalog` public surface or name `data_source_format` as a matchable field on a public neutral type, which decision [1] and `vs-adapter/unity-catalog-client` forbid. Instead amended decision [2] and the plan.md Patterns row to state that message wording is intentionally co-owned — the single-owner claim applies to the skip DECISION (client-owned); the opaque `detail` string keeps `data_source_format` off the neutral surface while the adapter owns the sentence structure.
- **Promotes to ADR:** no
