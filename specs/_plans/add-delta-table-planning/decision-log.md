# Decision Log: add-delta-table-planning

## Interview

**Q:** How should format dispatch work — a `TableFormat` enum crossing the `CatalogClient` boundary?
**A:** Rejected. Introduce a `FormatReader` trait instead — the same design pattern as the existing
`CatalogClient` trait from #318, which returns per-catalog-kind behavior for listing and loading
tables. `FormatReader` is the per-table-format equivalent: it knows how to list the parquet data
files, delete/DV references, and partition/projection info for one table format. Two
implementations: `IcebergFormatReader` and `DeltaFormatReader`.

**Q:** Where does `FormatReader` live, given `CatalogClient` lives in `lakehouse-catalog`?
**A:** In `lakehouse-engine`, co-located with `ScanSpec`/`FileEntry` and the existing Iceberg planning
code — not in `lakehouse-catalog`. `CatalogClient` stays metadata-only. `lakehouse-catalog` has no
`iceberg-rust` or `delta_kernel` dependency today and must not gain one. The actual Iceberg
file-planning code already lives in `lakehouse-engine`
(`adapter/pushdown/file_resolution.rs`) and does not go through `CatalogClient` — it self-issues its
own REST calls via `CatalogSession`. The dependency direction is one-way, `lakehouse-engine` →
`lakehouse-catalog`. A small dispatcher inside `lakehouse-engine`, keyed on the table's format,
constructs `IcebergFormatReader` (wrapping the existing `resolve_file_list` logic) or
`DeltaFormatReader` (new, using `delta_kernel`). This preserves the recorded crate boundary and
avoids migrating already-shipped, tested Iceberg code into a crate that was never meant to hold it.

**Q:** `CatalogTable` does not carry the table's format today — Unity's `neutral_table` computes
`data_source_format` and discards it, and `table_id` never reaches it either. How is that resolved?
**A:** The planner must design how `CatalogTable` (or a sibling type) surfaces enough of
`{format, table_id, storage_location}` for the `lakehouse-engine`-side dispatcher and
`DeltaFormatReader` to work, for both catalog kinds. Iceberg REST is implicitly always Iceberg
format; Unity's listing already filters to Delta base tables per decision 065, so Unity ⇒ Delta is a
safe simplifying assumption today — but the format tag must still be explicit and checked, not
silently assumed, so a change there fails loud rather than silently misrouting. Vended-credential
vending (`resolve_uc_vended_storage` and the `POST /temporary-table-credentials` call, which needs
`table_id`) has no production caller yet; #319 is its first, because Delta log replay needs a
credentialed object store to read `_delta_log`.

**Q:** Should #319 wire Delta planning into the production pushdown path?
**A:** No — test-only, no production wiring. The existing hard refusal in
`crates/lakehouse-engine/src/adapter/mod.rs` (lines ~159-168) must stay in place. #319 adds and
integration-tests `DeltaFormatReader` and the dispatcher standalone against the local Unity+Delta
fixture from #325, but does not remove or narrow that refusal and does not touch `handle_pushdown`'s
live production path. #320 removes the refusal and supersedes the normative scenario "A pushdown
request under the Unity Catalog kind is refused as not yet executable".

**Q:** Why is early wiring unsafe rather than merely incomplete?
**A:** It would risk silently wrong results, not a clean failure. `FileEntry.deletes` and its consumer
`crates/lakehouse-engine/src/scan/positional_deletes.rs` only understand Iceberg positional-delete
files. A test (`scan_rejects_puffin_deletion_vector`) proves the scan code fails loud on a delete
mechanism it recognizes as unsupported — but only because it recognizes the shape. A Delta deletion
vector is structurally different (a reference into a separate `.bin` file, not an Iceberg delete-file
path) and is not modelled as a `DeleteFileRef` at all, so if `DeltaFormatReader` left `deletes` empty
the scan would silently run the delete-free path and deleted rows would reappear. Separately, Delta
does not duplicate partition-column values inside the physical Parquet file — they exist only in the
transaction log — and `register_file_list` in `crates/lakehouse-engine/src/scan/raw_scan.rs` has no
mechanism to inject a value that is not physically in the file, so partition columns could come back
NULL rather than erroring clearly. This is the class of bug the mission's Core Capability 6 and
CLAUDE.md's "usable engine" constraint treat as unacceptable, and the same reasoning that led to
hard-refusing Unity Catalog pushdown in the first place. Do not build an interim guard either — that
would pre-build a slice of #320's job.

**Q:** Should the plan carry per-file min/max statistics from the Delta log?
**A:** No. Carry partition values for correctness — Delta stores partition-column values off the
physical file, unlike Iceberg where they are usually present in-file — but no per-file min/max
statistics. Stats-based file pruning over Delta log stats is #321, which the #325 fixture harness
already earmarks a dedicated fixture for (`multi-part-stats`). Keep #319 minimal; do not design the
stats wire shape before its consumer exists.

**Q:** Which tasks should be routed to the expert implementer?
**A:** Tag with `[expert]` any task requiring deep reasoning — the `FormatReader` trait design, the
`CatalogTable` format-tag extension across both catalog kinds, the delta-kernel `ObjectStore` wiring,
and the log-replay-to-`ScanSpec` field mapping.

**Q:** Is there an adversarial plan-review round?
**A:** No — the review phase is explicitly opted out. The planner is sole author and judge.

## Design Decisions

### [1] `FormatReader` lives in the engine and each implementation owns its whole resolution

- **Decision:** Define `FormatReader` in `crates/lakehouse-engine/src/adapter/pushdown/format/`. One
  method resolves a table's whole scan — catalog request, storage credential, and file discovery —
  returning a single `ResolvedScan` value. `CatalogClient` gains no method.
- **Alternatives:** Add a file-planning method to `CatalogClient` — rejected: `lakehouse-catalog` MUST
  NOT name `iceberg`, `datafusion`, `arrow`, `parquet`, or `object_store`
  (`vs-adapter/catalog-crate-structure`), and `delta_kernel` falls under that rule for the same reason.
  Split resolution so a shared caller pre-fetches table metadata and each reader only plans files —
  rejected: the Iceberg arm needs the catalog's own `TableMetadata` to build an `iceberg::table::Table`
  while the Delta arm needs only a table-root URL plus a credentialed object store, so the pre-fetch
  step would itself have to fork per format, reintroducing the fork the trait removes.
- **Rationale:** The interface is deep even though one implementation is thin: a caller asks one
  question and learns nothing about catalog protocol, credential vending, or whether files come from
  manifests or a JSON commit log. The recorded clause that `CatalogClient`'s two listing operations be
  shaped so adding an operation later is purely additive holds unedited — nothing is added.
- **Promotes to ADR:** yes

### [2] Format dispatch matches `ScanSource`, never `CatalogKind` and never a bare format tag

- **Decision:** One `ScanSource` enum whose each variant pairs a live catalog session with the table it
  reads (`IcebergRest { session, catalog_props }`, `UnityDelta { session, table }`). `format_reader`
  matches it exhaustively at exactly one site and fails loud, naming the table and its reported format,
  when the Unity variant is handed a non-Delta table.
- **Alternatives:** Match `CatalogKind` — rejected: `vs-adapter/catalog-kind-selection` freezes its
  match sites with a source-level probe asserting the variant names appear in no production module
  beyond the enum, its resolver, the client construction site, credential validation, and the pushdown
  refusal. Match a bare `TableFormat` — rejected: a format tag cannot carry the session each reader
  needs, and obtaining the tag for a Unity table requires a `load_table` the reader would then repeat,
  double-loading. A single input struct carrying both sessions as `Option`s — rejected: every arm would
  read a field the other arm sets, and an unset one would be a runtime error rather than a type error.
- **Rationale:** `ScanSource` is not a second `CatalogKind`: the kind is a parsed virtual-schema
  property, `ScanSource` a resolved session plus a loaded table. Matching `ScanSource` is precisely
  what removes the need for a second `CatalogKind` match site. The variant name `UnityDelta` states the
  Unity-implies-Delta coupling out loud rather than pretending to a generality the code does not have;
  a second Delta-hosting catalog is the trigger to revisit it.
- **Promotes to ADR:** yes

### [3] `CatalogTable` gains a neutral format tag and an opaque vending key

- **Decision:** Add a closed `TableFormat` enum (Iceberg, Delta) and `vended_credential_key:
  Option<String>` to `CatalogTable`. The raw Unity `data_source_format` and `table_id` wire fields stay
  crate-private; only their neutral projections cross. Unity's `load_table` fails loud on an absent or
  unrecognized format. Supersedes the recorded clause that `data_source_format` "MUST NOT appear in any
  neutral type the engine can name".
- **Alternatives:** A `CatalogClient::resolve_table_storage` method keeping `table_id` inside the crate
  — rejected: the Iceberg REST arm's equivalent prefix (the `loadTable` GET plus vended-storage
  resolution) lives engine-side, so implementing it would force re-plumbing the shipped Iceberg path
  for zero #319 benefit. A separate `UnityCatalogSession` method returning table plus storage —
  rejected: it duplicates `load_table`'s work on a second public entry point. Re-issuing
  `GET /tables/{full_name}` inside the vending step to recover `table_id` — rejected: it breaks
  "resolve metadata once per query". A newtype wrapping the key — rejected: it puts a Unity concept on
  the crate's enumerated public surface and buys no invariant the neutral table's own privacy already
  gives.
- **Rationale:** Two decisions were conflated under the recorded prohibition. The listing-admission
  decision — which entries are Delta base tables — stays owned inside the client, unchanged. The
  table's FORMAT is not that decision: it is data the engine's dispatch reads, and withholding it would
  force the engine to assume Unity implies Delta rather than check it, which is the silent-misroute the
  interview ruled out. Naming the key for the decision it serves rather than for the wire field it
  holds is what keeps a caller from reading a Unity concept out of it.
- **Promotes to ADR:** yes

### [4] `IcebergFormatReader` is a deliberately thin delegator, with the collapse scheduled for #320

- **Decision:** `IcebergFormatReader::resolve_scan` calls `resolve_file_list` unchanged and packs its
  five-tuple into `ResolvedScan` with an absent Delta block. `resolve_file_list` keeps its name, `pub`
  visibility, signature, and every call site. Collapsing it into the reader is deferred to #320.
- **Alternatives:** Move `resolve_file_list`'s body into the reader now and delete the free function —
  rejected: it relocates roughly 160 lines of shipped, spec-covered, credential-carrying code, changes
  every join leg and external test caller, and would breach the recorded clause that
  `resolve_file_list` "ALONE SHALL KEEP its name and its `pub` visibility on the façade" — all for zero
  behavior gain in a plan whose value is verified by its own tests. Change only its return type to
  `ResolvedScan` — rejected: it edits every caller for a cosmetic gain and forfeits the zero-diff
  guarantee on the shipped Iceberg path.
- **Rationale:** A function whose whole body calls another with the same arguments is the shallow-module
  red flag this project records and normally deletes (see `vs-adapter/catalog-crate-structure` on
  `load_table_signed`). It is accepted here only because it buys a zero-byte diff on the shipped path
  and only until #320 removes the direct callers — a named, scheduled follow-up rather than an
  open-ended one.
- **Promotes to ADR:** yes

### [5] Delta log replay takes an injected object store

- **Decision:** The replay step's signature takes an `Arc<dyn ObjectStore>` and a table-root URL and
  builds no store. `DeltaFormatReader` builds the store; the replay step never does.
- **Alternatives:** Have the replay step build its store from the `StorageBackend` — rejected: it would
  make every replay test require S3 or a mock, and would put store construction in two homes.
- **Rationale:** It makes replay correctness — active-file selection across commits, partition values,
  deletion-vector references, column mapping — testable offline against the vendored fixtures over a
  local-filesystem store, in a plain `cargo test`. The live `unity-e2e` suite is then reserved for what
  only the stack can prove: catalog resolve, credential vending, and reading `_delta_log` over S3.
- **Promotes to ADR:** yes

### [6] One optional Delta block per struct, not scattered Delta fields

- **Decision:** `CommonScanSpec.delta: Option<DeltaTableSpec>` and `FileEntry.delta:
  Option<DeltaFileSpec>`, each absent from JSON when absent in the value.
- **Alternatives:** Add `delta_column_mapping`, `delta_partition_columns`, `partition_values`, and
  `deletion_vector` as separate optional fields — rejected: four skip-serialize gates instead of one,
  and #321's stats and #322's reader-feature data would add more, spreading one format's decisions
  across a shared struct.
- **Rationale:** One home per format keeps the Iceberg encodings byte-identical behind a single gate,
  makes `Some(delta)` the scan side's single signal that this is a Delta scan, and lets #321 and #322
  extend the block without touching `CommonScanSpec` or `FileEntry`.
- **Promotes to ADR:** no

### [7] A Delta file-list entry serializes as a JSON object, not a fourth tuple slot

- **Decision:** Add a self-describing JSON-object variant to the private `FileEntryWire` untagged enum,
  carrying path, size, an optional deletes list, and the Delta block. The 2-tuple and 3-tuple variants
  and their precedence are untouched.
- **Alternatives:** A `[path, size, deletes, delta]` 4-tuple — rejected: it forces an always-empty
  `deletes` array onto every Delta entry, and shortest-form serialization over four slots is harder to
  keep lossless.
- **Rationale:** Object and tuple shapes are disjoint, so untagged deserialization stays unambiguous
  and every existing golden file-list encoding is unchanged. The variant carries `deletes` as well so
  `Into<FileEntryWire>` is total and lossless for every value the struct admits, rather than dropping a
  field in a combination the type permits but construction never produces.
- **Promotes to ADR:** no

### [8] Fail loud on an unmapped Delta type; perform no reader-feature gating

- **Decision:** Map only `boolean`, `integer`, `long`, `float`, `double`, `string`, `date`,
  `timestamp`, `timestamp_ntz`, and `decimal(p,s)` — the Delta primitives that already have an Arrow
  type tag. Return a `UdfError` naming the column, its Delta type, and issue #322 for anything else.
  Add no reader-feature gate.
- **Alternatives:** Map `byte`/`short` to the `int32` tag and nested types to `VARCHAR(2000000)` via
  JSON now — rejected: a near-miss tag returns wrong values, and the broad mapping plus its
  Exasol-domain rules is #322's whole subject. Gate `deletionVectors` and `columnMapping` now —
  rejected: it would refuse the very fixtures this plan resolves.
- **Rationale:** Refusing is the only option that cannot return a wrong value, and #322 supersedes the
  refusal with the mapping. #325 already recorded that the kernel reads "unsupported" tables without
  erroring, so gating must be engine-side and belongs with the type work rather than split across two
  plans.
- **Promotes to ADR:** no

### [9] Extract the undecorated store builder rather than reuse `build_side_store`

- **Decision:** Extract `StorageBackend` → `Arc<dyn ObjectStore>` construction out of
  `build_side_store` into one `pub(crate)` builder; `build_side_store` wraps its result in
  `SpecSizedObjectStore` as before.
- **Alternatives:** Call `build_side_store` from the Delta reader — rejected: `SpecSizedObjectStore`
  answers HEAD from a known file-size index, and at plan time the `_delta_log` file sizes are exactly
  what is unknown. Build a second store in the Delta reader — rejected: it duplicates the
  credential-configuration decision, including the trap that `with_client_options` replaces the whole
  `ClientOptions` and must precede `with_allow_http` or plain-HTTP MinIO silently breaks.
- **Rationale:** One home for "how a `StorageBackend` becomes an object store", two callers that differ
  only in whether they decorate it.
- **Promotes to ADR:** no

### [10] The live test lands in the existing `e2e_unity_test.rs` binary

- **Decision:** Add the vended/static Delta planning test to
  `crates/lakehouse-engine/tests/e2e_unity_test.rs` under the existing `unity-e2e` feature. Put the
  offline replay tests in a new, ungated `crates/lakehouse-engine/tests/delta_log_replay.rs`.
- **Alternatives:** A new `--test` target for the live test — rejected: the Makefile's
  `test-e2e-unity` target and CI's `e2e-unity` job each name one `--test` target and must stay
  flag-identical (the CI comment declares itself the authority), so a second binary would run in
  neither without editing both — which is #328's settled territory. A new cargo feature for
  planning-only tests — rejected: it forks the fail-not-skip contract for one test.
- **Rationale:** Existing infrastructure covers it with no CI change. The offline file needs no gate at
  all, because the fixtures are in the repository.
- **Promotes to ADR:** no

### [11] Apache Iceberg spec check: no Iceberg behavior changes, and the overlapping rule is already a recorded trade-off

- **Decision:** Record the check rather than change anything on the Iceberg side.
- **Alternatives:** Implement Iceberg Column Projection rule (1) in the same plan — rejected as
  unrelated scope.
- **Rationale:** The Iceberg table spec's "Column Projection" ordered resolution defines rule (1) as
  "Return the value from partition metadata if an Identity Transform exists for the field and the
  partition value is present in the `partition` struct on `data_file` object in the manifest".
  `datafusion-scan/scan-execution-field-id-projection` already records rule (1) as unimplemented and as
  a deliberate, accurately-scoped trade-off, with the `initial-default` (rule 3) result preferred where
  both could resolve. Nothing in this plan touches that. Delta has no analogous escape — it never
  writes a partition column's value into the data file — so carrying partition values is mandatory for
  the Delta path rather than an edge case, which is why they are added to `FileEntry` for Delta only.
  No Iceberg scanning, pushdown, or schema/type behavior changes, proven by the characterization gate.
- **Promotes to ADR:** no

### [12] The Unity Catalog E2E harness description is superseded, not silently outgrown

- **Decision:** Supersede the recorded description sentence "The suite stops at createVirtualSchema —
  it lists tables and columns and runs no scan, because Delta scan execution lands in #319/#320", and
  retain "runs no scan" with the accurate reason.
- **Alternatives:** Leave the description and add the test — rejected: the description would assert
  something false about the suite. Move the live test elsewhere to keep the description true —
  rejected: no other binary runs in CI without editing the CI job.
- **Rationale:** The suite no longer stops at createVirtualSchema; it reads `_delta_log` from MinIO at
  plan time. "Runs no scan" still holds — #319 issues no scan-driving query and no scan UDF invocation
  — so the bound is the absence of scan EXECUTION, not the absence of everything past catalog metadata.
- **Promotes to ADR:** no

## Review Findings

<!-- No adversarial plan-review round was run for this plan; the reviewer phase was explicitly opted
out. Code-review findings are appended here by speq-implement. -->
