# Decision Log: add-direct-storage-catalog-kind

## Interview

**Q:** Issue #407 lists `MERGE_SCHEMA` widening as part of the direct-storage kind. Does the full
widening ship in this plan, or does the plan ship a single-footer read and defer widening to a
follow-up issue?
**A:** The full `MERGE_SCHEMA` widening ships in THIS plan, as one PR. Do not split widening into a
follow-up issue.

**Q:** How should the footer merge reuse the engine's existing type machinery, and what happens if
the reuse does not fit?
**A:** The footer-merge seam widens ONLY across pairs that `datafusion-scan/type-relaxation`
already proves castable, and the scan side applies the merged schema through the EXISTING per-file
cast adapter. No new cast logic and no new pair table. Direct-storage logical fields use identity
column binding, carrying no field-id and no declared physical name. If the column-binding adapter's
binding-key contract turns out incompatible with identity binding for a multi-file merged schema,
flag it as a design gap rather than silently dropping widening.

## Design Decisions

### [1] One shared directory seam answers "which files and what schema" for both callers

- **Decision:** A single function, `adapter/parquet_directory.rs`, takes an object store, a prefix,
  and a merge mode. It returns the data-file list with sizes, the folded Arrow schema, and each
  file's parsed Parquet metadata. Table discovery and query planning both call it. Neither carries
  a listing filter, a footer reader, or a merge policy.
- **Alternatives:** A listing helper in the catalog client plus a second one in the format reader,
  each reading `MERGE_SCHEMA` its own way. Rejected: two policies over one property is the exact
  back-door leakage that makes a declared schema and a scanned schema disagree.
- **Rationale:** The seam is the deep module of this plan. Its interface is three inputs. Its
  internals absorb recursion, filtering, ordering, bounded concurrent footer reads, and widening.
  It names no catalog kind, no table format, and no Exasol property, so issues #408 and #412 extend
  it rather than duplicating it. This decision stays plan-local rather than promoting. It is an
  internal module-design choice for one feature's seam. Its reach is that seam plus the two
  follow-on issues that extend the same feature area, #408 and #412. It binds no code outside the
  direct-storage kind and states no project-wide rule.
- **Consequences:** The seam returns parsed footers rather than the schema alone, so #412 re-reads
  no footer. The merge mode's sample-one-file arm requires a deterministic listing order, because
  the refresh path and the plan path must sample the same file.
- **Promotes to ADR:** no

### [2] A `CatalogClient` implementor may be declared in `lakehouse-engine`

- **Decision:** `DirectStorageCatalogClient` is declared in `crates/lakehouse-engine`. It
  implements the `CatalogClient` trait declared in `crates/lakehouse-catalog`. The catalog crate
  gains no source file, no manifest dependency, and no public item for this kind beyond three
  neutral enum variants.
- **Alternatives:** Declare the client in `lakehouse-catalog` beside the two shipped implementors.
  Rejected for two reasons. The client needs the engine-side object-store builder.
  `vs-adapter/catalog-crate-structure` forbids the catalog crate from declaring `object_store`.
- **Rationale:** Rust's orphan rule permits the impl because the type is local. No recorded rule
  requires an implementor to live in the catalog crate. The recorded requirement is that the engine
  reach every enumeration and table-load operation through the trait. This placement keeps that
  requirement. The single boxed-client construction site is already engine-side, so the placement
  adds no seam.
- **Consequences:** A future catalog kind that needs an engine-side dependency follows the same
  route. The dependency edge stays one-way. The compiler enforces that direction.
- **Promotes to ADR:** yes

### [3] A structural invariant is not enforced by a test that matches production source text

- **Decision:** This plan does not build the source-level probe that
  `vs-adapter/catalog-kind-selection` has required since issue #318. The delta supersedes that
  clause. It records that exhaustive matching plus review holds the permitted-site set. That
  guarantee is weaker than the clause promised. The edit to
  `crates/lakehouse-catalog/tests/catalog_public_surface.rs` adds only external-vantage
  construction of the new variants, never a source-text assertion over a variant list.
- **Alternatives:** Build the probe the recorded clause names, reading every production source file
  and matching `CatalogKind` variant names against an allowlist. Rejected on project convention.
- **Rationale:** This entry invokes the process-convention override. The rule is this: no test in
  this project may enforce a structural invariant by matching production source text. That rule
  binds every future plan in this repository rather than this plan alone. It is a corollary of no
  other decision. Recording a probe that nobody will build is worse for the next reader than
  recording honestly that the invariant rests on compile errors and review.
- **Consequences:** The three matching sites stay exhaustive, so a fourth catalog kind is a build
  failure at each. A fifth matching site added later is visible in review as a new `CatalogKind`
  import rather than as a test failure.
- **Promotes to ADR:** yes

### [4] Identity binding already supports a multi-file merged schema, so no design gap is flagged

- **Decision:** Direct-storage logical fields carry no field-id and no declared physical name. The
  existing column-binding adapter binds them by name. The interview's escape hatch is not
  taken.
- **Alternatives:** Synthesize an ordinal field-id per column. Rejected: a synthesized id is a
  value no writer put in any file and would invite a false match against a file that does carry
  field-ids.
- **Rationale:** The binding adapter's resolution order ends in an identity name match. The adapter
  runs per file. It never compares the physical field's Arrow type. A column absent from one file
  already takes the NULL-fill, `initial-default`, or required-absent path. The per-file widening
  cast is inserted by the delegated physical expression adapter. Every part the merged schema needs
  is therefore already shipped for the Delta `none` column-mapping mode.
- **Consequences:** The merged schema declares every column NULLABLE, because a column absent from
  one file must not fail the scan. Two column names equal after the declaration's uppercase fold
  are rejected, because identity binding cannot bind two logical fields to one physical name.
- **Promotes to ADR:** no

### [5] The widening pair set gains a production owner, checked against the existing test pin

- **Decision:** `types/widening.rs` owns the 13 recorded relaxation pairs as production code. It
  answers which of two Arrow types is the wider. `scan/type_relaxation_tests.rs` KEEPS its concrete
  17-entry `supported_relaxation_pairs` list and asserts it AGAINST that owner. The two are checked
  against each other rather than one generated from the other.
- **Alternatives:** Give the fold its own pair table. Rejected by the interview answer and by the
  drift it would create. Use `arrow::compute::can_cast_types` as the oracle. Rejected: it also
  accepts narrowing casts that neither format permits. Generate the test list from the owner.
  Rejected: a generated list asserts only what its generator accepts. That withdraws the
  arrow-castability pin the suite exists to hold.
- **Rationale:** The fold needs the set at plan time, so the set needs a production home. The
  concrete list stays because it is the independent pin. A rule-based owner cannot be enumerated
  anyway. Rows 9 to 12 are parameterized over precision and scale, so no representative pair derives
  from them.
- **Consequences:** Adding a pair is two coordinated edits rather than one. A row added to the
  table with no matching owner rule fails the suite. The fold introduces no pair, so `long` to
  `double` stays absent.
- **Promotes to ADR:** no

### [6] The neutral column carries an Arrow type as a tag string

- **Decision:** `ColumnSourceType` gains a `Parquet` variant carrying the column's logical Arrow
  type as a tag string of the engine's existing scan-spec tag vocabulary. The engine's mapping arm
  reads the tag back through the existing parser and returns the Arrow-input answer.
- **Alternatives:** Carry an Arrow `DataType`. Rejected: `vs-adapter/catalog-crate-structure`
  forbids `lakehouse-catalog`'s manifest from declaring `arrow`. Carry the resolved Exasol type
  string. Rejected: it would move the Exasol mapping into the catalog crate.
- **Rationale:** The tag vocabulary is an existing owned wire format. It is exactly "an Arrow type
  spelled without depending on arrow". The client applies the JSON-fallback string substitution
  before rendering the tag. Every tag it emits is therefore inside the vocabulary. The round trip
  is lossless.
- **Promotes to ADR:** no

### [7] `arrow_to_exasol_type` keeps its guard and gains a reachability argument

- **Decision:** The Arrow-input decimal guard is not folded into the shared catalog-decimal
  predicate. `datafusion-scan/type-mapping-module-structure`'s per-producer clause replaces its
  "no call site" argument with the Apache Parquet format's own constraint on its `DECIMAL` logical
  type.
- **Alternatives:** Route the Arrow-input guard through `exasol_representable_catalog_decimal`.
  Rejected on the scale types. That predicate takes an unsigned scale. The Arrow scale is a signed
  `i8`, so the fold would accept a negative scale that Exasol rejects.
  `datafusion-scan/type-mapping` records the separation deliberately.
- **Rationale:** Parquet requires a precision above zero and a scale between zero and the
  precision. The new producer therefore cannot emit the pairs the loose guard would mishandle. The
  recorded bound holds, now from the producer's domain instead of from the absence of a producer.
- **Promotes to ADR:** no

### [8] `MERGE_SCHEMA` defaults to TRUE

- **Decision:** An absent `MERGE_SCHEMA` folds every footer. `FALSE` samples the first file in the
  deterministic listing order, on both the refresh path and the plan path.
- **Alternatives:** Default to sampling one footer for speed. Rejected: a directory whose files
  differ is the case this feature exists for. A silently wrong declared type costs more than a
  slower refresh.
- **Rationale:** The safe answer is the default. The cheap answer is opt-in.
- **Consequences:** `FALSE` implies a strict narrowing cast at scan time for a file wider than the
  sampled footer. That cast surfaces a clean error rather than a wrong value. The E2E suite pins
  that consequence.
- **Promotes to ADR:** no

### [9] An unfoldable column pair fails the enumeration

- **Decision:** A column whose declared types are covered by no supported pair fails the fold. The
  error names the column, both types, and both file paths.
- **Alternatives:** Declare the column `VARCHAR(2000000)`. Drop the column. Take one file's type.
  Each rejected: they answer a correctness question by guessing. Two of the three silently
  change what a query returns.
- **Rationale:** The operator can act on an error that names the files. None of the alternatives
  leaves a signal at all.
- **Promotes to ADR:** no

### [10] `NAMESPACE` becomes kind-scoped rather than optional everywhere

- **Decision:** `NAMESPACE` stays required under every catalog kind. Under `DIRECT_STORAGE` it
  becomes optional and names a subtree of the CONNECTION address.
- **Alternatives:** Drop the requirement entirely. Rejected: under a catalog kind the adapter
  cannot guess a namespace, so dropping it trades a clear create-time error for a confusing empty
  schema.
- **Rationale:** The property means different things per kind. Under direct storage an absent value
  has a correct meaning: enumerate the address itself.
- **Promotes to ADR:** no

### [11] `TABLE_MAP` records the bare original-cased directory name

- **Decision:** A direct-storage table's neutral identifier carries an EMPTY namespace. The shared
  identifier helper therefore yields the bare directory name. `TABLE_MAP` records that name.
- **Alternatives:** Record a dotted identifier as the two catalog kinds do. Rejected: a directory
  name may itself contain a dot. A dotted form could not be split back unambiguously.
- **Rationale:** The shared flatten and collision helpers then work with no branch on catalog kind.
- **Consequences:** A recovered pushdown identifier that is empty or carries a path separator is
  rejected. Such a value names no first-level directory. Composing a table root from it would reach
  outside the base path.
- **Promotes to ADR:** no

### [12] The scheme-versus-credential check is a method on `StorageBackend`

- **Decision:** A method on `StorageBackend`, with no catch-all arm, decides whether a CONNECTION
  address scheme agrees with the credential shape.
- **Alternatives:** A new adapter module holding the check. Rejected: it would add a seventh module
  to `vs-adapter/storage-backend-enum`'s permitted variant-naming list for a two-arm decision.
- **Rationale:** The decision is a property of the backend the credentials select. With no
  catch-all arm, a future backend is a compile error at that method.
- **Consequences:** The accepted scheme list adds `s3a` beyond the `s3://` and `abfss://` issue #407
  names. That is a deliberate addition, not an accidental widening. Hadoop-written configurations
  spell the S3 scheme `s3a`. An operator copying a working Hadoop address would otherwise be
  rejected for a spelling that names the same store.
- **Promotes to ADR:** no

### [13] One admission-limited object store per adapter call, capped at 16

- **Decision:** The adapter builds exactly one `LimitStore`-wrapped object store per adapter call.
  It shares that store across every table the call touches. The cap is 16. The HTTP
  connection-retention budget derives from that same constant.
- **Alternatives:** One store per table. No admission limiter, relying on the retention cap alone.
  Rejected: the retention cap is an idle-connection budget, not an admission gate, so it bounds
  nothing when a refresh fans out over thousands of footers.
- **Rationale:** One store means the cap bounds the whole call rather than each table. The limiter
  is distinct from the scan UDF's per-instance connection budget. It neither reads nor modifies
  that budget.
- **Consequences:** 16 is a deliberately conservative starting value to revisit with measurements.
  A refresh sweeping many thousands of footers tolerates far more concurrency than a per-query
  plan.
- **Promotes to ADR:** no

### [14] Concurrent join-leg resolution ships in this plan

- **Decision:** The sequential join-leg loop becomes concurrent resolution over the request's one
  shared session, preserving leg-index order and byte-identical generated SQL and scan specs.
- **Alternatives:** Leave the loop sequential and file a follow-up. Rejected for two reasons. The
  direct-storage kind reads footers at plan time, so a two-leg join would pay both legs' footer
  reads in series. The change is shared code that benefits Iceberg and Delta equally.
- **Rationale:** The loop's only mutation is a push. Every later step indexes a side by its leg
  rather than by its name. Order preservation is therefore the whole contract.
- **Consequences:** The committed join golden strings must pass unedited. That is the test that
  the refactor changed nothing but latency.
- **Promotes to ADR:** no

### [15] No table-format detection heuristic

- **Decision:** A directory that holds an Iceberg or a Delta table is read as raw Parquet under this
  kind. The engine does not detect such a directory and does not refuse it.
- **Alternatives:** Detect a `_delta_log` or `metadata` directory and refuse the table. Rejected: a
  user may legitimately want the raw files. A heuristic would fail a supported read on a layout
  it guessed wrong about.
- **Rationale:** The operator selected a `CATALOG_KIND` that names no table format. The trade-off is
  stated normatively in the spec, pinned by an E2E scenario, and required in the user
  documentation.
- **Promotes to ADR:** no

### [16] Four gaps ship as tracked exceptions, and two ship untracked with one issue owed before merge

- **Decision:** Path-based partition pruning (#408), footer-statistics file pruning (#412), Unity
  Catalog PARQUET-table routing (#409), and non-Iceberg Glue tables (#410) are out of scope. Each is
  cited inline in the specs that would otherwise read as complete. Two further gaps have NO tracking
  issue today: the absence of any file-count or file-size safety limit, and the unmeasured admission
  cap of 16. Both are stated as untracked gaps in `plan.md` § Impact. The first is also stated in
  `vs-adapter/direct-storage-table-planning`. ONE new issue covering both MUST be opened before
  this plan merges and cited inline in that spec clause.
- **Alternatives:** Ship one of them inside this plan. Leave them unstated. Both rejected: the
  first widens a plan that already spans six clusters. The second is the silent-gap failure the
  project rule forbids.
- **Rationale:** `HIVE_PARTITIONING` is parsed and validated now, so the property's vocabulary
  exists once. The seam already splits `key=value` segments and returns parsed footers. Neither
  follow-up therefore rewrites the seam. The file-count limit and the admission cap share one issue
  because one measurement answers both. That measurement has two parts: how many concurrent footer
  reads a refresh sustains, and at what table size that refresh stops being acceptable.
- **Consequences:** An exception recorded against an issue that tracks something else is worse than
  no citation. A reader who follows it concludes the gap is owned. An earlier draft of this entry
  cited #409 and #410 for gaps neither issue covers. Every citation here is therefore verified
  against the tracker rather than written from memory.
- **Promotes to ADR:** no

### [17] Two E2E features share one test binary

- **Decision:** `e2e-harness/direct-storage-e2e` and `e2e-harness/direct-storage-e2e-properties`
  are two features over one binary and one `OnceLock`-guarded setup, split by question rather than
  by file.
- **Alternatives:** One feature with twelve scenarios. Two binaries. Rejected: the first crosses
  this library's per-spec organization threshold. The second pays the stack setup twice.
- **Rationale:** One binary keeps the Makefile registration and the build-convention guard to one
  entry each.
- **Promotes to ADR:** no

## Review Findings

Round 1 returned 7 BLOCKER and 7 ADVISORY findings. The requester resolved the one HUMAN escalation
by reinstating the dropped coverage. All 14 are resolved below.

Round 2 confirmed every round-1 blocker resolved and returned 3 new BLOCKER and 4 new ADVISORY
findings, none of them requiring a human decision. The 3 blockers are resolved in the last three
entries of this section. The 4 advisories stay open by this project's plan-review workflow, which
bounds plan review at 2 adversarial rounds and treats a later round's advisories as report-only.

### [plan-review] GROUP BY and two-table join coverage dropped from the parity smoke test

- **Finding:** `[SCOPE_REDUCTION]` BLOCKER, escalated HUMAN. Issue #407 specifies the parity smoke
  test as projection, filter, LIMIT, GROUP BY, and a two-table join. The plan shipped the first
  three plus a single-group aggregate. The join path is the one this plan reshapes hardest, through
  concurrent leg resolution (decision [14]) and listing-based broadcast sizing, and both landed with
  no live proof over this kind.
- **Direction change:** The requester settled the escalation by reinstating the coverage here rather
  than deferring it. `e2e-harness/direct-storage-e2e-properties` § Projection, filter, and LIMIT
  reach the direct-storage scan gains a GROUP BY clause and a two-table inner equi-join clause, the
  latter requiring both legs to resolve through the request's one shared admission-limited store.
  Its Background names why those two shapes are in the smoke test rather than assumed covered. Task
  6.4 names `group_by_aggregate_matches_the_unpushed_answer` and the two-table join test, which the
  round-2 finding below renames to `two_table_join_matches_the_unpushed_answer_in_one_request`. Both
  appear in § Scenario Coverage.
- **Promotes to ADR:** no

### [plan-review] The scan-spec tag vocabulary is silently lossy for the new producer

- **Finding:** `[UNSTATED_ASSUMPTION]` BLOCKER. The plan rested on a lossless Arrow-to-tag round
  trip the codebase refutes. `arrow_type_to_tag` spells twelve types and falls through to `"utf8"`
  for everything else, while `compatible_exasol_type` returns a non-VARCHAR Exasol type for `Int8`,
  `Int16`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `LargeUtf8`, and `Timestamp(Millisecond, _)`. None
  of those is nested or unrepresentable, so the JSON-fallback substitution never fires for them. A
  Spark-written `INT64 TIMESTAMP(MILLIS)` column would be declared `VARCHAR(2000000)` and registered
  as `Utf8` with no error anywhere.
- **Direction change:** `datafusion-scan/type-mapping` gains a scenario requiring the vocabulary to
  carry one entry for every Arrow type the classifier admits, naming those eight cases and requiring
  a round trip for each, and its Background states the defect rather than the assumption.
  `datafusion-scan/scan-execution-field-id-projection` gains a DELTA:CHANGED round-trip scenario
  superseding the recorded twelve-tag enumeration in its own Background and GIVEN, while keeping the
  vocabulary primitive-only. `vs-adapter/direct-storage-table-discovery` now requires an
  inexpressible folded type to FAIL the enumeration naming the column and the type rather than
  tagging `utf8`. Task 3.5 implements the widening and its round-trip test.
- **Promotes to ADR:** no

### [plan-review] Three tracked-exception citations named the wrong issues

- **Finding:** `[COMPLETENESS_GAP]` BLOCKER. `plan.md` § Design Non-Goals and decision [16] cited
  #409 for a file-count or file-size safety limit and #410 for non-Parquet file formats. Verified
  against the tracker, #409 is "route Unity Catalog PARQUET tables to the Parquet reader" and #410
  is "read non-Iceberg Glue tables". No open issue tracks a file-count or file-size limit at all, so
  the plan's largest operational risk was recorded against an issue that owns something else.
- **Direction change:** Both citations corrected to their real meaning in § Design Non-Goals and in
  decision [16], which also gains a consequence naming the miscitation so the next reader sees why
  every citation is now tracker-verified. The file-count and file-size limit is removed from the
  tracked list and stated as an untracked gap in `plan.md` § Impact, beside the plan-time footer
  cost caution, with one new issue owed before merge.
  `vs-adapter/direct-storage-table-planning` § Every plan-time footer is read records the absence of
  any bound as an explicit exception, to be cited inline once the issue exists.
- **Promotes to ADR:** no

### [plan-review] Two deltas contradicted each other about `load_table`

- **Finding:** `[COMPLETENESS_GAP]` BLOCKER. `vs-adapter/direct-storage-table-discovery` claimed the
  adapter reaches "both trait operations" through the trait as it does for the other two kinds.
  `CatalogClient` declares `list_tables` and `load_table`, and `load_table` has exactly one engine
  call site, on the Unity Catalog path alone. `vs-adapter/direct-storage-table-planning` states the
  opposite, that this kind loads no table. No scenario and no task said what
  `DirectStorageCatalogClient::load_table` returns, yet the trait forces an implementation.
- **Direction change:** The clause now says the adapter reaches this client's enumeration through
  `list_tables` exactly as it does for the other two kinds, and two further clauses state that
  `load_table` is unreachable for this kind and must return a clear error naming the kind, never a
  panic and never an empty or synthesized table. Task 4.2 names that behavior, and § Scenario
  Coverage gains `load_table_returns_a_clear_error_naming_the_direct_storage_kind`.
- **Promotes to ADR:** no

### [plan-review] The seam's return shape was unspecified under sample-one-file mode

- **Finding:** `[AMBIGUOUS_REQUIREMENT]` BLOCKER. The seam returned "each file's parsed Parquet
  metadata" unconditionally, while the sample-one-file mode reads exactly one footer and returns the
  full file list. Whether the metadata side is a shorter sequence, a per-file option, or a keyed map
  was never stated, so no pass or fail test could be written. The issue #407 promise that a later
  consumer re-reads no footer cannot hold in that mode at all.
- **Direction change:** `vs-adapter/parquet-directory-seam` § The merge mode selects every footer or
  exactly one now requires the metadata to be PAIRED with the files whose footers the mode read and
  ABSENT for every other listed file, and forbids positional indexing against the file list. The
  forward-compatibility clause in § One seam answers the file list and the schema for both callers
  is scoped to the fold-every-file mode, and a consumer needing statistics for an unread file asks
  the seam rather than opening a footer behind it. Task 2.5 restates the paired-and-absent shape.
- **Promotes to ADR:** no

### [plan-review] The overflow-asymmetry claim contradicts the recorded type-relaxation spec

- **Finding:** `[REQUIREMENT_CONFLICT]` BLOCKER. The delta claimed the read behavior is unchanged.
  The recorded spec states that every supported pair is a widening, so the two cast sites' opposite
  overflow policies are unreachable. `MERGE_SCHEMA = 'FALSE'` makes the narrowing direction
  reachable for the first time. The plan stated that consequence twice, in decision [8] and in the
  E2E delta, but no delta superseded the recorded clause, so the merged library would hold two
  contradictory statements.
- **Direction change:** `datafusion-scan/type-relaxation` § Background gains a bullet superseding
  the recorded sentence, its opening bullet no longer claims the read behavior is unchanged, and the
  amended scenario carries the supersession normatively: the narrowing cast is reachable, the
  read-side `safe: false` site returns a clean error naming the column and both types, and the emit
  boundary's `safe: true` policy stays unreachable because the read-side cast fails first.
- **Promotes to ADR:** no

### [plan-review] Deriving the widening pair list from its owner is impossible and would withdraw the pin

- **Finding:** `[AMBIGUOUS_REQUIREMENT]` BLOCKER. The scenario required one production item of shape
  `fn(&DataType, &DataType) -> Option<DataType>` and required `supported_relaxation_pairs` to be
  derived from it. A function cannot be enumerated, and rows 9 to 12 of the recorded table are
  parameterized, so no representative pair derives from them. Worse, a generated list would assert
  only what the generator accepts, withdrawing the guarantee
  `arrow_castability_pins_every_supported_relaxation_pair` exists to hold.
- **Direction change:** The clause now requires the 17-entry list to STAY the concrete pin and to be
  asserted AGAINST the owner, with a curated refusal set returning no answer and a table row lacking
  an owner rule failing the suite. The two are checked against each other rather than one generated
  from the other. Task 2.1, the delta's Background, and decision [5] are restated to match.
- **Promotes to ADR:** no

### [plan-review] The `s3a` scheme is an addition beyond issue #407

- **Finding:** `[SCOPE_CREEP]` ADVISORY. `vs-adapter/connection-credentials-direct-storage` accepts
  `s3a` alongside `s3` and `abfss`, while issue #407 names only `s3://` and `abfss://`. Defensible,
  but untraceable as written.
- **Direction change:** Decision [12] gains a consequence recording `s3a` as a deliberate addition
  and naming Hadoop-written configurations as the reason, so a reader does not read it as accidental
  widening.
- **Promotes to ADR:** no

### [plan-review] Mixed timestamp units fail the fold with no operator-facing warning

- **Finding:** `[COMPLETENESS_GAP]` ADVISORY. The recorded relaxation set holds no
  timestamp-to-timestamp row, so two files declaring one column `TIMESTAMP(MICROS)` and
  `TIMESTAMP(NANOS)` fail the whole refresh. Mixed units across writers are common, and no scenario,
  manual-test row, or documentation line warned about it.
- **Direction change:** Task 6.7 now requires the `docs/catalogs.md` recipe to name the mixed
  timestamp-unit case as a limitation, with `MERGE_SCHEMA = 'FALSE'` stated as the workaround.
- **Promotes to ADR:** no

### [plan-review] A named coverage test was built by no task

- **Finding:** `[TRACEABILITY_GAP]` ADVISORY. § Scenario Coverage named
  `stale_declaration_decides_the_emitted_width`, which appeared in neither task 6.3's nor task 6.4's
  enumerated test list.
- **Direction change:** Added to task 6.3's list.
- **Promotes to ADR:** no

### [plan-review] The Arrow tag string crosses a crate boundary nothing enforces

- **Finding:** `[INFORMATION_LEAKAGE]` ADVISORY. `lakehouse-catalog` holds
  `ColumnSourceType::Parquet` carrying a tag whose vocabulary lives in `lakehouse-engine` and whose
  validity the catalog crate cannot check, because its manifest prohibition keeps `arrow` out. The
  alternatives are worse, so this is a cost to name rather than a design to reverse. It is also the
  mechanism through which the tag-vocabulary blocker stayed silent.
- **Direction change:** `vs-adapter/catalog-crate-public-surface-extensions` now requires the
  engine-side tag renderer and parser to be the ONLY producer and consumer of that variant's string,
  and requires a test that every tag the direct-storage client emits parses back to the Arrow type
  it was rendered from.
- **Promotes to ADR:** no

### [plan-review] The admission cap of 16 had no scheduled revisit

- **Finding:** `[TACTICAL_SHORTCUT]` ADVISORY. The cap was recorded as a conservative starting value
  to revisit with measurements, with no issue tracking that revisit.
- **Direction change:** Folded into the untracked-gap note added to `plan.md` § Impact, so the
  admission cap and the file-count bound are opened as ONE follow-up issue before merge rather than
  left to memory. Decision [16] states why they share an issue.
- **Promotes to ADR:** no

### [plan-review] Three Background lines no scenario step depended on

- **Finding:** `[IMPLEMENTATION_LEAKAGE]` ADVISORY. `vs-adapter/direct-storage-table-planning` §
  Background carried a milestone-ordinal sentence and a table-format-tag bullet whose only consumers
  are scenarios in two other specs. `vs-adapter/direct-storage-properties` § Background stated that
  the three properties are ignored rather than rejected under the other two kinds, which no scenario
  step tested.
- **Direction change:** Both leaked lines deleted from `direct-storage-table-planning`, since
  `catalog-crate-public-surface-extensions` already carries the table-format-tag clause. In
  `direct-storage-properties`, the cross-kind fact is kept and made load-bearing by a new clause in
  § An unparseable MERGE_SCHEMA or HIVE_PARTITIONING value is rejected, never defaulted.
- **Promotes to ADR:** no

### [plan-review] Governed prose broke four writing guardrails

- **Finding:** `[PROSE_BLOAT]` ADVISORY. Em dashes in `plan.md`'s Goals and Non-Goals bullets,
  semicolons at six `decision-log.md` lines, two § Context sentences over the 25-word cap joining
  two ideas with "and", and "behaviour" where the same document writes "behavior".
- **Direction change:** Em dashes replaced with colons, both § Context sentences split into one idea
  each, "behaviour" corrected, and each of the six semicolons replaced with a period and a new
  sentence.
- **Promotes to ADR:** no

### [plan-review] The reinstated two-table join clause named no fixture and asserted an unobservable property

- **Finding:** `[AMBIGUOUS_REQUIREMENT]` BLOCKER, round 2. The join clause's GIVEN named only "a
  second fixture directory whose rows share a join key with it", with no path, no file count, and no
  join-key column, while every other fixture in the plan carries all three. No task created it. The
  clause also required the live test to prove that both legs resolve through one shared
  admission-limited object store, which Exasol SQL exposes no way to observe, so
  `two_table_join_resolves_both_legs_through_one_store` would have passed while asserting only row
  equality.
- **Direction change:** The GIVEN now names `s3://warehouse/direct/event_labels/`, one Parquet file,
  the columns `EVENT_ID` and `LABEL`, and the `events/` fixture's own `EVENT_ID` column as the join
  key. The join clause asserts row equality against the equivalent unpushed join and one
  `EXPLAIN VIRTUAL` pushdown request naming both tables. A further clause forbids asserting the
  one-shared-store property here and names its two unit owners,
  `one_session_or_store_per_request_serves_every_leg` and
  `one_admission_limited_store_serves_the_whole_call`. The test is renamed
  `two_table_join_matches_the_unpushed_answer_in_one_request` in task 6.4 and in § Scenario
  Coverage, and task 6.4 now creates the fixture.
- **Promotes to ADR:** no

### [plan-review] One shared fixture root made four E2E scenarios unsatisfiable together

- **Finding:** `[REQUIREMENT_CONFLICT]` BLOCKER, round 2. The incompatible-pair fixture sat at
  `s3://warehouse/direct/incompatible/` and its scenario requires every enumeration of that root to
  FAIL. Enumeration walks every first-level directory of the base path, and the mixed-type,
  widening, `MERGE_SCHEMA = 'FALSE'`, and Delta-caveat scenarios each create a virtual schema over
  that same root and require it to succeed. The suite could not create one working virtual schema.
- **Direction change:** The incompatible-pair fixture moves to the isolated base path
  `s3://warehouse/direct_incompatible/incompatible/`, with its virtual schema created over
  `s3://warehouse/direct_incompatible/`. A Background bullet states the general rule: a fixture whose
  scenario requires a failed enumeration MUST sit under a base path no passing scenario shares. Every
  remaining GIVEN names its base path literally instead of writing "that base path", including the
  parity scenario of `e2e-harness/direct-storage-e2e-properties`. Task 6.3 carries both base paths.
- **Promotes to ADR:** no

### [plan-review] The tag-vocabulary widening contradicted the recorded Delta type-mapping spec

- **Finding:** `[REQUIREMENT_CONFLICT]` BLOCKER, round 2. `specs/vs-adapter/delta-type-mapping/
  spec.md` states as fact that the shared tag vocabulary has no `int8` or `int16` entry, and pins
  `byte` and `short` to the `int32` tag for that reason. Task 3.5 adds exactly those two entries, and
  no delta in this plan superseded the recorded premise. The recorded rationale is also wrong against
  the code: `compatible_exasol_type` returns `DECIMAL(3,0)`, `DECIMAL(5,0)`, and `DECIMAL(10,0)` for
  `Int8`, `Int16`, and `Int32`, which the recorded mapping table itself already states.
- **Direction change:** A new CHANGED delta,
  `specs/_plans/add-direct-storage-catalog-kind/vs-adapter/delta-type-mapping/spec.md`, supersedes
  the premise in its Background and carries the supersession normatively in a DELTA:CHANGED block on
  the native-type scenario. Delta's answer is unchanged: `byte` and `short` keep the `int32` tag,
  restated as the Delta reader's own deliberate choice rather than as the absence of a tag, with the
  shared-shape claim deleted and no declared Exasol type and no emitted value changed. The feature
  row is added to `plan.md` § Features, the delta is named in group C's Knowledge column and in task
  3.5, and § Scenario Coverage maps the amended scenario to the existing pin
  `every_natively_representable_delta_type_maps_to_its_own_arrow_tag`.
- **Promotes to ADR:** no

## Cleanup Pass

The requester ordered one cleanup pass after round 2, outside the adversarial review loop. It
changed one promotion flag and the governed prose of this plan. It re-litigated no scope, content,
or correctness decision of either round, and it touched no code.

### [cleanup] Decision [1] was over-promoted to ADR and is demoted

- **Finding:** `[ADR_OVERPROMOTION]`. Decision [1], "One shared directory seam answers 'which files
  and what schema' for both callers", carried an ADR promotion flag of yes. The decision is
  feature-scoped. It is an internal module-design choice for this feature's seam,
  `adapter/parquet_directory.rs`, justified by two follow-on issues that extend the same feature
  area, #408 and #412. It binds nothing outside the direct-storage kind.
- **Direction change:** Decision [1] now carries a promotion flag of no. Its Rationale states why
  it stays plan-local. The decision entry itself is kept, because it remains a real design decision
  worth recording in this plan. Decisions [2] and [3] keep their flag of yes. Decision [2] sets the
  crate-placement precedent for any future catalog kind. Decision [3] invokes the project-wide
  process-convention override and binds every future plan in this repository.
- **Promotes to ADR:** no

### [cleanup] Both review rounds scored Prose Quality without reading the spec deltas

- **Finding:** `[PROSE_BLOAT]`. Round 1 recorded a `[PROSE_BLOAT]` advisory against `plan.md` and
  `decision-log.md` alone. Round 2 recorded none. Neither round swept the 20 spec delta files,
  although those files were explicit review input. Live violations remained in both swept files and
  in the unswept deltas: em dashes in six spec descriptions and Background bullets, semicolons in
  governed prose across eleven files, and sentences over the 25-word cap joining two or three ideas
  with `and`, `which`, or `while`.
- **Direction change:** One prose sweep ran over `plan.md`, `decision-log.md`, and all 20 spec
  deltas, against `/speq:writing-guardrails`. Every em dash and semicolon in governed prose is
  replaced. Over-length and multi-idea sentences are split. British `behaviour` is corrected to
  `behavior` in governed prose. Gherkin steps, table cells, ASCII diagrams, delta markers, and RFC
  keyword casing are untouched, and no normative clause changed what it asserts.
- **Promotes to ADR:** no
