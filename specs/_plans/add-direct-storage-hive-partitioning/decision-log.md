# Decision Log: add-direct-storage-hive-partitioning

## Interview

**Q:** Should the spec deltas be one combined feature spec for discovery and pruning, or two
separate feature specs, mirroring the Iceberg and Delta split of table planning and file pruning?
**A:** One new feature spec covering both (`vs-adapter/direct-storage-hive-partitioning`). Discovery
and pruning ship together in one issue, and pruning has no existence without discovery. The
existing `parquet-directory-seam`, `direct-storage-table-planning`, and `direct-storage-properties`
specs get small delta edits that drop the "until #408" language and wire the new behavior in.

**Q:** Should any "deliberate decision" callout (always `VARCHAR`, case-insensitive collision fails
the refresh, `MERGE_SCHEMA = 'FALSE'` divergence degrades to NULL) be promoted to an ADR?
**A:** No ADR for this plan. These follow from recorded stances (no type inference from paths,
`MERGE_SCHEMA` correctness over performance). Keep the rationale inline in the feature spec.

**Q:** Does the plan-time absent-column fill re-derive the table's schema from whichever files a
query's pruning happens to keep, or does it read the table's own already-fixed `REFRESH`-time
schema?
**A:** It reads the table's own already-fixed `REFRESH`-time schema, echoed per query in the
pushdown request's `involvedTables[].columns`. `adapter/pushdown/support.rs::column_types` already
reads it this way. Decision [2] and the matching feature-spec scenario state
the source explicitly, scoped to an absent column, so the wording cannot be read as re-deriving the
table's schema from a query's surviving files.

**Q:** Should partition pruning cover only equality-family predicates (`=`, `<>`, `IN`, `IS NULL`,
`IS NOT NULL`), declining range predicates (`<`, `<=`, `>`, `>=`, `BETWEEN`) to Exasol, given the
unverified assumption about Exasol's `VARCHAR` comparison order?
**A:** No. Range predicates are in scope (decision [3]). The ordering assumption is verified live by
this plan's own E2E coverage (task 2.7), against a fixture whose values rank differently under byte
order than under case-insensitive or locale order, rather than treated as a reason to decline scope.

**Q:** How should a partition key that folds onto a Parquet column of the same identity be
resolved: fail the refresh, or let the directory value override the file's stored value, matching
common Hive-writer practice?
**A:** The directory value overrides the file's stored value when EVERY file carrying that column
also carries the key's directory segment (decision [7]). A file carrying the column but missing the
key's segment, while another file of the same table declares the key, FAILS `CREATE`/`REFRESH`
naming the column, the key, and that file's path. Two DIFFERENT keys folding to the same name fail
the refresh.

## Design Decisions

### [1] Partition pruning runs inside the seam, before footer reads

- **Decision:** `resolve_parquet_directory` takes a file-keep predicate over partition values. It
  applies the predicate after key declaration and before any footer read. The planning reader
  builds the predicate. Enumeration passes keep-all.
- **Alternatives:** Prune the returned file list in the reader after the fold. Rejected: every
  footer is still read, and the issue ships pruning to bound that cost.
- **Rationale:** The seam stays free of Exasol filter JSON. Pruning still precedes the footer
  reads.
- **Consequences:** The plan-time fold covers kept files only (see [2]). `SampleOneFile` keeps
  sampling the first unfiltered file, so both paths still sample one file, at a cost of at most one
  extra footer read.
- **Promotes to ADR:** no

### [2] Declared columns missing from a pruned fold are filled from the table's already-fixed schema, never re-derived

- **Decision:** `TableScanResolver::resolve` receives the request's full column set from Exasol's
  pushdown JSON (`involvedTables[].columns`): the table's schema exactly as it was fixed at the
  last `REFRESH`, echoed verbatim on every pushdown request, not anything computed from the files a
  query happens to keep. Each of those declared columns absent from the fold and the partition
  columns becomes a nullable logical field typed by `exasol_type_to_arrow`, with `Utf8` as the
  fallback.
- **Alternatives:** Leave the logical schema at the fold. Rejected: a query projecting a column
  that only pruned files carry fails in the scan's projection.
- **Rationale:** Plan-time pruning narrows which files are read, never what an ABSENT column's
  declared type is. An absent column reads NULL on every kept row. Its type still comes from the
  fixed declaration Exasol already holds, so aggregates and comparisons stay typed as Exasol
  declared them. A PRESENT column is a different case: its Arrow type still comes from the kept
  files' fold, per the recorded `vs-adapter/parquet-directory-seam` scenario "The declaration
  decides the emitted width and the footer decides the structure", which already covers file
  pruning for present columns.
- **Promotes to ADR:** no

### [3] Pruning evaluates equality-family AND range nodes, as sets of reachable truth values

- **Decision:** `=`, `<>`, `IN`, `IS NULL`, `IS NOT NULL`, `<`, `<=`, `>`, `>=`, and `BETWEEN` of a
  partition column against non-empty string literals evaluate exactly per file, comparing values
  with Rust's native string ordering (byte/codepoint order). Every other node is
  `{TRUE, FALSE, NULL}`. A file is kept iff TRUE is reachable.
- **Alternatives:** Mirror `delta_predicate.rs` (drop, forfeit, exact flag). Rejected: two rule
  sets for one soundness argument. Equality-family only, declining ranges to Exasol. Rejected:
  range pruning is a real cost win on partitioned tables and this project's verification
  rule does not block adding it, only assuming it without checking.
- **Rationale:** Partition values are constant per file, so a partition-only node is exact.
  Treating any other node as every truth value is sound by construction. Whether byte-order string
  comparison matches Exasol's own `VARCHAR` ordering is verified LIVE by the range-pruning E2E test
  (task 2.7), against a fixture (`REGIONS`, task 2.6) whose values rank differently under byte order
  than under case-insensitive or locale order, so the test can actually falsify a mismatch rather
  than pass under any ordering. This is not a fact taken from documentation or memory.
- **Promotes to ADR:** no

### [4] Value normalization: empty and default read NULL, deepest repeat wins, key verbatim

- **Decision:** `__HIVE_DEFAULT_PARTITION__` and an empty value are stored as no value. A key
  repeated within one path takes its deepest value. Only the value is percent-decoded.
- **Alternatives:** Keep an empty string as `Some("")`. Rejected: the scan already reads an empty
  partition value as NULL (`scan/partition_values.rs`), so pruning would evaluate a different value
  than the scan emits.
- **Rationale:** One representation for pruning and scan. Deepest-wins keeps the seam's current
  behavior. The issue specifies decoding for values only.
- **Promotes to ADR:** no

### [5] Direct-storage file entry paths are percent-encoded

- **Decision:** `file_entry` percent-encodes each relative path segment for the characters that do
  not survive `ListingTableUrl::parse`.
- **Alternatives:** none. The scan builds each `ObjectMeta` through `ListingTableUrl::parse`, which
  percent-decodes, so a raw `region=a%2Fb` key would address `region=a/b`.
- **Rationale:** The issue's URL-encoded fixture depends on it. The same defect affects any
  direct-storage key holding `%`, `#`, or `?`.
- **Promotes to ADR:** no

### [6] One derivation site for the seam's two switches

- **Decision:** `DirectStorageProperties::directory_options()` builds `DirectoryOptions` for both
  paths, replacing the two `MergeMode::for_merge_schema` call sites.
- **Alternatives:** Thread a second `bool` beside `merge_mode` through every carrier. Rejected: two
  parallel fields at four carriers, and two derivation sites that could diverge.
- **Rationale:** The seam still names no property. One carrier field replaces `merge_mode`.
- **Promotes to ADR:** no

### [7] A partition key that names a Parquet column overrides it only when every file carrying that column carries the segment: a mixed layout fails, and two colliding keys fail

- **Decision:** When a declared partition key folds (uppercase) onto a Parquet column's name, and
  EVERY file carrying that column also carries the key's directory segment, the seam drops the
  Parquet column from `fold_schemas`'s output rather than adding it: the key's own nullable `Utf8`
  partition column is the only column of that name, and its value always comes from
  `FileEntry.partition_values`, never from the Parquet reader. A file's own path carries the segment
  when its pre-fill raw carried-keys set from task 1.2 holds the key, which is true for
  `key=__HIVE_DEFAULT_PARTITION__` and `key=` (empty value) as well as any other value, and false
  only when the path has no `key=` segment at all. When at least one file carrying that column has
  NO `key=` segment, while the key is declared from another file, `CREATE`/`REFRESH` FAILS naming
  the colliding column, the key, and the path of one such segment-less file. That file has neither a
  directory value nor permission to fall back to its own stored value. Two DIFFERENT declared keys
  that fold onto each other (e.g. `Year=` and `year=`) FAIL the refresh: neither spelling is a
  file's own stored value, so there is no comparable precedent to resolve between them.

  These three checks are bounded by which files' footers and paths the merge mode makes visible.
  Under `MERGE_SCHEMA` TRUE every kept file's footer is read, so all three checks run over the whole
  table. Under `MERGE_SCHEMA = 'FALSE'` only the sampled file's footer is read and only the sampled
  file's own path is declared, so the override check and the segment-less-file check both run
  against that one file alone, and the two-colliding-keys check can only ever see spellings that
  appear within that single file's own path. A file this mode never samples that carries the
  identical problem goes undetected, matching the one-layout precondition `MERGE_SCHEMA = 'FALSE'`
  already documents.
- **Alternatives:** Fail the refresh for the key-vs-column case too. Rejected: a Hive-style writer
  commonly encodes a partition value both in the directory and inside the file, and failing refresh
  for a table shaped exactly as common tooling produces it is unnecessarily strict. Read a
  segment-less file's `K` as NULL rather than failing. Rejected: no such precedent exists (see
  Rationale), the rule would silently discard real data such as a Spark backfill file with no
  partition directory, and an equality filter on `K` would then silently prune those files with no
  error. Read a segment-less file's own stored value. Rejected: it needs a per-file
  partition-or-file binding in `scan/partition_values.rs`, which conflicts with this plan's
  no-scan-change Non-Goal, and it would also mean deleting the last `key=` directory silently
  changes an already-returned value. Suffix one of the two colliding columns for the key-vs-column
  case. Rejected: Polars considered and rejected this for the identical collision, because a filter
  naming the ambiguous column would not say which one it meant
  (https://github.com/pola-rs/polars/issues/12036), a concern this design sidesteps by dropping the
  Parquet column outright rather than keeping both under any name. Under `MERGE_SCHEMA = 'FALSE'`,
  always read the column NULL rather than checking the sampled file's own path. Rejected: this
  reproduces the outcome already rejected above for a segment-less file, a Spark backfill file's
  column silently reads NULL and an equality filter on it silently drops the file, for exactly the
  one file `'FALSE'` happens to sample. The chosen rule needs no extra footer read beyond the one
  `'FALSE'` already takes, and matches DuckDB's own default binding, which checks only the bound
  file's own path.
- **Rationale:** DuckDB's `MultiFileReader::BindOptions`
  (`src/common/multi_file/multi_file_reader.cpp`, current main, lines 316-337) overrides a column
  only when every file carries the first file's keys, and fails any file whose keys differ from the
  first file's, naming the file, the other file, and the missing key (lines 357-359 apply the
  override once that check passes). This plan keeps that override and additionally unions key sets
  DuckDB rejects outright, such as the `mixed/` layout. This plan fails only a file that stores the
  colliding column without the segment, not every file whose key set differs from the first file's.
  This engine's `VARCHAR`-only partition typing (no type inference from paths) already matches
  DuckDB forcing the hive-side type over the file's own type in the collision case. Under
  `MERGE_SCHEMA = 'FALSE'`, checking only the sampled file's own path also matches DuckDB's default
  binding, which checks the bound file's path alone rather than reading every file's footer to
  confirm a table-wide guarantee.
- **Consequences:** `fold_schemas` needs a set of "keys declared so far" to check a folded column's
  name against, in addition to today's collision check between columns, and needs each file's
  pre-fill raw carried-keys set (task 1.2), not the post-fill map (task 1.3), so a
  `key=__HIVE_DEFAULT_PARTITION__` or `key=` file is never mistaken for a segment-less file. The E2E
  collision fixture splits in two, each under its own base path: one table where every file carries
  the segment, asserting the override; one table with a segment-less file, asserting the
  `CREATE`/`REFRESH` failure naming the column, the key, and the file's path. A new unit test covers
  the `MERGE_SCHEMA = 'FALSE'` sampled-file-missing-segment failure. This is a cross-engine-
  compatibility stance about data precedence (directory beats file, for a uniformly laid-out table)
  that tables will come to depend on.
- **Promotes to ADR:** no

## Review Findings

Round-1 `/speq:plan-review` findings and their resolution.

### [plan-review] A segment-less file's NULL read rested on a DuckDB precedent that does not exist

- **Finding:** `[UNSTATED_ASSUMPTION]` BLOCKER, escalated HUMAN. The plan ruled that a file storing
  a column colliding with a declared key, whose own path lacks that key's segment, reads the column
  NULL and never falls back to its stored value, citing DuckDB's `MultiFileReader::BindOptions` as
  precedent. DuckDB does not support that layout: `BindOptions` throws a hive-partition-mismatch
  error for any file lacking a key another file declares, and overrides the value only when every
  file carries the segment. The cited `duckdb#24555` and `duckdb#24752` establish no more than
  that. The NULL rule would silently discard real data (e.g. a Spark backfill file with no
  partition directory) with no error, and an equality filter on the column would then silently
  prune those files.
- **Resolution:** Resolved. Human decision: a file carrying every declared key's segment still gets
  the directory-value override (unchanged). A file missing the segment, while the key is declared
  from another file, now fails `CREATE`/`REFRESH VIRTUAL SCHEMA` naming the column, the key, and
  that file's path, matching DuckDB's actual mismatch behavior. Updated: the collision scenario in
  `vs-adapter/direct-storage-hive-partitioning/spec.md` (split into an override scenario and a new
  failure scenario), decision [7] (Decision, Alternatives, Rationale, Consequences), `plan.md` task
  1.4, task 2.6 (fixture split into `collision_override` and `collision_missing_segment`), task
  2.7, the Impact bullet, and the Consequences table.

### [plan-review] The range-pruning E2E fixture could not falsify the VARCHAR-ordering claim it was cited for

- **Finding:** `[UNSTATED_ASSUMPTION]` BLOCKER. The only range fixture values (`2024`, `2025`,
  `2026`) are equal-length ASCII digits, so byte order, codepoint order, case-insensitive order, and
  every locale collation rank them identically. The E2E test could pass under any ordering, so it
  did not verify the load-bearing claim that Rust's byte-order string comparison matches Exasol's
  own `VARCHAR` order. A declined filter is self-applied by Exasol per
  `vs-adapter/pushdown-declined-filter-self-apply`, so a mismatch would silently drop rows Exasol
  itself would have kept.
- **Resolution:** Resolved. Added a `REGIONS` fixture (`region=B`, `region=a`, `region=é`) whose
  values rank differently under byte order than under case-insensitive or locale order. Task 2.7
  now runs `WHERE REGION > 'Z'`, `WHERE REGION < 'z'`, and `WHERE REGION BETWEEN 'B' AND 'a'`,
  in-process and through `EXPLAIN VIRTUAL`, asserting the kept file set against what Exasol itself
  selects natively on the same connection, plus one variant that declines to Exasol's outer `WHERE`
  self-apply. Task 2.2 states that a mismatch stops implementation and returns the plan to
  planning. The feature spec's pruning scenario and decision [3]'s Rationale are reworded to state
  the requirement without claiming a specific test proves it by itself.

### [plan-review] Three recorded scenarios contradicted the plan with no delta

- **Finding:** `[REQUIREMENT_CONFLICT]` BLOCKER. The recorded `vs-adapter/parquet-directory-seam`
  union scenario ("The folded column set is the union of the files' column sets") and widening
  scenario ("Footers fold into one schema under the proven-castable widening pairs"), and the
  recorded `vs-adapter/direct-storage-properties` MERGE_SCHEMA scenario, state rules this plan's
  tasks 1.3, 1.4, and 1.5 contradict (dropping a column from the union, never reaching the widening
  check for a dropped column, and reading only kept files' footers at pushdown), with no delta
  recorded for any of the three.
- **Resolution:** Resolved. Added `DELTA:CHANGED` blocks for both `parquet-directory-seam`
  scenarios, exempting a column whose uppercase fold equals a declared partition key from the
  union, the collision failure, and the widening check (citing
  `vs-adapter/direct-storage-hive-partitioning`), and stating the seam's schema ends with the
  partition columns. Added a `DELTA:CHANGED` block for the `direct-storage-properties` MERGE_SCHEMA
  scenario stating "every KEPT data file's footer" at pushdown, citing the seam's file-keep
  predicate.

### [plan-review] The new feature spec's Background carried rationale no step depended on

- **Finding:** `[IMPLEMENTATION_LEAKAGE]` BLOCKER. Background bullet 4 (Hive-writer practice,
  DuckDB code, Polars' rejected suffix) carried decision rationale with no dependent
  GIVEN/WHEN/THEN step, and part of it was the now-corrected DuckDB claim. Bullet 2's third
  sentence (sharing an outcome with sibling pruning specs, none of their mechanism) had no
  dependent step. Bullet 1 (seam ownership) had no dependent step either.
- **Resolution:** Resolved. Deleted bullet 4; its corrected content lives only in decision [7].
  Deleted bullet 2's third sentence. Folded bullet 1 into the union scenario as an explicit step
  ("table enumeration and query planning SHALL declare the same partition columns, because both
  obtain them from `vs-adapter/parquet-directory-seam`"). Kept the Iceberg/Delta
  specification-check bullet unchanged.

### [plan-review] Decision [7] promoted itself to an ADR for a local data-precedence rule

- **Finding:** `[ADR_OVERPROMOTION]` BLOCKER. Decision [7] marked itself as ADR-worthy for a
  data-precedence rule scoped to one catalog kind's partition discovery, though the interview
  recorded "No ADR for this plan" and the behavior is already recorded normatively in the collision
  scenario. The `/speq:planning` gate reserves ADR promotion for genuinely new project-wide
  constraints.
- **Resolution:** Resolved. Decision [7]'s ADR-promotion field is set to the non-promoting value
  and the ADR sentence is deleted from its Consequences line.

Round-2 `/speq:plan-review` findings and their resolution.

### [plan-review] The missing-segment failure could not run under MERGE_SCHEMA = 'FALSE'

- **Finding:** `[REQUIREMENT_CONFLICT]` BLOCKER, escalated HUMAN. The override and the
  missing-segment failure both need a file's footer to know whether it carries the colliding
  column, and `MERGE_SCHEMA = 'FALSE'` reads only one footer, the sampled file's. The seam sorts
  paths by byte order, so for a table holding `k=1/p1.parquet` and `p2.parquet` (no `k=` segment,
  both carrying `K`), the sampled file is `p1.parquet`. The seam never reads `p2.parquet`'s footer
  and fills its `k` with `None`, so `p2.parquet`'s stored `K` reads NULL with no error and
  `WHERE K = '1'` silently prunes it: the outcome the user already rejected under `MERGE_SCHEMA`
  TRUE. Neither the feature spec, task 1.4, nor decision [7] named a `MERGE_SCHEMA = 'FALSE'` rule,
  and no test covered a collision under `'FALSE'`. Two options were put to the user: (a) fail
  `CREATE`/`REFRESH` only when the SAMPLED file itself lacks the segment, a plain path check on the
  one footer already read, matching DuckDB's own default-binding behavior. (b) always read NULL
  under `'FALSE'`, matching the general "'FALSE' precondition: divergent layout degrades to NULL,
  never fails" text literally, but reproducing the rejected outcome for exactly the sampled file.
- **Resolution:** Resolved. Human decision: option (a). Under `MERGE_SCHEMA = 'FALSE'` the
  missing-segment check and the override check both run against the sampled file's own path and
  footer alone: a sampled file storing the colliding column without its own `key=` segment fails
  `CREATE`/`REFRESH` the same way as under `MERGE_SCHEMA` TRUE. An unsampled file carrying the
  identical problem goes undetected under `'FALSE'`, which is `'FALSE'`'s existing accepted
  tradeoff, not a new gap. The two-colliding-keys check is scoped to `MERGE_SCHEMA` TRUE. Under
  `'FALSE'` it can only see spellings carried by the sampled file's own path. Updated: the three
  collision scenarios in `vs-adapter/direct-storage-hive-partitioning/spec.md`, decision [7]
  (Decision, Alternatives, Rationale, Consequences), `plan.md` task 1.4, and the `docs/catalogs.md`
  paragraph in task 2.8. Added a unit test for the `'FALSE'`-mode sampled-file-missing-segment
  failure.

### [plan-review] The planned per-file state could not tell a default-valued segment from a missing one

- **Finding:** `[COMPLETENESS_GAP]` BLOCKER. Task 1.3's fill step wrote `None` into every file's
  `partition_values` map both for a path lacking the `key=` segment entirely and for
  `key=__HIVE_DEFAULT_PARTITION__` or `key=` (a real, valid NULL value). Task 1.4's missing-segment
  check ran after that fill, so a check on the map's VALUE would wrongly fail `CREATE`/`REFRESH` for
  a common, valid `__HIVE_DEFAULT_PARTITION__` layout (the exact shape Spark writes for a NULL
  partition value), while a check on key PRESENCE never fired at all, since the fill makes every
  key present, silently reproducing the NULL-with-no-error outcome the user already rejected. No
  fixture or test covered a default-valued or empty segment on a colliding key.
- **Resolution:** Resolved. Task 1.2 now records, per file, the RAW set of keys its own path
  segments actually carry, before task 1.3's union-fill. That pre-fill set holds a key for
  `key=__HIVE_DEFAULT_PARTITION__` and `key=` (empty value) exactly as it does for any other value,
  and omits the key only when the path carries no `key=` segment at all. Task 1.4's missing-segment
  and override checks now test that raw pre-fill set, never the post-fill map value. Added the AND
  step to "A file missing the colliding key's segment fails the refresh" stating that a
  `key=__HIVE_DEFAULT_PARTITION__` or `key=` file carries the segment and reads `K` as NULL under
  the override rather than failing. Added both values as cases to the collision-drops-column unit
  test and to the missing-segment-fails unit test, the latter proving the filled map alone does not
  suppress the failure.

### [plan-review] The two collision fixtures shared one base path, so the override test could not pass and the failure test could not fail

- **Finding:** `[UNSTATED_ASSUMPTION]` BLOCKER, escalation MECHANICAL. Task 2.6 put
  `collision_override/` and `collision_missing_segment/` under one base path
  (`s3://warehouse/direct_hive_collision/`). Per the recorded `vs-adapter/direct-storage-table-discovery`
  rule, a first-level directory under the base path is a table, so both directories become tables of
  ONE virtual schema, and enumeration of `collision_missing_segment` fails by design. A failing
  table fails the whole `CREATE VIRTUAL SCHEMA` statement, per the suite's own recorded constraint
  (`tests/e2e_direct_storage_test.rs:521-522`: a failing fixture must not share a base path with any
  passing scenario), so the override test's "`CREATE VIRTUAL SCHEMA` succeeds" assertion would
  itself fail. Scoping either test with `NAMESPACE` does not fix it: it makes the collision
  directory itself the base path, which deletes the `k=1` key from the test entirely. Task 2.7 also
  asserted the missing-segment failure on `REFRESH VIRTUAL SCHEMA`, but a virtual schema whose
  `CREATE` already failed cannot be refreshed.
- **Resolution:** Resolved. Task 2.6 now gives `collision_missing_segment/` its own base path
  (`s3://warehouse/direct_hive_collision_missing/`) and its own CONNECTION, separate from
  `collision_override/` at `s3://warehouse/direct_hive_collision/`. Task 2.7's missing-segment test
  and Manual Testing row 6 now point at the new base with no `NAMESPACE`. The `REFRESH VIRTUAL
  SCHEMA` half of the missing-segment assertion is deleted from the E2E test. `REFRESH`'s use of the
  same seam is covered by the seam's own unit test instead, matching the existing CREATE-only
  precedent. The `HIVE_PARTITIONING = 'FALSE'` variant now runs over both bases separately, so
  `collision_missing_segment` also proves it succeeds once hive partitioning, and so the key
  collision, is turned off.
