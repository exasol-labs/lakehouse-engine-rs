# Decision Log: add-glue-catalog-kind

## Interview

**Q:** What is the guiding rule when Glue behavior could differ from Unity or direct storage?
**A:** Behavior depends on what the source can tell us, not on which catalog it is. Each rule has one shared implementation. Differences exist only where the spec states their cause.

**Q (D1):** How does the adapter report a table it skips?
**A:** In `ADAPTER_NOTES` only, under a new key, for every catalog kind. CREATE never fails on an empty or all-skipped listing.

**Q (D2):** How are Glue partitions pruned?
**A:** One pruner that takes each partition column's declared type. Direct storage passes utf8 and keeps string comparison. Unity and Glue pass their declared types. No `ScanSpec` or `FileEntry` change. Delta and Iceberg keep their own pruning. No predicate goes into Glue's `Expression`.

**Q (D3):** How are Glue types and `binary` handled?
**A:** One classifier: a Glue Hive type parser feeds `build_delta_table_schema`. `binary` is refused everywhere, citing #351. This is a deliberate breaking change for Iceberg and direct storage. Nested types stay JSON `VARCHAR(2000000)`.

**Q (D3 follow-up, binary reach, from plan review round 1):** The first draft refused more than `binary`. It refused Iceberg `fixed(L)`, and on direct storage every Arrow binary type. `parquet` 58.4.0 maps a UUID-annotated `FIXED_LEN_BYTE_ARRAY(16)` to `FixedSizeBinary(16)` (`src/arrow/schema/primitive.rs:348`). It maps an unannotated `BYTE_ARRAY` and the `ENUM`, `BSON`, `GEOMETRY`, and `GEOGRAPHY` annotations to `Binary` (`primitive.rs:280-300`). A direct-storage UUID column, a legacy Impala or Hive string, and a parquet-avro enum would therefore become refused. Should the refusal reach them?
**A:** Option (a). Refuse `binary` only where the source declares it: Iceberg `binary` and `fixed(L)`, and Delta, Unity, and Glue `binary` (Glue through the Hive `binary` type string). Iceberg `uuid` stays allowed. Direct storage keeps its current behavior with no refusal, because a Parquet footer gives no declaration to act on. The refusal text names the declared type. Direct-storage binary handling is an open question under #351.

**Q (D3 second follow-up, binary reach, user revision):** Does the refusal reach every binary type on every source, direct storage included?
**A:** Yes. Every binary type is refused at plan time on every source, nested members included, with a reason naming the declared or physical type and #351. The real fix is #351, and the limitation is documented. Iceberg: `binary`, `fixed(L)`, `uuid`. Delta, Unity, and Glue: `binary`. Direct storage decides by what the file declares: a Parquet `ENUM` is text and is read as text, while an unannotated `BYTE_ARRAY`, `BSON`, `GEOMETRY`, `GEOGRAPHY`, and a `UUID` `FIXED_LEN_BYTE_ARRAY(16)` are refused. The breaking cases (legacy unannotated strings, UUIDs) are approved. This answer supersedes the D3 follow-up answer. The user supplied a live Iceberg baseline (plan.md § Spec compliance). The run-first task confirms the `ENUM` and unannotated calls before implementation continues.

**Q (type verification, user revision):** How is read-path type behavior proven?
**A:** Live, not by unit tests (`CLAUDE.md` § Verification discipline). A run-first task on unchanged `main` builds and runs a type-matrix E2E and records what it observes. A contradiction of the recorded type-mapping spec, or of the `ENUM` and unannotated calls, stops the run and returns the plan for review. Other bugs are listed as open questions with `(#TBD)` and are not fixed. The matrix stays in the suite for DIRECT_STORAGE, Iceberg REST, and Glue. It is table-driven and asserts the declared type and the values or the refusal text.

**Q (D4):** What happens with a non-Parquet partition?
**A:** It fails the query and names the partition.

**Q (D5/D8):** Which objects under a Glue location are data files?
**A:** One internal glob-pattern parameter on the shared listing: `**/*.parquet` for direct storage and Unity (unchanged), `*` for Glue. The `_`/`.` segment rule stays fixed. A pattern without `**` lists through a delimiter. A user-facing `FILE_PATTERN` property is `(#TBD)` and out of scope.

**Q (D5/D8 follow-up, zero-length objects, from plan review round 1):** The shared listing drops zero-length objects. `object_store` 0.13.2 `Path::parse` strips a trailing `/` (`src/path/mod.rs:186`), so `data_file_segments` already drops a directory marker at the listed location (`segments.last()?`). The remaining cases are Hadoop `<dir>_$folder$` sibling markers and truly empty files. Today a zero-length `*.parquet` object fails the footer read on direct storage and Unity. Should the rule apply only under `*`, or under every pattern?
**A:** Option (b). The rule applies to every pattern, including direct storage and Unity `**/*.parquet`. The user accepts this change for those sources.

**Q (D6):** Which Glue client?
**A:** `aws-sdk-glue = { version = "1.170", default-features = false, features = ["behavior-version-latest", "default-https-client", "rt-tokio"] }`, without `aws-config`. Never enable the SDK features `rustls` or `legacy-https-client`. The new AWS crates declare MSRV 1.94.1: fix the stale "MSRV 1.91.1" comment and consider bumping the workspace pins. `sigv4.rs` stays. Measure the `.so` size and confirm the SLC CA bundle.

**Q:** Does the plan slim `reqwest`?
**A:** Yes, in this plan: `reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls-native-roots", "http2"] }`. `lakehouse-engine` keeps `blocking`.

**Q (D7):** How is the kind gated end to end?
**A:** A CI job `e2e-glue` with `needs: [build-so]`, not in `release`'s needs. Update the comment at `ci.yml:639`. The suite panics, never skips. `make test-e2e-glue` sources `./test.env`. Fixtures are per run: database `lh_e2e_<run_id>` plus an S3 prefix, with an orphan sweep. A static-key IAM user scoped by ARNs.

**Q:** Which E2E configuration names?
**A:** `GLUE_ACCESS_KEY_ID` and `GLUE_SECRET_ACCESS_KEY` are secrets. `GLUE_REGION` and `GLUE_FIXTURE_BUCKET` are variables. Add placeholder lines to `test.env.example`. Who provisions the IAM user and bucket, who pays the AWS bill, and how fork PRs behave are open questions.

**Q:** What else from #410 stands?
**A:** Everything as written: the CONNECTION field table, §1 routing, §2 Hive parser, §3 partitions, §5 refused projection tables, §7 errors. Lake Formation, cross-account `CatalogId`, and assume-role are out of the first cut, stated as scoped exceptions.

## Design Decisions

### [1] One catalog-declared Parquet reader and one Iceberg planner serve Glue

- **Decision:** Glue Parquet tables use the Unity Parquet reader, generalized over a type source (Unity `type_json` or Glue Hive string) and a file source (table directory or Glue partitions). A Hive parser feeds the shared Spark classifier. Glue Iceberg tables use the one Iceberg planner, fed from `metadata_location` instead of REST `loadTable`.
- **Alternatives:** A `GlueParquetFormatReader` copy, rejected because two readers drift on schema authority and refusal rules. A second Iceberg path for Glue, rejected because it duplicates delete handling, name mapping, and pruning.
- **Rationale:** Differences exist only where the source differs: where types and files come from, and where the Iceberg metadata comes from.
- **Consequences:**
  - The shared partition predicate compares under declared types. Direct storage passes utf8 and keeps string comparison.
  - The shared listing takes a file pattern. Unity and direct storage pass `**/*.parquet`, and Glue passes `*`.
  - No predicate goes into Glue's `Expression`, because a second translator's error returns wrong rows under full delegation.
  - The plan adds no `ScanSpec`, `FileEntry`, or `LogicalField` field.
- **Promotes to ADR:** yes

### [2] [user-decision] Every binary type is refused on every source until #351

- **Decision:** Iceberg `binary`, `fixed(L)`, and `uuid`, Delta, Unity, and Glue `binary`, and each direct-storage column that a Parquet file declares binary are refused at plan time at every depth. The one cause text names the source's type and #351. A top-level Parquet `ENUM` column is text and reads as text. This supersedes the decision "Binary is refused where the source declares it, until #351".
- **Alternatives:** Refuse declared types only and leave direct storage unrefused, rejected by the user. Refuse by Arrow type alone, rejected because it refuses `ENUM`, which the Parquet spec defines as text. Keep the Iceberg text path, rejected because a `binary` value that is not valid UTF-8 fails the whole query and `fixed(L)` and `uuid` never read (live baseline).
- **Rationale:** No source renders binary faithfully before #351, and a plan-time refusal names the cause where a scan error does not.
- **Consequences:**
  - Breaking for an Iceberg `binary` column that holds UTF-8 text, and for direct-storage legacy strings and UUIDs (plan.md § Impact).
  - The listing still declares each column `VARCHAR(2000000)`.
  - A nested `ENUM` member is refused (#TBD). The JSON renderer reads the member's physical `Binary` type and would render hexadecimal, and decoding it as text needs a nested decode hint that this plan does not add.
- **Promotes to ADR:** no

### [3] The Glue client uses aws-sdk-glue without aws-config

- **Decision:** `aws-sdk-glue` with default features off, explicit CONNECTION credentials, and no ambient credential chain. The SDK features `rustls` and `legacy-https-client` stay off. `reqwest` moves to rustls with native roots, so the `.so` links no OpenSSL.
- **Alternatives:** A hand-written client on `reqwest` plus `sigv4.rs`, rejected because it lacks retry, error-code parsing, and clock-skew correction. `iceberg-catalog-glue`, rejected because it requires `aws-config` and loads only Iceberg tables.
- **Rationale:** A UDF must sign only with the CONNECTION's identity. Environment, profile, and instance-metadata credentials would silently change the identity.
- **Consequences:**
  - MSRV rises to 1.94.1, and every CI compiler must be at least that.
  - One TLS stack (rustls) serves the whole `.so`.
- **Promotes to ADR:** yes

### [4] Skipped tables are recorded in ADAPTER_NOTES for every catalog kind

- **Decision:** `SKIPPED_TABLES` holds a JSON array of `{table, reason}`, written on every CREATE and REFRESH. CREATE never fails because every table was skipped.
- **Alternatives:** Fail CREATE on an all-skipped listing, rejected in the interview (D1).
- **Rationale:** A skip reason is otherwise visible only through `SCRIPT_OUTPUT_ADDRESS`.
- **Promotes to ADR:** no

### [5] Per-run Glue E2E fixtures

- **Decision:** Each run owns its database and prefix, and a weekly sweep removes leftovers.
- **Alternatives:** A shared static fixture database, rejected because concurrent runs would race on it.
- **Rationale:** Follows the Azure gate (ADR 051).
- **Promotes to ADR:** no

### [6] A kept partition that cannot be read faithfully fails the query

- **Decision:** A kept ORC partition or a kept partition in another bucket fails the query, naming the partition. A pruned one does not.
- **Alternatives:** Skip it, rejected because a skipped partition returns wrong rows.
- **Rationale:** The store router registers one store per `scheme://host`. Cross-bucket reads are `(#TBD)`.
- **Promotes to ADR:** no

### [7] Zero-length objects are never data files

- **Decision:** The shared listing drops every zero-length object, under every pattern.
- **Alternatives:** Apply the rule only under the `*` pattern, rejected by the user in the D5/D8 follow-up.
- **Rationale:** A Hadoop `<dir>_$folder$` sibling marker and a truly empty file hold no rows. A directory marker at the listed location needs no rule, because `data_file_segments` already drops it. For direct storage and Unity, the rule is a deliberate, user-approved change: a zero-length `*.parquet` object formerly failed the footer read.
- **Promotes to ADR:** no

### [8] Hive timestamp maps to timestamp_ntz

- **Decision:** Hive `timestamp` parses to Spark `timestamp_ntz`.
- **Alternatives:** `timestamp` (UTC-adjusted), rejected because Hive timestamps carry no zone.
- **Rationale:** Matches the Hive 3 and Trino semantics of a zone-less timestamp. The Exasol declaration is the same for both Spark types (`unity_type_name_to_exasol`).
- **Promotes to ADR:** no

### [9] [user-decision] Type behavior is proven live by a run-first type matrix

- **Decision:** Task group 0 builds a table-driven type-matrix E2E and runs it on unchanged `main` before any behavior change. The matrix stays in the suite for DIRECT_STORAGE, Iceberg REST, and Glue.
- **Alternatives:** Unit tests only, rejected because `mapping_tests.rs` asserts only lookups, and the E2E suites cover only 12 types (Int32, Int64, Float32, Float64, Boolean, Utf8, Date32, Timestamp, Decimal128(10,2), List, Struct, Map).
- **Rationale:** `CLAUDE.md` § Verification discipline requires a claim about type behavior to be verified against a live Exasol.
- **Consequences:**
  - A contradiction of the recorded type-mapping spec, or of decision [2]'s `ENUM` and unannotated calls, stops the implementation and returns the plan for review.
  - Other bugs that the run finds are listed as open questions with `(#TBD)` and are not fixed in this plan.
  - The Glue matrix adds AWS cost to `e2e-glue` (plan.md open question 7).
- **Promotes to ADR:** no

## Review Findings

### [1] [plan-review] Zero-length rule widened past the D5/D8 answer

- **Finding:** Decision [7] dropped zero-length objects for direct storage and Unity, against the answer "unchanged". Its rationale was partly wrong: `data_file_segments` already drops a directory marker at the listed location.
- **Direction change:** The user chose option (b), recorded in § Interview. The rule applies to every pattern. Decision [7] and the plan.md Consequences row name the `_$folder$` and empty-file cases, and state the direct-storage and Unity change as user-approved.
- **Promotes to ADR:** no

### [2] [plan-review] Binary refusal reached undeclared types

- **Finding:** The plan refused Iceberg `fixed(L)` and every direct-storage Arrow binary type, which caught UUID, legacy-string, and enum columns the user never approved.
- **Direction change:** The user chose option (a), recorded in § Interview. `vs-adapter/binary-column-refusal` refuses only declared types and states the direct-storage gap as a scoped exception with its own scenario. The direct-storage refusal task, E2E fixture, and Impact sentence are removed. `binary_cause` takes the declared type. Open question 3 tracks direct storage under #351.
- **Promotes to ADR:** no

### [3] [plan-review] Task 6.5 declared a second SkippedTable

- **Finding:** `adapter/mod.rs:35` already imports `lakehouse_catalog::SkippedTable`, so a second struct fails with E0255.
- **Direction change:** Task 6.5 passes `&[lakehouse_catalog::SkippedTable]` to `build_adapter_notes` and builds each object from `catalog_identifier_string` and `skip_reason`.
- **Promotes to ADR:** no

### [4] [plan-review] Recorded Iceberg binary statements contradicted the refusal

- **Finding:** `vs-adapter/delta-type-mapping` and `datafusion-scan/nested-json-rendering` state that Iceberg binary renders as text or hexadecimal. A "SUPERSEDES" sentence in a new feature does not edit them.
- **Direction change:** Two `DELTA:CHANGED` Background deltas change only their Iceberg statements to cite `vs-adapter/binary-column-refusal`. The SUPERSEDES bullet is deleted. Both deltas are in plan.md § Features.
- **Promotes to ADR:** no

### [5] [plan-review] SKIPPED_TABLES conflicted with the recorded adapterNotes rules

- **Finding:** `vs-adapter/create-virtual-schema` and `vs-adapter/create-virtual-schema-adapter-notes` permit only `TABLE_MAP` and pushdown-read entries in adapterNotes.
- **Direction change:** The `create-virtual-schema-skipped-tables` feature is deleted. Its three scenarios are `DELTA:NEW` in `create-virtual-schema-adapter-notes`, whose description and Background name `SKIPPED_TABLES` as the one write-only diagnostic entry. A `DELTA:CHANGED` scenario in `create-virtual-schema` permits it. Task 6.6 measures the adapterNotes size limit and caps the list only if a limit exists.
- **Promotes to ADR:** no

### [6] [plan-review] Task 2.7 widened a source-text probe

- **Finding:** Adding `glue/*.rs` to `CATALOG_SOURCES` widens `include_str!` source matching, which ADR 091 forbids.
- **Direction change:** The `CATALOG_SOURCES` clause is deleted from the export task (now 2.8).
- **Promotes to ADR:** no

### [7] [plan-review] Backgrounds stated facts no scenario uses

- **Finding:** Seven Background lines carried SDK, history, IAM, crate-list, and scope facts that no step depends on.
- **Direction change:** Each line is deleted. The Lake Formation scope is in task 8.1's `docs/security.md` item. The Glue kind's IAM actions are in task 8.1's `docs/catalogs.md` item, and the harness policy is a `test.env.example` comment (task 7.4).
- **Promotes to ADR:** no

### [8] [plan-review] Binary refusal decision over-promoted

- **Finding:** Decision [2] is temporary until #351, and its rule lives in `vs-adapter/binary-column-refusal`.
- **Direction change:** Decision [2] is not promoted to an ADR.
- **Promotes to ADR:** no

### [9] [plan-review] Skipped-tables decision over-promoted

- **Finding:** Decision [4] is a user-visible feature contract, not an architecture decision.
- **Direction change:** Decision [4] is not promoted to an ADR.
- **Promotes to ADR:** no

### [10] [plan-review] Spec deltas compared against the pre-plan state

- **Finding:** Scenarios and descriptions used "unchanged", "before this feature", and "byte-identical to before", which lose meaning once merged.
- **Direction change:** Each phrase is restated as current behavior or cut. The Iceberg REST regression guard stays in task 5.3.
- **Promotes to ADR:** no

### [11] [plan-review] Two more Iceberg binary sentences contradicted the refusal

- **Finding:** The two `DELTA:CHANGED` Backgrounds of finding [4] still stated that the Iceberg reader refuses no column, and `nested-json-rendering` still opened its binary bullet with "MUST NOT change".
- **Direction change:** The `delta-type-mapping` bullet states that the Iceberg reader refuses a declared `binary` or `fixed(L)` column per `vs-adapter/binary-column-refusal`. The `nested-json-rendering` layer bullet states the Iceberg scan-time failure in the past tense, as the cause that feature removed. Its binary bullet opens with "Binary rendering is out of scope (#351)." A grep of both deltas for "EMPTY refused", "refuses none", and "refuses no" finds no hit.
- **Promotes to ADR:** no

### [12] [plan-review] The shared partition predicate bound the Delta and Iceberg readers

- **Finding:** The clause "every source SHALL call the ONE predicate" contradicted the recorded Delta kernel pruning and Iceberg `plan_files` pruning, which interview answer D2 keeps.
- **Direction change:** The clause in `partition-predicate-declared-types` names the direct-storage, Unity Parquet, and Glue Parquet readers only.
- **Promotes to ADR:** no

### [13] [plan-review] Absent CATALOG_KIND promised pre-feature output

- **Finding:** The recorded scenario required byte-identical pre-feature output under an absent `CATALOG_KIND`. This plan adds `SKIPPED_TABLES` and refuses a declared Iceberg `binary` column on that kind.
- **Direction change:** A `DELTA:CHANGED` block in `catalog-kind-selection` keeps the kind resolution, the grant-count, and the REST-path clauses as current behavior, and drops the pre-feature comparison. plan.md § Scenario Coverage maps the scenario to `absent_catalog_kind_resolves_iceberg_rest` and the two existing `client_tests.rs` session tests.
- **Promotes to ADR:** no
