# Decision Log: change-exasol-type-model

## Interview

Source: issue #584 and its last comment ("Spike results and decisions", branch `spike/584-exasol-type-model`, commit d097963c). The comment settles each design question, so no further questions were asked.

- Q: Where does `ExasolType` live? → A: module of `vs-expression`, re-exported by `lakehouse-engine`; no new crate
- Q: Unsupported types: `Unsupported` variant or real variants? → A: real variants INTERVAL, GEOMETRY, HASHTYPE, plus TIMESTAMP WITH LOCAL TIME ZONE with precision; `is_udf_emittable()` false for these four, so projection/aggregate/GROUP BY key of such a type declines to the wrapper
- Q: Lenient or strict parse of Exasol input? → A: strict: missing `selectListDataTypes`, incomplete or out-of-range type, unparsable virtual-table column type are errors; only default left is TIMESTAMP precision 3 (8.x omits it)
- Q: When does the outer SQL cast a merged value? → A: only when merging can change the type (SUM/COUNT/AVG merges, GROUP BY keys traveling as text); `VARCHAR(2000000)` stops meaning "unknown"
- Q: Typed NULL? → A: wrapper casts top-level `literal_null` to its declared type
- Q: DECIMAL validation? → A: `Decimal::new(p, s) -> Option`, `1<=p<=36`, `s<=p`; negative/out-of-range Arrow scale takes JSON VARCHAR fallback; only DECIMAL validated in constructors
- Q: TIMESTAMP precision? → A: `precision: u8`; 3 renders bare `TIMESTAMP` in Exasol dialect, DataFusion dialect renders `TIMESTAMP(3)` (fixes 8.x wrong results)
- Q: Split into PRs? → A: one PR (issue's earlier split dropped: item 2 cannot ship before item 1)
- Q: Spec impact? → A: narrow `vs-expression-translator-cast` refusal of INTERVAL/GEOMETRY/HASHTYPE/TSLTZ to the DataFusion dialect; change missing-precision and EMITS-precision rules; new scenarios for typed NULL, strict-input errors, 8.x `CAST(ts AS TIMESTAMP)` filter, `TIMESTAMP(p) WITH LOCAL TIME ZONE` cast; update two E2E tests

Follow-up after the plan review, with live repros on Exasol 2025.1.16 (spike build):

- Q: Exasol omits `characterSet` for UTF8 CHAR/VARCHAR in `selectListDataTypes` and involved-table columns; strict parse? → A: an absent `characterSet` means UTF8; the `selectListDataTypes` length check applies only when a `selectList` is present (`SELECT *` sends no `selectList` and one type per column)
- Q: Push a DATE, TIMESTAMP, or numeric value converted to text (implicit, as in `C_DATE LIKE '2024%'`, or explicit `CAST(... AS VARCHAR(n))`)? → A: never, in any position (filter, projection, GROUP BY key); Exasol formats it by session `NLS_DATE_FORMAT`, `NLS_TIMESTAMP_FORMAT`, and `NLS_NUMERIC_CHARACTERS`, which the request does not carry. Repros: with `NLS_DATE_FORMAT='DD.MM.YYYY'`, `C_DATE LIKE '2024%'` native 0 rows, VS 10; with `NLS_NUMERIC_CHARACTERS=',.'`, `CAST(C_DECIMAL_A AS VARCHAR(20)) LIKE '%.%'` native 0, VS 6; with default settings `CAST(C_TS AS VARCHAR(40))` native `2024-01-01 00:00:00.100000`, VS `2024-01-01T00:00:00.100`, so `LIKE '2024-01-01 %'` native 10, VS 0. A projected `CAST(C_DATE AS VARCHAR(10))` under `DD.MM.YYYY` returns `2024-01-01` through the VS (superseded by the second re-scope below)
- Q: Text-to-text CAST with a size or ASCII in the scan? → A: decline in the DataFusion dialect (Exasol applies it): native `CAST(c || 'é' AS VARCHAR(10) ASCII) IS NOT NULL` errors, VS returns 12 rows; native `CAST(c || 'abcdefghijkl' AS VARCHAR(5)) IS NOT NULL` errors with right truncation, VS returns 12. `VARCHAR(2000000)` UTF8 still pushes (superseded by the third re-scope below)
- Q: Users lose pushdown for date-as-text filters? → A: accepted and documented; `C_DATE >= DATE '2024-01-01' AND C_DATE < DATE '2025-01-01'` and `YEAR(C_DATE) = 2024` push down (verified with `EXPLAIN VIRTUAL`). `TO_CHAR` with an explicit format is a possible follow-up, out of scope (superseded by the second re-scope below)
- Q: This plan or a separate issue? → A: this plan (superseded by the second re-scope below)
- Q: Include BOOLEAN-to-text (scan renders `true`, Exasol `TRUE`)? → A: yes, it declines (superseded by the second re-scope below)
- Q: Decline computed non-text operands too (`(C_DECIMAL_A + 1) || 'x'`, `(C_QTY + 1) LIKE '1%'`), removing the two "left untouched" scenarios? → A: yes (superseded by the second re-scope below)
- Q: Move the MIN/MAX-over-CHAR scenario example from `MIN(CAST(... AS CHAR(20)))` to a `CASE` yielding CHAR? → A: yes (superseded by the third re-scope below)

Re-scope after the second plan review (it found the text rule missing from the broadcast join condition, and GROUP BY keys returning wrong dates under `NLS_DATE_FORMAT='YYYY-DD-MM'`):

- Q: Why does the text rule need changes at so many call sites? → A: the scan translator has no column types; type checks are a separate adapter pass that the WHERE and select-list paths run, while GROUP BY keys, aggregate arguments, scalar-over-aggregate, and the broadcast join condition call the translator directly. GROUP BY keys also travel from the scan as untyped text
- Q: Keep the text-conversion rule in this plan? → A: no. This plan stays on #584: shared strict type model, absent `characterSet` = UTF8 and the select-list length check, real INTERVAL/GEOMETRY/HASHTYPE/TSLTZ variants, DECIMAL and TIMESTAMP fixes, typed NULL, merge casts, and the DataFusion-dialect decline of sized or ASCII text CAST targets. The rule "the scan never converts a DATE, TIMESTAMP, numeric, or BOOLEAN value to text", and the BOOLEAN and computed-operand choices move to a follow-up plan. The CHAR-example change stays, because the sized CAST decline stays (the sized CAST decline and the CHAR-example change are superseded by the third re-scope below)
- Q: What does the follow-up plan hold? → A: one typed entry point for every expression sent to the scan (the translator receives column types, so no path skips the checks); the text-conversion rule defined once there; GROUP BY keys returned from the scan in their declared type. Issue (#587)
- Q: Which Exasol versions must the strict `selectListDataTypes` check support? → A: only 8.29.x and later (the supported floor); 2025.x is verified, and older versions are out of support
- Q: Does dropping the rule make anything worse? → A: no; the NLS wrong results exist on `main` today and stay as they are until the follow-up

Re-scope after the third plan review (it found that the target-only decline also moves `CAST(C_DATE AS VARCHAR(10))`, `CAST(C_TS AS VARCHAR(40))`, and `CAST(C_BOOL AS VARCHAR(5))` out of the scan):

- Q: Does this plan need the DataFusion-dialect decline of a `CHAR(n)`, a shorter `VARCHAR(n)`, or an `ASCII` CAST target? → A: no; it moves to (#587), because a rule that keeps a safe CAST in the scan needs the source type that the typed translator adds. The DataFusion dialect keeps rendering these targets as on `main`, the CHAR group-key pad stays, and the MIN/MAX-over-CHAR scenario keeps its `CAST(... AS CHAR(20))` example
- Q: Does dropping the decline make anything worse? → A: no; an over-length or non-ASCII CAST returns rows instead of the native error on `main` today, and stays so until (#587)

## Design Decisions

### [1] The Exasol SQL type lives in `vs-expression`, with no separate types crate

- **Decision:** `vs-expression` defines the Exasol SQL type, reads it from VS `dataType` JSON, and renders it as Exasol SQL and as `dataType` JSON. `lakehouse-engine` re-exports the type and keeps only the mapping from Arrow, Iceberg, Delta, and Hive types to it.
- **Rationale:** `/speq:adr-rules` rule-2 criterion 1: the type is shared by `vs-expression`, type-mapping, and the pushdown planner. Criterion 4: a plausible alternative is rejected. Both CAST dialects need the type, and `vs-expression` cannot depend on `lakehouse-engine`, while `lakehouse-engine` already depends on `vs-expression`. The module is deep: callers get one parse and one render and never see Exasol's wire format, limits, or spellings. ADR digest: `catalog-crate-owns-credential-types-no-shared-types-crate` rejects a shared-types crate for the credential types only, and no ADR covers the Exasol SQL type.
- **Alternatives:** A new leaf crate for the type. Rejected: it groups code by technical role and adds a crate whose only consumers are the two crates that already depend on each other in one direction.
- **Architecture:** § Components, § Constraints
- **Promotes to ADR:** yes

### [2] The unrenderable-predicate scenario is proved by unit tests

- **Decision:** `pushdown/pushdown-declined-filter-self-apply` "A predicate unrenderable under both dialects returns a clean error" is proved by the unit tests over synthesized requests. The E2E test that used a HASHTYPE CAST as its example now compares CASE-guarded GEOMETRY, HASHTYPE, and INTERVAL casts with native rows, because the original HASHTYPE cast of `C_VARCHAR` raises `Invalid hash format` natively.
- **Rationale:** Once the four Exasol-only CAST targets render in the Exasol dialect, no well-formed Exasol request is known to carry a predicate that neither dialect renders. `specs/testing.md` § Coverage rule proves such a state with a unit test over a synthesized request.
- **Alternatives:** Keep an E2E test with another predicate. Rejected: the spike's 46 working queries and its failure table found no reachable example, and the E2E harness sends only well-formed requests.
- **Promotes to ADR:** no

### [3] The adapter parses `selectListDataTypes` once per request

- **Decision:** Pushdown handling parses `selectListDataTypes` once, fails the query on an absent list or an invalid entry, and hands the parsed list to every consumer.
- **Rationale:** One parse site owns the strictness decision, and no consumer can turn an error into "no type".
- **Alternatives:** The spike validates the list once and lets each consumer re-parse it, discarding errors with `.ok()`. Rejected: every consumer repeats the parse, and the discarded error hides a failure path.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] A target-only text CAST decline also moves safe casts out of the scan

- **Finding:** `[UNSTATED_ASSUMPTION]`: a decline that reads only the CAST target also moves `CAST(C_DATE AS VARCHAR(10))`, `CAST(C_TS AS VARCHAR(40))`, and `CAST(C_BOOL AS VARCHAR(5))` out of the scan.
- **Direction change:** This plan keeps the DataFusion rendering of `CHAR(n)`, shorter `VARCHAR(n)`, and ASCII CAST targets as on `main`, and the DataFusion-side CHAR group-key pad stays. A rule that reads the source type moves to (#587) (Interview, third re-scope).
- **Promotes to ADR:** no

### [plan-review] Task 2.3 cannot end green

- **Finding:** `[TASK_GRANULARITY]`: task 2.3 routes CAST targets through the lenient parser, which caps a TIMESTAMP precision at 9 until the strict-parser task (now 3.3). The DataFusion declines of `renders_cast_timestamp_precision_per_dialect` for precisions 10 and 99 then fail.
- **Direction change:** Task 2.2 removes the parser's TIMESTAMP precision cap, so the interim parser reads a precision as `render_cast_target` on `main` reads it. Task 2.3 names its changed `TIMESTAMP` expectations. It also moves `merge_select_declines_when_the_scalar_structure_fails_to_render` off an unknown CAST target name, which the lenient parser reads as `VARCHAR(2000000)` until task 3.3.
- **Promotes to ADR:** no

### [plan-review] Two raw readers of the CAST target type stay

- **Finding:** `[SCOPE_REDUCTION]`: `render_cast` and `rewrite_decimal_stringifications` read the raw `type` string of a CAST `dataType`, so `dataType` JSON is parsed in more than one place, against #584's acceptance.
- **Direction change:** `render_cast` reads the parsed target's `is_character()` (task 2.3), and `rewrite_decimal_stringifications` reads the target through `exasol_type_from_json` (task 3.3). The decimal trim rewrite itself stays until (#587).
- **Promotes to ADR:** no

### [plan-review] The accepted CAST-to-text deviation is undocumented

- **Finding:** `[REQUIREMENT_CONFLICT]`: the CAST delta accepts a text-target deviation, but `specs/mission.md` Core Capability 12 accepts a deviation only when `docs/semantic-differences.md` documents it.
- **Direction change:** Task 1.9 adds a CAST-to-text section to `docs/semantic-differences.md`, with example rows from the Interview and (#587) and the date-range recommendation.
- **Promotes to ADR:** no

### [plan-review] One task joined the parse-once refactor and the strict switch

- **Finding:** `[TASK_GRANULARITY]`: task 3.2 changed every consumer signature and switched the parser to strict in one step, and the unprojected group key's `groupBy` `dataType` parse had no failure rule.
- **Direction change:** Task 3.2 hands the leniently parsed list to every consumer without a behavior change. Task 3.3 makes the parser strict, and an invalid `groupBy` `dataType` fails the query, which the failure scenario's CAST-node case states.
- **Promotes to ADR:** no
