# Decision Log: add-export-into-script-entry-points

## Interview

- Q: Writer script kind? → A: Rust SET script, new `.so` entry point
- Q: Default WRITERS_PER_NODE? → A: 1 per node
- Q: How does SINGLE_COMMIT run one writer? → A: constant GROUP BY key (no IPROC)

## Design Decisions

### [1] LAKEHOUSE_EXPORT is one Rust entry point that answers the export call and runs as the writer

- **Decision:** `LAKEHOUSE_EXPORT` is the fourth Rust entry point in the `.so` and the only export script, a `RUST SET SCRIPT` that returns `DECIMAL(18,0)`. Its export hook answers Exasol's `generate_sql_for_export_spec` call, and its `run` function is the writer: the SELECT the hook returns calls `LAKEHOUSE_EXPORT` itself over the source rows.
- **Rationale:** Exasol sends the export call to the hook and a SELECT call to `run`, so one script serves both roles. A spike `.so` built in the Makefile's `UDF_BUILDER_IMAGE` against the pinned SDK version with the `export` feature, and loaded on the SLC of the same version, showed this on the local Exasol: one SET RETURNS script answered `EXPORT ... INTO SCRIPT`, and the SELECT its hook returned called the same script as the writer. One script needs one `EXECUTE` and one script-scoped `ACCESS` grant. Option validation and SQL rendering are pure logic that Rust unit tests cover, and the writer count is computed in the `export` module, so the scan's shard-count code stays untouched. The `export` module keeps the options grammar, the writer spec format, and the guard SQL behind two functions that `lib.rs` calls, and it depends on no other engine module.
- **Alternatives:**
  - A separate writer script, `LAKEHOUSE_EXPORT_WRITER`. Rejected: it adds a DDL statement, an `EXECUTE` grant, and an `ACCESS` grant, and adds no capability, because one SET script serves both roles.
  - A Lua export script, like `LAKEHOUSE_DISTRIBUTE_FILES`. Rejected: its validation would have no unit layer.
- **Promotes to ADR:** no

### [2] The column type check is a per-row guard in the generated SELECT, keyed on `MOD(ROWNUM, G)`

- **Decision:** For every source column, the generated SELECT carries a `WHERE` guard that raises an error naming the column and `TYPEOF(<column>)` when the type is GEOMETRY, HASHTYPE, INTERVAL, or TIMESTAMP WITH LOCAL TIME ZONE. The group key is `MOD(ROWNUM, G)` in every mode, and `SINGLE_COMMIT = 'true'` sets G to 1, where the key is always 0. Exasol has no error function, so the guard raises the error by casting a message string to `DECIMAL(1,0)`. The guard races Exasol's own UDF type check, so a refused export carries either the guard error or `Column type not supported: <type>`.
- **Rationale:** Exasol passes no source column types to the export call. Without the guard, its own handling of these types varies between runs of the same statement: `Column type not supported: <type>`, `VM error: Unknown data type found`, or a silent export, and no message names the column. Exasol documents `ROWNUM` as "an incremental unique integer value ... beginning with 1", so a source of at least G rows yields every key from 0 to G - 1. Measured with the pinned SLC on Exasol 2025.1.16 and 8.29.13, with one script as the export target and the writer (decision [1]):
  - The key ran exactly G writers at G = 1, 4, and 300, over tables of 1, 4, 300, 1,000, and 10,000 rows, a filtered query of 1,428 rows, and a join query of exactly 300 rows. It ran 2 writers for a 2-row source at G = 3.
  - With the guard, the guard error came first in some runs and Exasol's `Column type not supported: <type>` in others. A one-row table with five unsupported columns gave the guard error in 6 of 10 runs and Exasol's in 4 of 10 on 2025.1.16, and a single GEOMETRY column gave the guard error in 10 of 10. An empty source with such a column errored in 3 to 9 of 10 runs and exported 0 rows otherwise.
  - At G = 1 the key keeps interview answer 3: one key value and no `IPROC()`.
- **Alternatives:**
  - The writer checks its input metadata. Rejected: Exasol can refuse the column before writer code runs.
  - The export script checks types. Rejected: it receives no types.
  - A literal key for SINGLE_COMMIT. Rejected: it gives no better ordering of the guard error and Exasol's type error than the per-row key.
  - `MOD(FLOOR(RANDOM() * G), G)`. Rejected: a writer receives no row with a small probability, so the writer count is not exact.
  - `ROW_NUMBER() OVER ()`. Rejected: Exasol allows analytic functions only in the select list ("Analytic functions may only be used as select list elements"), and an export SELECT over a subquery of `DUAL` fails with `object X.C0 not found`.
- **Consequences:**
  - The error text carries Exasol's cast-error prefix (`[22018] ... invalid character value for cast; Value: '...'`). The user documentation quotes it.
  - The guard costs a `TYPEOF` and four `LIKE` tests per column per row, and the cost of `ROWNUM` numbering on a multi-node cluster is unmeasured. A follow-up issue (#TBD, plan.md § Open Questions) measures both in the export performance work.
  - Gap-free `ROWNUM` numbering across nodes is proven only by the `cloud-e2e` test over exactly 2 × node count rows (task 1.12).
  - A source that returns no rows may skip the check and export 0 rows, or be refused by Exasol's type check. The export makes no promise about it.
- **Promotes to ADR:** no

### [3] Exporting users get a dedicated role that the installer prints, and the export script reads the CONNECTION in the hook and in each writer

- **Decision:** `LAKEHOUSE_EXPORT_ROLE_<schema>` holds the `EXPORT` privilege, the `CONNECTION` privilege, `EXECUTE` on `LAKEHOUSE_EXPORT`, and `GRANT ACCESS ON CONNECTION ... FOR SCRIPT <schema>.LAKEHOUSE_EXPORT`. The installer prints these statements in its next-step template for the template's CONNECTION, as it prints the virtual schema grants, and runs none of them. The operator creates the CONNECTION, runs the statements, and grants the role to each exporting user. `LAKEHOUSE_EXPORT` reads the CONNECTION in its hook before it returns SQL, and in each writer when it starts. It formats no CONNECTION value into any string.
- **Rationale:** Exasol runs the script as the exporting session user. Measured: `AT <connection>` needs the user's `CONNECTION` privilege, reading the CONNECTION in a script needs a script-scoped `ACCESS` grant, and one grant on `LAKEHOUSE_EXPORT` covers the hook and the writers. The installer runs before any CONNECTION exists, so it cannot grant access to one. One CONNECTION then serves both a virtual schema and an export.
- **Alternatives:**
  - Add the export grants to `LAKEHOUSE_ENGINE_ROLE_<schema>`. Rejected: an exporting user would then also hold the scan script's CONNECTION grant, which ADR `plan-visibility-execution-privilege-split` keeps with the virtual schema owner.
  - The installer runs the grants for a CONNECTION named by an `--export-connection` flag. Rejected: the CONNECTION does not exist at install time (Review Findings).
  - The writer reads no CONNECTION until it writes data. Rejected: a direct `SELECT` call of `LAKEHOUSE_EXPORT` skips the hook, so the writer's read is the only CONNECTION check on that path.
- **Consequences:**
  - A holder of the export role can call `LAKEHOUSE_EXPORT` directly in a `SELECT` with a forged writer spec and any rows. That call runs the writer and skips every check the hook makes. The writer treats its writer spec as untrusted input, and `docs/security.md` states the bypass.
  - A re-install drops the role's `EXECUTE` and `ACCESS` grants on the replaced script, so the operator runs those two template lines again, as for the virtual schema grants.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] The installer prints the export grants as a template

- **Finding:** Round 1 `[SCOPE_REDUCTION]` (HUMAN). #565's acceptance criterion reads "The installer creates both scripts and grants them access to the catalog CONNECTION." This entry supersedes the round-1 direction, in which the installer ran the grants under `--export-connection`.
- **Direction change:** The requester withdrew the criterion: "That doesn't make sense, because the installer runs before any CONNECTION exists. Export should work like virtual schemas: install the scripts, then users create CONNECTIONs and grant access to them." The requester will reword #565. The installer creates the one export script (decision [1]), prints the export grants as a template, and runs none; `--export-connection` is removed. Decision [3], task 2.2, the installer scenario, and the § Context deviation bullet carry the change.
- **Promotes to ADR:** no

### [plan-review] An unsupported column type is refused with either error

- **Finding:** Round 3 `[SCOPE_REDUCTION]` (HUMAN). #565's criterion reads "A column of an unsupported type ... fails the export with an error that names the column and its type". Exasol's own UDF type check races the guard, so the refusal carries the guard error or `Column type not supported: <type>`, and an empty source with such a column may be refused or export 0 rows.
- **Direction change:** The requester accepted either error as a scoped exception to #565 and rewords #565. § Background and the unsupported-type scenario of `export/export-into-script`, the empty-source part of `docs/export.md` (task 1.14), and the § Context deviation bullet state it. No scenario covers an empty source with an unsupported column type.
- **Promotes to ADR:** no

### [plan-review] The installer unit tests cover the installed export script

- **Finding:** Round 3 `[SCOPE_REDUCTION]` (HUMAN). #565 requires E2E coverage of "The installer creates both scripts and grants them access to the catalog CONNECTION." The plan proved the installer only with the `exapump` stub and one manual run.
- **Direction change:** The installer no longer runs grants (entry above), so the installer scenario rests on the installer unit tests, which check the export DDL and the printed role template. A live-Exasol CI step was dropped after the independent review: creating the script over a release `.so` succeeds, so such a step proved only the DDL shape that the unit tests already check.
- **Promotes to ADR:** no
