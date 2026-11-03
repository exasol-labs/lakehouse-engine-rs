# Decision Log: add-export-into-script-entry-points

## Interview

- Q: Writer script kind? → A: Rust SET script, new `.so` entry point
- Q: Default WRITERS_PER_NODE? → A: 1 per node
- Q: How does SINGLE_COMMIT run one writer? → A: constant GROUP BY key (no IPROC)

## Design Decisions

### [1] LAKEHOUSE_EXPORT is a Rust entry point in the `.so`, not a Lua script

- **Decision:** The export script is a fifth Rust entry point that answers Exasol's `generate_sql_for_export_spec` call through the SDK's export hook. It shares the `.so` with the writer.
- **Rationale:** Option validation and SQL rendering are pure logic that Rust unit tests cover, and the writer count reuses the scan's 300-group cap from `sharding` instead of restating it. A spike `.so` built in the Makefile's `UDF_BUILDER_IMAGE` against the pinned SDK version with the `export` feature, and loaded on the SLC of the same version, answered `EXPORT ... INTO SCRIPT` on the local Exasol. `ctx.node_count()`, `ctx.script_schema()`, `ctx.current_user()`, and `ctx.connection()` worked inside the hook. The `export` module keeps the options grammar, the writer spec format, and the guard SQL behind two functions that `lib.rs` calls, and it depends only on `sharding`.
- **Alternatives:** A Lua script, like `LAKEHOUSE_DISTRIBUTE_FILES`. Rejected: its validation would have no unit layer, and it would restate the 300-group cap.
- **Promotes to ADR:** no

### [2] The column type check is a per-row guard in the generated SELECT, keyed on `MOD(ROWNUM, G)`

- **Decision:** For every source column, the generated SELECT carries a `WHERE` guard that raises an error naming the column and `TYPEOF(<column>)` when the type is GEOMETRY, HASHTYPE, INTERVAL, or TIMESTAMP WITH LOCAL TIME ZONE. The group key is `MOD(ROWNUM, G)` in every mode, and `SINGLE_COMMIT = 'true'` sets G to 1, where the key is always 0. Exasol has no error function, so the guard raises the error by casting a message string to `DECIMAL(1,0)`.
- **Rationale:** Exasol passes no source column types to the export call. Without the guard, its own handling of these types varies between runs of the same statement: `Column type not supported: <type>`, `VM error: Unknown data type found`, or a silent export, and no message names the column. Exasol documents `ROWNUM` as "an incremental unique integer value ... beginning with 1", so a source of at least G rows yields every key from 0 to G - 1. Measured with the pinned SLC on Exasol 2025.1.16 and 8.29.13:
  - The key ran exactly G writers at G = 1, 4, and 300, over tables of 1, 4, 300, 1,000, and 10,000 rows and a filtered query of exactly 300 rows. On 2025.1.16 it also ran 300 writers over a join query of exactly 300 rows, and 2 writers for a 2-row source at G = 3.
  - With the guard, at G = 1, 4, and 300 and for all five types, the guard error came first in every run over one-row and 1,000-row sources (90 of 90 on 2025.1.16, 60 of 60 on 8.29.13). Empty sources exported 0 rows (45 of 45, 30 of 30).
  - At G = 1 the key keeps interview answer 3: one key value and no `IPROC()`.
- **Alternatives:**
  - The writer checks its input metadata. Rejected: Exasol can refuse the column before writer code runs.
  - The export script checks types. Rejected: it receives no types.
  - A literal key for SINGLE_COMMIT. Rejected: with a literal key, whether the guard error or Exasol's type error comes first depends on the SLC version. With a per-row key, the guard error came first on every measured version.
  - `MOD(FLOOR(RANDOM() * G), G)`. Rejected: a writer receives no row with a small probability, so the writer count is not exact.
  - `ROW_NUMBER() OVER ()`. Rejected: Exasol allows analytic functions only in the select list ("Analytic functions may only be used as select list elements"), and an export SELECT over a subquery of `DUAL` fails with `object X.C0 not found`.
- **Consequences:**
  - The error text carries Exasol's cast-error prefix (`[22018] ... invalid character value for cast; Value: '...'`). The user documentation quotes it.
  - The guard costs a `TYPEOF` and four `LIKE` tests per column per row, and the cost of `ROWNUM` numbering on a multi-node cluster is unmeasured. A follow-up issue (#TBD, plan.md § Open Questions) measures both in the export performance work.
  - Gap-free `ROWNUM` numbering across nodes is proven only by the `cloud-e2e` test over exactly 2 × node count rows (task 1.12).
  - A source that returns no rows skips the check and exports 0 rows.
- **Promotes to ADR:** no

### [3] Exporting users get a dedicated role, the installer grants it under `--export-connection`, and both export scripts read the CONNECTION

- **Decision:** `LAKEHOUSE_EXPORT_ROLE_<schema>` holds the `EXPORT` privilege, the `CONNECTION` privilege, `EXECUTE` on both export scripts, and `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` on both. With `--export-connection <name>`, `install.sh` creates the role when it does not exist and runs its grants when that CONNECTION exists. Without the flag, or when the CONNECTION does not exist, the next-step template prints the same statements. The operator grants the role to each exporting user. `LAKEHOUSE_EXPORT` reads the CONNECTION before it returns SQL, and each writer reads it when it starts. Neither script formats a CONNECTION value into any string.
- **Rationale:** Exasol runs both scripts as the exporting session user. Measured: `AT <connection>` needs the user's `CONNECTION` privilege, and reading the CONNECTION in a script needs a script-scoped `ACCESS` grant for that script, through a role or directly. `CREATE OR REPLACE` drops script grants on every re-install, so the installer runs the grants on each run with the flag. Measured: a second `CREATE ROLE` fails ("role name ... conflicts with another user or role name"), and every repeated `GRANT` succeeds, so the installer checks `EXA_ALL_ROLES` before it creates the role. Reading the CONNECTION in both scripts makes a missing grant fail with a named error before the writers write data in a later issue.
- **Alternatives:**
  - Add the export grants to `LAKEHOUSE_ENGINE_ROLE_<schema>`. Rejected: an exporting user would then also hold the scan script's CONNECTION grant, which ADR `plan-visibility-execution-privilege-split` keeps with the virtual schema owner.
  - Print the grants and never run them. Rejected: #565 asks the installer to grant them (Review Findings).
  - The writer reads no CONNECTION until it writes data. Rejected: the writer grant would have no observable effect, and no test could prove it.
- **Consequences:**
  - A holder of the export role can call `LAKEHOUSE_EXPORT_WRITER` directly with any writer spec and any rows, which skips every check `LAKEHOUSE_EXPORT` makes. The writer treats its writer spec as untrusted input, and `docs/security.md` states the bypass.
  - The flag needs an installing user that may create roles and grant `EXPORT` and the CONNECTION, such as a DBA. A failed grant aborts the install naming the statement.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] The installer runs the export role grants under `--export-connection`

- **Finding:** Round 1 `[SCOPE_REDUCTION]` (HUMAN). #565's acceptance criterion reads "The installer creates both scripts and grants them access to the catalog CONNECTION." The plan had the installer only print the grants.
- **Direction change:** The requester chose to have the installer run the grants. `install.sh --export-connection <name>` creates `LAKEHOUSE_EXPORT_ROLE_<schema>` and runs its grants when that CONNECTION exists. Without the flag, the template prints the same lines. Decision [3], task 2.2, and the installer scenarios carry the change.
- **Promotes to ADR:** no

### [plan-review] An empty source with an unsupported column type exports 0 rows

- **Finding:** Round 3 `[SCOPE_REDUCTION]` (HUMAN). #565's criterion reads "A column of an unsupported type ... fails the export with an error that names the column and its type". The type check runs on source rows, so an empty source with such a column exports 0 rows without an error.
- **Direction change:** The requester accepted this as a scoped exception to #565. The two empty-source scenarios and § Background of `export/export-into-script`, the empty-source part of `docs/export.md` (task 1.14), and the § Context deviation bullet state it.
- **Promotes to ADR:** no

### [plan-review] The installer's export role grants are proven on a live Exasol

- **Finding:** Round 3 `[SCOPE_REDUCTION]` (HUMAN). #565 requires E2E coverage of "The installer creates both scripts and grants them access to the catalog CONNECTION." The plan proved the installer only with the `exapump` stub and one manual run.
- **Direction change:** The requester chose to extend the `install-script-e2e` CI job (task 2.6). The job runs `install.sh --export-connection` twice against a live Exasol and reads the role's six grants from the privilege views after the second run. The § Context deviation is removed.
- **Promotes to ADR:** no
