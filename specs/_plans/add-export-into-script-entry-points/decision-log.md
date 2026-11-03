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

### [2] The column type check is a per-row guard in the generated SELECT, keyed on `MOD(FLOOR(RANDOM() * G), G)`

- **Decision:** For every source column, the generated SELECT carries a `WHERE` guard that raises an error naming the column and `TYPEOF(<column>)` when the type is GEOMETRY, HASHTYPE, INTERVAL, or TIMESTAMP WITH LOCAL TIME ZONE. The group key is `MOD(FLOOR(RANDOM() * G), G)` in every mode, and `SINGLE_COMMIT = 'true'` sets G to 1, where the key is always 0. Exasol has no error function, so the guard raises the error by casting a message string to `DECIMAL(1,0)`.
- **Rationale:** Exasol passes no source column types to the export call. Without the guard, its own handling of these types varies between runs of the same statement: `Column type not supported: <type>`, `VM error: Unknown data type found`, or a silent export, and no message names the column. Measured with the pinned SLC on Exasol 2025.1.16 and 8.29.13, at G = 1 and G = 4, for all five types: the guard error came first in every run (140 runs and 105 runs), and the key produced exactly 1 group at G = 1 and 4 groups at G = 4. Exasol documents `RANDOM()` on the closed interval [0,1], so `FLOOR(RANDOM() * G)` alone can reach G. `MOD` keeps every key below G, so SINGLE_COMMIT has one key value and the writer count stays within the 300 cap. At G = 1 the key keeps interview answer 3: one key value and no `IPROC()`.
- **Alternatives:**
  - The writer checks its input metadata. Rejected: Exasol can refuse the column before writer code runs.
  - The export script checks types. Rejected: it receives no types.
  - A literal key for SINGLE_COMMIT. Rejected: with a literal key, whether the guard error or Exasol's type error comes first depends on the SLC version. With the random key, the guard error came first on every measured version.
- **Consequences:**
  - The error text carries Exasol's cast-error prefix (`[22018] ... invalid character value for cast; Value: '...'`). The user documentation quotes it.
  - The guard costs a `TYPEOF` and four `LIKE` tests per column per row. A follow-up issue (#TBD, plan.md § Open Questions) measures that cost and revisits the mechanism in the export performance work.
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
