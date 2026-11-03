# Decision Log: add-export-into-script-entry-points

## Interview

- Q: Writer script kind? → A: Rust SET script, new `.so` entry point
- Q: Default WRITERS_PER_NODE? → A: 1 per node
- Q: How does SINGLE_COMMIT run one writer? → A: constant GROUP BY key (no IPROC)

## Design Decisions

### [1] LAKEHOUSE_EXPORT is a Rust entry point in the `.so`, not a Lua script

- **Decision:** The export script is a fifth Rust entry point that answers Exasol's `generate_sql_for_export_spec` call through the SDK's export hook. It shares the `.so` with the writer.
- **Rationale:** Option validation and SQL rendering are pure logic that Rust unit tests cover, and the writer count reuses the scan's 300-group cap from `sharding` instead of restating it. A spike `.so` built in `rust:1.94-trixie` against SDK 0.30.0 with the `export` feature answered `EXPORT ... INTO SCRIPT` on the local Exasol, and `ctx.node_count()`, `ctx.script_schema()`, and `ctx.connection()` worked inside the hook. The `export` module keeps the options grammar, the writer spec format, and the guard SQL behind two functions that `lib.rs` calls, and it depends only on `sharding`.
- **Alternatives:** A Lua script, like `LAKEHOUSE_DISTRIBUTE_FILES`. Rejected: its validation would have no unit layer, and it would restate the 300-group cap.
- **Promotes to ADR:** no

### [2] The column type check is a per-row guard in the generated SELECT, keyed on a random group expression

- **Decision:** For every source column, the generated SELECT carries a `WHERE` guard that raises an error naming the column and `TYPEOF(<column>)` when the type is GEOMETRY, HASHTYPE, INTERVAL, or TIMESTAMP WITH LOCAL TIME ZONE. The group key is `FLOOR(RANDOM() * G)` in every mode, and `SINGLE_COMMIT = 'true'` sets G to 1. Exasol has no error function, so the guard raises the error by casting a message string to `DECIMAL(1,0)`.
- **Rationale:** Measured on the local Exasol 2025.1.16 and Exasol 8.29.13: Exasol passes no source column types to the export call, and it refuses these four types as UDF input before writer code runs, with messages that do not name the column (`Column type not supported: GEOMETRY`, `VM error: Unknown data type found`). `TYPEOF` exists on both versions and is evaluated per row. With the random key, the guard's error preceded Exasol's own type error for all four types. With a literal key (`GROUP BY 'single'`), Exasol's error came first for HASHTYPE, GEOMETRY, and TIMESTAMP WITH LOCAL TIME ZONE. This refines interview answer 3: SINGLE_COMMIT still uses one key value and no `IPROC()`.
- **Alternatives:** The writer checks its input metadata: rejected, because the writer never starts for these types. The export script checks types: rejected, because it receives no types. A literal constant key for SINGLE_COMMIT: rejected for the ordering measured above.
- **Consequences:**
  - The error text carries Exasol's cast-error prefix (`data exception - invalid character value for cast; Value: '...'`). The user documentation quotes it.
  - The guard costs a `TYPEOF` and four `LIKE` tests per column per row. Performance work is out of scope for #565.
  - A source that returns no rows skips the check. Exasol still refuses an empty TIMESTAMP WITH LOCAL TIME ZONE column with its own message.
- **Promotes to ADR:** no

### [3] Exporting users get a dedicated role, and both export scripts read the CONNECTION

- **Decision:** The installer's next-step template creates `LAKEHOUSE_EXPORT_ROLE_<schema>` with the `EXPORT` privilege, the `CONNECTION` privilege, `EXECUTE` on both export scripts, and `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` on both, and tells the operator to grant the role to each exporting user. `LAKEHOUSE_EXPORT` reads the CONNECTION before it returns SQL, and each writer reads it when it starts. Neither script formats a CONNECTION value into any string.
- **Rationale:** Exasol runs both scripts as the exporting session user. Measured: `AT <connection>` needs the user's `CONNECTION` privilege, and reading the CONNECTION in a script needs a script-scoped `ACCESS` grant for that script, through a role or directly. The installer cannot run the grants, because the operator creates the CONNECTION after the install. #565 requires both scripts to hold the grant, and reading the CONNECTION now makes a missing grant fail with a named error before the writers write data in a later issue.
- **Alternatives:**
  - Add the export grants to `LAKEHOUSE_ENGINE_ROLE_<schema>`. Rejected: an exporting user would then also hold the scan script's CONNECTION grant, which ADR `plan-visibility-execution-privilege-split` keeps with the virtual schema owner.
  - The writer reads no CONNECTION until it writes data. Rejected: the writer grant would have no observable effect, and no test could prove it.
- **Promotes to ADR:** no

## Review Findings
