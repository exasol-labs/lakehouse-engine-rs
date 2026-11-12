# Decision Log: add-export-iceberg-append

## Interview

- Q: Where do spec deltas live so stack fixes (#607-#610) never conflict? → A: New feature `export/export-iceberg-append` only, ADDED scenarios, no MODIFIED on `export-iceberg-table` or `export-options`
- Q: Base for plan and implementation? → A: Top of stack, `docs/export-iceberg-lakekeeper-e2e-and-specs` (#610). Current checkout is #607 and lacks `specs/export/export-iceberg-table`, so read stack files with `git show origin/<branch>:<path>`; do not switch branches or edit stacked files in the plan
- Q: Required-column NULL check? → A: Per writer, fail on first NULL before that writer writes or commits; with SINGLE_COMMIT no commit happens if any writer fails
- Q: Issue scope? → A: As in issue #567 (depends on #566): append by column name, widening allowed, one error listing all mismatches, SINGLE_COMMIT exactly one commit or none, no partitioned tables, no schema evolution, E2E for export-created and other-engine tables
- Q: Table column missing from the export, optional one included? → A: Follow issue literally: every table column must be bound, no NULL fill
- Q: Zero-row source, no writer runs, no column check? → A: Accept and document the gap
- Q: Iceberg format versions 1 and 3? → A: v2 only, list v1 and v3 as out of scope

## Design Decisions

### [1] Each writer checks the column binding, not the pre-read hook

- **Decision:** Each writer checks the binding and the column types after it opens the table and before it writes any data. The pre-read hook refuses a table only for its format version or its partitioning.
- **Rationale:** Exasol passes the hook the source column names without their types (`ExportSpec` carries only `source_column_names`). The one error must list type mismatches together with name mismatches.
- **Alternatives:** Check names in the hook and types in the writer. Rejected: a source with both kinds of mismatch gets two errors in turn, not one.
- **Consequences:** A source with no rows runs no writer, so it checks no column and appends nothing (see Review Findings). A new module, `export/binding.rs`, owns which table column types accept a source type, for both the create and the append path. `batch.rs` keeps the mapping from Exasol to Arrow types and takes only its per-field source index, so it does not depend on `binding.rs` and no module cycle forms.
- **Promotes to ADR:** no

### [2] Name binding falls back to a case-insensitive match

- **Decision:** A source column binds to the table column of the same name, and otherwise to the one table column whose name differs only in letter case.
- **Rationale:** Exasol reports an unquoted identifier in upper case, and Spark, Trino, and PyIceberg usually create lower-case names.
- **Alternatives:** Exact names only. Rejected: every append into such a table needs a quoted alias per column. Case-insensitive names only. Rejected: a table with the columns `id` and `ID` could take neither.
- **Promotes to ADR:** no

### [3] Integer columns accept a DECIMAL source by its declared precision

- **Decision:** An `int` column accepts `DECIMAL(p,0)` with p ≤ 9, and a `long` column with p ≤ 18. The source column's type decides, not its values.
- **Rationale:** Every value of such a source type fits the table type, so the check runs before any data is written. A cast to `DECIMAL(18,0)` resolves a refused column.
- **Alternatives:** Accept any `DECIMAL(p,0)` and fail at the first value out of range. Rejected: it admits narrowing types and fails only after rows were read and files written.
- **Promotes to ADR:** no

### [4] Spark authors the other-engine tables in the local suite's REST catalog

- **Decision:** The Spark fixture job writes the other-engine tables into the shared Iceberg REST catalog, and their append tests run in the local Docker suite. The Lakekeeper suite tests appends into tables that an export created.
- **Rationale:** The Lakekeeper warehouses exist only after the test harness creates them at suite start, which is after the stack's one-shot jobs have run. The Spark job already runs against the shared catalog at `make e2e-up`.
- **Alternatives:** A Spark job against Lakekeeper. Rejected: it needs its own Lakekeeper bootstrap and warehouse creation ahead of the harness. Seeding through iceberg-rust. Rejected: the export writes with the same library, so it is not an independent engine.
- **Promotes to ADR:** no

### [5] Record-time deltas on export-iceberg-table and export-into-script

- **Decision:** Task 5.1 writes deltas on export/export-iceberg-table (a REMOVED and a CHANGED scenario, a CHANGED Background) and on export/export-into-script (a CHANGED description and Background) after #607 to #610 merge into main.
- **Rationale:** The refused-table scenario and the new-table wording contradict the new feature. Interview Q1 keeps these deltas out of the plan while the stacked PRs may change, and writing them after the merge keeps that purpose.
- **Alternatives:** Leave the contradiction in the library. Rejected: the library would state both that an existing table is refused and that it takes an append.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] Every table column must be bound

- **Finding:** The Binding bullet wrote NULL into an optional table column that no source column binds. Issue #567 fails the export when "a table column is missing from the export".
- **Direction change:** The requester chose to follow #567 literally. Every unbound table column, required or optional, is a mismatch, and the NULL fill is gone. A table with a column of a type the export cannot write, such as `float` or a struct, takes no append. The Spark fixtures split into the writable `spark_target` and `spark_unwritable`, which a mismatch test uses.
- **Promotes to ADR:** no

### [plan-review] A source with no rows is not checked

- **Finding:** A source with no rows runs no writer, so it skips every mismatch check, against the #567 criterion that each mismatch kind fails before any data is written.
- **Direction change:** The requester accepted the gap. The spec Background states it, as export/export-into-script does for type checks, and the empty-source scenario uses a source whose columns do not match the table. Checking names in the pre-read hook stays rejected (decision [1]).
- **Promotes to ADR:** no

### [plan-review] Format version 2 only

- **Finding:** The plan refused format-version 1 and 3 tables, which #567 does not exclude, and justified the v3 refusal with row lineage that the pinned iceberg-rust fast append assigns (`first_row_id`).
- **Direction change:** The requester limited the append to format version 2. plan.md § Out of Scope names versions 1 and 3. The spec states the refusal as a scope limit and no longer cites row lineage.
- **Promotes to ADR:** no
