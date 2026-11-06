# Decision Log: add-export-iceberg-rest-write

## Interview

- Q: SINGLE_COMMIT atomicity route → A: raw REST `stage-create` (iceberg-rust hardcodes false), commit with assert-create; table appears only with its data
- Q: Pre-read check for a bad CONNECTION → A: catalog `namespace_exists` call plus a probe write/delete with static storage keys in `generate_export_sql`; vended keys only get the catalog check (scoped to a table that may not exist yet). Superseded by decision [1] after review.
- Q: Concurrent and retried commits → A: raise `commit.retry.num-retries` on the created table and tag each snapshot with an export-id summary property, re-checked before every re-commit

## Design Decisions

### [1] The pre-read check probes storage at a staged table location, for static and vended credentials alike

- **Decision:** Before any source row is read, the export creates the missing namespace levels, stage-creates the target table without committing it, and writes and deletes one probe object at the staged location. It uses the credentials the writers will use: the CONNECTION's static keys, or the credentials vended in the stage-create answer.
- **Rationale:** Interview answer 2 limited vended credentials to a catalog call because no table location exists before the table does. A stage-create answer carries the location and, on Lakekeeper, vended credentials scoped to it. Measured on the local Lakekeeper stack: HTTP 200, `metadata.location` set, vended keys present, the staged table invisible, and a later create of the same name succeeds, so staging reserves nothing. One probe path then covers static keys, vending, and an `http://` vended endpoint without `ALLOW_HTTP`.
- **Alternatives:** Probe static keys at the namespace `location` property. Rejected: the namespace may not exist yet, and the Iceberg REST spec does not require that property.
- **Consequences:**
  - Every export, parallel mode included, needs a catalog that accepts `stage-create: true`. Lakekeeper and the local Iceberg REST fixture accept it. On a catalog that refuses it, every export fails before any source row is read, with an error that names the catalog and states that the export needs staged table creation.
  - A missing namespace is created even when the export later fails or writes no rows.
  - The probe delete is best effort, so storage that refuses deletes keeps one probe object under a location no table uses.
- **Promotes to ADR:** no

### [2] The export commit loop belongs to lakehouse-catalog and re-checks the writer's tag before every re-commit

- **Decision:** iceberg-rust builds a writer's snapshot (manifests, manifest list, and the commit's updates), and `lakehouse-catalog` sends the commit through its own catalog HTTP client. After a conflict or an unknown outcome, the loop reloads the table with all snapshots and commits again only when no snapshot carries the writer's tag.
- **Rationale:** Rule-2 criteria 2 and 4. iceberg-rust's `Transaction::commit` reloads and re-applies after HTTP 409, and the catalog HTTP client repeats a POST after HTTP 502 or 504. A commit the catalog applied but whose answer was lost therefore returns 409, and a blind re-apply adds the rows twice. The Iceberg REST spec answers a commit with 500, 502, or 504 when "the commit state is unknown". iceberg-rust's `RestCatalog` would also bypass the one catalog HTTP client that § Constraints requires. ADR digest search (export, write, commit, stage): no ADR covers catalog writes.
- **Alternatives:** `RestCatalog` with `Transaction::commit` and a raised retry count, as in the spike. Rejected for the duplicate rows and the bypassed client.
- **Consequences:** The loop reads the table's `commit.retry.num-retries`, which the export sets to 20 on the tables it creates, as interview answer 3 chose. Later write features reuse the loop instead of iceberg-rust's. A rev bump of iceberg-rust runs the Lakekeeper export E2E before merge.
- **Architecture:** § Components, § Constraints
- **Promotes to ADR:** yes

### [3] TIMESTAMP(7) to TIMESTAMP(9) are refused, not truncated

- **Decision:** The export refuses `TIMESTAMP(p)` with p > 6 with the unsupported-type error, which suggests a cast to `TIMESTAMP(6)`. It reads p from the column's declared precision.
- **Rationale:** Exasol truncates a `TIMESTAMP(7)` to `TIMESTAMP(9)` value to microseconds before the UDF reads it, without an error (in-UDF spike: `.999999999` arrives as `.999999`). The writer cannot see the lost digits, so a refusal by declared precision is the only way to keep the acceptance criterion that every value reads back unchanged. A plain `TIMESTAMP` reports as `TIMESTAMP(3)`, so only explicitly declared precisions above 6 are affected.
- **Alternatives:** Write the truncated value: rejected, a silent value change. Create version 3 tables with `timestamp_ns`: rejected, because the nanoseconds are gone before the export reads the value.
- **Promotes to ADR:** no

### [4] ALLOW_HTTP is an export option

- **Decision:** The export takes `ALLOW_HTTP` as a WITH option, default `false`. Without it the export refuses every plain-HTTP request it would send: catalog, OAuth2 server, STS, and static or vended storage. The virtual schema property keeps its own, narrower gate.
- **Rationale:** An export has no virtual schema, so the consent gate of ADR `allow-http-threaded-as-vended-selector-parameter` needs its own carrier. The option conforms to that ADR: consent is explicit, defaults to false, and is never derived from an endpoint's scheme. The checks run only on the export's path, in the export module and the export-only write session, so the virtual schema behaves as before.
- **Promotes to ADR:** no

### [5] A fault-injection test proves the retried unknown-outcome commit

- **Decision:** A `lakehouse-catalog` test proves "A commit retried after an unknown outcome adds its rows once" with the existing `spawn_server` helper replaying recorded Lakekeeper answers. The Lakekeeper E2E proves the tags that the check reads.
- **Rationale:** No component of the Lakekeeper stack can drop an answer after the catalog applied a commit: the proxy filter only answers 403 before it forwards. `specs/testing.md` § Coverage rule admits a fault-injection test for a failure that E2E cannot produce. Every other acceptance case of #566 runs as E2E on the local or the Lakekeeper stack.
- **Promotes to ADR:** no

### [6] Vended credentials are not refreshed while a writer writes its data files

- **Decision:** A writer writes its data files with the vended credentials of its create or stage-create answer. It resolves a fresh catalog session, and in parallel mode reloads the table, before it commits. The docs state that with vended credentials a writer must finish within the vended lifetime.
- **Rationale:** Lakekeeper vends credentials for one hour (measured: 3,599 s), so only a writer that writes its files for longer than an hour fails. A staged table cannot be reloaded, so under `SINGLE_COMMIT` the commit's manifests also use the stage-create credentials. Lakekeeper also returns `client.refresh-credentials-endpoint`, and wiring it into the storage credential loader is a follow-up (#TBD).
- **Alternatives:** Refresh through iceberg-rust's custom S3 credential loader and the catalog's credentials endpoint. Deferred to a follow-up the requester accepted; it is out of scope here.
- **Promotes to ADR:** no

## Review Findings

### [1] [plan-review] Decision [1] replaces interview answer 2, and the requester accepted it

- **Finding:** Decision [1] replaced interview answer 2 and added two user-visible effects: a missing namespace is created before any row is read, and every export needs a catalog that accepts `stage-create: true`.
- **Direction change:** The requester accepted decision [1] as written. Decision [1] Consequences, plan.md § Impact, the `export/export-iceberg-table` delta (a Background line and the scenario "A catalog that refuses staged table creation fails the export before any data is read"), and docs task 5.1 now state the stage-create requirement, the namespace side effect, and the error a refusing catalog produces.
- **Promotes to ADR:** no

### [2] [plan-review] The export's ALLOW_HTTP gates every plain-HTTP request in the export's own code

- **Finding:** No task refused an `http://` catalog address or static storage endpoint, and a gate in the shared `CatalogSession::resolve` would also change virtual schemas.
- **Direction change:** Decision [4] now gives the export option a wider gate than the virtual schema property. Only the export's path checks it (tasks 1.2 and 3.5).
- **Promotes to ADR:** no

### [3] [spike] Exasol truncates TIMESTAMP(9) values before the export reads them

- **Finding:** The in-UDF spike measured that a `TIMESTAMP(9)` value reaches the UDF truncated to microseconds, without an error.
- **Direction change:** Decision [3] keeps the refusal and now rests on this measurement. A version 3 `timestamp_ns` table is no longer a lossless alternative. Task 5.1 documents the reason.
- **Promotes to ADR:** no

### [4] [spike] Exasol refuses most unsupported column types before the writer runs

- **Finding:** `GEOMETRY`, `HASHTYPE`, `INTERVAL`, and `TIMESTAMP WITH LOCAL TIME ZONE` columns fail with Exasol's own error before any writer code runs, and for `GEOMETRY` and `INTERVAL YEAR TO MONTH` the error names no column. The plan stated that the writer refused them, naming the column.
- **Direction change:** The unsupported-type scenario is split. Exasol's errors are the expected outcome for those types, which issue #566 accepts. The writer's own refusal, which names the column and its type, covers `TIMESTAMP(7)` to `TIMESTAMP(9)` and any type outside the allowlist. Task 2.1 classifies a column by `typ` and `type_name` together, using the measured spellings, instead of by `type_name` alone.
- **Promotes to ADR:** no

### [5] [spike] A storage fault after the data files are written proves that a failed SINGLE_COMMIT export leaves no table

- **Finding:** Exasol evaluates the source before the writer reads its first row. A source that fails at row 500 therefore never reaches the writer, and the planned test could not catch a writer that creates the table before it commits.
- **Direction change:** `export_failed_single_commit_leaves_no_table` moves to the local suite. The proxy filter answers HTTP 403 to the writes under the table's `metadata/` path, so the writer fails after it wrote its data files and before its commit, and the test asserts that a data file of the export exists. Rejected: `KILL STATEMENT` once a data file appears, because a data file appears in storage only when the writer closes it, which for a source below the 128 MiB target file size happens just before the commit. A separate scenario keeps the source-error case and states that it does not test the writer.
- **Promotes to ADR:** no
