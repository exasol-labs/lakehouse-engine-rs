# Decision Log: add-export-iceberg-rest-write

## Interview

- Q: SINGLE_COMMIT atomicity route → A: `SINGLE_COMMIT` means the table appears only with its data. Iceberg REST does it by raw `stage-create` (iceberg-rust hardcodes false) and a commit with assert-create. Atomicity is single-writer because the export has no final commit (decision [7])
- Q: Pre-read check for a bad CONNECTION → A: catalog calls only (connect, authenticate, table absent, create namespace). A storage error shows when the writers write. Replaces an earlier storage probe after review (decision [1]).
- Q: Concurrent and retried commits → A: raise `commit.retry.num-retries` on the created table and tag each snapshot with an export-id summary property, re-checked before every re-commit

## Design Decisions

### [1] The pre-read check makes catalog calls only, and never probes storage

- **Decision:** Before any source row is read, the export connects and authenticates to the catalog, refuses an existing table, and creates the missing namespace levels. It writes no probe object and stages no table. A storage error (wrong keys, no write access) shows when the writers write.
- **Rationale:** A probe catches only storage errors. In return it needed a catalog with staged creation (an optional Iceberg REST feature that Glue, Delta, and direct storage lack), could leave an object that no table owns, added an early credential refresh and its own E2E tests, and in parallel mode checked the staged location, not the location the writers use. Catalog calls exist for every catalog the export will gain, so they are the common pre-read check.
- **Alternatives:** Probe at a staged table location (rejected in review, reasons above). Probe at the namespace `location` property: the namespace may not exist yet, and the Iceberg REST spec does not require that property.
- **Consequences:**
  - A missing namespace is created even when the export later fails or writes no rows.
  - A wrong storage key fails the writer, not the pre-read check. In parallel mode the first writer may already have created the table, so the failed export can leave an empty table behind. Under `SINGLE_COMMIT` no table exists.
  - Staged creation is needed only for `SINGLE_COMMIT` (decision [8]).
- **Promotes to ADR:** no

### [2] The export commit loop belongs to the catalog write crate and re-checks the writer's tag before every re-commit

- **Decision:** iceberg-rust builds a writer's snapshot, and `lakehouse-catalog-write` sends the commit through the session method of `lakehouse-catalog` (decisions [9] and [10]). After a conflict or an uncertain outcome, the loop reloads the table with all snapshots and commits again only when no snapshot carries the writer's tag. It retries HTTP 409, 429, 502, 503, 504, and uncertain network outcomes with bounded backoff and a bounded attempt count, and never retries authentication (401, 403), schema, invalid-request, or existing-table errors.
- **Rationale:** No final commit exists (decision [7]), so each writer's commit must be safe to repeat. Rule-2 criteria 2 and 4. iceberg-rust's `Transaction::commit` re-applies after HTTP 409, and the catalog HTTP client repeats every POST after HTTP 502 or 504 today, so a commit whose answer was lost returns 409 and a blind re-apply adds its rows twice. The Iceberg REST spec answers a commit with 500, 502, or 504 when "the commit state is unknown". `RestCatalog` would also bypass the one catalog HTTP client that § Constraints requires. ADR digest search (export, write, commit, stage, retry): no ADR covers catalog writes.
- **Alternatives:** `RestCatalog` with `Transaction::commit` and a raised retry count, as in the spike: duplicate rows and a bypassed client.
- **Retries:** The commit is not safe to repeat blindly, so it is sent as a request the client never retries, and this loop is its only retry layer (decision [9]).
- **Consequences:**
  - The loop reads the table's `commit.retry.num-retries`, which the export sets to 20 on the tables it creates (interview answer 3).
  - A writer whose last commit outcome is unknown (HTTP 500 included) fails and keeps its data files, because they may be in the table.
  - Later write features reuse the loop. A rev bump of iceberg-rust runs the Lakekeeper export E2E before merge.
- **Architecture:** § Components, § Constraints
- **Promotes to ADR:** yes

### [3] TIMESTAMP(7) to TIMESTAMP(9) are refused, not truncated

- **Decision:** The export refuses `TIMESTAMP(p)` with p > 6, read from the column's declared precision, with the unsupported-type error, which suggests a cast to `TIMESTAMP(6)`.
- **Rationale:** Exasol truncates these values to microseconds before the UDF reads them, without an error (measured: `.999999999` arrives as `.999999`). The writer cannot see the lost digits, so only a refusal by declared precision keeps every value unchanged on read-back. A plain `TIMESTAMP` reports as `TIMESTAMP(3)` and is unaffected.
- **Alternatives:**
  - Write the truncated value: a silent value change.
  - Create version 3 tables with `timestamp_ns`: the nanoseconds are gone before the export reads the value.
- **Promotes to ADR:** no

### [4] ALLOW_HTTP is an export option

- **Decision:** The export takes `ALLOW_HTTP` as a WITH option, default `false`. Without it the export refuses every plain-HTTP request it would send: catalog, OAuth2 server, STS, and static or vended storage. The virtual schema property keeps its own, narrower gate.
- **Rationale:** An export has no virtual schema, so the consent gate of ADR `allow-http-threaded-as-vended-selector-parameter` needs its own carrier. The option conforms to that ADR: consent is explicit, defaults to false, and is never derived from an endpoint's scheme. Only the export's own code checks it, so virtual schemas behave as before.
- **Promotes to ADR:** no

### [5] A fault-injection test proves the retried unknown-outcome commit

- **Decision:** A `lakehouse-catalog` test proves "A commit retried after an unknown outcome adds its rows once" with the existing `spawn_server` helper replaying recorded Lakekeeper answers. The Lakekeeper E2E proves the tags that the check reads.
- **Rationale:** No component of the Lakekeeper stack can drop an answer after the catalog applied a commit: the proxy filter answers 403 before it forwards. `specs/testing.md` § Coverage rule admits a fault-injection test for a failure that E2E cannot produce.
- **Promotes to ADR:** no

### [6] Credentials with an expiry are renewed before they expire, through one holder, and the renew call belongs to the catalog

- **Decision:** `lakehouse-catalog` gets one expiring-credential holder: a value, its expiry, a renewal skew, an injectable clock, and a lock so concurrent callers wait for one renewal. The Unity OAuth token source (`unity/auth.rs`, `OAUTH_REFRESH_SKEW_SECS`) and the Iceberg REST OAuth2 token move onto it, so the pattern exists once and reads and writes get the same renewal. The export's vended storage credentials, which live in `lakehouse-catalog-write`, are a further user, with Iceberg REST's `loadCredentials` as the renew call. Unity's temporary credentials and an assumed AWS role plug in later by supplying their own renew call.
- **Rationale:**
  - A writer can outlive Lakekeeper's 3,599 s credentials. Under `SINGLE_COMMIT` the staged table cannot be reloaded, and Lakekeeper answers `loadCredentials` for a staged table (plan.md § Context).
  - The commit loop keeps the writer table's FileIO (task 1.3), so the renewal lives in that FileIO, through iceberg-rust's `customized_credential_load` hook.
  - Five minutes is the prefetch time of Iceberg Java's `VendedCredentialsProvider`. Lakekeeper serves a cached credential for half its lifetime, so the renewal gets new credentials for every lifetime of ten minutes or more, the 900 s STS minimum included.
  - The Iceberg REST spec defines `loadCredentials` and the `/v1/config` `endpoints` list: "If a server does not send the `endpoints` field, a default set of endpoints is assumed", and that set omits `loadCredentials`. The spec path on the CONNECTION's catalog address keeps the catalog token on that address, under the one catalog HTTP client, the proxy settings, and `ALLOW_HTTP`.
- **Alternatives:**
  - Rebuild the whole catalog session, with a new token and a new config call, before each renewal and each commit: a second pattern for the same problem, and a burst of token requests around commit time with many writers.
  - No renewal, with a documented one-hour limit: a `SINGLE_COMMIT` export runs one writer for the whole source.
  - Renew after an expired-token failure: the failed request inside iceberg-rust fails the writer, and SeaweedFS reports expiry as `InvalidAccessKeyId`.
  - Call `client.refresh-credentials-endpoint`: the catalog would choose where the catalog token goes.
  - Reload the table: a staged table cannot be loaded, and re-staging at a location that holds files answers HTTP 400.
  - A second, export-only token cache: the pattern would exist twice.
- **Consequences:**
  - A writer keeps one catalog session for its whole run. The session's token holder renews an OAuth2 token before it expires, for the renewal request and the commit alike. An expired static bearer token fails the request.
  - Under `SINGLE_COMMIT`, a racing statement that stages the same name at another location makes the renewal fail, naming the location.
  - Credentials without `s3.session-token-expires-at-ms` are never renewed. Static storage keys keep `StorageBackend::file_io()`.
  - With the storage probe gone (decision [1]), no E2E can observe a renewal, because no E2E writer outlives a 900 s credential. Unit tests with `spawn_server` prove the time trigger, the `loadCredentials` request, and the refusals (decision [5]).
- **Promotes to ADR:** no

### [7] There is no final commit step, so each writer commits on its own

- **Decision:** Exasol's UDF model has no step that runs after all writers finish, so the export has no single final commit. Each writer commits its own data files. That is why a failed parallel export can leave partial data, and why `SINGLE_COMMIT` exists: it trades speed for atomicity by running one writer, whose one commit makes the table appear only with its data. This drives the whole commit design (decision [2]) and holds for every catalog the export gains.
- **Rationale:** Exasol's UDF model has no step after the last group of a SET script. A final commit would need a second statement or a coordinator, which the engine does not have (mission: no background processes, no state across statements). The consequences hold for every catalog the export gains.
- **Alternatives:** A coordinator writer that waits for all others: Exasol gives writers no channel to each other and may run them in any order.
- **Consequences:**
  - A failed parallel export can leave the commits of some writers in the table.
  - Exactly-once per writer (decision [2]) is the only duplicate protection.
  - Atomic exports are single-writer (`SINGLE_COMMIT`).
  - Every later catalog implements per-writer commits plus its own way of making a single-writer table appear only with its data (decision [8]).
- **Promotes to ADR:** yes

### [8] The export is a fixed flow of steps, split at the Arrow batch, with Iceberg REST as its first implementation

- **Decision:** The export does four steps for every target: (1) connect to the catalog and check the target, (2) create the table or open it, (3) turn rows into files, (4) commit the files. The only step that is the same for every format is turning Exasol rows into Arrow batches (type conversion, decimal scaling, batching, streaming), in `export/batch.rs` and `export/writer.rs`. The seam sits right after it. An `ExportTarget` trait in `export/target.rs` has `prepare` (step 1) and `open_table` (step 2), and `open_table` returns an `ExportSink` that the target owns: it reports the table's Arrow schema, takes Arrow batches, writes files its own way, and commits them (steps 3 and 4). `export/iceberg_rest.rs` is the first implementation. Field IDs, Iceberg statistics, the Iceberg file writers, staged creation, snapshot tags, and `loadCredentials` appear only there.
- **Rationale:** The read side separates catalog from table format, and its scan stays format-neutral. Unity with Delta, Glue, and direct storage add implementations, not reshape the flow: Delta has no field IDs, keeps its statistics as JSON in the log entry, and uses table-relative paths, and direct storage has no commit at all. A shared Parquet writer over an Iceberg schema would branch on the format or be rebuilt then, and moving the seam is cheap now and a refactor once the write path and its tests exist. This plan builds only Iceberg REST.
- **Alternatives:**
  - Iceberg REST calls inside the writer: the next catalog reshapes the flow.
  - A trait for steps 1, 2, and 4 with a shared Iceberg-shaped file writer (the first draft of this decision): Iceberg's schema and data files pass through the neutral trait.
- **Consequences:**
  - `SINGLE_COMMIT` means "the table appears only with its data". Iceberg REST does it by staged creation, Delta would write its first log entry with the data, and a target with no such mechanism refuses the option. Without a probe, staged creation is needed only for `SINGLE_COMMIT`.
  - Types map Exasol to Arrow once (`export/arrow_schema.rs`, which owns the type rules and refusals), and the Iceberg schema derives from the Arrow schema inside the Iceberg sink. Delta and plain Parquet will derive theirs the same way.
  - The Exasol column metadata (`ColumnInfo`) is read in one place, `exasol_type_from_column_info` in vs-expression, which the scan's `target_arrow_type` (`scan/emit.rs`) moves onto. The scan keeps its own layer on top (small-integer decimals emit as `Int32` or `Int64`, an unrecognized type emits as `Utf8` JSON), and the export keeps its stricter layer (decimals always, `TIMESTAMP(7)` to `TIMESTAMP(9)` and anything unrepresentable refused). Exasol's quirks, such as integer decimals arriving as `Int32` or `Int64` and a plain `TIMESTAMP` reporting precision 3, are then fixed once.
  - The writer follows the sink's table schema: it binds source columns to table fields by name and takes decimal scales from the table, so append (#567) adds only the schema-matching rules. "Table exists" is then the only create-only check.
- **Architecture:** § Components (export), § Data Flow
- **Promotes to ADR:** no

### [9] Catalog requests that change state are retried only by the operation that can check its outcome

- **Decision:** The session method that sends a request through the catalog HTTP client takes a required `safe_to_repeat` flag. The client retries a transient failure only for a request marked safe. Every read is marked safe. Create namespace, create table, and commit are marked unsafe, and each owns its retries because it can check its own outcome: create namespace accepts HTTP 409 as "already exists", create table loads the table on 409 and compares `lakehouse.export-id`, and the commit loop reloads the table and looks for the writer's tag (decision [2]). The three share one bounded retry policy (backoff, attempt count) in `lakehouse-catalog-write`.
- **Rationale:** The client retried every request after a timeout or HTTP 502 or 504, which was harmless while every request only read. A commit that the catalog applied but whose answer was lost, retried blindly by the client, returns 409, and the commit loop then finds its own tag, so the result is right only because the loop cleans up after a hidden retry. The two layers had separate attempt counts and wait times, and the next state-changing request (a Delta commit, append in #567) would get blind retries with nothing to catch them. With the flag required, a new write cannot be retried by accident.
- **Alternatives:**
  - Keep the client's retries for every request: two retry layers with different rules, and a hidden retry that the commit test has to model.
  - Retry unsafe requests in the client with an idempotency key: Iceberg REST commits carry none, and a catalog that ignores one would repeat the commit.
- **Consequences:**
  - A timeout or an unknown-outcome answer to an unsafe request reaches its operation, which checks the outcome and decides.
  - Every later write feature (Delta commits, append) marks its requests the same way and owns its retries.
  - The catalog client's retry text in `architecture.md` § Constraints says "retries a transient failure of a request marked safe".
- **Architecture:** § Components (lakehouse-catalog, lakehouse-catalog-write), § Constraints
- **Promotes to ADR:** yes

### [10] Catalog writes live in a separate crate on top of the catalog crate

- **Decision:** A new crate `lakehouse-catalog-write` depends on `lakehouse-catalog`. The catalog crate keeps sessions, auth, config, listing and loading, vended-credential resolution, redaction, and the expiring-credential holder, and gains one public session method that sends an authenticated catalog request through the one HTTP client (decision [9]). The write crate gets namespace and table creation, staged creation, the commit loop, and the storage-credential hook for writers. Dependencies run engine, then write crate, then catalog crate, and the catalog crate never depends on the write crate. Read calls stay in the catalog crate, so the session method is not a back door for reads.
- **Rationale:** The catalog crate is read-only today and has no iceberg write machinery. The HTTP client and the auth are private to it, so the first draft put writes into it as the only place that could reach them. One public method gives the write crate that reach without a second client or a second auth, keeps the read side clean, and leaves a clear place for Delta commits and append (#567).
- **Alternatives:** Writes inside `lakehouse-catalog`: a read-only crate becomes read and write, and the next writer has no clear home.
- **Consequences:**
  - The write crate carries the `iceberg` snapshot-building, `async-trait`, `rand`, and `reqsign-core` dependencies. The catalog crate's manifest gains none.
  - Both crates compile into the engine's cdylib, and the Makefile's `VS_SRCS` lists the new crate.
- **Architecture:** § Components, § Constraints
- **Promotes to ADR:** no

## Review Findings

### [1] [pr-review] The storage probe is removed from the pre-read check

- **Finding:** The probe needed staged creation for every export and could not become a common pre-read check for other catalogs.
- **Direction change:** Decision [1] keeps catalog calls only. Parallel mode can leave an empty table behind, and plan.md § Impact and task 4.1 state it.
- **Promotes to ADR:** no

### [2] [spike] A storage fault, not a source error, proves that a failed SINGLE_COMMIT export leaves no table

- **Finding:** Exasol evaluates the source before the writer reads its first row, so a failing source never reaches the writer.
- **Direction change:** The local E2E injects a proxy storage fault after the writer wrote its data files and before its commit. Rejected: `KILL STATEMENT` once a data file appears, because a data file appears only when the writer closes it, just before the commit.
- **Promotes to ADR:** no

### [3] [refresh] Vended credentials are refreshed in this plan

- **Finding:** The requester asked for vended-credential refresh in this plan, not in a follow-up.
- **Direction change:** Decision [6] and tasks 1.7, 3.2, and 4.1.
- **Promotes to ADR:** no

### [4] [pr-review] Round 2 moves the seam, the crate boundary, the retry rule, the token, and the type reader

- **Finding:** The export trait had Iceberg's shape, writes sat in the read-only catalog crate, state-changing requests were retried blindly, the Iceberg REST token had its own renewal, and the export read Exasol column types with new code beside the scan's.
- **Direction change:** Decisions [8], [9], [10], and [6], and the matching tasks.
- **Promotes to ADR:** no
