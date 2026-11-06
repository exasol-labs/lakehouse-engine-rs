# Decision Log: add-export-iceberg-rest-write

## Interview

- Q: SINGLE_COMMIT atomicity route → A: raw REST `stage-create` (iceberg-rust hardcodes false), commit with assert-create; table appears only with its data
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

### [2] The export commit loop belongs to lakehouse-catalog and re-checks the writer's tag before every re-commit

- **Decision:** iceberg-rust builds a writer's snapshot, and `lakehouse-catalog` sends the commit through its own catalog HTTP client. After a conflict or an uncertain outcome, the loop reloads the table with all snapshots and commits again only when no snapshot carries the writer's tag. It retries HTTP 409, 429, 502, 503, 504, and uncertain network outcomes with bounded backoff and a bounded attempt count, and never retries authentication (401, 403), schema, invalid-request, or existing-table errors.
- **Rationale:** Rule-2 criteria 2 and 4. iceberg-rust's `Transaction::commit` re-applies after HTTP 409, and the catalog HTTP client repeats a POST after HTTP 502 or 504, so a commit whose answer was lost returns 409 and a blind re-apply adds its rows twice. The Iceberg REST spec answers a commit with 500, 502, or 504 when "the commit state is unknown". `RestCatalog` would also bypass the one catalog HTTP client that § Constraints requires. ADR digest search (export, write, commit, stage, retry): no ADR covers catalog writes.
- **Alternatives:** `RestCatalog` with `Transaction::commit` and a raised retry count, as in the spike: duplicate rows and a bypassed client.
- **Open:** The catalog HTTP client retries every POST, so a commit can be retried by it and by this loop under different rules. Which catalog writes are safe to repeat, who retries them, and how a writer learns an unknown outcome is an open question (plan.md § Open Questions), not decided here.
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

### [6] Storage credentials with an expiry are renewed before they expire, and the renew call belongs to the catalog

- **Decision:** `lakehouse-catalog` gets one expiring-credential holder: a value, its expiry, a renewal skew, an injectable clock, and a lock so concurrent callers wait for one renewal. The Unity OAuth token source (`unity/auth.rs`, `OAUTH_REFRESH_SKEW_SECS`) moves onto it, so the pattern exists once. The export's vended storage credentials are its second user, with Iceberg REST's `loadCredentials` as the renew call. Unity's temporary credentials and an assumed AWS role plug in later by supplying their own renew call.
- **Rationale:**
  - A writer can outlive Lakekeeper's 3,599 s credentials. Under `SINGLE_COMMIT` the staged table cannot be reloaded, and Lakekeeper answers `loadCredentials` for a staged table (plan.md § Context).
  - The commit loop keeps the writer table's FileIO (task 1.3), so the renewal lives in that FileIO, through iceberg-rust's `customized_credential_load` hook.
  - Five minutes is the prefetch time of Iceberg Java's `VendedCredentialsProvider`. Lakekeeper serves a cached credential for half its lifetime, so the renewal gets new credentials for every lifetime of ten minutes or more, the 900 s STS minimum included.
  - The Iceberg REST spec defines `loadCredentials` and the `/v1/config` `endpoints` list: "If a server does not send the `endpoints` field, a default set of endpoints is assumed", and that set omits `loadCredentials`. The spec path on the CONNECTION's catalog address keeps the catalog token on that address, under the one catalog HTTP client, the proxy settings, and `ALLOW_HTTP`.
- **Alternatives:**
  - No renewal, with a documented one-hour limit: a `SINGLE_COMMIT` export runs one writer for the whole source.
  - Renew after an expired-token failure: the failed request inside iceberg-rust fails the writer, and SeaweedFS reports expiry as `InvalidAccessKeyId`.
  - Call `client.refresh-credentials-endpoint`: the catalog would choose where the catalog token goes.
  - Reload the table: a staged table cannot be loaded, and re-staging at a location that holds files answers HTTP 400.
  - A second, export-only token cache: the pattern would exist twice.
- **Consequences:**
  - A renewal resolves a fresh catalog session, because the writer's OAuth2 token may have expired. An expired static bearer token fails the renewal.
  - Under `SINGLE_COMMIT`, a racing statement that stages the same name at another location makes the renewal fail, naming the location.
  - Credentials without `s3.session-token-expires-at-ms` are never renewed. Static storage keys keep `StorageBackend::file_io()`.
  - With the storage probe gone (decision [1]), no E2E can observe a renewal, because no E2E writer outlives a 900 s credential. Unit tests with `spawn_server` prove the time trigger, the `loadCredentials` request, and the refusals (decision [5]).
- **Promotes to ADR:** no

### [7] There is no final commit step, so each writer commits on its own

- **Decision:** Each writer commits its own data files. The export has no step that runs once after all writers finish, and `SINGLE_COMMIT` trades speed for atomicity by running one writer.
- **Rationale:** Exasol's UDF model has no step after the last group of a SET script. A final commit would need a second statement or a coordinator, which the engine does not have (mission: no background processes, no state across statements). The consequences hold for every catalog the export gains.
- **Alternatives:** A coordinator writer that waits for all others: Exasol gives writers no channel to each other and may run them in any order.
- **Consequences:**
  - A failed parallel export can leave the commits of some writers in the table.
  - Exactly-once per writer (decision [2]) is the only duplicate protection.
  - Atomic exports are single-writer.
- **Promotes to ADR:** yes

### [8] The export is a fixed flow of steps, with Iceberg REST as its first implementation

- **Decision:** The export does four steps for every target: (1) connect to the catalog and check the target, (2) create the table or open it, (3) turn rows into Parquet files, (4) commit the files. Steps 1, 2, and 4 sit behind an `ExportTarget` trait in `export/target.rs`, implemented first by `export/iceberg_rest.rs`. Step 3 is the same for every target and lives in `export/writer.rs`. Staged creation, snapshot tags, and `loadCredentials` appear only in the Iceberg REST implementation.
- **Rationale:** The read side separates catalog from table format. Unity with Delta, Glue, and direct storage will add implementations, not reshape the flow. This plan builds only Iceberg REST.
- **Alternatives:** Iceberg REST calls inside the writer: the next catalog reshapes the flow.
- **Consequences:**
  - `SINGLE_COMMIT` means "the table appears only with its data". Iceberg REST does it by staged creation, Delta would write its first log entry with the data, and a target with no such mechanism refuses the option. Without a probe, staged creation is needed only for `SINGLE_COMMIT`.
  - Types map Exasol to Arrow once (`export/arrow_schema.rs`, which owns the type rules and refusals), and the Iceberg schema derives from the Arrow schema. Delta and plain Parquet will derive theirs the same way. The Parquet writer needs the Arrow schema anyway.
  - The writer follows the target table's schema: it binds source columns to table fields by name and takes decimal scales from the table, so append (#567) adds only the schema-matching rules. "Table exists" is then the only create-only check.
- **Architecture:** § Components (export), § Data Flow
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
