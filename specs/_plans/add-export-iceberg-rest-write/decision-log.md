# Decision Log: add-export-iceberg-rest-write

## Interview

- Q: SINGLE_COMMIT atomicity route → A: raw REST `stage-create` (iceberg-rust hardcodes false), commit with assert-create; table appears only with its data
- Q: Pre-read check for a bad CONNECTION → A: catalog `namespace_exists` call plus a probe write/delete with static storage keys in `generate_export_sql`; vended keys only get the catalog check (scoped to a table that may not exist yet). Superseded by decision [1] after review.
- Q: Concurrent and retried commits → A: raise `commit.retry.num-retries` on the created table and tag each snapshot with an export-id summary property, re-checked before every re-commit

## Design Decisions

### [1] The pre-read check probes storage at a staged table location, for static and vended credentials alike

- **Decision:** Before any source row is read, the export creates the missing namespace levels, stage-creates the target table without committing it, and writes and deletes one probe object at the staged location. It uses the credentials the writers will use: the CONNECTION's static keys, or the credentials vended in the stage-create answer.
- **Rationale:** Interview answer 2 limited vended credentials to a catalog call, because no table location exists before the table does. A stage-create answer carries the location and, on Lakekeeper, vended credentials scoped to it, and the staged table stays invisible and reserves nothing (measured, plan.md § Context). One probe path then covers static keys, vending, and an `http://` vended endpoint without `ALLOW_HTTP`.
- **Alternatives:** Probe static keys at the namespace `location` property: the namespace may not exist yet, and the Iceberg REST spec does not require that property.
- **Consequences:**
  - Every export, parallel mode included, needs a catalog that accepts `stage-create: true`. On a catalog that refuses it, every export fails before any source row is read, naming the catalog.
  - A missing namespace is created even when the export later fails or writes no rows.
  - The probe delete is best effort, so storage that refuses deletes keeps one probe object under a location no table uses.
- **Promotes to ADR:** no

### [2] The export commit loop belongs to lakehouse-catalog and re-checks the writer's tag before every re-commit

- **Decision:** iceberg-rust builds a writer's snapshot, and `lakehouse-catalog` sends the commit through its own catalog HTTP client. After a conflict or an uncertain outcome, the loop reloads the table with all snapshots and commits again only when no snapshot carries the writer's tag. It retries HTTP 409, 429, 502, 503, 504, and uncertain network outcomes with bounded backoff and a bounded attempt count, and never retries authentication (401, 403), schema, invalid-request, or existing-table errors.
- **Rationale:** Rule-2 criteria 2 and 4. iceberg-rust's `Transaction::commit` re-applies after HTTP 409, and the catalog HTTP client repeats a POST after HTTP 502 or 504, so a commit whose answer was lost returns 409 and a blind re-apply adds its rows twice. The Iceberg REST spec answers a commit with 500, 502, or 504 when "the commit state is unknown". `RestCatalog` would also bypass the one catalog HTTP client that § Constraints requires. ADR digest search (export, write, commit, stage, retry): no ADR covers catalog writes.
- **Alternatives:** `RestCatalog` with `Transaction::commit` and a raised retry count, as in the spike: duplicate rows and a bypassed client.
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

### [6] The FileIO of an export table refreshes vended credentials through the catalog's credentials endpoint before they expire

- **Decision:** With vended credentials, the FileIO of every table the export writes takes its S3 credentials from a credential source in `lakehouse-catalog`. The source starts with the credentials of the create, stage-create, or load answer, and loads new ones through the table's Iceberg REST `loadCredentials` endpoint on the CONNECTION's catalog address when they expire within five minutes. The pre-read check refreshes the staged table's credentials once. On a catalog that does not advertise the endpoint, a writer that outlives its credentials fails with an error that states their expiry.
- **Rationale:**
  - A writer can outlive Lakekeeper's 3,599 s credentials. Under `SINGLE_COMMIT` the staged table cannot be reloaded, and Lakekeeper answers `loadCredentials` for a staged table (plan.md § Context).
  - The commit loop keeps the writer table's FileIO (task 1.3), so the refresh lives in that FileIO, through iceberg-rust's `customized_credential_load` hook.
  - Five minutes is the prefetch time of Iceberg Java's `VendedCredentialsProvider`. Lakekeeper serves a cached credential for half its lifetime, so the refresh gets new credentials for every lifetime of ten minutes or more, the 900 s STS minimum included.
  - The Iceberg REST spec defines `loadCredentials` and the `/v1/config` `endpoints` list: "If a server does not send the `endpoints` field, a default set of endpoints is assumed", and that set omits `loadCredentials`. The spec path on the CONNECTION's catalog address keeps the catalog token on that address, under the one catalog HTTP client, the proxy settings, and `ALLOW_HTTP`.
  - The pre-read refresh fails an export whose refresh would fail before any row is read, and it is the refresh an E2E can observe, because no E2E writer outlives a 900 s credential.
- **Alternatives:**
  - No refresh, with a documented one-hour limit: a `SINGLE_COMMIT` export runs one writer for the whole source.
  - Refresh after an expired-token failure: the failed request inside iceberg-rust fails the writer, and SeaweedFS reports expiry as `InvalidAccessKeyId`.
  - Call `client.refresh-credentials-endpoint`: the catalog would choose where the catalog token goes.
  - Reload the table: a staged table cannot be loaded, and re-staging at a location that holds files answers HTTP 400.
- **Consequences:**
  - Each refresh resolves a fresh catalog session, because the writer's OAuth2 token may have expired. An expired static bearer token fails the refresh.
  - A catalog that advertises `loadCredentials` but refuses it for a staged table fails every vended export before any row is read.
  - Under `SINGLE_COMMIT`, a racing statement that stages the same name at another location makes the refresh fail, naming the location.
  - Credentials without `s3.session-token-expires-at-ms` are never refreshed. Static storage keys keep `StorageBackend::file_io()`.
- **Promotes to ADR:** no

## Review Findings

### [1] [plan-review] Decision [1] replaces interview answer 2

- **Finding:** Decision [1] replaces interview answer 2 and adds two user-visible effects: a missing namespace is created before any row is read, and every export needs a catalog that accepts `stage-create: true`.
- **Direction change:** The requester accepted decision [1]. plan.md § Impact, the `export/export-iceberg-table` delta, and task 4.1 state both effects.
- **Promotes to ADR:** no

### [2] [spike] A storage fault, not a source error, proves that a failed SINGLE_COMMIT export leaves no table

- **Finding:** Exasol evaluates the source before the writer reads its first row, so a failing source never reaches the writer.
- **Direction change:** The local E2E injects a proxy storage fault after the writer wrote its data files and before its commit. Rejected: `KILL STATEMENT` once a data file appears, because a data file appears only when the writer closes it, just before the commit.
- **Promotes to ADR:** no

### [3] [refresh] Vended credentials are refreshed in this plan

- **Finding:** The requester asked for vended-credential refresh in this plan, not in a follow-up.
- **Direction change:** Decision [6] and tasks 1.7, 3.2, and 4.1.
- **Promotes to ADR:** no
