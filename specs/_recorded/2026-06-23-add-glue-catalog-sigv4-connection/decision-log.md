# Decision Log: add-glue-catalog-sigv4-connection

Date: 2026-06-23

## Interview

**Q:** Same plan as the memory-budget items from `next.md`, or separate?
**A:** Combine the SDK bump + `ctx.memory_limit()` wiring with the Glue/vended-credentials work in THIS plan. The accurate-`ResourcesExhausted`-error item and the spill-free-chunking research from `next.md` are explicitly a FOLLOW-UP plan — out of scope here.

**Q:** Where do AWS credentials come from in the UDF environment?
**A:** An Exasol CONNECTION object — NOT plain VS properties (that is the current model being replaced/augmented).

**Q:** What does "vended credentials" mean here?
**A:** The Iceberg REST credential-vending protocol: when the adapter calls Glue's `load_table` endpoint, the response carries temporary short-lived S3 credentials that override the static creds for data-file access.

**Q:** How do E2E tests get AWS credentials?
**A:** A feature-gated smoke/performance test against a real Exasol cluster + Glue catalog loaded with meaningful data, runnable from CI or triggered manually. Credentials come from environment variables. Unlike the local Docker E2E (which must FAIL when the stack is down), this cloud test is opt-in: it SKIPS when the AWS creds env vars are absent.

**Q:** Which SDK accessor exposes the CONNECTION and the memory limit?
**A:** The accessor is `ctx.connection(name) -> ConnectionObject {kind, address, user, password}` (NOT `get_connection()`, not injected via request JSON), and `ctx.memory_limit() -> u64` (0 = unavailable sentinel). Both ship in `exasol-udf-sdk` 0.16.0 behind the already-enabled `connect-back` feature. Mirror the sibling project's CONNECTION convention: `address` = catalog URI, `password` = a JSON object string holding all credentials.

## Design Decisions

### [1] Source credentials from an Exasol CONNECTION object (mirror the sibling project)

- **Decision:** Read the catalog URI + S3 credentials from `ctx.connection(<CATALOG_CONNECTION>)`: `address` is the URI, `password` is a JSON object parsed for `warehouse`, `endpoint`, `region`, `access_key`, `secret_key`, and optional `session_token`/`path_style`/`use_sigv4`/`use_vended_credentials`. Replace `extract_connection_props`'s plain-property reads. Errors never echo the password.
- **Alternatives:** Keep reading plain VS properties (rejected — leaks creds into `CREATE VIRTUAL SCHEMA` text and the query profile); inject creds via request JSON (rejected — not how the SDK surfaces them).
- **Rationale:** CLAUDE.md mandates mirroring the sibling project's VS conventions; CONNECTION keeps secrets out of SQL text and lets Exasol access-control them.
- **Promotes to ADR:** yes

### [2] Self-issue a SigV4-signed load_table GET instead of using RestCatalogBuilder for Glue

- **Decision:** For the Glue path, sign catalog/load_table requests with `aws-sigv4` and parse `iceberg_catalog_rest::LoadTableResult` ourselves, rather than routing through `RestCatalogBuilder`.
- **Alternatives:** `RestCatalogBuilder::with_client(reqwest::Client)` (rejected — 0.9.1 takes only a plain client, dispatches internally, so per-request SigV4 signing and `reqwest-middleware` cannot attach); fork iceberg-catalog-rest (rejected — heavier to maintain).
- **Rationale:** Research confirmed 0.9.1 exposes no per-request signing seam, its inner client is private, and `load_table()` silently drops the `storage_credentials` block. A self-issued signed GET is the only clean in-tree path that also recovers vended creds.
- **Promotes to ADR:** yes

### [3] Use `aws-sigv4` + `aws-credential-types` for signing

- **Decision:** Add `aws-sigv4 = "1.4"` and `aws-credential-types = "1.2"`.
- **Alternatives:** `aws-sign-v4` (rejected — no declared MSRV, single-maintainer, last release 2024); hand-rolled SigV4 (rejected — canonicalization is error-prone for session tokens and payload hashing).
- **Rationale:** Both declare MSRV 1.91.1 and build on rustc 1.92; official and maintained. Implementation must still confirm co-resolution with the iceberg-0.9.1 arrow-57 / workspace arrow-58 split and the `fastnum 0.7.4` pin.
- **Promotes to ADR:** no

### [4] Apply vended creds via the sibling project's `merge_vended_into_storage` shape

- **Decision:** Extract vended `s3.access-key-id`/`s3.secret-access-key`/`s3.session-token` from the load_table response (`storage_credentials[*].config`, longest-prefix match, with fallback to the flat `config` map), override the static keys in each `ScanSpec.storage`, preserving static endpoint/region/path_style. Resolve once in the planning layer.
- **Alternatives:** Rely on iceberg-catalog-rest to auto-apply vended creds (rejected — 0.9.1 drops `storage_credentials`); vend per-node in the scan UDF (rejected — violates resolve-once + stateless-UDF invariants).
- **Rationale:** Vending must happen once per query in the thin VS layer; the scan UDF only ever receives final credentials in its spec and never re-authenticates.
- **Promotes to ADR:** yes

### [5] Wire `ctx.memory_limit()` into the DataFusion pool budget

- **Decision:** Thread `ctx.memory_limit()` from the scan `run()` into `build_session_context`, replacing the `scan/mod.rs:445` 0-sentinel; keep `0` as the unknown → default-budget sentinel. `build_runtime_env` already sizes the pool at 0.6×limit.
- **Alternatives:** Leave the hardcoded default (rejected — never uses the real per-instance budget).
- **Rationale:** The SDK accessor now exists (0.16.0); `runtime.rs` was already written for this; only the call site needed the real value.
- **Promotes to ADR:** no

### [6] Separate `cloud-e2e` cargo feature with skip-when-absent semantics

- **Decision:** Gate the Glue smoke/perf test behind a new `cloud-e2e` feature, distinct from `exasol-e2e`; skip (early return, no failure, no network call) when the AWS creds env vars are absent.
- **Alternatives:** Reuse `exasol-e2e` (rejected — that suite must FAIL when its stack is down; a cloud account is not always attached).
- **Rationale:** The user explicitly wants an opt-in cloud test that is safe to run without credentials; mixing it into `exasol-e2e` would break the local fail-when-down contract.
- **Promotes to ADR:** yes

### [7] Default `use_sigv4` and `use_vended_credentials` to false

- **Decision:** Both flags default false in the parsed credential block, so a CONNECTION that omits them reproduces the existing unsigned MinIO/local-REST behaviour exactly.
- **Alternatives:** Infer signing from the URI host (rejected — brittle, surprising).
- **Rationale:** Preserves the local Docker E2E path unchanged and keeps the cloud behaviour explicit/opt-in per CONNECTION.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
