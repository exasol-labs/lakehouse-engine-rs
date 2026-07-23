# Decision Log: add-lakekeeper-e2e

## Interview

Headless plan (`speq-plan-pr`); no live interview. The orchestrator supplied a discovery
brief in place of interview answers. Key facts treated as answers:

**Q:** How does this engine talk to catalogs, and what does "different catalog" mean here?
**A:** Exclusively through the Iceberg REST protocol via `iceberg-catalog-rest`. Different
catalogs = different auth modes on one REST client, not different client implementations.

**Q:** What is Lakekeeper's typical auth, and can the engine's existing CONNECTION fields
express it?
**A:** OAuth2 client-credentials against an external OpenID provider (Keycloak). The engine
already ships `client_id`/`client_secret`/`oauth2_server_uri`/`scope`
(`vs-adapter/rest-catalog-oauth-auth`); prove interop through that shape, do not invent fields.

**Q:** Additive or a replacement of the existing unauthenticated REST-catalog E2E stack?
**A:** Additive — a new opt-in suite, keeping the fast baseline unchanged, unless research shows
a cleaner path. Decide CI-gating and document it (headless assume-and-document call).

**Q:** How should discovered gaps be handled?
**A:** A gap becomes an in-scope fix in this same plan, or — if a deliberate, accurately-scoped
trade-off — a tracked GitHub issue cited inline in the spec, never a silent gap. Any
scanning/pushdown/schema change must be checked against the Iceberg spec during planning.

## Design Decisions

### [1] Additive `lakekeeper-e2e` feature, not a baseline replacement

- **Decision:** Add a dedicated `lakekeeper-e2e` cargo feature and an overlay compose file; leave
  the unauthenticated `exasol-e2e` baseline and its stack untouched.
- **Alternatives:** Replace the `apache/iceberg-rest-fixture` baseline with Lakekeeper; fold
  Lakekeeper services into the baseline `docker-compose.yml`.
- **Rationale:** The baseline suite is fast and unauthenticated by design; Lakekeeper adds Postgres
  + Keycloak + OIDC weight. Isolation keeps the baseline fast and its failure signal clean.
- **Promotes to ADR:** yes

### [2] Verify continuously in a dedicated CI job

- **Decision:** Add an `e2e-lakekeeper` CI job that brings up the overlay stack and runs the suite,
  rather than an opt-in-only path.
- **Alternatives:** Opt-in-only (never in CI, mirroring `cloud-e2e`); fold into the existing `e2e`
  job.
- **Rationale:** The user's intent is to verify Lakekeeper actually works, and the stack is all
  local containers (unlike `cloud-e2e`, which needs real AWS). Continuous CI catches regressions;
  a separate job protects the baseline job's stability and runtime.
- **Promotes to ADR:** yes

### [3] Reuse existing OAuth2 CONNECTION fields; no schema change

- **Decision:** Use `client_id`/`client_secret`/`oauth2_server_uri`/`scope` for catalog auth and
  carry the Lakekeeper warehouse-NAME in the existing `warehouse` field. Add no CONNECTION field.
- **Alternatives:** Introduce Lakekeeper-specific auth or warehouse fields.
- **Rationale:** Research confirms Lakekeeper uses the standard OAuth2 client-credentials grant and
  the standard `/v1/config?warehouse=` prefix mechanism, both already implemented. The engine needs
  no new field to reach Lakekeeper.
- **Promotes to ADR:** yes

### [4] No adapter code change expected; interop-fix path is contingent

- **Decision:** Plan no adapter code on the green path. The base-path + per-warehouse-prefix
  behavior the E2E depends on is recorded as a `CHANGED` scenario on
  `vs-adapter/rest-catalog-oauth-auth`. A real gap found at implementation lands as a `CHANGED`
  delta here, or a deliberate trade-off as a tracked GitHub issue cited inline.
- **Alternatives:** Pre-author speculative adapter deltas for gaps that may not exist.
- **Rationale:** `resolve_load_table_prefix`, `build_load_table_url`, the OAuth2 grant, the
  access-delegation header, and STS extraction already implement every Lakekeeper mechanism. Adding
  speculative deltas would over-scope; the one CHANGED scenario pins the genuinely-uncovered
  base-path/prefix contract.
- **Promotes to ADR:** no

### [5] Test both static and vended S3 credential modes as hard requirements

- **Decision:** Create two Lakekeeper warehouses — one with S3 access delegation disabled (static
  MinIO credentials) and one `sts-enabled` (vended credentials) — and cover both in scans as hard
  pass/fail requirements. Configure MinIO's STS AssumeRole endpoint and an IAM policy/role
  deterministically in the stack so the vended path is reproducible.
- **Alternatives:** Vended-only (Lakekeeper's recommended default); static-only (closest to the
  existing MinIO baseline); treating vended as best-effort with a skip-if-flaky off-ramp.
- **Rationale:** Both are shipped engine credential modes (mission Capability 8). MinIO's STS
  AssumeRole is a deterministic, configurable feature, not an environmental gamble; configuring it
  explicitly (plan task 1.3) removes the earlier flakiness concern and lets vended stand as a hard
  requirement alongside static. Two warehouses cost only two management-API POSTs.
- **Supersedes:** the earlier framing of static as a "low-risk floor if STS-against-MinIO proves
  flaky"; vended is no longer a fallback-guarded best-effort path.
- **Promotes to ADR:** no

### [6] Keycloak as the IdP, pre-seeded via realm import

- **Decision:** Run Keycloak with a realm-export JSON defining a confidential client, the
  client-credentials grant, and an audience mapper whose `aud` matches
  `LAKEKEEPER__OPENID_AUDIENCE`.
- **Alternatives:** A mock OAuth2 token endpoint; Lakekeeper built-in auth (none documented).
- **Rationale:** Keycloak is Lakekeeper's documented reference IdP; a real grant exercises the
  engine's shipped OAuth2 code. Lakekeeper validates `aud`, so the audience mapper is mandatory
  for the token to be accepted.
- **Promotes to ADR:** no

### [7] In-process bootstrap and warehouse creation

- **Decision:** Perform Lakekeeper bootstrap and warehouse creation (management API) in the Rust
  harness, not via compose init containers.
- **Alternatives:** `curlimages/curl` init containers (as in Lakekeeper's own example compose).
- **Rationale:** Mirrors the existing harness, which already does SLC install and VS creation
  in-process; keeps the compose overlay lean and all provisioning in one place.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] Vended-credential path was staked on an unresolved STS-against-MinIO uncertainty

- **Finding:** The harness spec encoded the vended-credential scan as an unconditional, always-on
  passing scenario with fail-not-skip discipline, while plan.md and decision-log [5] reserved a
  static-only fallback "if STS-against-MinIO proves flaky." That contingency fit neither category
  task 6 covers (an engine gap fixable in-plan, or a deliberate engine trade-off tracked as an
  issue), so it caught nothing — leaving it ambiguous whether vended interop was a hard requirement
  or best-effort, while a mandatory always-on CI scenario depended on it.
- **Direction change:** Committed to vended as a hard requirement (reviewer option (a)). Deleted the
  "fallback if flaky" language from plan.md Patterns and Consequences and from decision-log [5].
  Added explicit MinIO STS AssumeRole configuration as a deterministic stack step (new plan task
  1.3) and to the harness Background and bootstrap scenario, so the vended path is reproducible. The
  vended scenario's fail-not-skip discipline now applies unconditionally; plan.md, decision-log, and
  the spec agree that both static and vended are hard pass/fail requirements.
- **Promotes to ADR:** no

### [plan-review] Advisory findings addressed

- **Finding:** Three advisories flagged alongside the blocker — (1) task 4.1 ("reuse
  `tests/common/seed.rs`") understated effort because `build_seed_catalog` (seed.rs:133) hardcodes
  static `minioadmin` S3 creds and injects no catalog auth; (2) the plan's "no adapter code change"
  claim assumed the `iceberg-catalog-rest` built-in OAuth2 client and the adapter's own
  `oauth2_client_credentials_grant` both interoperate with Keycloak, without stating the
  two-implementation split or verifying both; (3) the always-on CI job had no stated runtime budget
  or per-service health-gate timeouts. A fourth advisory flagged the Summary exceeding the prose
  length guideline.
- **Direction change:** (1) Split task 4 into an authenticated seed-catalog variant (task 4.1,
  parameterizing `build_seed_catalog` with optional OAuth2 client-credentials and storage creds)
  plus seeding (task 4.2). (2) Stated the two-OAuth2-implementation split in the Decision section,
  mapped the built-in client to the enumeration test (task 5.2) and the self-issued grant to the
  scan tests (tasks 5.3, 5.4), and named `iceberg-catalog-rest`'s OAuth2-vs-external-IdP behavior as
  the primary task-6 candidate. (3) Added task 7.2 for per-service health-gate timeouts and a
  wall-clock CI budget with fail-fast. (4) Split the Summary into two sentences, outcome first.
- **Promotes to ADR:** no
