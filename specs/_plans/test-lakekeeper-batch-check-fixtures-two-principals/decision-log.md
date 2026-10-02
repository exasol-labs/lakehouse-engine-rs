# Decision Log: test-lakekeeper-batch-check-fixtures-two-principals

## Interview

**Q:** The stack runs Lakekeeper `allowall` (no OpenFGA). Per-principal grants and batch-check on the OSS authorizer need OpenFGA. How to add the two-principal environment?
**A:** Separate OpenFGA overlay (Recommended). Superseded by the PR #456 review: OpenFGA goes straight into `docker-compose.lakekeeper.yml` (decision [1]).

**Q:** How should the two principals be identified in Keycloak?
**A:** Two new Keycloak client-credentials clients. Each service-account client gets its own Lakekeeper user id, which exercises the opaque-sub case directly. The review put them in the realm file (decision [2]).

**Q:** How should findings (id convention, required privilege, base-path relation) be reported?
**A:** Spec scenarios asserted by e2e tests plus a notes file. Superseded by the PR #456 review: findings live in decision [5] and the fixtures README, and become specs in #415 as our behaviour (decision [5]).

## Design Decisions

### [1] OpenFGA joins `docker-compose.lakekeeper.yml`, and the one Lakekeeper suite runs on it

- **Decision:** `docker-compose.lakekeeper.yml` gains `openfga-db`, `openfga-migrate`, and `openfga` on the `lakehouse` network, and sets `LAKEKEEPER__AUTHZ_BACKEND=openfga` on `lakekeeper-migrate` and `lakekeeper`. New tests go into `e2e_lakekeeper_test.rs`. The Azure overlay names `openfga` again in its `!override` `depends_on`, the Azure and Lakekeeper CI `up` lists gain the OpenFGA services, and `deploy/README.md` stops claiming the bench box runs the same services. OpenFGA is pinned to `v1.14.2`.
- **Alternatives:** A separate overlay, feature, binary, make target, and CI job (rejected by the PR #456 review: one suite, some tests use permissions and some do not). A second Lakekeeper service (rejected: it duplicates migrate and serve configuration). OpenFGA's `memory` datastore (rejected: a restart loses every tuple while Lakekeeper's catalog persists). OpenFGA `v1.8.16` (rejected: Lakekeeper documents v1.11 or later as required and tests against v1.14).
- **Rationale:** One stack and one suite are less to maintain, and running the existing scenarios on the enforcing stack proves permissions do not break them.
- **Consequences:** Lakekeeper rebuilds no grants across a backend switch after bootstrap, so a persisted local stack needs `down -v` once. The compose header says so.
- **Promotes to ADR:** no

### [2] The three test clients live in the shared realm file

- **Decision:** `lakehouse-reader-a`, `lakehouse-reader-b`, and `lakehouse-checker` are added to `scripts/keycloak-realm-iceberg.json`, with fixed test secrets.
- **Alternatives:** Keycloak Admin REST API at provisioning time (rejected by the review: more harness code for no gain). A second realm file or name (rejected: it duplicates the realm or changes the issuer).
- **Rationale:** `deploy/lakekeeper-stack` is a short-lived benchmark box that accepts traffic only from the deployer's IP. The file already contains `lakehouse` and its secret, so the new clients expose nothing new.
- **Consequences:** Keycloak still mints each principal's `sub` per stack, so the suite reads ids at runtime and never hardcodes one. No realm-file guard test is needed.
- **Promotes to ADR:** no

### [3] A third principal, `lakehouse-checker`, carries the privilege finding

- **Decision:** The privilege tests run as `lakehouse-checker`, whose server, project, and warehouse assignments each test sets explicitly before it asserts.
- **Alternatives:** A reader as the checker (rejected: it changes grants that the two-principal scenario asserts). The `lakehouse` operator as the only positive case (rejected: `operator` is the broadest role and proves no minimum).
- **Rationale:** Only a principal that moves between a negative and a positive state shows which grant is enough.
- **Promotes to ADR:** no

### [4] Fixtures live with their consumer, pair each request with its response, and are compared by shape

- **Decision:** `crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/` holds `allowed.json`, `denied.json`, `missing.json`, and `cannot-inspect.json`. Each file records the calling client id, the request, the status, and the response, with ids as placeholders. The drift test compares shapes and carries no scenario. `LH_LAKEKEEPER_FIXTURE_CAPTURE=1` rewrites the files from the live stack.
- **Alternatives:** `crates/lakehouse-engine/tests/assets/` (rejected: #415 adds `batch_check` beside `CatalogSession` in `crates/lakehouse-catalog`). Byte-equal comparison (rejected: message text and `Error ID` values change per run). Response-only fixtures (rejected: a response is meaningless to #415 without its request).
- **Rationale:** #414 asks for reusable fixtures, and the reuse is #415's parser and request builder. The README sits where #415's author looks.
- **Consequences:** `cannot-inspect.json` goes beyond the three requested cases, because it is the error #415 meets when its service account lacks the privilege. The `error-on-not-found: true` 404 stays in the README only, because #415 sends `false`. The fixtures are a capture of the pinned Lakekeeper and OpenFGA versions, so a version change needs a recapture.
- **Promotes to ADR:** no

### [5] Lakekeeper findings handed to #415

Specs describe our behaviour, not Lakekeeper's, and #414 changes nothing a user can see, so these findings are not a spec. They become specs in #415, rewritten as the adapter's behaviour: user mapping, which endpoint it calls, and what happens on a denied or missing table. Each finding is also in the fixtures README with its evidence. The Lakekeeper `v0.13.1` source check in the PR #456 review corrected the first draft.

- **Decision:** This plan records three findings and leaves every resulting design choice to #415.
  - Principal id: a direct login's id is `oidc~<sub>`, an opaque UUID on Keycloak under Lakekeeper's default subject claims (`oid`, then `sub`). A grant to a template-derived id takes effect before that user logs in. A `USER_MAPPING` template therefore needs no per-user configuration only when grants name template-derived ids, or when `LAKEKEEPER__OPENID_SUBJECT_CLAIM` names a claim the template reproduces.
  - Privilege: checking another identity needs `can_read_assignments` on each checked object. Warehouse `manage_grants` is enough across a warehouse but is not the minimum. Namespace `manage_grants` also works, because `manage_grants` is inherited from the parent, and `can_read_assignments` includes `can_grant_select`. Server `admin` and warehouse `select` do not suffice. `manage_grants` also permits writing grants, and a missing privilege fails the whole batch with 403. Task 1.7 ran them live on OpenFGA `v1.14.2`: `manage_grants` on the checked table or the namespace, table `ownership`, and table `pass_grants` with `select` (the `can_grant_select` path) each answer 200, while `pass_grants` alone (table, namespace, or warehouse), `select` alone on the table, and `manage_grants` on a different table answer 403. Project `project_admin` also works. Every re-run `v1.8.16` probe fact held on `v1.14.2`; the management base under `x-forwarded-prefix` was not re-checked.
  - Base path: Lakekeeper builds both bases from the same base URL (`{base}/catalog` and `{base}/management`), and `x-forwarded-prefix` moves both. The routes stay at the server root, so the case that breaks deriving the management base from a catalog URI is a gateway that rewrites paths, not a catalog that never advertises it. An explicit property is still the right recommendation for #415.
- **Alternatives:** Choose #415's property names and defaults here (rejected: #415 owns its configuration surface). A `lakekeeper-authz-contract` spec (rejected by the review: it describes Lakekeeper).
- **Rationale:** #414's scope asks to confirm each fact and hand the result to #415. The planning probe and the upstream sources in plan.md's Context back each finding.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] The plan targeted the MinIO stack, but its base branch ran SeaweedFS

- **Finding:** The plan named MinIO services that PR #441 replaces with `seaweedfs`.
- **Direction change:** Resolved. PR #441 is merged, so the plan targets the SeaweedFS stack and the dependency note is gone.
- **Promotes to ADR:** no

### [pr-review] PR #456 review by antonireus: simplify the plan

- **Finding:** (1) A `lakekeeper-authz-contract` spec describes Lakekeeper, not our system. (2) A second suite, feature, make target, CI job, and compose overlay are unnecessary. (3) The test clients can go in the realm file. (4) The findings had three errors: the OpenFGA version, the privilege minimum, and the base-path explanation.
- **Direction change:** The contract spec is deleted. The harness delta keeps three scenarios: the stack enforces permissions, two principals hold different table grants, and the existing scenarios still pass with permissions on. Findings moved to decision [5] and the fixtures README. OpenFGA goes into `docker-compose.lakekeeper.yml` (decision [1]) with the Azure overlay, CI, and `deploy/README.md` follow-ups. The test clients go in the realm file (decision [2]), which drops the Admin REST API provisioning, the old decision [2] ADR, and the realm-file guard test. The allowall guard test and the separate feature, binary, target, and job are dropped. The OpenFGA pin is `v1.14.2`, the privilege is described as "enough across a warehouse" with the narrower options listed, and the base-path finding is corrected. Task 1.7 checks all of it live.
- **Promotes to ADR:** no
