# Decision Log: test-lakekeeper-batch-check-fixtures-two-principals

## Interview

**Q:** The stack runs Lakekeeper `allowall` (no OpenFGA). Per-principal grants and batch-check on the OSS authorizer need OpenFGA. How to add the two-principal environment?
**A:** Separate OpenFGA overlay (Recommended). A new docker-compose overlay adds OpenFGA and switches Lakekeeper to the openfga authz backend, leaving existing allowall suites untouched.

**Q:** How should the two principals be identified in Keycloak?
**A:** Two new Keycloak client-credentials clients. Each service-account client gets its own Lakekeeper user id, which exercises the opaque-sub case directly.

**Q:** How should findings (id convention, required privilege, base-path relation) be reported?
**A:** Spec scenarios asserted by e2e tests plus a notes file. Each finding becomes a checked scenario in the lakekeeper-e2e spec and a test, and a notes file records rationale for #415.

## Design Decisions

### [1] The OpenFGA overlay switches the existing `lakekeeper` service in place

- **Decision:** `docker-compose.lakekeeper.openfga.yml` adds `openfga-db`, `openfga-migrate`, and `openfga`, and sets `LAKEKEEPER__AUTHZ_BACKEND=openfga` on the existing `lakekeeper-migrate` and `lakekeeper` services. The authorization suite's setup asserts `authz-backend` `openfga`, and the allowall suite asserts `allow-all` (decision [6]).
- **Alternatives:** A second Lakekeeper service beside the allowall one (rejected: it duplicates the migrate and serve configuration with a second port and IP, and the interview chose switching). OpenFGA in `docker-compose.lakekeeper.yml` (rejected: it changes the authorizer of the allowall suites). OpenFGA's `memory` datastore (rejected: an OpenFGA restart loses every tuple while Lakekeeper's catalog persists, the same failure as a backend switch).
- **Rationale:** Interview answer 1. `docker-compose.lakekeeper.azure.yml` edits the same service in place.
- **Consequences:** One stack runs one authorizer, so the two suites never share a running stack. Lakekeeper rebuilds no grants across a backend switch after bootstrap, so a persisted local stack needs `down -v` before it changes overlays. The overlay header and the Makefile comment say so.
- **Promotes to ADR:** no

### [2] Test principals are created through Keycloak's Admin REST API, never in the shared realm file

- **Decision:** The harness creates `lakehouse-reader-a`, `lakehouse-reader-b`, and `lakehouse-checker` at provisioning time through Keycloak's Admin REST API. No test-only principal enters `scripts/keycloak-realm-iceberg.json`.
- **Alternatives:** Add the clients to `scripts/keycloak-realm-iceberg.json` (rejected: `deploy/lakekeeper-stack/main.tf:94` uploads that file and the AWS Keycloak imports it, so principals with committed secrets would reach a public-IP deployment that runs `allowall`, where any valid token reads every table). A second realm file mounted by the overlay (rejected: it duplicates the realm and lets the `lakehouse` client definitions drift). A second realm name (rejected: it changes the token issuer Lakekeeper trusts and therefore the `idp-id` of every principal).
- **Rationale:** The realm file is shared with a real deployment, so adding a test principal to it widens that deployment's attack surface without any test gain.
- **Consequences:** Keycloak mints each principal's `sub` per stack, so the suite reads ids at runtime and never hardcodes one. The scenario "The shared realm file carries no authorization-suite principal" enforces the rule.
- **Promotes to ADR:** yes

### [3] A third principal, `lakehouse-checker`, carries the privilege finding

- **Decision:** The privilege scenarios run as `lakehouse-checker`, whose server, project, and warehouse assignments each test sets explicitly before it asserts.
- **Alternatives:** A reader as the checker (rejected: it changes grants that the two-principal scenario asserts). The `lakehouse` operator as the only positive case (rejected: `operator` is the broadest role and proves no minimum).
- **Rationale:** #414 asks for the privilege a service account needs, verified on the OSS authorizer. Only a principal that moves between a negative and a positive state proves a minimum. The interview answer's two clients stay the two principals with distinct table grants.
- **Promotes to ADR:** no

### [4] A separate `lakekeeper-authz-e2e` feature, binary, make target, and CI job, with no Exasol

- **Decision:** The suite is `crates/lakehouse-engine/tests/e2e_lakekeeper_authz_test.rs` behind `lakekeeper-authz-e2e`, run by `make test-e2e-lakekeeper-authz` and the `e2e-lakekeeper-authz` CI job. Its stack starts no Exasol.
- **Alternatives:** Gate the binary behind `lakekeeper-e2e` (rejected: `cargo test --features lakekeeper-e2e` would then run a binary that fails on the allowall stack, which changes that feature's meaning). Add the tests to `e2e_lakekeeper_test.rs` (rejected: that binary provisions Exasol and runs on `allowall`).
- **Rationale:** `azure-e2e` is the precedent for a stack variant with its own feature. The contract needs Keycloak, Lakekeeper, OpenFGA, and SeaweedFS only.
- **Consequences:** The make target has no `cross-udf-build` prerequisite. The CI job still declares `needs: [build-so]`, because the shared `e2e-setup` action downloads the `.so`. Whether the job becomes a required check is a human decision.
- **Promotes to ADR:** no

### [5] Fixtures live with their consumer, pair each request with its response, and are compared by shape

- **Decision:** `crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/` holds `allowed.json`, `denied.json`, `missing.json`, and `cannot-inspect.json`. Each file records the calling client id, the request, the status, and the response, with ids as placeholders. The drift test compares shapes, and `LH_LAKEKEEPER_FIXTURE_CAPTURE=1` rewrites the files from the live stack.
- **Alternatives:** `crates/lakehouse-engine/tests/assets/` (rejected: #415 adds `batch_check` beside `CatalogSession` in `crates/lakehouse-catalog`, whose unit tests would then read across crates). Byte-equal comparison (rejected: message text and `Error ID` values change per run). Response-only fixtures (rejected: a response is meaningless to #415 without the request that produced it).
- **Rationale:** #414 asks for reusable fixtures, and the reuse is #415's parser and request builder.
- **Consequences:** `cannot-inspect.json` goes beyond the three requested cases, because it is the error #415 meets when its service account lacks the privilege. The `error-on-not-found: true` 404 stays in the README only, because #415 sends `false`.
- **Promotes to ADR:** no

### [6] The allowall suite asserts its backend

- **Decision:** `e2e_lakekeeper_test.rs` gains `lakekeeper_stack_runs_allowall_backend`, which fails unless `GET /management/v1/info` reports `allow-all`. No other allowall test changes.
- **Alternatives:** Keep the allowall suite byte-unchanged and rely on review (rejected: "the existing allowall overlay is unchanged" would stay untested). A text check of the compose file (rejected: substring matching on YAML is unreliable, and a YAML parser is a new dependency for one assertion).
- **Rationale:** One read-only assertion keeps interview answer 1's "allowall suites untouched" true over time. A later overlay edit cannot silently change the authorizer they run on.
- **Promotes to ADR:** no

### [7] The #415 notes file is the fixture directory's README

- **Decision:** The findings' rationale lives in `crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/README.md`. Each entry states whether a suite test or only the planning probe verified it.
- **Alternatives:** A note in this plan directory (rejected: `speq record` archives the plan to the gitignored `specs/_recorded/`). A page in `docs/` (rejected: `docs/` is operator-facing, and these notes describe an internal contract until #415 ships the property).
- **Rationale:** Interview answer 3. #415 reads the fixtures, so the README sits where its author looks.
- **Promotes to ADR:** no

### [8] Contract findings handed to #415

- **Decision:** This plan records three findings and leaves every resulting design choice to #415.
  - Principal id: a direct login's id is `oidc~<sub>`, an opaque UUID on Keycloak under Lakekeeper's default subject claims (`oid`, then `sub`). A grant to a template-derived id takes effect before that user logs in. A `USER_MAPPING` template therefore needs no per-user configuration only when grants name template-derived ids, or when `LAKEKEEPER__OPENID_SUBJECT_CLAIM` names a claim the template reproduces.
  - Privilege: the least standing grant that lets a caller check another identity across a warehouse is `manage_grants` on that warehouse. Server `admin` and `select` do not suffice. `manage_grants` also permits writing grants, and model `v4.7` has no read-only inspect relation. A missing privilege fails the whole batch with 403.
  - Base path: `/management/v1` sits beside `/catalog` at the server root, the catalog never advertises it, and a proxy prefix moves only the advertised catalog URI. Deriving the management base from a catalog URI is defined only for a URI ending in `/catalog`, so it is not a safe default. #415 needs an explicit property.
- **Alternatives:** Choose #415's property names and defaults here (rejected: #415 owns its configuration surface).
- **Rationale:** #414's scope asks to confirm each fact and to hand an unsafe derivation to #415 as a finding. The planning probe and the upstream sources in plan.md's Context back each finding, and the suite re-asserts each one.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] The plan targets the MinIO stack, but its base branch runs SeaweedFS

- **Finding:** The plan named the `origin/main` MinIO services (`minio`, `minio-init`). It sits on `feat/add-aws-assume-role-credentials` (open PR #441), whose commit `a173738` replaces MinIO with `seaweedfs`. Task 3.6's `docker wait` on `minio-init` and every `up --wait minio` command fail on that stack. The harness delta's "(OIDC + MinIO)" title contradicts the recorded "(OIDC + SeaweedFS)" spec. No artifact named #441.
- **Direction change:** plan.md § Dependencies lists PR #441 as a merge-order prerequisite. § Context names the SeaweedFS stack as the target and states that `-bucket=warehouse` removes the storage one-shot. The architecture diagram, tasks 1.1 and 3.6, Manual Testing rows 1 and 6, and decision [4] name `seaweedfs` and drop `minio-init`. The harness delta carries the recorded "(OIDC + SeaweedFS)" title and storage wording. The probe paragraph keeps MinIO, because the 2026-09-30 probe ran on MinIO. It states that no confirmed fact depends on the object store. The contract delta's "(static MinIO credentials)" clause is deleted under the next finding.
- **Promotes to ADR:** no

### [plan-review] The contract delta's description and Background carry facts no scenario uses

- **Finding:** No scenario step in `lakekeeper-authz-contract` depends on the description's "the adapter calls no management API" (false once #415 ships). The same holds for Background bullet 1's PostgreSQL store, Exasol absence, and version pins, bullet 3's Keycloak Admin REST API, and bullet 4's MinIO credentials and "metadata-only".
- **Direction change:** The description sentence and every listed clause are deleted. The Lakekeeper `v0.13.1` and OpenFGA `v1.8.16` pins move into the GIVEN of "Committed batch-check fixtures match the live contract". The fixtures are a capture of those versions, so a version change needs a recapture. plan.md, decision [2], decision [4], and task 1.5 keep the deleted facts.
- **Promotes to ADR:** no
