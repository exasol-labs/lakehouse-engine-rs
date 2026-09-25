# Decision Log: add-aws-assume-role-credentials

## Interview

**Q:** Where should the STS-assumed-role credentials be used once obtained?
**A:** Storage + SigV4 catalog signing (Recommended). "Assumed-role temp credentials replace the static access_key/secret_key everywhere those would otherwise be used: signing SigV4 Glue catalog requests (when use_sigv4 is set) AND accessing S3 for Parquet files on the non-vended storage path."

**Q:** How often should the engine call STS AssumeRole, and how do the resulting short-lived credentials reach each parallel shard's UDF?
**A:** Once per query, sealed and distributed (Recommended). "The VS/plan layer calls STS AssumeRole once per query, then ships the resulting short-lived credentials to every shard through the same sealed-envelope mechanism already used for catalog-vended credentials. Avoids up to G (≤300) separate STS calls per query and reuses existing crypto machinery."

**Q:** A CONNECTION could set both use_vended_credentials=true (catalog vends short-lived S3 credentials per table) and aws_assume_role_arn (role assumption for storage). How should that combination be handled?
**A:** Allow both, assumed role wins for storage. "Assumed-role credentials override vended per-table credentials for storage access, on the theory that an explicit role assumption is a stronger operator intent than the catalog's default vending." The user declined the recommended option, which rejected the combination.

**Q:** Should role assumption be wired into the generic static-credential pipeline (so it also works for Delta/Unity Catalog static-storage CONNECTIONs), or scoped specifically to the Iceberg REST/Glue path from the ticket's example?
**A:** Generic (Recommended). "Build assume-role into the shared ConnectionCreds → StorageCreds resolution used by every catalog kind, per the project's format-neutral field rule. It works for Iceberg REST (incl. Glue) and Delta/Unity static-storage CONNECTIONs alike."

**Q:** Does aws_assume_role_arn require access_key/secret_key to also be supplied (the base IAM user identity that calls sts:AssumeRole), or should an ambient AWS credential chain (env vars, instance profile) be supported as the base identity too?
**A:** Static access_key/secret_key required (Recommended). "Matches the ticket's example exactly: aws_assume_role_arn requires access_key and secret_key on the same CONNECTION as the base identity for the AssumeRole call. No ambient/instance-profile credential resolution." Ambient credential-chain support is out of scope.

**Q:** (Settled by the ticket text, not re-asked.) How do `aws_assume_role_arn` and `aws_external_id` relate?
**A:** "aws_assume_role_arn and aws_external_id are optional but if either of them is present the other must be present." This is a project-chosen guard, stricter than AWS, which accepts AssumeRole without an ExternalId.

## Design Decisions

### [1] The assumed role becomes the request's single AWS identity, substituted into the credential set once per request

- **Decision:** Both adapter entry points call one `lakehouse-catalog` function, `resolve_aws_identity`, once per request inside the request's async runtime. It sends the STS `AssumeRole` request and returns the `ConnectionCreds` with `access_key`, `secret_key`, and `session_token` replaced by the session's. The adapter rebuilds the static storage backend from that set with `storage_block`. Every downstream reader then reads the session without naming the role.
- **Alternatives:** (a) A parallel effective-identity type threaded to every reader. Rejected: the catalog session, both format readers, both catalog clients, and redaction would each learn about role assumption. (b) The scan UDF calls STS per shard. Rejected by the interview (Q2): up to 300 calls per query. (c) Resolve the identity only where a request needs AWS access, skipping CREATE on an unsigned Iceberg catalog. Rejected: two call patterns instead of one, and CREATE no longer surfaces an STS misconfiguration.
- **Rationale:** After validation, every reader of the key triple wants the identity AWS requests are made as. The SigV4 signing region is different. ADR `derived-signing-region-separate-from-connection-region` keeps a derived signing region out of `region`, because `region` has a second reader with a different purpose, store placement. No reader of the key triple needs the base identity after STS returns. Validation and the sealing key read the CONNECTION before the substitution.
- **Consequences:**
  - SigV4 catalog signing, static storage, Iceberg manifest reads, Delta log reads, the direct-storage store, and value-based redaction need no change.
  - `createVirtualSchema`, `refresh`, and `setProperties` also call STS, so an STS misconfiguration fails at CREATE.
  - The scan UDF never calls STS.
  - The STS module redacts its own errors against the base `secret_key`, base `session_token`, and external id, because the rebuilt backend no longer holds them.
- **Promotes to ADR:** yes

### [2] One function owns the storage credential source precedence: assumed role, then vending, then the CONNECTION

- **Decision:** `ConnectionCreds::storage_credential_source()` returns `StorageCredentialSource::{AssumedRole, Vended, Connection}`. Every site that decides where storage credentials come from matches it exhaustively. The five sites are the Iceberg and Delta readers' split, the access-delegation header, the Unity Catalog temporary-credentials request, `scan_storage_for`, and the `path_style` guard.
- **Alternatives:** (a) Reject a CONNECTION that sets both a role and vending. This was the recommended interview option, and the user declined it (Q3). (b) Run the vended resolution, then swap the session into the resolved S3 backend. Rejected: it requests credentials it discards, it fails when the catalog vends none, and it splits one precedence across two steps. (c) A compound boolean check at each site. Rejected: the precedence would leak into five sites.
- **Rationale:** Interview Q3 sets the precedence. One owner keeps the five sites from disagreeing, and an exhaustive match makes a fourth source a build error at each site.
- **Consequences:**
  - `use_vended_credentials` has no effect on a CONNECTION that names a role. The adapter sends no `X-Iceberg-Access-Delegation` header and requests no Unity Catalog temporary credentials.
  - The `path_style` guard applies to a role CONNECTION with vending, because its storage resolves on the static path.
  - The wire variant is sealed for both `AssumedRole` and `Vended`. This applies ADR `seal-vended-storage-block-hkdf-aes-gcm-refuse-when-no-key-material` to a second credential the CONNECTION does not state. A role CONNECTION always has key material, because it requires `secret_key`.
  - A role CONNECTION reads S3 only. An `abfss://` table location fails as it fails for any static-S3 CONNECTION today.
- **Promotes to ADR:** yes

### [3] The STS client is a signed Query-API GET, parsed with the `quick-xml` already compiled into the workspace

- **Decision:** `sts.rs` sends `GET <endpoint>/?Action=AssumeRole&Version=2011-06-15&RoleArn=…&RoleSessionName=lakehouse-engine&ExternalId=…`, signed by the existing `sign_request` for the service `sts`. It parses `AssumeRoleResponse` and `ErrorResponse` with `quick-xml` 0.39 and serde.
- **Alternatives:** (a) `aws-sdk-sts` with `aws-config`. Rejected: the workspace deliberately carries no AWS SDK, and `aws-config` pulls a large dependency tree into the `.so`. (b) The `AssumeRole` provider of `reqsign-aws-v4`, already compiled through `opendal`. Rejected: a second signing stack with its own credential-provider abstraction. (c) A `POST` form body. Rejected: `sign_request` signs an empty body only. (d) `Accept: application/json`. Rejected: the AWS STS API reference documents XML only. (e) A hand-written substring parser. Rejected: it decodes no XML entities and breaks on namespace prefixes.
- **Rationale:** `Cargo.lock` already carries `quick-xml 0.39.4` with its `serde` dependency, through `object_store 0.13.2`, so the direct dependency adds no package. The AWS STS API reference's sample request is the URL (GET) form of the Query API.
- **Consequences:**
  - A later `object_store` bump can leave two `quick-xml` versions. `Cargo.lock` already tolerates three (0.39, 0.40, 0.41).
  - `sign_request` sends a signed `x-amz-content-sha256` header on every request. The STS API reference neither requires nor forbids it. The cloud scenario of `e2e-harness/cloud-e2e-harness` checks it against real STS.
- **Promotes to ADR:** no

### [4] The STS endpoint derives from the SigV4 signing region, with an `aws_sts_endpoint` override

- **Decision:** The endpoint is `aws_sts_endpoint` when stated, else `https://sts.<region>.amazonaws.com` for the region `ConnectionCreds::sigv4_signing_region` resolves, else the global `https://sts.amazonaws.com` signed for `us-east-1`. An `http://` override requires `ALLOW_HTTP`.
- **Alternatives:** (a) No override. Rejected: the local E2E stub, VPC interface endpoints, and the China partition (`.amazonaws.com.cn`) would be unreachable. (b) Reuse the CONNECTION's S3 `endpoint`. Rejected: on AWS an S3 endpoint is not an STS endpoint, and MinIO's own `AssumeRole` ignores `RoleArn`. (c) A virtual-schema property. Rejected: credential-path configuration belongs on the CONNECTION.
- **Rationale:** AWS recommends regional STS endpoints (IAM User Guide, "AWS STS Regions and endpoints"). Reusing `sigv4_signing_region` keeps one owner for the region a CONNECTION's AWS requests are signed for.
- **Consequences:**
  - The `connection-credentials-sigv4` Background gains the STS request as a third reader of the signing region.
  - `aws_sts_endpoint` is a third CONNECTION field beyond the ticket's two. It is optional and rejected without a role.
- **Promotes to ADR:** no

### [5] `aws_assume_role_arn` and `aws_external_id` are required together, as a product rule

- **Decision:** A CONNECTION supplying exactly one of the two fields is rejected, naming the absent one.
- **Alternatives:** Follow AWS and make `ExternalId` optional. Rejected by the ticket text.
- **Rationale:** The AWS STS API reference lists `ExternalId` as "Required: No". The stricter rule is defense in depth against the confused-deputy problem for cross-account roles. The spec states the rule as a product choice, not as an AWS requirement.
- **Promotes to ADR:** no

### [6] The base identity is the CONNECTION's own key pair, and no ambient credential chain is read

- **Decision:** A role CONNECTION requires `access_key` and `secret_key`, with an optional `session_token`. The error text states that no environment variable or instance profile is read.
- **Alternatives:** Resolve the base identity from the AWS default credential chain. Rejected by the interview (Q5).
- **Rationale:** The explicit-CONNECTION credential model keeps every credential under Exasol's access control. The ticket asks only for the explicit-ARN case, so no follow-up issue is required.
- **Promotes to ADR:** no

### [7] The AssumeRole request uses fixed parameters, a 30-second timeout, and no retry

- **Decision:** `RoleSessionName` is the constant `lakehouse-engine`. The request omits `DurationSeconds`, so the session lasts the STS default of 3600 seconds. The client times out after 30 seconds and does not retry.
- **Alternatives:** (a) Configurable session name and duration. Rejected: no stated need. (b) A duration above one hour. Rejected: a value above the role's maximum fails the call, and role chaining caps sessions at one hour. (c) Retries. Rejected: a failure is a configuration defect in the common case, and a fast named error serves the operator better.
- **Rationale:** `lakehouse-engine` matches the `RoleSessionName` pattern `[\w+=,.@-]*` and identifies the engine in CloudTrail. The existing catalog client sets no timeout. The new call gets one, so an unreachable STS endpoint fails in bounded time rather than stalling the adapter.
- **Consequences:** A query whose scan phase ends more than one hour after planning fails with the store's expired-token error, as a vended credential does. `docs/catalogs.md` states this limit. STS `Throttling` and 5xx responses are also not retried: a concurrent workload that drives one STS call per `createVirtualSchema`/`refresh`/`setProperties`/pushdown into throttling surfaces as failed user queries rather than a transient retry. This is an accepted limitation, not an oversight; task 5.1 states it in `docs/catalogs.md` so an operator sizing a role CONNECTION for concurrent load knows the failure mode. No follow-up issue: no reporter has hit it, and adding a bounded retry has no stated need yet.
- **Promotes to ADR:** no

### [8] `pushdown-planning-cloud-credentials` is scoped by its description, other specs by scenario edits

- **Decision:** The `pushdown-planning-cloud-credentials` description states that the feature covers CONNECTIONs naming no role. The one exception is § "SigV4/Glue derives the catalogs/{account-id} REST prefix on every catalog request", because the prefix rule does not depend on the credential source. `connection-credentials`, `connection-credentials-direct-storage`, `delta-table-planning`, `rest-catalog-oauth-auth`, `scan-spec-credential-reference`, and `storage-backend-enum` edit only the scenario clauses a role CONNECTION would contradict.
- **Alternatives:** (a) Narrow every conflicting scenario of `pushdown-planning-cloud-credentials`. Rejected: at least eight scenario copies for one scoping fact. (b) Add a scoping bullet to that feature's Background. Rejected: the delta must copy the whole Background of about 110 lines to add one bullet.
- **Rationale:** One scoping sentence in a short description removes eight conflicts without growing the permanent spec. Elsewhere only one or two clauses conflict, so a targeted edit is smaller.
- **Consequences:** Scenario-level retrieval (`speq feature get "<domain>/<feature>/<scenario>"`) prints a scenario without its feature description, so a reader of one `pushdown-planning-cloud-credentials` scenario does not see the no-role scope.
- **Promotes to ADR:** no

### [9] The Iceberg and Delta compliance gate does not apply to this plan

- **Decision:** No Iceberg table spec or Delta protocol section is cited. The one Iceberg-adjacent change, omitting `X-Iceberg-Access-Delegation` for a role CONNECTION, cites the Iceberg REST OpenAPI.
- **Alternatives:** none
- **Rationale:** The plan resolves credentials. It changes no scanning, pushdown, or schema or type handling. The Iceberg REST OpenAPI marks `X-Iceberg-Access-Delegation` `required: false`, an "Optional signal", so omitting it is compliant. The Delta protocol does not cover Unity Catalog's temporary-credentials API.
- **Promotes to ADR:** no

### [10] The local E2E models role assumption with an STS stub and two MinIO users, and real AWS runs in the opt-in cloud suite

- **Decision:** A stdlib-only Python stub verifies the SigV4 signature independently, checks `RoleArn` and `ExternalId`, and mints MinIO session credentials as a role user. A base MinIO user with no policy is denied. The opt-in `cloud-e2e` suite checks real STS and Glue against IAM resources in `deploy/data-stack`.
- **Alternatives:** (a) MinIO's own `AssumeRole`. Rejected: MinIO documents that `--role-arn` is "not meaningful", and the session inherits the caller's policy, so it cannot deny the base user and allow the role. (b) LocalStack. Rejected: IAM enforcement is not in its community edition. (c) moto server. Rejected: the seeded Iceberg tables live in MinIO, and moto does not enforce trust-policy conditions. (d) Defer the E2E to a manual check. Rejected: the stub makes both spike scenarios automatable against the Docker Exasol container.
- **Rationale:** The ticket note "Check this if it works with MinIO" reads as regression safety. MinIO cannot honour `aws_assume_role_arn` natively. A CONNECTION naming no role keeps the MinIO path unchanged, and the local E2E runs session credentials against MinIO. The stub's own SigV4 code is an independent check of the engine's canonicalization.
- **Consequences:**
  - The stub checks the engine against the AWS STS API as documented. Only the cloud suite checks real AWS behavior, and it skips when its environment variables are absent.
  - The stub mints sessions through the `pgsty/silo` MinIO fork's `AssumeRole`, which silo's own console login uses (`pgsty/silo` PR #146). Task 3.1 confirms it live before the stub is written.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] The recorded direct-storage scenario required a CONNECTION reference for a sealed role CONNECTION

- **Finding:** `connection-credentials-direct-storage` § "Credential vending is unreachable under the direct-storage kind" requires a CONNECTION reference for every direct-storage scan spec. This plan seals every role CONNECTION, and task 3.6 tests a direct-storage role CONNECTION. After merge the library would state both "reference" and "sealed" for one input.
- **Direction change:** A new `vs-adapter/connection-credentials-direct-storage` delta changes that scenario. The scan spec carries a CONNECTION reference when the storage credential source is the CONNECTION, and the sealed envelope when it is an assumed role. `plan.md` § Features, Group B's Knowledge, and § Scenario Coverage list the feature. Task 2.5 adds `a_direct_storage_role_connection_seals_its_storage_block` to `pushdown_tests.rs`. Decision [8] names the feature among the scenario-edited specs. The softer stale Background bullets stay unchanged, per decision [8].
- **Promotes to ADR:** no

### [plan-review] The no-role description scope also removed the SigV4/Glue prefix rule for a role CONNECTION

- **Finding:** The `pushdown-planning-cloud-credentials` description scoped every scenario to a CONNECTION that names no role. That scope also removed § "SigV4/Glue derives the catalogs/{account-id} REST prefix on every catalog request" for the ticket's own role CONNECTION shape. Scenario-level retrieval does not print the description, so the scope was also invisible to a reader of one scenario.
- **Direction change:** The description excepts the prefix scenario. `connection-credentials-assume-role` § "Session credentials sign every SigV4 catalog request" gains a step that requires the same prefix, unchanged by the role. Task 1.7's `assumed_session_signs_load_table_and_namespace_enumeration` asserts the `catalogs/<warehouse>` path segment, and § Scenario Coverage maps it to the prefix scenario. Decision [8] states the exception and gains a Consequences line on scenario-level retrieval.
- **Promotes to ADR:** no

### [plan-review] The connection-credentials-assume-role Background repeated the session-lifetime fact

- **Finding:** Background bullet 4 already states `DurationSeconds defaults to 3600`. Bullet 8 restated the same 3600-second lifetime and the THEN step's own "MUST NOT carry `DurationSeconds`" clause, adding no information the reader did not already have. Left unfixed since round 1, the same class of repetition PR #422 and PR #424 review flagged in shipped specs and ADRs ("restates the other spec, the scenarios, or the decision log").
- **Direction change:** Background bullet 8 is deleted from `vs-adapter/connection-credentials-assume-role/spec.md`. Bullet 4 and the request-shape scenario's THEN step remain the sole statements of the fact.
- **Promotes to ADR:** no

### [plan-review] STS throttling was rejected as a retry case with no accepted-limitation record

- **Finding:** Decision [7] rejects retries because "a failure is a configuration defect in the common case," but STS `Throttling` and 5xx responses are transient, not configuration defects, and every `createVirtualSchema`/`refresh`/`setProperties`/pushdown now makes one STS call. Nothing recorded this as an accepted limitation or told an operator about the failure mode, so the plan could ship without the gap being visible anywhere — the same scope-accuracy concern PR #424 review raised about stating a delivered guarantee precisely (there, the TopK/HashJoinExec placement scope).
- **Direction change:** Decision [7] Consequences now states the limitation explicitly and points to task 5.1's `docs/catalogs.md` documentation of it, rather than adding a retry (no reporter has hit this yet; retries stay a follow-up if one does).
- **Promotes to ADR:** no

### [plan-review] The MinIO spike answer was not stated where an operator or requester would read it

- **Finding:** The ticket's "Check this if it works with MinIO" note is read in decision [10] as regression safety, but the plan's own evidence answers it negatively — MinIO's `AssumeRole` ignores `RoleArn` and inherits the caller's policy. Neither plan.md § Summary nor task 5.1's documentation said so, so the requester could not confirm the spike closed and an operator pointing `aws_sts_endpoint` at MinIO would get silent base-identity access instead of the named role.
- **Direction change:** plan.md § Summary states the spike answer. Task 5.1 gains a `docs/catalogs.md` line stating MinIO's `AssumeRole` ignores `RoleArn`.
- **Promotes to ADR:** no

### [plan-review] The headline cloud-AWS path had no Checklist gate

- **Finding:** `cloud_assume_role_reaches_glue_and_s3_through_the_role` is the only check that real AWS STS and Glue accept the engine's signed requests, and it skips cleanly without four SSM variables and a manual `tofu apply`. § Checklist had no row for it, so the plan could reach PASS and ship without that path ever running against AWS — the same "does the delivered guarantee actually run" concern PR #424 review raised.
- **Direction change:** § Checklist gains a "Cloud assume-role" row requiring 2 passed, 0 skipped. Task 4.2 states the PR is not marked ready until that row has passed once.
- **Promotes to ADR:** no

### [plan-review] Three deltas narrated the change instead of stating the resulting state

- **Finding:** `connection-credentials`, `rest-catalog-oauth-auth`, and `storage-backend-enum` each carried a "SUPERSEDING/SUPERSEDES the recorded clause that …, because …" clause inside a scenario's THEN step — history the merged spec would carry forward forever, not the current rule. `pushdown-planning-cloud-credentials/spec.md` already accumulates nine such clauses from prior plans, and PR #422's own cleanup commit had to collapse a Background bloated the same way.
- **Direction change:** All three clauses are rewritten to state only the resulting behavior; the storage-backend-enum clause keeps its normative content (the join planner MUST NOT reject differing-backend sides) without narrating that a guard was deleted. plan.md gains a "Spec Delta Prose" principle governing this for the rest of the plan's authoring and for `/speq:record`.
- **Promotes to ADR:** no

### [plan-review] The assume-role E2E Background held design rationale and an untested stub signature check

- **Finding:** Background bullet 1 of `e2e-harness/assume-role-e2e` was design rationale that no scenario step depended on, and decision [10] already held it. Bullet 3 stated that the stub rejects a bad signature with `SignatureDoesNotMatch`, but no scenario sent a badly signed request. A stub that skips signature verification passed every scenario.
- **Direction change:** Bullet 1 is deleted. The new scenario "A wrong base secret fails CREATE VIRTUAL SCHEMA with the STS signature error" requires the `SignatureDoesNotMatch` code and no secret key in the error. Task 3.5 adds `a_wrong_base_secret_fails_create_with_signature_does_not_match`. § Scenario Coverage maps it to the new scenario and to the signed-request scenario of `connection-credentials-assume-role`.
- **Promotes to ADR:** no
