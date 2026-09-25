# Decision Log: add-aws-assume-role-credentials

## Interview

**Q:** Where should the STS-assumed-role credentials be used once obtained?
**A:** Storage + SigV4 catalog signing (Recommended). "Assumed-role temp credentials replace the static access_key/secret_key everywhere those would otherwise be used: signing SigV4 Glue catalog requests (when use_sigv4 is set) AND accessing S3 for Parquet files on the non-vended storage path."

**Q:** How often should the engine call STS AssumeRole, and how do the resulting short-lived credentials reach each parallel shard's UDF?
**A:** Once per query, sealed and distributed (Recommended). "The VS/plan layer calls STS AssumeRole once per query, then ships the resulting short-lived credentials to every shard through the same sealed-envelope mechanism already used for catalog-vended credentials. Avoids up to G (≤300) separate STS calls per query and reuses existing crypto machinery."

**Q:** A CONNECTION could set both use_vended_credentials=true (catalog vends short-lived S3 credentials per table) and aws_assume_role_arn (role assumption for storage). How should that combination be handled?
**A:** Allow both, assumed role wins for storage. "Assumed-role credentials override vended per-table credentials for storage access, on the theory that an explicit role assumption is a stronger operator intent than the catalog's default vending." The user declined the recommended option, which rejected the combination. Superseded by decision [2] after PR #441 review.

**Q:** Should role assumption be wired into the generic static-credential pipeline (so it also works for Delta/Unity Catalog static-storage CONNECTIONs), or scoped specifically to the Iceberg REST/Glue path from the ticket's example?
**A:** Generic (Recommended). "Build assume-role into the shared ConnectionCreds → StorageCreds resolution used by every catalog kind, per the project's format-neutral field rule. It works for Iceberg REST (incl. Glue) and Delta/Unity static-storage CONNECTIONs alike."

**Q:** Does aws_assume_role_arn require access_key/secret_key to also be supplied (the base IAM user identity that calls sts:AssumeRole), or should an ambient AWS credential chain (env vars, instance profile) be supported as the base identity too?
**A:** Static access_key/secret_key required (Recommended). "Matches the ticket's example exactly: aws_assume_role_arn requires access_key and secret_key on the same CONNECTION as the base identity for the AssumeRole call. No ambient/instance-profile credential resolution." Ambient credential-chain support is out of scope.

**Q:** (Settled by the ticket text, not re-asked.) How do `aws_assume_role_arn` and `aws_external_id` relate?
**A:** "aws_assume_role_arn and aws_external_id are optional but if either of them is present the other must be present." This is a project-chosen guard, stricter than AWS, which accepts AssumeRole without an ExternalId. Superseded by decision [5] after PR #441 review.

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

### [2] The assumed role replaces the CONNECTION's key pair only where that pair is read, and leaves credential vending unchanged

- **Decision:** The session of decision [1] replaces the key pair for its two reads: SigV4 catalog signing, and object storage when `use_vended_credentials` is false. Every site that chooses between vending and static storage keeps branching on `use_vended_credentials`: the Iceberg and Delta readers' split, the `X-Iceberg-Access-Delegation` header, the Unity Catalog temporary-credentials request, and the `path_style` guard. The one changed site is `scan_storage_for`. It returns the CONNECTION reference only when the CONNECTION neither vends nor names a role, and the sealed envelope otherwise.
- **Alternatives:** (a) An assumed role wins over vending for storage, through a `StorageCredentialSource` precedence enum matched at five sites. Rejected: it changes how vending works, it drops vended credentials the operator asked for, and it edits four sites that never read the key pair for storage. (b) Reject a CONNECTION that names a role and sets `use_vended_credentials`. Rejected: the combination is valid, because the session signs the catalog requests and the catalog vends storage. (c) A three-variant source enum consumed only by `scan_storage_for`. Rejected: that function already owns the wire variant (`vs-adapter/scan-spec-credential-reference`), so the enum adds a public type with one caller.
- **Rationale:** Interview Q1 scopes the session to SigV4 signing and the non-vended storage path. Vending never reads the key pair for storage, so a role has nothing to replace there. The substitution of decision [1] already reaches both reads, so no reader learns about roles. Whether AWS Glue vends credentials is unverified. If it does, vending supplies storage and the session signs the request that asks for it, with no special case.
- **Consequences:**
  - A role CONNECTION with `use_vended_credentials` sends the access-delegation header, requests Unity Catalog temporary credentials, and skips the `path_style` guard, exactly as without the role.
  - A role CONNECTION without vending carries session credentials that the CONNECTION does not state, so its storage block is sealed. This applies ADR `seal-vended-storage-block-hkdf-aes-gcm-refuse-when-no-key-material` to a second credential. A role CONNECTION always has key material, because it requires `secret_key`.
  - A role CONNECTION without vending reads S3 only. An `abfss://` table location fails as it fails for any static-S3 CONNECTION today.
  - **Known open gap, left unresolved per PR #441 review:** `pushdown-planning-cloud-credentials` §§ "Unsigned catalog path is unchanged when SigV4 and vending are both disabled" and "Static credentials are used for data files when vending is disabled", and `delta-table-planning` § "Delta planning resolves its storage credential through the table's own catalog", each still say, unconditionally and without a role carve-out, that a non-vended CONNECTION's scan spec carries a bare CONNECTION REFERENCE. That is now false for a non-vended role CONNECTION, which this decision requires to seal. This plan carries no delta for either spec, per the reviewer's explicit instruction to remove them (see the `[pr-review]` finding below). The actual code stays correct regardless — `scan_storage_for` is the ONE function every storage-block site calls (`vs-adapter/scan-spec-credential-reference`), and it already seals a non-vended role CONNECTION — but the recorded prose of these two specs will read as contradicting `connection-credentials-assume-role` for that one input until someone edits them. Flagged for the reviewer to confirm before implementation.
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

### [5] `aws_external_id` is optional and requires a role

- **Decision:** A CONNECTION states no role, a role alone, or a role with an external id. The adapter rejects `aws_external_id` without `aws_assume_role_arn`, naming the field. The `AssumeRole` request carries `ExternalId` only when the CONNECTION states one.
- **Alternatives:** (a) Require the role and the external id together. Rejected: it blocks a same-account role whose trust policy has no `sts:ExternalId` condition, which AWS accepts. (b) Accept an external id without a role and ignore it. Rejected: the value qualifies a role assumption only, so a lone external id is a configuration mistake that ignoring it would hide.
- **Rationale:** The AWS STS API reference lists `ExternalId` as "Required: No". The role's trust policy decides whether an external id is required, and STS denies a request that omits a required one. That denial surfaces as the credential-safe error of `connection-credentials-assume-role` § "A failed AssumeRole is a clear, credential-safe error". The rule matches `aws_sts_endpoint`, which also requires a role.
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

### [8] `connection-credentials-direct-storage`, `rest-catalog-oauth-auth`, and `scan-spec-credential-reference` edit only the scenario clauses a role CONNECTION without vending contradicts; `pushdown-planning-cloud-credentials` and `delta-table-planning` are not edited

- **Decision:** `connection-credentials-direct-storage`, `rest-catalog-oauth-auth`, and `scan-spec-credential-reference` edit only the scenario clauses that a role CONNECTION without vending contradicts. `connection-credentials` gains only a description pointer to the new sibling feature. `pushdown-planning-cloud-credentials` and `delta-table-planning` carry no delta at all, per PR #441 review (see the `[pr-review]` finding below) — their recorded scenarios stand unedited, and decision [2]'s Consequences record the resulting gap.
- **Alternatives:** (a) Narrow each conflicting scenario of `pushdown-planning-cloud-credentials`: § "Catalog REST requests to Glue are SigV4-signed when enabled", § "Unsigned catalog path is unchanged when SigV4 and vending are both disabled", and § "Static credentials are used for data files when vending is disabled". Rejected: three scenario copies, two of which carry recorded SUPERSEDING clauses that the copy would reproduce. (b) Add a bullet to that feature's Background. Rejected: the delta must copy the whole Background of about 110 lines to add one bullet. (c) Keep a narrowed delta-table-planning/pushdown-planning-cloud-credentials delta stating the sealed exception for a non-vended role CONNECTION. Rejected by PR #441 review, which asked for both deltas removed outright.
- **Rationale:** For the three specs that keep a delta, one targeted edit is smaller than a full scenario copy and avoids growing the permanent spec. `pushdown-planning-cloud-credentials` and `delta-table-planning` carry no delta because the reviewer asked for their removal; see decision [2] for what that leaves unresolved in the recorded text.
- **Consequences:** Scenario-level retrieval (`speq feature get "<domain>/<feature>/<scenario>"`) prints a scenario without its feature description, so a reader of one `connection-credentials-direct-storage`/`rest-catalog-oauth-auth`/`scan-spec-credential-reference` scenario relies on the scenario's own edited clause, not a description-level scope note.
- **Promotes to ADR:** no

### [9] The Iceberg and Delta compliance gate does not apply to this plan

- **Decision:** No Iceberg table spec or Delta protocol section is cited.
- **Alternatives:** none
- **Rationale:** The plan resolves credentials. It changes no scanning, pushdown, or schema or type handling. A role leaves the `X-Iceberg-Access-Delegation` header and the Unity Catalog temporary-credentials request unchanged (decision [2]), so no catalog-protocol behavior changes either.
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

### [pr-review] An assumed role overrode credential vending instead of leaving it unchanged

- **Finding:** Decision [2] made an assumed role win over `use_vended_credentials` for storage. A role CONNECTION with vending sent no access-delegation header, requested no Unity Catalog temporary credentials, and fell under the `path_style` guard. The role only needs to replace the key pair where the key pair is read: SigV4 catalog signing, and storage without vending.
- **Direction change:**
  - Decision [2] states the narrower rule. The `StorageCredentialSource` enum and its method are removed, because only `scan_storage_for` still distinguishes a role, and it already owns the wire variant.
  - Tasks 1.2 (enum), 1.3 (header), and 2.2 (`path_style` guard) are removed. Task 2.4 only extends a Delta-reader test with a role case. Old tasks 1.4-1.8 are now 1.2-1.6, and old tasks 2.3 and 2.5 are now 2.2 and 2.3. Task 2.3 seals when the CONNECTION vends or names a role.
  - The `storage-backend-enum` delta and both `connection-credentials` scenario edits are removed. `connection-credentials` keeps its description pointer, without the precedence phrase.
  - `connection-credentials-assume-role` drops the precedence and access-delegation Background bullets and both precedence scenarios. It gains § "A role leaves credential vending unchanged", and scopes its storage and sealing scenarios to a CONNECTION that does not vend.
  - `delta-table-planning` and `pushdown-planning-cloud-credentials` carry no delta, removed outright per the reviewer's literal instruction. Their recorded scenarios are left exactly as recorded, unedited. Decision [8] states the new scope. **This reintroduces the same class of conflict round-1 plan-review flagged as a BLOCKER for `connection-credentials-direct-storage`** (a recorded scenario asserting REFERENCE for an input `connection-credentials-assume-role` requires SEALED): decision [2]'s Consequences records it as a known open gap in the spec text, left unresolved because the reviewer asked for these two deltas removed rather than narrowed. The actual behavior stays correct because `scan_storage_for` is the single owner of the wire-variant decision (`vs-adapter/scan-spec-credential-reference`) and already seals this case; only the prose of these two other specs is stale.
  - `rest-catalog-oauth-auth`, `scan-spec-credential-reference`, and `connection-credentials-direct-storage` name `use_vended_credentials` and `aws_assume_role_arn` in place of the enum. `catalog-crate-public-surface-extensions` drops the enum and the method. Decision [9] drops the Iceberg REST OpenAPI citation, because the header is no longer omitted.
- **Promotes to ADR:** no

### [pr-review] `aws_external_id` was required beside every role

- **Finding:** Decision [5] required `aws_assume_role_arn` and `aws_external_id` together. AWS lists `ExternalId` as optional, and a same-account role often has no `sts:ExternalId` condition, so the rule rejected a valid CONNECTION. Only an external id without a role has no meaning.
- **Direction change:** Decision [5] makes `aws_external_id` optional and rejects it only without a role. `connection-credentials-assume-role` replaces the pairing scenario with § "An external id is accepted only beside a role", deletes the pairing Background bullet, folds the "Required: No" quote into the request-shape bullet, and sends `ExternalId` only when stated. Task 2.1 and § Scenario Coverage replace `assume_role_arn_and_external_id_are_required_together` with `external_id_without_a_role_is_rejected` and `a_role_without_an_external_id_is_accepted`. Task 1.3 adds `a_role_without_an_external_id_sends_no_external_id_parameter`. The Interview answer that quotes the pairing rule points to decision [5].
- **Promotes to ADR:** no

### [pr-review] A Unity Catalog role CONNECTION with static keys had no test reaching a Delta scan

- **Finding:** Removing the format-reader precedence task left no test in which a Unity Catalog CONNECTION with static S3 keys and a role reads a Delta table through the session.
- **Direction change:** `e2e-harness/assume-role-e2e` gains § "A Unity Catalog CONNECTION with static keys naming the role reads a Delta table through the session". Task 3.7 adds `unity_role_connection_reads_a_delta_table_through_the_session` to `e2e_unity_test.rs`. The Unity suite already runs a static-key Unity Catalog CONNECTION through real Delta queries against MinIO. MinIO denies the base user, so returned rows prove that the session reached the Delta log read and the scan. `adapter_tests.rs` has no live Unity Catalog: its one Unity case proves routing only by the load-table failure it surfaces. Task 3.7 also starts `sts-stub` in `unity-up`, in the `e2e-unity` CI job, and in the `docker-compose.unity.yml` Exasol hosts loop. The suite-gate scenario names both CI jobs.
- **Promotes to ADR:** no
