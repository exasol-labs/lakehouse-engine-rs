# Plan: add-aws-assume-role-credentials

## Summary

A CONNECTION can name an AWS IAM role that the adapter assumes with one signed STS `AssumeRole` call per request (issue #139). The session credentials replace the static key pair for SigV4 catalog signing and S3 storage, and reach the scan only inside the sealed envelope. MinIO cannot model role assumption natively — its `AssumeRole` ignores `RoleArn` and inherits the caller's policy — so the local suite proves both spike scenarios (base identity denied, role identity allowed) against an STS stub instead.

## Design

### Context

Issue #139 reports that enterprise AWS setups grant a base IAM user only `sts:AssumeRole`. Glue and S3 are reachable only as a role, which often requires an external id. Today the CONNECTION's static `access_key` and `secret_key` sign Glue requests directly (`iceberg_io::authed_get_json`, the signed namespace enumeration). The same pair reaches storage through `storage_block`. Under the reference wire variant, the scan UDF re-reads that pair from the CONNECTION (`scan_storage_for`).

Six forces shape the design:

1. Role assumption runs once per query, never once per shard (G ≤ 300), per interview Q2.
2. UDFs keep no state between invocations.
3. No credential appears in plaintext in the returned SQL (#135, #378).
4. The change is generic across catalog kinds (interview Q4) and keeps `ScanSpec` unchanged.
5. The workspace signs with `aws-sigv4` and carries no AWS SDK and no XML crate of its own.
6. The precedence over vending (interview Q3) has one home.

- **Goals**: role assumption for Iceberg REST (including Glue), Unity Catalog, and direct-storage CONNECTIONs. Session credentials for SigV4 signing and storage. One STS call per request. Sealed transport to the scan. Credential-safe errors. Both spike scenarios proven against the Docker Exasol container.
- **Non-Goals**: an ambient AWS credential chain (interview Q5). Session renewal for scans that outlive the 3600-second STS session. Configurable `DurationSeconds` or `RoleSessionName`. `AssumeRoleWithWebIdentity` or SAML. MinIO-native role semantics. Role assumption for Azure storage.

### Decision

#### Architecture

```
CONNECTION password JSON
  │ read_connection (sync, ctx.connection)  ── validate_creds (+ validate_assume_role_creds)
  │                                         ── sealing key = HKDF(password bytes)
  ▼
ResolvedConnectionConfig { creds: as stated, storage: storage_block(as stated) }
  │ once per request, inside the request's tokio runtime, before any catalog/storage call
  ▼
lakehouse_catalog::resolve_aws_identity(creds, catalog_uri, allow_http)
  │  no role ── returns creds unchanged, no network
  │  role ───── GET <sts endpoint>/?Action=AssumeRole&...  signed (service "sts") by the base key pair
  │             ◀── AssumeRoleResponse XML (quick-xml)
  │             returns creds with {access_key, secret_key, session_token} = session
  ▼
ResolvedConnectionConfig { creds: effective, storage: storage_block(effective) }
  ├── CatalogSession / namespace enumeration: SigV4 signs with creds.* (the session) ──▶ Glue
  ├── format readers: match creds.storage_credential_source()
  │      Connection | AssumedRole ──▶ static storage (the session for a role)
  │      Vended                   ──▶ resolve_vended_storage / Unity temporary credentials
  ├── load_table_any_auth: X-Iceberg-Access-Delegation only for Vended
  └── scan_storage_for: Connection ──▶ ScanStorage::Connection (reference)
                        AssumedRole | Vended ──▶ ScanStorage::Sealed ──▶ scan UDF unseals, never calls STS
```

`lakehouse-catalog` owns the AWS protocol: endpoint and region resolution, signing, XML parsing, and the precedence enum. `lakehouse-engine` owns the Exasol delivery: the CONNECTION read, validation, the two entry points, and the scan wire variant. The dependency stays one-way, engine to catalog.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Substitute at the boundary | `resolve_aws_identity`, called at both adapter entry points | One decision point. Every downstream reader already reads the key triple, so none learns about roles. |
| Single-owner precedence enum | `ConnectionCreds::storage_credential_source()` | The Q3 precedence lives once. Exhaustive matches turn a fourth source into a build error at every site. |
| Reuse the existing seams | `sign_request` (service `sts`), `seal_storage` | No second signer and no second credential transport. |

#### Quick Diagnostic (new module `sts.rs`, new enum `StorageCredentialSource`)

| Question | Answer |
|----------|--------|
| Does a one-sentence summary capture each responsibility? | Yes. `sts.rs` turns a CONNECTION credential set into the AWS identity a request acts as. The enum says where a table's storage credential comes from. |
| Is calling it easier than reimplementing it? | Yes. The caller passes the credential set, the catalog URI, and `ALLOW_HTTP`, and receives a credential set. The endpoint, region, query encoding, signing, timeout, XML, and redaction stay inside. |
| Would an internal change force an edit outside? | No. The XML crate, the endpoint rule, and the timeout are private to `sts.rs`. |
| Does the public doc comment explain the reasoning? | Planned. `resolve_aws_identity`'s doc comment states why the substitution is safe: no post-validation reader wants the base identity. |
| Does exactly one module own each decision? | Yes. The enum owns precedence, `sts.rs` owns the STS protocol, and `sealed.rs` owns transport. |
| Are the boundaries clear without reading internals? | Yes. The catalog crate names no Exasol mechanism, and the engine names no STS detail. |
| Does a tactical shortcut have a follow-up? | None taken. The one-hour session limit is a stated non-goal, documented in `docs/catalogs.md`. |
| Does business logic depend only inward? | Yes. `sts.rs` depends on `reqwest` and `aws-sigv4` as the catalog crate already does, and the engine depends on the catalog crate. |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Substitute the session into `ConnectionCreds` once per request (decision-log [1]) | A parallel identity type; per-shard STS in the scan UDF | No post-validation reader wants the base identity. One STS call per query. |
| One precedence enum: role, then vending, then the CONNECTION (decision-log [2]) | Reject role plus vending; vend then swap credentials | The user chose "assumed role wins" (Q3). One owner for five sites. |
| Signed Query-API GET plus `quick-xml` (decision-log [3]) | `aws-sdk-sts`, `reqsign`, POST, JSON, a substring parser | Zero new packages. It reuses `sign_request`. Entity-correct parsing. |
| `aws_sts_endpoint` override, regional default (decision-log [4]) | No override; reuse the S3 `endpoint` | Covers the E2E stub, VPC endpoints, and China. AWS recommends regional endpoints. |
| Local STS stub plus opt-in real-AWS suite (decision-log [10]) | MinIO's AssumeRole, LocalStack, moto, a manual check | MinIO ignores `RoleArn`. The stub automates both spike scenarios. |

### Spec Delta Prose

A spec Background or Scenario states the resulting behavior as if it had always been the rule, never as a transition. No bullet in a § Features delta may narrate "previously X, now Y" or "SUPERSEDING/SUPERSEDES the recorded clause that …, because …" — a reader of the merged spec should not see history, only current state. That narration belongs in this plan.md, and may repeat, more briefly, in decision-log.md — never in a spec.md. Where a delta must tell `/speq:record` which recorded clause it replaces, name the clause by its exact heading and let the DELTA marker do the rest; the replacement does not need explaining inline. Keep this in mind when finishing or reviewing this plan's own deltas and at record time — general bloat aside, this specific narrative habit is how the permanent library accumulates SUPERSEDES chains across plans (already present in `pushdown-planning-cloud-credentials/spec.md`).

## Features

| Feature | Status | Spec |
|---------|--------|------|
| connection-credentials-assume-role | NEW | `specs/_plans/add-aws-assume-role-credentials/vs-adapter/connection-credentials-assume-role/spec.md` |
| connection-credentials | CHANGED | `specs/_plans/add-aws-assume-role-credentials/vs-adapter/connection-credentials/spec.md` |
| connection-credentials-sigv4 | CHANGED | `specs/_plans/add-aws-assume-role-credentials/vs-adapter/connection-credentials-sigv4/spec.md` |
| connection-credentials-direct-storage | CHANGED | `specs/_plans/add-aws-assume-role-credentials/vs-adapter/connection-credentials-direct-storage/spec.md` |
| pushdown-planning-cloud-credentials | CHANGED | `specs/_plans/add-aws-assume-role-credentials/vs-adapter/pushdown-planning-cloud-credentials/spec.md` |
| scan-spec-credential-reference | CHANGED | `specs/_plans/add-aws-assume-role-credentials/vs-adapter/scan-spec-credential-reference/spec.md` |
| delta-table-planning | CHANGED | `specs/_plans/add-aws-assume-role-credentials/vs-adapter/delta-table-planning/spec.md` |
| rest-catalog-oauth-auth | CHANGED | `specs/_plans/add-aws-assume-role-credentials/vs-adapter/rest-catalog-oauth-auth/spec.md` |
| storage-backend-enum | CHANGED | `specs/_plans/add-aws-assume-role-credentials/vs-adapter/storage-backend-enum/spec.md` |
| catalog-crate-public-surface-extensions | CHANGED | `specs/_plans/add-aws-assume-role-credentials/vs-adapter/catalog-crate-public-surface-extensions/spec.md` |
| assume-role-e2e | NEW | `specs/_plans/add-aws-assume-role-credentials/e2e-harness/assume-role-e2e/spec.md` |
| cloud-e2e-harness | CHANGED | `specs/_plans/add-aws-assume-role-credentials/e2e-harness/cloud-e2e-harness/spec.md` |

## Impact

- **No breaking change.** A CONNECTION without the new fields sends no STS request, keeps the CONNECTION-reference wire variant, and resolves as before.
- **New optional CONNECTION fields**: `aws_assume_role_arn`, `aws_external_id` (required together), and `aws_sts_endpoint`. A role CONNECTION requires `access_key` and `secret_key`.
- **`use_vended_credentials` has no effect on a role CONNECTION.** The adapter requests no vended credential for it.
- **`EXPLAIN VIRTUAL` of a role CONNECTION shows the sealed envelope** instead of the CONNECTION reference. The scan script still needs the script-scoped CONNECTION grant, because the sealing key derives from the CONNECTION password.
- **New network dependency.** The adapter node needs outbound HTTPS to the STS endpoint: `sts.<region>.amazonaws.com`, or the global `sts.amazonaws.com`. A China-region or VPC-endpoint deployment sets `aws_sts_endpoint`.
- **CREATE, REFRESH, and SET PROPERTIES call STS** for a role CONNECTION, so an STS misconfiguration fails at CREATE.
- **Session lifetime.** A query whose scan phase ends more than 3600 seconds after planning fails with the store's expired-token error.
- **Local stack.** `docker-compose.yml` gains the `sts-stub` service and two MinIO users. `make test-e2e` gains one binary.
- **AWS deployment.** `deploy/data-stack` gains an IAM user, its access key, a role, and SSM parameters. An operator applies them manually.

## Dependencies

- `quick-xml` 0.39 with the `serialize` feature, as a direct dependency of `lakehouse-catalog`. `Cargo.lock` already resolves `quick-xml 0.39.4` with `serde` through `object_store 0.13.2`, so no package is added.
- A pinned `python:3.12-slim` image for the STS stub.
- Issue #139 exists. The implementing commit carries `Closes #139`.

## Implementation Tasks

### A. Catalog-crate AWS identity (`lakehouse-catalog`)

- [ ] 1.1 Add `aws_assume_role_arn`, `aws_external_id`, and `aws_sts_endpoint` (`Option<String>`) to `ConnectionCreds`. Render `aws_external_id` as `[redacted]` in its `Debug`. Test in `creds_tests.rs`.
- [ ] 1.2 Add `pub enum StorageCredentialSource { Connection, AssumedRole, Vended }` and `ConnectionCreds::storage_credential_source()`. Apply the precedence role, then vending, then CONNECTION. Test every flag combination in `creds_tests.rs`.
- [ ] 1.3 Send the `X-Iceberg-Access-Delegation` header in `load_table_any_auth` only for `StorageCredentialSource::Vended`. Test in `session_tests.rs` with a recording catalog:
  - role plus vending sends no header
  - vending alone sends the header
- [ ] 1.4 Add `quick-xml = { version = "0.39", features = ["serialize"] }` to `[workspace.dependencies]` and to `crates/lakehouse-catalog/Cargo.toml`. Confirm that `Cargo.lock` gains no `[[package]]` entry. Run `cargo deny check`.
- [ ] 1.5 Implement the request side of `crates/lakehouse-catalog/src/sts.rs`. [expert]
  - Signature: `pub async fn resolve_aws_identity(creds: ConnectionCreds, catalog_uri: &str, allow_http: bool) -> Result<ConnectionCreds, UdfError>`.
  - Return the set unchanged, with no request, when it names no role.
  - Resolve the endpoint and region through `sigv4_signing_region`, and apply the `http` and scheme gates.
  - Build the percent-encoded `GET` query with the fixed `RoleSessionName`, and sign it with `sign_request(…, "sts")`.
  - Set a 30-second client timeout, injectable through a crate-private parameter for tests.
  - Add a recording STS helper to `test_support_tests.rs`.
  - Test in `sts_tests.rs`: the request shape, endpoint precedence, the plaintext gate, the timeout, and the no-role passthrough.
  - Recompute the signature from the received raw query for an external id holding `+=,.@:/-`, and assert that it matches.
- [ ] 1.6 Implement the response side of `sts.rs`.
  - Parse `AssumeRoleResponse` and `ErrorResponse` with `quick-xml`, trimming whitespace and decoding entities.
  - Return a named error for each missing element and for a malformed body.
  - Replace only `access_key`, `secret_key`, and `session_token` in the returned set.
  - Build transport errors from `reqwest::Error::without_url()`.
  - Redact every error against the base `secret_key`, the base `session_token`, and the external id.
  - Test in `sts_tests.rs`: decoded credentials, each missing element, a malformed body, a 403 `ErrorResponse`, a refused connection, and the untouched fields.
- [ ] 1.7 Add a `session_tests.rs` integration test. Assert that the session from `resolve_aws_identity` signs the `loadTable` GET and the signed namespace enumeration:
  - `Credential=<session key id>/…/glue/aws4_request`
  - `x-amz-security-token` equal to the session token
  - the `catalogs/<warehouse>` path segment on both requests
- [ ] 1.8 Re-export `resolve_aws_identity` and `StorageCredentialSource` from `lib.rs`. Edit `tests/catalog_public_surface.rs` to name both items, the new method, and the new fields. Keep its demotion assertions.

### B. Adapter identity wiring and storage precedence (`lakehouse-engine` adapter)

- [ ] 2.1 Parse the three fields in `parse_creds`. Add `validate_assume_role_creds` to `validate_creds`, and test it in `connection_tests.rs`:
  - the pairing rule
  - the base key pair, with the no-ambient-credential message
  - `aws_sts_endpoint` requiring a role
- [ ] 2.2 Make `validate_path_style_with_endpoint` skip only for `StorageCredentialSource::Vended`. Test that the guard fires for role plus vending and stays silent for vending alone.
- [ ] 2.3 Resolve the AWS identity once per request at both entry points. [expert]
  - Call `resolve_aws_identity` inside the request runtime in `dispatch`'s pushdown arm and in `handle_create_virtual_schema`.
  - Call it before `TableScanResolver::for_request` and before `construct_catalog_client`.
  - Replace `ResolvedConnectionConfig.creds` with the result, and rebuild `storage` with `storage_block`.
  - Test in `adapter_tests.rs` with a recording STS server and a `TestContext` CONNECTION.
  - Assert one STS request for a create, and one for a two-table join pushdown whose catalog is unreachable.
  - Assert zero STS requests for a CONNECTION without a role.
  - Assert that an STS 403 fails the request with no credential value and no catalog request.
- [ ] 2.4 Replace the `use_vended_credentials` branches in `IcebergFormatReader::resolve_scan` and `DeltaFormatReader::effective_storage` with an exhaustive match on `storage_credential_source()`. [expert]
  - `Connection` and `AssumedRole` take the connection's static backend.
  - `Vended` takes the vended resolution.
  - Test in `iceberg_tests.rs` and `delta_format_reader_tests.rs`: role plus vending reads the session backend and requests no temporary credentials.
  - Test that a vended CONNECTION without a role is unchanged.
- [ ] 2.5 Make `scan_storage_for` match `storage_credential_source()`. [expert]
  - `Connection` returns `ScanStorage::Connection`.
  - `AssumedRole` and `Vended` return the sealed variant, or the refusal without key material.
  - Test in `support_tests.rs`: a role seals, the payload unseals field-for-field to the session backend, and a join seals each side.
  - Extend `no_connection_credential_reaches_the_generated_sql` and `catalog_auth_secrets_never_in_scan_spec_with_vending` with a role case.
  - Add `a_direct_storage_role_connection_seals_its_storage_block` to `pushdown_tests.rs`. Assert that a direct-storage role pushdown carries the sealed variant and no CONNECTION reference.

### C. Local assume-role E2E

- [ ] 3.1 Write the STS stub.
  - Confirm live that the stack's `pgsty/silo` MinIO answers `AssumeRole` for a MinIO user. If it does not, stop and escalate rather than weaken the stub.
  - Write `scripts/sts-stub/sts_stub.py` with the Python standard library only, configured through environment variables.
  - Serve `GET /health`, `GET /__requests`, and `AssumeRole` over a GET query or a POST form.
  - Verify the SigV4 signature independently: service `sts`, the base identity, and the signed headers `Authorization` names.
  - Check `RoleArn` and `ExternalId`, then mint a session through MinIO's `AssumeRole` as the role user.
  - Answer in the AWS `AssumeRoleResponse` shape, or in the `ErrorResponse` shape with 403 `AccessDenied` or `SignatureDoesNotMatch`.
- [ ] 3.2 Extend `docker-compose.yml`.
  - `minio-init` creates the base user with no policy.
  - `minio-init` creates the role user with a `warehouse` read policy: `s3:GetObject`, `s3:ListBucket`, `s3:GetBucketLocation`.
  - Add the `sts-stub` service: pinned `python:3.12-slim`, static IP `10.84.0.12`, a `/health` healthcheck, host port `${LH_STS_STUB_PORT:-19090}`.
  - Add `sts-stub` to Exasol's `extra_hosts` and to its entrypoint hosts loop.
- [ ] 3.3 Register the suite.
  - Add `--test e2e_assume_role_test` to `make test-e2e`.
  - Add `sts-stub` to the `e2e` job's `docker compose up -d --wait` set in `.github/workflows/ci.yml`.
  - Add `build_convention.rs` tests for both registrations.
- [ ] 3.4 Extend `tests/common/stack.rs`. Add the three optional fields to `CatalogConnectionPassword`, serialized only when set. Add `sts_stub_url()`, `sts_stub_url_internal()`, and `sts_stub_request_count()`. Test the serialization.
- [ ] 3.5 Write the Iceberg REST scenarios in `crates/lakehouse-engine/tests/e2e_assume_role_test.rs` under `exasol-e2e`.
  - The setup panics when the stub is down.
  - The base-only CONNECTION is denied, and the stub count is unchanged.
  - The role CONNECTION returns the unfiltered and filtered seeded counts, and the stub count grows.
  - `EXPLAIN VIRTUAL` carries the sealed envelope and neither the base secret nor the external id.
  - A wrong external id fails `CREATE VIRTUAL SCHEMA`.
  - A wrong base secret fails `CREATE VIRTUAL SCHEMA` with `SignatureDoesNotMatch`: `a_wrong_base_secret_fails_create_with_signature_does_not_match`.
- [ ] 3.6 Add the direct-storage scenario to `e2e_assume_role_test.rs`. Write a Parquet fixture with `tests/common/raw_parquet.rs` under `s3://warehouse/assume_role_direct/`. Assert the role CONNECTION's CREATE listing and its read.

### D. Cloud E2E and AWS provisioning

- [ ] 4.1 Extend `deploy/data-stack`.
  - Add an assume-role base IAM user with an access key and an inline policy that allows only `sts:AssumeRole` on the new role.
  - Add a role whose trust policy names that user and requires `sts:ExternalId` from a sensitive `assume_role_external_id` variable.
  - Attach `aws_iam_policy.engine_reader` to the role.
  - Add SSM parameters, outputs, and a `terraform.tfvars.example` placeholder.
  - Run `tofu validate` and `tofu fmt -check`.
- [ ] 4.2 Add two assume-role tests to `crates/lakehouse-engine/tests/cloud_e2e_test.rs`, with their own environment struct. Skip with a message naming the absent variable. Document the four variables in the module doc and in `deploy/README.md`, read from SSM. This plan's headline path is real AWS STS and Glue accepting the engine's signed requests; the PR is not marked ready until the § Checklist "Cloud assume-role" row has run with the variables exported and reported 2 passed, 0 skipped.

### E. Documentation

- [ ] 5.1 Update `docs/catalogs.md`.
  - In § "Connection fields", document the three fields, the pairing rule, and the base-identity requirement.
  - Document the endpoint and region rule, `ALLOW_HTTP` for an `http` STS endpoint, and the China override.
  - Document the precedence over vending and the 3600-second session.
  - Add an AWS Glue assume-role example CONNECTION: the ticket's example plus `region`.
  - State that MinIO's `AssumeRole` ignores `RoleArn` and inherits the caller's policy, so role assumption needs an AWS-compatible STS endpoint; a MinIO-backed `aws_sts_endpoint` does not apply the named role.
  - State that a failed STS request is not retried, so `Throttling` and 5xx responses under concurrent load surface as failed queries rather than a transient retry.
- [ ] 5.2 Update `docs/security.md`. State that the sealed envelope also carries assumed-role session credentials. State that the base identity needs only `sts:AssumeRole`.
- [ ] 5.3 Update `specs/mission.md`. Add an AWS STS row to § External Dependencies. Name STS role assumption in the `lakehouse-catalog` project-structure line.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Catalog-crate AWS identity | 1.1-1.8 | — | spec deltas `vs-adapter/connection-credentials-assume-role` (request, endpoint, response, failure, identity, SigV4, precedence scenarios) and `vs-adapter/catalog-crate-public-surface-extensions`; `crates/lakehouse-catalog/src/{creds,sts,session,lib}.rs`, `creds_tests.rs`, `sts_tests.rs`, `session_tests.rs`, `test_support_tests.rs`, `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, `Cargo.toml`, `crates/lakehouse-catalog/Cargo.toml` |
| B: Adapter identity wiring and storage precedence | 2.1-2.5 | A (consumes `resolve_aws_identity` and `StorageCredentialSource`; shares the `connection-credentials-assume-role` delta, so it runs after A) | spec deltas `vs-adapter/connection-credentials-assume-role` (validation, once-per-request, storage, vending, sealing scenarios), `vs-adapter/connection-credentials`, `vs-adapter/connection-credentials-sigv4`, `vs-adapter/connection-credentials-direct-storage`, `vs-adapter/pushdown-planning-cloud-credentials`, `vs-adapter/scan-spec-credential-reference`, `vs-adapter/delta-table-planning`, `vs-adapter/rest-catalog-oauth-auth`, `vs-adapter/storage-backend-enum`; `crates/lakehouse-engine/src/adapter/{connection,mod}.rs`, `connection_tests.rs`, `adapter_tests.rs`, `crates/lakehouse-engine/src/adapter/pushdown/{support.rs,support_tests.rs,pushdown_tests.rs}`, `crates/lakehouse-engine/src/adapter/pushdown/format/{iceberg.rs,iceberg_tests.rs,delta_format_reader.rs,delta_format_reader_tests.rs}` |
| C: Local assume-role E2E | 3.1-3.6 | B (the suite passes only once the engine assumes roles) | spec delta `e2e-harness/assume-role-e2e`; `docker-compose.yml`, `scripts/sts-stub/`, `Makefile`, `.github/workflows/ci.yml`, `crates/lakehouse-engine/tests/{e2e_assume_role_test.rs,build_convention.rs,common/stack.rs,common/raw_parquet.rs}` |
| D: Cloud E2E and AWS provisioning | 4.1-4.2 | C (reuses the `CatalogConnectionPassword` fields C adds in `tests/common/stack.rs`) | spec delta `e2e-harness/cloud-e2e-harness`; `deploy/data-stack/`, `deploy/README.md`, `crates/lakehouse-engine/tests/cloud_e2e_test.rs` |
| E: Documentation | 5.1-5.3 | B (documents the settled behavior) | `docs/catalogs.md`, `docs/security.md`, `specs/mission.md`; reads the `vs-adapter/connection-credentials-assume-role` delta |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | The plan adds `sts.rs` and replaces five `use_vended_credentials` checks with matches on one enum. No function, test, or module becomes unused. `storage_block` keeps both of its callers. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A role and its external id are supplied together or not at all | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs`; `crates/lakehouse-catalog/src/creds_tests.rs` | `assume_role_arn_and_external_id_are_required_together`; `connection_creds_debug_redacts_the_external_id` |
| A CONNECTION naming a role carries the base identity's key pair | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `assume_role_requires_the_base_key_pair_and_reads_no_ambient_credential` |
| An STS endpoint override is accepted only beside a role | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `sts_endpoint_without_a_role_is_rejected` |
| The adapter assumes the role exactly once per request, before any catalog or storage access | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `assume_role_sends_one_sts_request_per_create_and_per_join_pushdown`; `a_connection_without_a_role_sends_no_sts_request` |
| The AssumeRole request carries the role, the external id, and the base identity's signature | Integration, E2E | `crates/lakehouse-catalog/src/sts_tests.rs`; `crates/lakehouse-engine/tests/e2e_assume_role_test.rs` | `assume_role_request_is_a_signed_get_carrying_role_and_external_id`; `external_id_characters_survive_signing`; `role_connection_reads_the_rows_the_base_identity_was_denied`; `a_wrong_base_secret_fails_create_with_signature_does_not_match` (the stub verifies the signature independently) |
| The STS endpoint is resolved from the CONNECTION and gated on plaintext consent | Integration | `crates/lakehouse-catalog/src/sts_tests.rs` | `sts_endpoint_prefers_override_then_regional_then_global`; `plaintext_sts_endpoint_requires_allow_http`; `sts_request_times_out_naming_the_endpoint_host` |
| The AssumeRole response yields the session credentials | Integration | `crates/lakehouse-catalog/src/sts_tests.rs` | `assume_role_response_yields_trimmed_decoded_session_credentials`; `assume_role_response_missing_an_element_is_an_error_naming_it`; `a_malformed_assume_role_body_is_an_error_without_the_body` |
| A failed AssumeRole is a clear, credential-safe error | Integration | `crates/lakehouse-catalog/src/sts_tests.rs`; `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `assume_role_failure_is_credential_safe_for_denial_refusal_and_timeout`; `an_sts_denial_fails_the_request_before_any_catalog_request` |
| One call resolves the AWS identity a request acts as | Integration | `crates/lakehouse-catalog/src/sts_tests.rs`; `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `resolved_identity_replaces_only_the_key_triple`; `no_role_identity_is_returned_unchanged_without_a_request`; `validation_and_sealing_key_read_the_stated_credentials` |
| Session credentials sign every SigV4 catalog request | Integration | `crates/lakehouse-catalog/src/session_tests.rs` | `assumed_session_signs_load_table_and_namespace_enumeration` |
| Session credentials are the storage credential under every catalog kind | E2E, Integration | `crates/lakehouse-engine/tests/e2e_assume_role_test.rs`; `crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader_tests.rs` | `role_connection_reads_the_rows_the_base_identity_was_denied`; `direct_storage_role_connection_lists_and_reads_through_the_session`; `an_assumed_role_reads_through_the_session_backend` |
| An assumed role wins over vending for storage | Integration | `crates/lakehouse-catalog/src/session_tests.rs`; `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs`; `crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader_tests.rs`; `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `a_role_with_vending_sends_no_access_delegation_header`; `a_role_with_vending_resolves_the_session_backend`; `a_role_with_vending_requests_no_temporary_credentials`; `the_path_style_guard_applies_to_a_role_with_vending` |
| One function owns the storage credential source precedence | Unit | `crates/lakehouse-catalog/src/creds_tests.rs` | `storage_credential_source_prefers_role_then_vending_then_connection` |
| The scan receives the session credentials only inside the sealed envelope | Unit, E2E | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs`; `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`; `crates/lakehouse-engine/tests/e2e_assume_role_test.rs` | `a_role_storage_block_is_sealed_and_unseals_to_the_session_backend`; `no_connection_credential_reaches_the_generated_sql` (extended); `role_connection_reads_the_rows_the_base_identity_was_denied` (`EXPLAIN VIRTUAL` assertion) |
| A non-vended CONNECTION configuring a store endpoint without stating path_style is rejected | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `endpoint_without_a_stated_path_style_is_rejected_naming_the_field`; `the_path_style_guard_does_not_fire_without_an_endpoint_or_under_vending`; `the_path_style_guard_applies_to_a_role_with_vending` |
| Static storage credentials are ignored, not rejected, when vending is requested | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `static_storage_fields_with_vending_are_accepted_and_unused` |
| When SigV4 is enabled, access_key, secret_key, and a signing region are required | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `sigv4_requires_access_secret_region` |
| SigV4/Glue derives the catalogs/{account-id} REST prefix on every catalog request | Integration | `crates/lakehouse-catalog/src/session_tests.rs`; `crates/lakehouse-catalog/src/namespace_tests.rs` | `sigv4_resolve_prefix_derives_catalogs_segment`; `list_tables_signed_url_carries_catalogs_prefix`; `assumed_session_signs_load_table_and_namespace_enumeration` (role case) |
| The scan spec references the CONNECTION by name | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `no_connection_credential_reaches_the_generated_sql` |
| One pure function selects the wire variant and gates the sealed envelope | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `scan_storage_variant_follows_the_storage_credential_source` |
| Credential vending is unreachable under the direct-storage kind | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `a_direct_storage_role_connection_seals_its_storage_block` |
| Delta planning resolves its storage credential through the table's own catalog | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader_tests.rs` | `vending_without_a_vending_key_errors_and_never_falls_back_to_static`; `an_assumed_role_reads_through_the_session_backend` |
| Catalog auth props are never placed in any scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `catalog_auth_secrets_never_in_scan_spec_with_vending` (extended with a role case) |
| Every consumer holds a backend and no consumer names one | Integration, Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs`; `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `a_role_with_vending_resolves_the_session_backend`; `each_vended_selector_dispatches_every_vended_backend_kind` |
| The AWS identity resolver and the storage credential source extend the crate's public surface through an explicit reviewed edit | Unit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `aws_identity_resolver_and_storage_credential_source_are_public` |
| The assume-role binary and its STS stub are wired into the suite gate | Unit, E2E | `crates/lakehouse-engine/tests/build_convention.rs`; `crates/lakehouse-engine/tests/e2e_assume_role_test.rs` | `make_test_e2e_runs_the_assume_role_binary`; `ci_e2e_job_waits_for_the_sts_stub`; `the_sts_stub_is_reachable_or_the_suite_fails` |
| The base identity alone is denied the warehouse bucket | E2E | `crates/lakehouse-engine/tests/e2e_assume_role_test.rs` | `the_base_identity_alone_is_denied_the_warehouse_bucket` |
| Naming the role reads the rows the base identity was denied | E2E | `crates/lakehouse-engine/tests/e2e_assume_role_test.rs` | `role_connection_reads_the_rows_the_base_identity_was_denied` |
| A wrong external id fails CREATE VIRTUAL SCHEMA with the STS error | E2E | `crates/lakehouse-engine/tests/e2e_assume_role_test.rs` | `a_wrong_external_id_fails_create_virtual_schema` |
| A wrong base secret fails CREATE VIRTUAL SCHEMA with the STS signature error | E2E | `crates/lakehouse-engine/tests/e2e_assume_role_test.rs` | `a_wrong_base_secret_fails_create_with_signature_does_not_match` |
| A direct-storage CONNECTION naming the role lists and reads through the session | E2E | `crates/lakehouse-engine/tests/e2e_assume_role_test.rs` | `direct_storage_role_connection_lists_and_reads_through_the_session` |
| An assume-role CONNECTION reaches Glue and S3 through the assumed role | E2E (opt-in cloud) | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_assume_role_reaches_glue_and_s3_through_the_role` |
| The assume-role base identity alone is denied by Glue | E2E (opt-in cloud) | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_assume_role_base_identity_alone_is_denied` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| connection-credentials-assume-role | After `make test-e2e` brings up the stack, run in the Docker Exasol: `CREATE OR REPLACE CONNECTION ASSUME_DEMO TO 'http://iceberg-rest:8181' USER '' IDENTIFIED BY '{"warehouse":"s3://warehouse/","endpoint":"http://minio:9000","region":"us-east-1","path_style":true,"access_key":"<base user>","secret_key":"<base secret>","aws_assume_role_arn":"<stub role ARN>","aws_external_id":"<stub external id>","aws_sts_endpoint":"http://sts-stub:8080"}'`, then `CREATE VIRTUAL SCHEMA` over it with `ALLOW_HTTP = 'true'`, then `SELECT COUNT(*)` on the seeded events table | The seeded row count. `curl -s localhost:19090/__requests` reports a higher count than before the query. |
| connection-credentials-assume-role (base identity alone) | The same CONNECTION without the three `aws_*` fields, then the same `SELECT COUNT(*)` | The query fails with an access-denied error that contains no secret key. The `/__requests` count is unchanged. |
| connection-credentials | `CREATE OR REPLACE CONNECTION` with `aws_assume_role_arn` and no `aws_external_id`, then `CREATE VIRTUAL SCHEMA` | Fails with an error naming `aws_external_id` and stating that the two fields MUST be supplied together |
| scan-spec-credential-reference | `EXPLAIN VIRTUAL SELECT * FROM <vs>.<events table>` through `ASSUME_DEMO` | The storage block is the `sealed` variant. The output contains neither the base secret nor the external id. |
| assume-role-e2e | `make test-e2e` | `e2e_assume_role_test` reports 0 failures |
| cloud-e2e-harness | With the Glue variables and the four assume-role variables exported from SSM: `cargo test -p lakehouse-engine --features cloud-e2e --test cloud_e2e_test cloud_assume_role -- --test-threads=1` | 2 passed. Without the assume-role variables: 2 passed as skips, each naming the absent variable. |
| AWS provisioning | `cd deploy/data-stack && tofu plan` (with `enable_emr_serverless = true` in `terraform.tfvars`) | The plan adds the IAM user, access key, role, policy attachment, and SSM parameters, and destroys nothing |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors, 0 warnings |
| Format | `cargo fmt --check` | No changes |
| Dependencies | `cargo deny check` | Exit 0 |
| Lockfile | `git diff Cargo.lock` | No new `[[package]]` entry |
| Cloud assume-role | `cargo test -p lakehouse-engine --features cloud-e2e --test cloud_e2e_test cloud_assume_role -- --test-threads=1` with the four assume-role SSM variables exported | 2 passed, 0 skipped — the PR is not marked ready until this row has passed once |
