# Plan Review Findings: add-export-adls-storage

## Summary
- Axes checked: 4/4
- Total findings: 11 (Blockers: 1, Findings: 10)
- Intent Fidelity blockers: 1

## Intent Fidelity

Every acceptance criterion of #569 maps to a scenario: a new ADLS table with CONNECTION credentials, a vended SAS, read-back through a virtual schema, and append ("exports it again into that table"), all in the Azure E2E suite. No scope creep: the static `sas_token` E2E arm and the renewal design are interview answers.

#### [INTENT_DRIFT] BLOCKER
- Location: intent.md § Directions [D1], against intent.md § Interview (third question)
- Issue: The interview answer reads "An own Iceberg storage for ADLS, over object_store's Azure client, which accepts a credential provider. The scan already builds that Azure client for its data file reads, so the writer shares its construction." The user also asked the plan to "reproduce and reuse what the virtual schema's read path already does rather than reinventing it". D1 reverses that answer: "The writer's ADLS storage ... does not share the scan's Azure store builder." The rationale ("sharing would add a parameter for three lines of builder code") is the plan's own judgment, with no user agreement recorded.
- Fix: Do one of two things. (a) Conform to the answer: extract the scan's Azure client construction (`MicrosoftAzureBuilder::with_url` over the `abfss://` location plus client options) into one function both sides call, taking the credential as an object_store `CredentialProvider` so the scan passes its fixed credential through a static provider. It has to live in `crates/lakehouse-catalog-write` or the engine, because `specs/architecture.md` § Constraints forbids an `object_store` dependency in `lakehouse-catalog`. Name the extraction in plan.md § Implementation Notes and rewrite D1. (b) Or move D1 into plan.md's open questions for the user, and record the user's answer in intent.md § Interview before implementation starts. With (b), the storage choice (own object_store storage, opendal hook rejected) also meets ADR rule 2 (a rejected alternative whose reason a later reader needs), so consider marking that direction `ADR: yes`.

## Feasibility

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: intent.md § Interview (fourth question) and § Directions [D4]; plan.md § Verification
- Issue: SAS renewal only runs when the catalog sends `adls.sas-token-expires-at-ms.<host>`. When the key is absent, the SAS is "never renewed ahead of time". The plan never states whether the Lakekeeper version on the Azure stack sends this key for ADLS. If it does not, a long ADLS export through Lakekeeper never renews and fails when the SAS expires, which is the outcome the first interview answer rejected. The recorded `testdata/*.json` answers carry only the S3 expiry key. No E2E test exercises ADLS renewal, so nothing would catch this.
- Fix: Inspect a vended `loadTable` answer from the Azure stack's Lakekeeper warehouse for the expiry key and record the result as a direction. If the key is absent, add the consequence to docs/export.md and to plan.md § Out of scope.

#### [HIDDEN_DEPENDENCY] FINDINGS
- Location: plan.md § Dependencies; plan.md § Implementation Notes (ADLS write storage, Azure E2E SAS)
- Issue: `iceberg::io::Storage` and `StorageFactory` are `#[typetag::serde(tag = "type")]` traits at the pinned iceberg rev, so the new storage needs `Serialize`/`Deserialize` on a type that holds a live, renewing credential source. iceberg-storage-opendal handles the same problem with `skip_deserializing` on its custom loader. Implementing them also needs direct `typetag` and `serde` dependencies in `lakehouse-catalog-write`. Separately, minting an account SAS in the E2E harness needs HMAC-SHA256, and `crates/lakehouse-e2e/Cargo.toml` has no `hmac` crate. "An account SAS for its container" also mixes the account-SAS and service-SAS kinds.
- Fix: In § Dependencies, list `typetag` for `lakehouse-catalog-write` and the HMAC crate for `lakehouse-e2e`. In § Implementation Notes, add one line saying the ADLS storage's credential source is skipped on serialization and a deserialized instance refuses every request. Say whether the E2E mints an account SAS or a container (service) SAS.

#### [HIDDEN_DEPENDENCY] FINDINGS
- Location: plan.md § Verification § Checklist
- Issue: The plan widens `StorageCredentials`, `vended_s3_credentials`, and `CatalogSession::load_credentials`, which the vended S3 export also runs through. Only the Lakekeeper suite runs a vended S3 export end to end (`e2e_lakekeeper/export_write.rs`, scenario "Static and vended storage credentials both write the table"). The local suite runs none (`use_vended_credentials: false` throughout `e2e_local`). `.claude/CLAUDE.md` § Test suites requires the Lakekeeper suite for a change to Iceberg REST catalog behavior. The Checklist lists only the local and Azure suites.
- Fix: Add `make lakekeeper-up test-e2e-lakekeeper` to § Checklist, with the same "reported as not run without credentials" note as the Azure row.

## Requirement Quality

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: export/export-catalog-storage/spec.md § Scenario "A CONNECTION the export does not support is refused before any data is read", second WHEN/THEN
- Issue: "through a CONNECTION that `CREATE VIRTUAL SCHEMA` refuses ... *THEN* ... with the error `CREATE VIRTUAL SCHEMA` gives for that CONNECTION" also covers CONNECTIONs that `CREATE VIRTUAL SCHEMA` refuses only after it reaches the catalog: an unreachable address, a rejected token, an empty table listing. The scenario "A bad CONNECTION fails before any data is read" already gives those a different expected error ("names the catalog as the failing side"). As written, the two scenarios conflict, and no test can cover "every CONNECTION `CREATE VIRTUAL SCHEMA` refuses".
- Fix: Narrow the WHEN to "a CONNECTION whose credential fields `CREATE VIRTUAL SCHEMA` refuses before it contacts the catalog, such as one with Azure storage fields and `aws_assume_role_arn`".

#### [COMPLETENESS_GAP] FINDINGS
- Location: export/export-catalog-storage/spec.md § Scenario "Static and vended credentials write a table on ADLS Gen2"; plan.md § Scenario Coverage
- Issue: The issue asks that "creating new tables and appending both work on ADLS Gen2". `SINGLE_COMMIT = 'true'` creates a table through a separate path: staged creation, credentials vended for a staged table, and `loadCredentials` on that staged table (`testdata/load_credentials_staged.json` exists for S3). The new scenario and its E2E test cover only the parallel create and the append, so a staged ADLS create gets no proof.
- Fix: Add `SINGLE_COMMIT = 'true'` to the first export of at least the vended arm, in the scenario's WHEN and in the E2E test `azure_export_writes_and_appends_with_account_key_sas_and_vended_credentials`.

#### [COMPLETENESS_GAP] FINDINGS
- Location: export/export-catalog-storage/spec.md § Scenario "A writer that outlives its vended credentials ..."; storage-access/pushdown-planning-cloud-credentials-vended-storage/spec.md (REMOVED scenario)
- Issue: The removed scenario was the only spec text in which the export refused "a missing key" of vended credentials. The CHANGED scenario carries over only the unreadable-expiry refusal. So no export scenario says what happens when a create, stage, load, or `loadCredentials` answer lacks the S3 key pair, or lacks an `adls.sas-token.<host>` for the location's host. The code refuses both (`a_renewal_answer_without_a_key_is_refused_naming_the_key`).
- Fix: Add a WHEN/THEN pair to the CHANGED scenario: "*WHEN* the vended credentials, at creation or at renewal, lack an S3 access key or secret key, or an ADLS SAS for the table location's host *THEN* the writer SHALL fail with an error that names the missing key and contains no storage credential". Map it to the existing test plus an ADLS case.

#### [COMPLETENESS_GAP] FINDINGS
- Location: export/export-catalog-storage/spec.md § Scenario "A table location the CONNECTION's storage cannot address is refused", last AND; export/export-options/spec.md § Background (`ALLOW_HTTP` line)
- Issue: The plan reuses `resolve_vended_storage` as the `abfs` gate. Its error reads "the ALLOW_HTTP virtual-schema property is false", which points an export user at a virtual schema property. In the export, `ALLOW_HTTP` is an export option. export-options § Background also lists every request `ALLOW_HTTP` unlocks and does not list a vended `abfs://` location. An `abfs://` location writes over HTTPS anyway, so the export option's effect on ADLS goes unstated where users look it up.
- Fix: In the scenario, require the error to name the export option `ALLOW_HTTP`, and in plan.md § Implementation Notes say how the export gets that wording, for example a writer-side `abfs` check like `refuse_plaintext_endpoint`. Add the vended `abfs://` table location to export-options § Background with a CHANGED delta, and state that the write still uses HTTPS.

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md § Verification § Scenario Coverage
- Issue: The unit test `vended_credentials_without_an_expiry_never_expire_and_an_unreadable_expiry_is_refused` in `lakehouse-catalog` proves that the expiry is parsed, not "the writer SHALL use the credentials as vended and SHALL NOT renew them". That step is about the writer's holder in `lakehouse-catalog-write`. Separately, the new location scenario's static-Azure arm can be produced end to end on the local stack: an Azure CONNECTION against the local `s3://` warehouse. specs/testing.md § Coverage rule admits fault injection alone only for failures no E2E test can produce.
- Fix: Map the "SHALL NOT renew" step to a `vended_refresh_tests.rs` case that seeds a vended credential without an expiry and asserts that no `loadCredentials` request reaches the fake catalog. Add an E2E row (local suite, `export_refuses_unwritable_connections_and_column_names` or a sibling) for the static-Azure-to-`s3://` refusal, or state in plan.md why it cannot be produced end to end.

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: storage-access/pushdown-planning-cloud-credentials-vended-storage/spec.md (unchanged Background, kept by the delta)
- Issue: The Background keeps "The account name is the first dot-separated label of that host, taken verbatim, because `iceberg-storage-opendal` compares it byte-exactly ...", which is the reader's reason. D3 makes the writer address "the location's account" over object_store with no such guard. The two rules agree in effect, but export-catalog-storage § Background points to this feature for "the choice among vended credentials", and that pointer now carries a reason that does not hold for the writer.
- Fix: No spec edit is needed if the writer takes the account from the location host exactly as the reader does. Say so in D3 ("the account is the location host's first label, as on the read path"), so the implementation does not fall back to the CONNECTION's `account_name`.

## Design Depth

No `ADR: yes` directions, so `[ADR_OVERPROMOTION]` does not apply. The architecture delta has the BASE `e9cea50f...`, which matches `git hash-object specs/architecture.md`. It changes only the `lakehouse-catalog-write` Components line and the ADLS External Dependencies line, and it carries every structural fact in plan.md: the new ADLS write storage, the object_store client, and exports as an ADLS consumer. `specs/architecture.md` § Constraints (no `object_store` in `lakehouse-catalog`) holds, because the dependency lands in `lakehouse-catalog-write`. ADRs checked for conflict: `vended-extraction-forked-policy-shared` (the ADLS arm reuses `iceberg_vended_sas` and the shared `abfs` gate), `two-vended-static-selectors-on-disjoint-inputs`, `allow-http-threaded-as-vended-selector-parameter`, `adlscred-exactly-one-credential-type`, `azure-s3-mixed-credential-fields-rejected`, and `static-store-address-capability-narrowed-type`. No conflict: the removed scenario carries no `/// Scenario:` line, and the ADR's source probes in `catalog_public_surface.rs` stay.

#### [DESIGN_VIOLATION] FINDINGS
- Location: plan.md § Implementation Notes (bullets "Widen `StorageCredentials`", "Widen `vended_s3_credentials`", and "`adls_refused` ... stay")
- Issue: Principle: information hiding. Each backend has its own credential type, so one module should own each. `StorageCredentials` implements reqsign's `ProvideCredential<Credential = AwsCredential>`. Adding ADLS variants to its `Source` enum puts AWS and Azure credentials in one type, which then also has to implement object_store's Azure `CredentialProvider`. Every consumer must then handle the wrong-backend states at runtime: `loader()` on an ADLS source, an Azure credential from an S3 source. The plan keeps `adls_refused` and the ADLS refusal in `file_io_with_credential_loader` for a pairing it calls impossible ("an assumed role never pairs with Azure storage"). After the change, `adls_refused`'s only caller, the `WriteStorage::Static` arm, routes ADLS to the new factory. That leaves it dead, yet it is not in § Dead Code Removal. The accepted ADR `adlscred-exactly-one-credential-type` prefers "unrepresentable rather than merely validated" for the same kind of state.
- Fix: In § Implementation Notes, plan one credential source per backend: the existing S3 `StorageCredentials` unchanged, and an ADLS source over `ExpiringCredential<AzureCredential>`. Both share `keep_at_renewal_point`, `RENEWAL_SKEW`, and the `loadCredentials` call. Make `storage_of` return the FileIO for the backend the location selects, so no wrong-backend state can be built. Then list `adls_refused` in § Dead Code Removal, or name its remaining caller.
