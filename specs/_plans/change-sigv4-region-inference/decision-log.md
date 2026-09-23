# Decision Log: change-sigv4-region-inference

## Interview

**Q:** `ConnectionCreds.region` serves two purposes today: it signs the Glue catalog request (`sigv4.rs`), and it is the CONNECTION's preferred S3 storage address (`StaticStoreAddress::from`, which wins over a vended region when non-empty, per the shipped `pushdown-planning-cloud-credentials` delta). If the engine infers a region from the Glue hostname, does the inferred value flow into both uses, or stay scoped to catalog signing only?
**A:** Signing only. The inferred region authenticates the Glue catalog request only. `ConnectionCreds.region`, and everything downstream that reads it (including the CONNECTION-wins storage-address rule in `StaticStoreAddress::from`), stays exactly what the operator typed in the CONNECTION JSON, empty if they typed nothing. A Glue catalog's region is not necessarily the S3 bucket's region, so the inferred value never leaks into `ConnectionCreds.region` or any field derived from it. The inference is a separately computed "effective signing region", consumed only by the SigV4 signing call sites (`crate::sigv4::sign_request` in `iceberg_io.rs` and `namespace.rs`) and by the credential-validation guard that hard-requires `region` today, not by `parse_creds` or `ConnectionCreds` construction.

**Q:** Which Glue hostname shapes are recognized for inference?
**A:** Commercial only: `https://glue.{region}.amazonaws.com/...`. Do not recognize GovCloud (`amazonaws-us-gov.com`) or China (`amazonaws.com.cn`) partition hostnames. A CONNECTION against those keeps supplying `region` explicitly. Keep the recognized shape narrow and match only what the evidence base (existing test fixtures, current Glue usage) covers.

**Q:** When `use_sigv4` is enabled, `region` is omitted, and the catalog URI's hostname does not match a recognized Glue shape (for example a private VPC/PrivateLink endpoint), what happens?
**A:** Hard error, same as today. `validate_creds` keeps rejecting the CONNECTION and naming `region` as a missing required field for any URI that is not the recognized commercial Glue hostname shape. The error wording also explains that `region` may be omitted when the catalog URI is a standard commercial Glue hostname, so the operator knows the escape hatch exists. Non-standard and private endpoints see no behavior change.

**Q (not asked, stated by the orchestrator as following from the answers above):** When both an explicit `region` and an inferable URI are present, which wins?
**A:** The explicit value always wins and is used verbatim for signing. Inference is a fallback that activates only when `region` is empty or absent. The resolution order is: explicit, else inference, else error. **Superseded by decision [4]** after PR review (below): for a standard Glue endpoint the derived value always signs, and the explicit `region` only places the S3 store.

**Reviewer concern (verbatim from the user):** "try to adjust to prose of the specs. The agent has a tendency to document the 'transition', I mean to narrate: 'we had this property, now we don't have it'. This is not correct. The specs document the state. So there shouldn't be this 'narrative', the property shouldn't appear on the specs, as if it hasn't never been there in the first place. It must appear on the plan of course. And it may appear on the decisions, but shouldn't appear on the specs."

## Design Decisions

### [1] The derived region signs catalog requests only and never becomes the CONNECTION's `region`

- **Decision:** The SigV4 signing region is a value computed on demand from `ConnectionCreds` and the catalog URI. Only the adapter's SigV4 guard and the two catalog signing paths read it. `parse_creds` does not compute it. `ConnectionCreds.region` holds exactly what the CONNECTION states.
- **Alternatives:** Write the derived region into `ConnectionCreds.region` at parse time. Rejected: `ConnectionCreds.region` is a plain `String`, so a derived value becomes indistinguishable from a stated one. `StaticStoreAddress::from` (`crates/lakehouse-catalog/src/storage.rs:308-316`), `StorageCreds::from`, and `supplied_s3_fields` (`crates/lakehouse-engine/src/adapter/connection.rs:256`) would then read a catalog region as a storage region. Add a separate `signing_region` field to `ConnectionCreds`. Rejected: it conflates parsed input with a derived value, and it touches every `ConnectionCreds` struct literal across both crates and the E2E suites.
- **Rationale:** Interview answer 1. An AWS Glue catalog and the S3 buckets of its tables can sit in different regions. A derived catalog region that placed a store would send reads to the wrong regional S3 host.
- **Consequences:**
  - Storage addressing is unchanged. A CONNECTION that omits `region` reaches every storage rule with `region` unstated.
  - A non-vended Glue CONNECTION that omits `region` passes validation and `CREATE VIRTUAL SCHEMA` (catalog metadata only), then fails at scan time. `storage_block` hands `AmazonS3Builder::with_region("")` an empty region. object_store 0.13.2 defaults to `us-east-1` only when no region is set (`src/aws/builder.rs:1086`), and builds `https://{bucket}.s3.{region}.amazonaws.com` (`builder.rs:1218`), which yields `https://{bucket}.s3..amazonaws.com`. `docs/catalogs.md` therefore tells operators to keep stating `region` whenever the scan reads with static S3 keys.
  - Under vending, the store region follows `vs-adapter/pushdown-planning-cloud-credentials`: the CONNECTION's stated region, else the vended `client.region`. That spec records Glue's vended `client.region` as unverified.
- **Promotes to ADR:** yes

### [2] Only commercial AWS region codes qualify, enforced by the region-code shape rather than by the domain

- **Decision:** A standard AWS Glue endpoint is an `https` address whose host is exactly `glue.<region>.amazonaws.com`, where `<region>` matches the commercial region-code shape: two letters, a hyphen, one or more letters, a hyphen, one or more digits. The host is compared after `url::Url` normalization, which lowercases it. Port and path do not affect the match.
- **Alternatives:** Match on the `amazonaws.com` domain with any single-label region. Rejected: AWS GovCloud (US) Glue endpoints are `glue.us-gov-west-1.amazonaws.com` and `glue.us-gov-east-1.amazonaws.com`, so a domain-only rule admits GovCloud ([AWS Glue endpoints and quotas](https://docs.aws.amazon.com/general/latest/gr/glue.html)). That endpoints table lists no `amazonaws-us-gov.com` host for Glue, so the interview's GovCloud example does not describe the endpoints an operator configures. An explicit region allow-list. Rejected: it goes stale each time AWS opens a commercial region. A `us-gov-` prefix exclusion. Rejected: it is ad hoc and does not state the rule the operator chose.
- **Rationale:** Interview answer 2 asks for commercial only and a narrow shape. Commercial region codes have three hyphen-separated parts with a two-letter area (for example `us-east-1`, `ap-southeast-2`, `il-central-1`). GovCloud codes have four parts (`us-gov-west-1`), so the shape excludes them. China endpoints use the `amazonaws.com.cn` suffix, so the exact-suffix rule excludes them. FIPS (`glue-fips.`), dual-stack (`.api.aws`), and VPC interface endpoints (`vpce-*.glue.<region>.vpce.amazonaws.com`) fail the exact-host rule. `url::Url` parsing, not string slicing, decides the host, so `https://glue.us-east-1.amazonaws.com@evil.example/` resolves to host `evil.example` and does not match. A parsing rule local to one helper, not a system architecture decision — the scenario in `vs-adapter/connection-credentials` already pins the exact shape, so no ADR is needed to keep it discoverable.
- **Promotes to ADR:** no

### [3] An address that is not a standard AWS Glue endpoint keeps the hard `region` requirement

- **Decision:** When `use_sigv4` is true, `region` is empty, and the address is not a standard AWS Glue endpoint, `validate_sigv4_creds` rejects the CONNECTION and names `region`, as before. The error adds one clause stating that a CONNECTION whose address is a standard AWS Glue endpoint of the form `https://glue.<region>.amazonaws.com` can omit `region`.
- **Alternatives:** Fall back to a default region such as `us-east-1`. Rejected: a wrong signing scope fails at Glue with an HTTP 403 that names no field. Warn and sign with an empty region. Rejected: the same opaque failure, deferred.
- **Rationale:** Interview answer 3. A plan-time error that names the field is the cheapest failure an operator can act on.
- **Promotes to ADR:** no

### [4] A standard AWS Glue endpoint's derived region always signs; a stated `region` places the S3 store, independently

- **Decision:** `sigv4_signing_region` checks the address FIRST: when it is a standard AWS Glue endpoint, the method returns the region the host names, even when `region` is also stated and even when the two differ. Only when the address is NOT a standard Glue endpoint does the method fall back to the stated `region`. `ConnectionCreds.region` is untouched either way (decision [1]), so a stated `region` still places the S3 store regardless of which value signed the catalog request.
- **Alternatives:** The plan's original design — the stated `region` always wins for signing, with no mismatch check (round-1 planning; interview answer 4). Rejected after PR review: a Glue catalog and its tables' S3 bucket are commonly in different AWS regions (the reviewer's example: Glue in `eu-west-1`, the bucket in `us-east-1`). Under "stated always wins", an operator who states the bucket's region to place the store also forces Glue's signature to that same wrong region, and Glue rejects it — the exact cross-region deployment shape decision [1] exists to support becomes unreachable. A mismatch check that rejects a differing stated region. Rejected: it would forbid the cross-region configuration this change now exists to support. A second field carrying the signing region separately from `region`. Rejected: `ConnectionCreds.region` already serves as the pure storage-region field the moment signing stops reading it for standard Glue endpoints (decision [1]); a second field duplicates that separation for no benefit and touches the wire/JSON schema for no reason.
- **Rationale:** A Glue catalog and its tables' S3 buckets can sit in different regions. `region` needs to serve the STORAGE side in that case; making the SAME value also sign the Glue request makes the combination unsupportable, because Glue rejects a signature computed for the bucket's region. Deriving the signing region from the URL removes the conflict for the case the derivation already recognizes: a standard commercial Glue endpoint. This reverses this plan's own signing-precedence decision after review; getting it wrong again later (e.g. "simplifying" back to "stated always wins") would silently reintroduce the cross-region bug this change exists to fix — the reason this promotes to a permanent ADR below.
- **Consequences:**
  - A CONNECTION whose stated `region` differs from a standard Glue endpoint's own region now works: Glue signing uses the endpoint's region, and the S3 store places at the stated region.
  - A CONNECTION whose stated `region` matches the endpoint's region is unaffected (same signing outcome as before).
  - Not a breaking change: a CONNECTION with a differing stated `region` was already rejected by Glue before this change (signed for the wrong region); it now succeeds instead.
  - `vs-adapter/connection-credentials`'s "stated region wins" scenario is replaced by one asserting the endpoint's region signs regardless of a differing stated value.
- **Promotes to ADR:** yes

### [5] One public method on `ConnectionCreds`, declared in `sigv4.rs`, owns the host rule and the precedence

- **Decision:** `lakehouse-catalog` declares `pub fn sigv4_signing_region(&self, catalog_uri: &str) -> Option<String>` in an `impl ConnectionCreds` block inside `crates/lakehouse-catalog/src/sigv4.rs`, beside `sign_request`. The host parser stays private in that module. The engine's `validate_sigv4_creds` calls the method with the URI that `read_connection` already holds.
- **Alternatives:** A free `pub fn` re-exported at the crate root. Rejected: it adds a new item to the enumerated surface, while a method on an existing public type adds none. Parse the host in the engine's `connection.rs`. Rejected: the catalog signing paths cannot depend on the engine, so the rule would need a second copy (back-door leakage between the guard and the signers). Pass a resolved region into `CatalogSession::resolve` and `IcebergRestCatalogClient::new`. Rejected: it changes public signatures that `crates/lakehouse-engine/tests/catalog_session_signatures.rs` pins, and it moves Glue knowledge into the engine. Derive the region inside `sign_request` from the request host. Rejected: the generic signer would gain Glue knowledge, and the engine guard would still need its own copy.
- **Rationale:** Glue endpoint shape is catalog-access knowledge, and `connection.rs` pins the rule that the catalog crate owns catalog-consuming logic while the adapter owns CONNECTION delivery. One declaration keeps the guard and both signing paths from disagreeing about whether a signing region exists. `ConnectionCreds::has_catalog_auth` is the precedent for a `pub` method the adapter's guard calls.
- **Promotes to ADR:** no

### [6] Resolve the region once per session or enumeration, and refuse to sign without one

- **Decision:** `CatalogAuth::Sigv4` becomes `Sigv4 { region: String }`. `resolve_catalog_auth` fills it once per `CatalogSession`. `list_namespace_tables` resolves the region once per enumeration and passes it down `list_in_namespace_signed` to `signed_get_json`. Both resolution points call a crate-private `required_signing_region`, which returns a credential-safe error naming `region` when the method returns `None`. Neither signing call reads `creds.region` directly.
- **Alternatives:** Re-derive at each `sign_request` call. Rejected: it repeats URL parsing per request and keeps `&creds.region` reachable at the call sites. Sign with an empty region when none resolves (today's behavior for unvalidated credentials). Rejected: `CatalogSession::resolve` is public and `crates/lakehouse-engine/tests/cloud_e2e_test.rs:730` calls it without the adapter's guard, so the crate needs its own total behavior.
- **Rationale:** `CatalogSession` already resolves the auth strategy and prefix once per query (its doc comment). The region is part of that strategy.
- **Promotes to ADR:** no

### [7] Rename the SigV4 guard scenario instead of keeping its title

- **Decision:** The delta removes "When SigV4 is enabled, access_key, secret_key, and region are required" and adds "When SigV4 is enabled, access_key, secret_key, and a signing region are required".
- **Alternatives:** A `DELTA:CHANGED` block that keeps the old title. Rejected: the title would state an unconditional `region` requirement that the body and a sibling scenario contradict.
- **Rationale:** Scenario titles are the anchors other clauses cite. The vending scenario's changed clause cites the new title.
- **Promotes to ADR:** no

### [8] `pushdown-planning-cloud-credentials` receives a Background-only delta, and `scan-spec-credential-reference` receives none

- **Decision:** A `DELTA:CHANGED` `## Background` block on `vs-adapter/pushdown-planning-cloud-credentials` reproduces the recorded Background verbatim, except for four corrected clauses. The clauses sit in the recorded bullets at lines 76, 77, 92, and 94. Each presumes that every SigV4 or Glue CONNECTION states `region`. Bullet 76 loses its parenthetical that `use_sigv4` REQUIRES `region`. Bullet 77 names "a `region` it states" instead of a `region` that every SigV4 CONNECTION supplies. Bullet 92 names "a `region` that the Glue CONNECTION states". Bullet 94 states that a Glue CONNECTION that states `region` places the store with that value. It also states that a Glue CONNECTION that omits `region` places the store from the vended `client.region` alone. That case stays unverified. No scenario of that feature changes. `vs-adapter/scan-spec-credential-reference` receives no delta.
- **Alternatives:** Leave `pushdown-planning-cloud-credentials` unchanged. Rejected: bullets 76, 92, and 94 state as fact that every Glue CONNECTION states `region`. Bullet 94 discharges the Glue-region risk on that premise. The scenario "A standard AWS Glue endpoint supplies the SigV4 signing region when the CONNECTION omits region" falsifies the premise. Decision [9] requires an in-place correction. Restore the blocking `client.region` assertion in `e2e-harness/cloud-e2e-harness`. Rejected: a Glue CONNECTION that states `region` still places its store without that key. `docs/catalogs.md` names the region-less vended case to operators (task 4.1).
- **Rationale:** The corrected clauses follow decision [9]: they state the resulting behavior and gain no narrative wording. The scenario "Catalog REST requests to Glue are SigV4-signed when enabled" states in its GIVEN that the CONNECTION supplies `region`, so "the configured `region`" stays accurate and the scenario stays unchanged. The delta copies that scenario verbatim outside every marker, because `speq plan validate` rejects a delta file with no scenario. The derived case has its normative home in `vs-adapter/connection-credentials`. `scan-spec-credential-reference` names no region rule (checked with `grep -n region`).
- **Consequences:**
  - A region-less Glue vended CONNECTION places its store from Glue's vended `client.region` alone. `e2e-harness/cloud-e2e-harness` reports that key and does not assert it, so no suite verifies this case.
- **Promotes to ADR:** no

### [9] Spec deltas state the resulting behavior. The change narrative lives only in the plan and this log

- **Decision:** New scenarios and new Background bullets describe how the system behaves, with no "previously", "now", "this delta", or SUPERSEDES wording. Existing narrative bullets from earlier deltas stay verbatim, except for the clauses that the new scenarios contradict. Those clauses are corrected in place and gain no narrative wording.
- **Alternatives:** Keep the house style of "This delta ..." and SUPERSEDES bullets for the new material. Rejected by the user's reviewer concern (Interview section).
- **Rationale:** This is a project-wide process convention, and it meets the override: (a) it binds every future plan's spec deltas, (b) it is not scoped to this plan, and (c) it is not a corollary of another decision. The user stated it as a general rule: "The specs document the state." It is a spec-writing convention, though, not a decision about the system's architecture — it belongs in process docs (`speq:writing-guardrails`), not the ADR log, and copying it into `specs/_decision/` would also strand its own "lives only in the plan and this log" clause once it no longer does.
- **Consequences:**
  - Corrected clauses: the `region` half of the SUPERSEDES bullet and the trailing Background paragraph of `vs-adapter/connection-credentials`, the SigV4 clause of its vending scenario, and one clause each in the Background and the vended scenario of `e2e-harness/cloud-e2e-harness`.
- **Promotes to ADR:** no

### [10] The live Glue proof is metadata-only

- **Decision:** The new `cloud-e2e` test creates a virtual schema through a CONNECTION that omits `region` and asserts that the Glue table is listed. It reads no data file.
- **Alternatives:** Run a scan through the same virtual schema. Rejected: the derived region places no store (Decision [1]), so a non-vended scan fails on the empty S3 region, and a vended scan depends on Glue's unverified `client.region`. Either result tests storage rules this plan does not change.
- **Rationale:** `CREATE VIRTUAL SCHEMA` runs both signed paths from the same `Resolved.uri`: `IcebergRestCatalogClient::list_tables` enumerates through `list_namespace_tables`, then loads each table through `CatalogSession::resolve` and `load_table_any_auth` (`crates/lakehouse-catalog/src/client.rs`). A listed table therefore proves Glue accepted both signatures.
- **Promotes to ADR:** no

### [11] In-repo tests prove derived-region signing in two halves

- **Decision:** Pure unit tests pin the host rule and precedence in `sigv4_tests.rs`. Signing-site tests inject a resolved region into `authed_get_json` and `list_in_namespace_signed`, with `creds.region` empty, and assert the captured `Authorization` credential scope. Resolution tests assert that `resolve_catalog_auth` carries the derived region and that both resolution points refuse without one.
- **Alternatives:** One end-to-end test against a Glue hostname. Rejected: a Glue hostname is HTTPS and cannot target the local TCP listener that the existing tests use, and both signing paths build their own `reqwest::Client`.
- **Rationale:** The two halves join at `required_signing_region`, which both resolution points call. The `cloud-e2e` test (Decision [10]) closes the gap against real Glue.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] A recorded Background still discharges the Glue-region risk on the premise that every Glue CONNECTION states `region`

- **Finding:** Round 1 flagged `[REQUIREMENT_CONFLICT]` (BLOCKER, MECHANICAL). Bullets 76, 92, and 94 of the recorded `vs-adapter/pushdown-planning-cloud-credentials` Background state that every SigV4 or Glue CONNECTION states `region`. Bullet 94 discharges the "does Glue vend `client.region`" risk on that premise. This plan's new `connection-credentials` scenario lets a Glue CONNECTION omit `region`. Decision [8] exempted the spec for reasons that did not hold: a "read-only" brief that traces to nothing, and the length of the Background. `plan.md` § Impact also called the risk unverified. The recorded spec calls it discharged and observation-only.
- **Direction change:** The plan adds `specs/_plans/change-sigv4-region-inference/vs-adapter/pushdown-planning-cloud-credentials/spec.md`, a Background-only delta that corrects bullets 76, 77, 92, and 94 and keeps every other line verbatim. Decision [8] records that delta and drops both rejected reasons. `plan.md` lists the feature in § Features and in group B's Knowledge column, and drops it from the 4th Non-Goals bullet. The last sentence of the § Impact bullet "Storage region, operator action" states the discharged, observation-only status. Task 4.1 tells vended Glue operators that omitting `region` leaves store placement to the vended `client.region` alone. § Manual Testing adds the `cloud_glue_vends_the_s3_key_pair_for_the_table_location` report row.
- **Promotes to ADR:** no
