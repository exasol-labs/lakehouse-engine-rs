# Plan: change-sigv4-region-inference

## Summary

A SigV4 CONNECTION to a standard commercial AWS Glue endpoint (`https://glue.<region>.amazonaws.com/...`) signs its catalog requests for the host's region, whether or not `region` is also stated — and can omit `region` entirely. A stated `region` never signs a standard Glue endpoint's requests; it only places the S3 store, independently, so a Glue catalog and its tables' S3 bucket can be in different AWS regions (closes #127).

## Design

### Context

`validate_sigv4_creds` (`crates/lakehouse-engine/src/adapter/connection.rs:165-193`) rejects every SigV4 CONNECTION that omits `region`. Both catalog signing call sites pass `&creds.region` to `sign_request`: `authed_get_json` (`crates/lakehouse-catalog/src/iceberg_io.rs:52-58`) and `signed_get_json` (`crates/lakehouse-catalog/src/namespace.rs:177-183`). An AWS Glue endpoint encodes its region in the host, so the requirement is redundant for that address shape (issue #127).

`ConnectionCreds.region` is also the CONNECTION's store region. `StaticStoreAddress::from` (`crates/lakehouse-catalog/src/storage.rs:308-316`), `StorageCreds::from` (through `storage_block`), and `supplied_s3_fields` (`connection.rs:256`) read it. A Glue catalog and the S3 buckets of its tables can sit in different regions. A derived catalog region therefore must not enter that field (decision-log [1]).

`read_connection` (`connection.rs:46-90`) holds the catalog URI and the parsed credentials together, but it does not pass the URI to `validate_creds`. Both signing paths receive the same URI: `IcebergRestCatalogClient::list_tables` passes it to `list_namespace_tables`, and `CatalogSession::resolve` passes it to `resolve_catalog_auth`.

- **Goals**
  - A SigV4 CONNECTION whose address is a standard commercial AWS Glue endpoint passes validation without `region`.
  - Namespace enumeration and every `CatalogSession` request sign for a standard Glue endpoint's own derived region, even when the CONNECTION also states a (possibly different) `region` — so a Glue catalog and its tables' S3 bucket can be in different AWS regions.
  - For any other endpoint, `region` is required and signs the request, exactly as before.
  - One declaration owns the Glue host rule and the precedence. The adapter guard and both signing paths call it.
  - The missing-`region` error names the Glue-endpoint alternative.
- **Non-Goals**
  - Region derivation for AWS GovCloud (US), AWS China, FIPS, dual-stack, VPC interface, private, or proxy endpoints.
  - Any change to storage addressing. `region` places the S3 store exactly as before (decision-log [1]); this plan only changes which value SIGNS a standard Glue endpoint's requests.
  - A mismatch check, error, or warning when a stated `region` differs from a standard Glue endpoint's own region — the difference is the supported case (cross-region Glue and S3), not a defect to flag (decision-log [4]).
  - Changes to `vs-adapter/scan-spec-credential-reference` (decision-log [8]).

### Decision

#### Architecture

```
lakehouse-engine (adapter/connection.rs)          lakehouse-catalog
────────────────────────────────────────          ─────────────────────────────────────────────
read_connection(ctx, name, kind)
  uri   = conn.address
  creds = parse_creds(json)       (region = stated value, never derived)
  validate_creds(name, &creds, kind, &uri)
    validate_sigv4_creds(name, &creds, &uri) ───▶ ConnectionCreds::sigv4_signing_region(&uri)   sigv4.rs, pub
      None → error naming `region`                  glue_endpoint_region(uri) is Some(r) ──▶ Some(r)   private
             + Glue-endpoint clause                 else stated region non-empty ──▶ Some(stated)
                                                     else None
Resolved { uri, creds }
  │
  ├─ list_tables ─▶ list_namespace_tables(uri, ns, storage, creds)             namespace.rs
  │                   region = required_signing_region(creds, uri)?            sigv4.rs, pub(crate)
  │                   list_in_namespace_signed(uri, ns, prefix, creds, &region)
  │                     signed_get_json(url, creds, &region) ─▶ sign_request(.., &region, "glue")
  │
  └─ CatalogSession::resolve(uri, warehouse, creds)                            session.rs
       resolve_catalog_auth(client, uri, creds)                                 auth.rs
         CatalogAuth::Sigv4 { region: required_signing_region(creds, uri)? }
       authed_get_json(.., &auth, ..) ─▶ sign_request(.., region, "glue")       iceberg_io.rs

StaticStoreAddress::from(&creds) · StorageCreds::from(&creds) · supplied_s3_fields(&creds)
  read creds.region only, so none of them can observe a derived region
```

The host rule inside `glue_endpoint_region`:

1. Parse the URI with `url::Url`. If parsing fails or the scheme is not `https`, return `None`.
2. Read `host_str()`. The `url` crate lowercases hosts for special schemes. Userinfo and port never reach it.
3. Strip the exact prefix `glue.` and the exact suffix `.amazonaws.com`. If either is absent, return `None`.
4. The remainder is the region only if it has exactly three `-`-separated parts: two ASCII lowercase letters, one or more ASCII lowercase letters, and one or more ASCII digits. A dot anywhere in it fails the check.

`url` is already a `lakehouse-catalog` dependency (`crates/lakehouse-catalog/Cargo.toml:21`). The rule needs no `regex` crate.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Single owner for a derived value | `ConnectionCreds::sigv4_signing_region` in `sigv4.rs` | The guard and both signing paths cannot disagree about whether a signing region exists |
| Resolve once, carry in the strategy | `CatalogAuth::Sigv4 { region }` | `CatalogSession` already resolves auth and prefix once per query |
| Parse, then compare | `url::Url` host in `glue_endpoint_region` | String slicing mis-reads `https://glue.us-east-1.amazonaws.com@evil.example/` as a Glue host |
| Derived value kept out of parsed input | `ConnectionCreds.region` untouched | Storage rules keep reading only what the CONNECTION states |
| Endpoint recognized before stated value is read | `sigv4_signing_region` checks `glue_endpoint_region` first | A standard Glue endpoint's own region must sign regardless of what `region` states, so a stated bucket region can differ (decision-log [4]) |

#### Key Interfaces

```rust
// crates/lakehouse-catalog/src/sigv4.rs
impl ConnectionCreds {
    /// Region a SigV4-signed catalog request is signed for: the region a
    /// standard commercial AWS Glue endpoint host names, when the address is
    /// one — even when `region` is also stated, and even when the two differ.
    /// Otherwise the stated `region` when non-empty, else `None`. Signing-only
    /// by design: the result is never written back into `region`, because a
    /// Glue catalog and its tables' buckets can sit in different regions and
    /// `region` places the store, not the signature.
    pub fn sigv4_signing_region(&self, catalog_uri: &str) -> Option<String>;
}
pub(crate) fn required_signing_region(creds: &ConnectionCreds, catalog_uri: &str) -> Result<String, UdfError>;
fn glue_endpoint_region(catalog_uri: &str) -> Option<String>;

// crates/lakehouse-catalog/src/auth.rs
pub(crate) enum CatalogAuth { Sigv4 { region: String }, Bearer(String), None }

// crates/lakehouse-catalog/src/namespace.rs
fn list_in_namespace_signed<'a>(catalog_uri: &'a str, ns: &'a NamespaceIdent, warehouse: &'a str,
    creds: &'a ConnectionCreds, region: &'a str) -> Pin<Box<dyn Future<Output = ...> + Send + 'a>>;
async fn signed_get_json(url: &str, creds: &ConnectionCreds, region: &str) -> Result<serde_json::Value, UdfError>;

// crates/lakehouse-engine/src/adapter/connection.rs
fn validate_creds(name: &str, creds: &ConnectionCreds, kind: CatalogKind, catalog_uri: &str) -> Result<(), UdfError>;
fn validate_sigv4_creds(name: &str, creds: &ConnectionCreds, catalog_uri: &str) -> Result<(), UdfError>;
```

Error texts. Neither text contains a credential value or the catalog URI.

| Site | Text |
|------|------|
| `validate_sigv4_creds` | `CONNECTION '{name}' enables SigV4 signing but is missing field(s) required when SigV4 signing is enabled: {fields}`. When `{fields}` names `region`, it adds: `; a CONNECTION whose address is a standard AWS Glue endpoint of the form https://glue.<region>.amazonaws.com can omit region` |
| `required_signing_region` | `SigV4 catalog signing requires a region: neither the stated region nor the catalog URI supplies one` |

#### Quick Diagnostic (new public method)

| Question | Answer |
|----------|--------|
| One-sentence responsibility? | Yes: it returns the region that a SigV4-signed catalog request is signed for. |
| Easier to call than to reimplement? | Yes: callers pass a URI and get an `Option<String>`. URL parsing, the region-code shape, and the precedence stay hidden. |
| Would an internal change force an edit outside? | No: widening or narrowing the host rule edits only `sigv4.rs` and its tests. |
| Doc comment explains the reasoning? | Yes: it states the signing-only intent and why the result never enters `region`. |
| Exactly one owner per decision? | Yes: the host rule and the precedence live only in `sigv4.rs`. The engine asks and does not re-derive. |
| Boundary visible without reading internals? | Yes: the engine sees one method on a type it already imports. |
| Tactical shortcut with no follow-up? | None taken. |
| Business logic depends only inward? | Yes: the method takes a plain URI string and names no Exasol CONNECTION or UDF-context type. |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Derived region is signing-only (decision-log [1]) | Write it into `ConnectionCreds.region`, or add a `signing_region` field | A plain `String` cannot tell derived from stated values. Storage rules would read a catalog region as a bucket region. |
| Commercial region-code shape (decision-log [2]) | Domain-only match, region allow-list, `us-gov-` exclusion | GovCloud Glue endpoints share `amazonaws.com`. An allow-list goes stale. |
| Hard error off the standard shape (decision-log [3]) | Default region, or sign with an empty region | A named-field plan-time error is actionable. A Glue 403 is not. |
| A standard Glue endpoint's own region always signs, over a stated `region` (decision-log [4]) | Stated `region` always wins for signing (this plan's original design); a mismatch check or rejection | Glue and its tables' S3 bucket are commonly in different regions. "Stated always wins" makes that combination unsupportable: Glue rejects a signature computed for the bucket's region. |
| Method on `ConnectionCreds` in `sigv4.rs` (decision-log [5]) | Free `pub fn`, engine-side parser, new constructor parameters, inference inside `sign_request` | This option adds no new public item and keeps one owner. It changes no pinned signature. |
| Resolve once and refuse without a region (decision-log [6]) | Re-derive per request, or sign with an empty region | `CatalogSession::resolve` is public and is reachable without the adapter's guard. |

### Iceberg and Delta compliance gate

Not applicable. This plan changes CONNECTION validation and catalog request signing only. It touches no scanning, pushdown, schema, or type handling, so no Iceberg table spec or Delta protocol section governs it.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/connection-credentials | CHANGED | `specs/_plans/change-sigv4-region-inference/vs-adapter/connection-credentials/spec.md` |
| vs-adapter/catalog-crate-public-surface-extensions | CHANGED | `specs/_plans/change-sigv4-region-inference/vs-adapter/catalog-crate-public-surface-extensions/spec.md` |
| e2e-harness/cloud-e2e-harness | CHANGED | `specs/_plans/change-sigv4-region-inference/e2e-harness/cloud-e2e-harness/spec.md` |
| vs-adapter/pushdown-planning-cloud-credentials | CHANGED | `specs/_plans/change-sigv4-region-inference/vs-adapter/pushdown-planning-cloud-credentials/spec.md` |

What each delta changes, as a transition (the deltas themselves state only the resulting behavior, per decision-log [9]):

- `vs-adapter/connection-credentials`: `region` is required under SigV4 only when the address is not a standard commercial AWS Glue endpoint, where it used to be unconditional; and a standard Glue endpoint's own region always signs, even when `region` is also stated, where the stated value used to win unconditionally. The guard scenario is renamed (removed and re-added), two scenarios are added, and one clause of the vending scenario and two Background passages are corrected.
- `vs-adapter/catalog-crate-public-surface-extensions`: one scenario records the new `pub` method. The description drops its stale "four narrow additions" count.
- `e2e-harness/cloud-e2e-harness`: one live scenario is added. Two clauses that called `region` a SigV4 requirement are corrected.
- `vs-adapter/pushdown-planning-cloud-credentials`: four Background clauses that presume every SigV4 or Glue CONNECTION states `region` are corrected (decision-log [8]). The Glue-region discharge now covers only a Glue CONNECTION that states `region`. A region-less Glue vended CONNECTION places its store from the vended `client.region` alone. No scenario changes, so § Verification adds no coverage row for this delta.

## Impact

- **Operators, new option:** a SigV4 CONNECTION whose address is `https://glue.<region>.amazonaws.com/...` in a commercial region can omit `region`. `CREATE VIRTUAL SCHEMA`, `REFRESH`, and pushdown planning sign for the host's region.
- **Operators, cross-region Glue and S3 (new capability):** a CONNECTION whose Glue catalog and S3 bucket sit in different AWS regions can now state `region` as the BUCKET's region to place the S3 store; Glue signing derives its own region from the endpoint automatically, regardless of what `region` states. This directly resolves issue #127's open question on precedence, revised after review: the stated value no longer wins for signing on a standard Glue endpoint.
- **Existing CONNECTIONs:** a CONNECTION whose stated `region` matches its standard Glue endpoint's own region signs identically to before. A CONNECTION whose stated `region` differs from its endpoint's region was already rejected by Glue before this change (signed for the wrong region) — it now succeeds, signing for the endpoint's region while `region` places the S3 store. Not a breaking change.
- **Error text:** the missing-field error keeps its prefix. When it names `region`, it gains the Glue-endpoint clause. A caller that matches the full text exactly sees a longer message.
- **Storage region, operator action:** omitting `region` also leaves the S3 store region unstated. On the static-credential path, `AmazonS3Builder::with_region("")` builds `https://<bucket>.s3..amazonaws.com` (object_store 0.13.2 `src/aws/builder.rs:1086,1218`). The virtual schema is created, but scans fail at read time. `docs/catalogs.md` instructs operators to keep stating `region` whenever scans read with static S3 keys — including the bucket's own region when it differs from a standard Glue endpoint's region. Under vending, a Glue CONNECTION that omits `region` places the store from Glue's vended `client.region` alone. No suite asserts that key: `vs-adapter/pushdown-planning-cloud-credentials` records its verification obligation as discharged and observation-only.
- **GovCloud, China, FIPS, dual-stack, VPC interface, private, and proxy endpoints:** unchanged. They still state `region`, which still signs the request (no endpoint-derived region exists for these forms).
- **Library surface:** `lakehouse-catalog` gains one `pub` method, `ConnectionCreds::sigv4_signing_region`. No public signature changes.
- **Concurrent plan:** `refactor-nr-of-cores-detection` also changes `e2e-harness/cloud-e2e-harness`. Its blocks touch only the remote-bench scenarios, and this plan's blocks touch the Background, the vended scenario, and one new scenario. The two plans can record in either order.

## Dependencies

- No new crate. `url` is already a `lakehouse-catalog` dependency.
- Group B depends on group A (see Parallelization).
- GitHub issue #127 tracks this change. The implementing commit carries `Closes #127`.

## Migration

None required. Every existing CONNECTION whose stated `region` matches its standard Glue endpoint validates and signs as before; one whose stated `region` differed was already failing at Glue and now succeeds. An operator can remove `region` from a Glue CONNECTION only when its scans get the store region elsewhere, as the Impact entry "Storage region, operator action" describes. An operator whose Glue catalog and S3 bucket sit in different regions should now state `region` as the bucket's region — a standard Glue endpoint signs from its own hostname regardless of that value.

## Implementation Tasks

### 1. Signing-region resolver (`lakehouse-catalog`)

- [ ] 1.1 Write failing unit tests in `crates/lakehouse-catalog/src/sigv4_tests.rs` for `ConnectionCreds::sigv4_signing_region`. Cover the scenario host matrix: `https://glue.eu-west-1.amazonaws.com/iceberg`, an uppercase host, an explicit `:443` port, and `ap-southeast-2` all match. GovCloud, China, FIPS, VPC interface, dual-stack, private host, `http` scheme, the userinfo form `https://glue.us-east-1.amazonaws.com@evil.example/`, a trailing-dot host, a multi-label region, an empty URI, and an unparseable URI all fail. Also cover: a standard Glue endpoint with NO stated `region` derives the endpoint's region (unchanged); a standard Glue endpoint with a stated `region` that DIFFERS from the endpoint's own region STILL derives the endpoint's region, not the stated one; a non-standard endpoint with a stated `region` returns the stated value (unaffected); and the empty-region, non-Glue `None` case.
- [ ] 1.2 Implement `sigv4_signing_region` and the private `glue_endpoint_region` in `crates/lakehouse-catalog/src/sigv4.rs` with `url::Url`, per the host rule in Design. Write the doc comment as shown in Key Interfaces, and name no Exasol CONNECTION mechanism. [expert]
- [ ] 1.3 Add `pub(crate) fn required_signing_region` in `sigv4.rs`, returning the credential-safe refusal error from the Design table.

### 2. Signing paths carry the resolved region (`lakehouse-catalog`)

- [ ] 2.1 Change `CatalogAuth::Sigv4` to `Sigv4 { region: String }` in `crates/lakehouse-catalog/src/auth.rs`. Make `resolve_catalog_auth` fill it from `required_signing_region(creds, catalog_uri)?`.
- [ ] 2.2 In `crates/lakehouse-catalog/src/iceberg_io.rs`, make `authed_get_json` sign with the region the variant carries. Update its redaction match arm to `Sigv4 { .. }`. Update the match in `resolve_load_table_prefix` (`crates/lakehouse-catalog/src/session.rs:149`).
- [ ] 2.3 In `crates/lakehouse-catalog/src/namespace.rs`, resolve the region once in `list_namespace_tables` through `required_signing_region`, before the first request. Thread a `region: &str` parameter through `list_in_namespace_signed` and `signed_get_json`, and delete `&creds.region` from that `sign_request` call.
- [ ] 2.4 Update the tests that name the unit variant: `session_tests.rs:187` and `session_tests.rs:340`, and `auth_tests.rs:366`.
- [ ] 2.5 Add tests in `auth_tests.rs`: `sigv4_auth_carries_region_derived_from_glue_endpoint`, `sigv4_auth_derives_region_even_when_a_different_region_is_stated`, and `sigv4_auth_refuses_without_signing_region`. The SigV4 branch makes no network call.
- [ ] 2.6 Add `sigv4_request_is_signed_for_the_carried_region` in `iceberg_io_tests.rs`. Use a local TCP listener (the pattern of `namespace_tests.rs:74-143`), set `creds.region` to empty, pass `CatalogAuth::Sigv4 { region: "eu-west-1".into() }`, and assert that the captured `Authorization` header contains `/eu-west-1/glue/aws4_request`.
- [ ] 2.7 Add `signed_enumeration_is_signed_for_the_resolved_region` and `signed_enumeration_refuses_without_signing_region` in `namespace_tests.rs`, using the same capture pattern against `list_in_namespace_signed` and `list_namespace_tables`.
- [ ] 2.8 In `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, add `connection_creds_sigv4_signing_region_is_reachable`, which calls the method. Add `signing_region_steps_are_not_public`, which asserts from `sigv4.rs` source that it declares no `pub fn glue_endpoint_region` and no `pub fn required_signing_region`, and contains no `CONNECTION` or `UdfContext` token.

### 3. Adapter SigV4 guard (`lakehouse-engine`)

- [ ] 3.1 In `crates/lakehouse-engine/src/adapter/connection.rs`, pass `&uri` from `read_connection` into `validate_creds` and on to `validate_sigv4_creds`. Count `region` as missing only when `creds.sigv4_signing_region(catalog_uri)` is `None`.
- [ ] 3.2 Append the Glue-endpoint clause from the Design table when the error names `region`.
- [ ] 3.3 Rewrite the `REQUIRED_KEY` doc comment (`connection.rs:17-22`) so that it states the conditional `region` rule.
- [ ] 3.4 In `crates/lakehouse-engine/src/adapter/connection_tests.rs`, update `sigv4_requires_access_secret_region`: cite the new scenario title in its doc comment, and assert the Glue-endpoint clause when `region` is named.
- [ ] 3.5 Add `sigv4_region_required_for_non_standard_glue_hosts` in `connection_tests.rs`. Cover the GovCloud, China, FIPS, VPC interface, dual-stack, private-host, and `http` addresses. Each case is rejected with an error that names `region` and leaks no key.
- [ ] 3.6 Add four tests in `connection_tests.rs`. `sigv4_region_derived_from_standard_glue_endpoint_is_accepted` asserts `Ok`, an empty `resolved.creds.region`, an empty `StorageCreds::from(&resolved.creds).region`, and an empty `StaticStoreAddress::from(&resolved.creds).region()`. `sigv4_derived_region_places_no_store_under_vending` repeats the empty-region assertions with `use_vended_credentials: true`. `sigv4_cross_region_glue_and_s3_is_supported` builds a CONNECTION whose address is `https://glue.eu-west-1.amazonaws.com/iceberg` and whose stated `region` is `us-east-1`, asserts `resolved.creds.sigv4_signing_region(&uri) == Some("eu-west-1".into())` (the endpoint's own region signs) while `StaticStoreAddress::from(&resolved.creds).region() == "us-east-1"` (the stated region still places the store) — the reviewer's cross-region scenario end to end. `sigv4_stated_region_used_for_non_standard_endpoint` asserts a non-standard address (e.g. GovCloud) still signs with the stated `region`, unaffected by this change.

### 4. Operator documentation

- [ ] 4.1 In `docs/catalogs.md`, update the `region` row of the field table (line 41), § "Connection fields" (line 56), and § "AWS Glue Iceberg REST (SigV4)" (lines 92 and 114). Leave the other rows unchanged. State that a standard commercial Glue endpoint supplies the signing region, that other endpoints still need `region`, and that `region` still places the S3 store. Tell operators to state `region` whenever scans read with static S3 keys. At line 114, tell vended Glue operators that omitting `region` leaves store placement to Glue's vended `client.region` alone. State that no test asserts that Glue vends that key.

### 5. Live Glue proof (`cloud-e2e`)

- [ ] 5.1 In `crates/lakehouse-engine/tests/cloud_e2e_test.rs`, add `CloudEnv::catalog_connection_password_without_region` (`region: String::new()`). Let `setup_cloud_vs` take the password to use.
- [ ] 5.2 Add `cloud_sigv4_region_derived_from_glue_endpoint_lists_table`. Skip with a message when `GLUE_CATALOG_URI` does not begin with `format!("https://glue.{AWS_REGION}.amazonaws.com/")`. Compute that prefix in the test, not with the production method. Create CONNECTION `GLUE_CATALOG_CREDS_NO_REGION` and a virtual schema over the Glue namespace. Assert that `SYS.EXA_ALL_TABLES` lists the configured table for that schema. Read no data file.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Signing-region resolver and both catalog signing paths | 1.1-1.3, 2.1-2.8 | — | spec deltas `vs-adapter/connection-credentials` (scenarios "A standard AWS Glue endpoint supplies the SigV4 signing region when the CONNECTION omits region" and "A standard AWS Glue endpoint signs the catalog request even when the CONNECTION states a different region", signing clauses) and `vs-adapter/catalog-crate-public-surface-extensions`; `crates/lakehouse-catalog/src/sigv4.rs`, `sigv4_tests.rs`, `auth.rs`, `auth_tests.rs`, `iceberg_io.rs`, `iceberg_io_tests.rs`, `session.rs`, `session_tests.rs`, `namespace.rs`, `namespace_tests.rs`, `crates/lakehouse-catalog/tests/catalog_public_surface.rs` |
| B: Adapter guard, operator docs, and live Glue proof | 3.1-3.6, 4.1, 5.1-5.2 | A (calls `ConnectionCreds::sigv4_signing_region`) | spec deltas `vs-adapter/connection-credentials` (guard scenario, acceptance and no-leak clauses, vending scenario), `e2e-harness/cloud-e2e-harness`, and `vs-adapter/pushdown-planning-cloud-credentials` (Background, for task 4.1); `crates/lakehouse-engine/src/adapter/connection.rs`, `crates/lakehouse-engine/src/adapter/connection_tests.rs`, `docs/catalogs.md`, `crates/lakehouse-engine/tests/cloud_e2e_test.rs` |

Both groups cite `vs-adapter/connection-credentials`, so they run in sequence, not in parallel. They share no source file. Group A holds the catalog-side mental model: Glue endpoint rules, `CatalogAuth`, and the two signing paths. Group B holds the CONNECTION-delivery model: validation order, error text, the docs, and the cloud harness. Group B starts from A's finished method signature.

Only task 1.2 carries `[expert]`, so group A routes to the expert implementer and group B does not. Task 1.2 is correctness-sensitive: the host rule must reject userinfo, trailing-dot, FIPS, dual-stack, and GovCloud forms that naive string matching accepts.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | No function, test, or module becomes obsolete. The two `&creds.region` arguments to `sign_request` are replaced in place (tasks 2.2 and 2.3). |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| connection-credentials: When SigV4 is enabled, access_key, secret_key, and a signing region are required | Integration | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `sigv4_requires_access_secret_region`, `sigv4_region_required_for_non_standard_glue_hosts` |
| connection-credentials: A standard AWS Glue endpoint supplies the SigV4 signing region when the CONNECTION omits region (acceptance, no write-back) | Integration | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `sigv4_region_derived_from_standard_glue_endpoint_is_accepted` |
| connection-credentials: A standard AWS Glue endpoint supplies ... (host rule) | Unit | `crates/lakehouse-catalog/src/sigv4_tests.rs` | `signing_region_derived_from_standard_glue_endpoint`, `signing_region_absent_for_non_standard_hosts` |
| connection-credentials: A standard AWS Glue endpoint supplies ... (`loadTable` path signs for the derived region) | Integration | `crates/lakehouse-catalog/src/auth_tests.rs`, `crates/lakehouse-catalog/src/iceberg_io_tests.rs` | `sigv4_auth_carries_region_derived_from_glue_endpoint`, `sigv4_request_is_signed_for_the_carried_region` |
| connection-credentials: A standard AWS Glue endpoint supplies ... (namespace enumeration signs for the derived region) | Integration | `crates/lakehouse-catalog/src/namespace_tests.rs` | `signed_enumeration_is_signed_for_the_resolved_region` |
| connection-credentials: A standard AWS Glue endpoint supplies ... (real Glue accepts both signatures) | Integration (live, opt-in) | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_sigv4_region_derived_from_glue_endpoint_lists_table` |
| connection-credentials: A standard AWS Glue endpoint signs the catalog request even when the CONNECTION states a different region | Unit + Integration | `crates/lakehouse-catalog/src/sigv4_tests.rs`, `crates/lakehouse-catalog/src/auth_tests.rs`, `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `signing_region_derived_from_standard_glue_endpoint` (differing-stated-region case), `sigv4_auth_derives_region_even_when_a_different_region_is_stated`, `sigv4_cross_region_glue_and_s3_is_supported`, `sigv4_stated_region_used_for_non_standard_endpoint` |
| connection-credentials: Static storage credentials are ignored, not rejected, when vending is requested | Integration | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `static_storage_fields_with_vending_are_accepted_and_unused` (existing), `sigv4_derived_region_places_no_store_under_vending` |
| catalog-crate-public-surface-extensions: The SigV4 signing-region resolver is a public method of the shared credential type (reachability, crate-private steps, no delivery mechanism) | Integration | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `connection_creds_sigv4_signing_region_is_reachable`, `signing_region_steps_are_not_public`, `demoted_and_deleted_functions_are_not_declared_public` (existing, unweakened) |
| catalog-crate-public-surface-extensions: The SigV4 signing-region resolver ... (refusal on both signing paths) | Integration | `crates/lakehouse-catalog/src/auth_tests.rs`, `crates/lakehouse-catalog/src/namespace_tests.rs` | `sigv4_auth_refuses_without_signing_region`, `signed_enumeration_refuses_without_signing_region` |
| catalog-crate-public-surface-extensions: The SigV4 signing-region resolver ... (one-way dependency) | Integration | `crates/lakehouse-catalog/tests/catalog_crate_boundary.rs` | `catalog_manifest_declares_no_execution_engine_dependency` (existing) |
| cloud-e2e-harness: Vended credentials are exercised end to end against Glue | Integration (live, opt-in) | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_scan_reads_with_vended_credentials`, `cloud_glue_vends_the_s3_key_pair_for_the_table_location` (existing, unchanged) |
| cloud-e2e-harness: A Glue CONNECTION that omits region lists the Glue table through SigV4-signed catalog requests | Integration (live, opt-in) | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_sigv4_region_derived_from_glue_endpoint_lists_table` |

### Manual Testing

The local steps use the Docker Exasol that `make test-e2e` provisions. That run installs the adapter script `LHVS.LAKEHOUSE_ADAPTER`. Set `DSN="exasol://sys:$EXASOL_SYS_PASSWORD@$EXASOL_HOST:$LH_EXASOL_PORT?validateservercertificate=0"`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/connection-credentials (non-standard host keeps the requirement) | `exapump sql "CREATE OR REPLACE CONNECTION GLUE_VPCE TO 'https://vpce-0abc123.glue.eu-west-1.vpce.amazonaws.com/iceberg' IDENTIFIED BY '{\"warehouse\":\"123456789012\",\"access_key\":\"AKIDEXAMPLE\",\"secret_key\":\"EXAMPLEKEY\",\"use_sigv4\":true}'" -d "$DSN"`, then `exapump sql "CREATE VIRTUAL SCHEMA MANUAL_VPCE USING LHVS.LAKEHOUSE_ADAPTER WITH CATALOG_CONNECTION = 'GLUE_VPCE' NAMESPACE = 'db'" -d "$DSN"` | The statement fails. The message contains `missing field(s) required when SigV4 signing is enabled: region` and `https://glue.<region>.amazonaws.com`. It contains neither `AKIDEXAMPLE` nor `EXAMPLEKEY`. |
| vs-adapter/connection-credentials (standard host passes the guard) | The same two commands with the address `https://glue.eu-west-1.amazonaws.com/iceberg`, CONNECTION `GLUE_STD`, and schema `MANUAL_STD` | The statement fails with a catalog HTTP error from Glue (HTTP 403 for the example keys), or with a transport error when the container has no internet egress. The message does not contain `missing field(s)`. |
| e2e-harness/cloud-e2e-harness | With `GLUE_CATALOG_URI=https://glue.<AWS_REGION>.amazonaws.com/iceberg` and the other suite variables set: `cargo test -p lakehouse-engine --features cloud-e2e --test cloud_e2e_test cloud_sigv4_region_derived_from_glue_endpoint_lists_table -- --nocapture` | The test passes and prints the listed table name. The output contains no credential value. |
| vs-adapter/pushdown-planning-cloud-credentials (region-less vended Glue placement depends on the vended `client.region`) | With the suite variables set: `cargo test -p lakehouse-engine --features cloud-e2e --test cloud_e2e_test cloud_glue_vends_the_s3_key_pair_for_the_table_location -- --nocapture` | The test passes. Its report line states whether `client.region` is present in Glue's vended credential source. The output contains no credential value. |
| vs-adapter/catalog-crate-public-surface-extensions | `cargo test -p lakehouse-catalog --test catalog_public_surface` | All probe tests pass, including `connection_creds_sigv4_signing_region_is_reachable` and `signing_region_steps_are_not_public`. |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures |
| Cloud E2E (opt-in, live AWS) | `cargo test -p lakehouse-engine --features cloud-e2e --test cloud_e2e_test -- --test-threads=1` | 0 failures, or a clean skip when the AWS variables are absent |
| Lint | `cargo clippy --all-targets` | 0 errors, 0 warnings |
| Format | `cargo fmt --check` | No changes |
