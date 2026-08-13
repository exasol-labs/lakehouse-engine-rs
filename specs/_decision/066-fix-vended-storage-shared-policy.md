# Decisions: fix-vended-storage-shared-policy

## ADR: CONNECTION-wins-when-set addressing, resolved per field

**ID:** connection-wins-when-set-vended-addressing
**Plan:** fix-vended-storage-shared-policy
**Status:** Accepted

### Context

Issue #330 reported two defects in the vended-storage path: the Unity Catalog vended selector accepted a plaintext `abfs://` location with no operator consent, and the Iceberg vended selector rejected a legal Databricks AWS response that vends short-lived credentials with no endpoint and no region field at all — because the shipped rule required at least one of the two to place the store. Fixing the second defect required deciding, when both the vended response and the CONNECTION carry a non-empty endpoint or region, which one wins.

### Decision

Resolve the vended S3 store address per field, independently: `endpoint` from `ConnectionCreds.endpoint` when non-empty, else the vended `s3.endpoint`, else empty; `region` from `ConnectionCreds.region` when non-empty, else the vended `client.region`, else empty. A backend with both empty is returned successfully and resolves through the AWS default chain. The plaintext-transport consent gate applies to the RESOLVED endpoint regardless of which source supplied it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Resolve `endpoint` and `region` per field, CONNECTION wins when non-empty | ✓ Chosen — an operator who configured a store address means it, and a per-field rule loses no information |
| Vended-wins-when-present | ✗ Rejected — the interview overruled this reading of the issue's first clause |
| Whole-source precedence (one source wins both fields together) | ✗ Rejected — a CONNECTION stating only a region beside a response stating only an endpoint would discard one usable value for no reason |

### Consequences

Making the consent gate follow the resolved endpoint rather than its origin keeps the rule a statement about the transport the store will actually use, which is the only thing plaintext consent is about. A deployment setting `use_vended_credentials: true` alongside a non-empty CONNECTION `endpoint` or `region` changes behaviour: the CONNECTION value now wins where the vended one used to be discarded.

## ADR: path_style does not read the CONNECTION, because the field cannot express "unstated"

**ID:** path-style-connection-read-excluded-type-limitation
**Plan:** fix-vended-storage-shared-policy
**Status:** Accepted

### Context

The CONNECTION-wins-when-set addressing rule for `endpoint` and `region` left open whether `ConnectionCreds.path_style` should participate the same way, and how it composes with the existing vended-only `s3.path-style-access` override key.

### Decision

`path_style` = the vended `s3.path-style-access` when the response states a parseable boolean, else whether an endpoint was RESOLVED at all (the CONNECTION's when it won, the vended one otherwise). `ConnectionCreds.path_style` does NOT participate in the CONNECTION-wins rule and is not passed to the vended selectors.

### Options Considered

| Option | Verdict |
|--------|---------|
| Derive `path_style` from the vended value or the resolved-endpoint fallback; exclude `ConnectionCreds.path_style` | ✓ Chosen — the derivation is load-bearing: `register_side_store` treats `path_style` as the gate on whether `endpoint` reaches `AmazonS3Builder` at all |
| Admit `ConnectionCreds.path_style` under the CONNECTION-wins rule | ✗ Rejected — it is a plain `bool` defaulting to `true` and discards whether the key was present, so it cannot distinguish "the operator set false" from "the operator said nothing"; admitting it would make the vended override unreachable on every CONNECTION that omits the key |
| Widen `ConnectionCreds.path_style` to `Option<bool>` | ✗ Rejected — ripples into the static `storage_block` path, whose `true` default is shipped behaviour this plan must not change, and into every `ConnectionCreds` literal in the suites, for a knob whose vended derivation already has a correct answer |

### Consequences

The composition question resolves on a type limitation, not a preference, which stops the next reader re-opening it as an oversight. `path_style` keeps its shipped derivation on the vending path; the non-vended `storage_block` path is unaffected.

## ADR: Extraction stays forked; policy does not

**ID:** vended-extraction-forked-policy-shared
**Plan:** fix-vended-storage-shared-policy
**Status:** Accepted

### Context

The Iceberg REST and Unity Catalog vended selectors resolve the same output from different wire inputs. Only scheme classification was shared between them; every policy step after it — the plaintext consent gates and the S3 store-address rule — was copied. The copy stayed consistent in five places and diverged in two: the Unity ADLS arm took no `allow_http` parameter at all, so `abfs://` was always silently accepted over HTTPS, and only the Iceberg selector enforced the store-address rule.

### Decision

Record the principle in the spec library, not only in this plan: a catalog kind MAY fork on HOW a value is read off the wire; it MUST NOT fork on WHAT makes the resulting value acceptable. Consent gates, address rules, and target-variant decisions get exactly one home both kinds call. The per-catalog wire extraction (`select_credential_source` plus flat-map reads for Iceberg REST; typed `TemporaryTableCredentials` field reads for Unity) stays forked because the two wire shapes genuinely differ.

### Options Considered

| Option | Verdict |
|--------|---------|
| One shared home for consent gates, address rules, and target-variant decisions; wire extraction stays per-kind | ✓ Chosen — names the seam so the next divergence is a build or test failure instead of a silent gap |
| Fix the two defects in place in `unity/vended.rs` | ✗ Rejected — re-copies the gate rather than removing the seam that lost it; defect 1 exists precisely because a copy was incomplete |
| Unify the wire extraction too, behind a trait | ✗ Rejected as a shallow abstraction — a flat `HashMap` with longest-prefix credential-source selection and a typed three-family response have nothing in common above the neutral values they produce |

### Consequences

The next catalog kind's author reads the rule where it lives rather than re-deriving it. Both defects came from a copy that stayed consistent in five places and diverged in two; naming the seam is what makes the sixth and seventh divergence a build or test failure instead of a silent gap.

## ADR: Addressing arrives as a capability-narrowed type, and the credential guarantee moves from the signature to a probe

**ID:** static-store-address-capability-narrowed-type
**Plan:** fix-vended-storage-shared-policy
**Status:** Accepted

### Context

Before this plan, `resolve_vended_storage` took no `StorageBackend`, `ConnectionCreds`, or other CONNECTION-derived value, so "no CONNECTION storage field is read under vending" was a property of the signature itself. The CONNECTION-wins-when-set addressing decision requires the vended selectors to take a CONNECTION-derived value after all, which would otherwise reopen that guarantee to a silent regression — a vended credential falling back to a static one.

### Decision

Add `pub struct StaticStoreAddress` carrying exactly two addressing fields, `endpoint` and `region`, with `Default` and exactly one `impl From<&ConnectionCreds>`, and pass `&StaticStoreAddress` to both vended selectors. Both fields are declared NON-`pub` and read through `pub fn endpoint(&self) -> &str` / `pub fn region(&self) -> &str`, so outside `storage.rs` the type admits only `Default` and that one conversion and a field-by-field literal does not compile. Two source-level probes back it: one asserting the struct's own declaration names no field spelled `access_key`, `secret_key`, `session_token`, `token`, `account_key`, `sas_token`, or `password`, and one asserting the declaration keeps both fields non-`pub`.

### Options Considered

| Option | Verdict |
|--------|---------|
| A narrow type with private fields, accessor reads, `Default`, and exactly one conversion, backed by two source probes | ✓ Chosen — the compiler checks every call site in both crates, closing the half a text probe cannot see |
| Pass `&ConnectionCreds` | ✗ Rejected outright — makes "a vended credential never falls back to a static one" unenforceable, the guarantee the plan must preserve |
| Pass two bare `&str` parameters | ✗ Rejected — two adjacent same-typed parameters let an endpoint/region transposition compile silently |
| Build the value at each call site | ✗ Rejected — puts "which CONNECTION fields may cross into vended resolution" in as many places as there are callers |
| Keep both fields `pub` and enforce the one-construction rule with a source-level probe forbidding a `StaticStoreAddress {` literal outside `storage.rs` | ✗ Rejected — a text probe can only see the sources it enumerates and can be defeated by formatting, and it must carve out `storage.rs` itself, the file whose literals matter least |

### Consequences

The superseded clauses carried the credential guarantee on the signature; superseding them without a replacement mechanism would have left the guarantee as prose. Field privacy closes the "no credential field, no field-by-field construction" half at the compiler; the accessors keep the reading side honest, since `s3_backend` reads the address through them rather than leaving them as dead public surface.

## ADR: The behaviour change is a breaking change and is named as one

**ID:** vended-addressing-precedence-is-breaking-change
**Plan:** fix-vended-storage-shared-policy
**Status:** Accepted

### Context

Fixing defect 2 (an empty vended address is now legal) required also deciding a precedence rule between a CONNECTION-configured store address and a vended one. That precedence direction changes behaviour for a deployment that combines `use_vended_credentials: true` with a non-empty CONNECTION `endpoint` or `region` — a configuration no in-repo fixture exercises, since both in-repo vended fixtures carry an empty CONNECTION `endpoint` and `region`.

### Decision

Record in `plan.md` § Impact that a deployment setting `use_vended_credentials: true` alongside a non-empty CONNECTION `endpoint` or `region` changes behaviour: the CONNECTION value now wins where the vended one used to. Also record that the new plaintext gate on a CONNECTION-supplied endpoint tightens an existing path.

### Options Considered

| Option | Verdict |
|--------|---------|
| Name the precedence change as a breaking change in § Impact and § Migration | ✓ Chosen — the regression is invisible to every in-repo suite, so it must be written down rather than discovered by an operator |
| Frame the whole change as a bug fix | ✗ Rejected — defect 2's fix is a bug fix, but the precedence direction the interview chose changes a shipped resolution on configurations that work today |

### Consequences

No in-repo suite can observe this transition, so the written record in `plan.md` § Impact and § Migration is the only place an operator or a future reader learns that a stale plaintext CONNECTION endpoint beside a vending-enabled CONNECTION now either wins (under `ALLOW_HTTP = true`) or fails loud at plan time (otherwise) instead of being silently discarded. `e2e-harness/lakekeeper-e2e-harness`'s delta promotes its empty-`endpoint` assertion to a stated precondition for the same reason.
