# Decisions: change-vended-storage-resolution-scheme-driven

## ADR: Two selectors on disjoint inputs, not one selection site

**ID:** two-vended-static-selectors-on-disjoint-inputs
**Plan:** `change-vended-storage-resolution-scheme-driven`
**Status:** Accepted
**Supersedes:** storage-backend-exhaustive-variant-naming-owners

### Context

`vs-adapter/storage-backend-enum` recorded `storage_block` as the ONLY place a backend is selected from input. Once the vended arm must select its variant from the table location's URI scheme rather than from the CONNECTION credential shape, that clause becomes technically true and factually misleading: a second input now drives a second selection. A single selector could only be kept by deferring CONNECTION parsing past `loadTable` for every request, including the non-vended path, and would still have no answer for a request that resolves no table.

### Decision

`storage_block` stays the STATIC selector, reading the CONNECTION credential shape and running only when `use_vended_credentials` is false. `resolve_vended_storage` becomes the VENDED selector, reading the `loadTable` response's table location scheme and running only when vending is true. Exactly one site — the `use_vended_credentials` branch in `resolve_file_list` — chooses between them, so the count of decision points stays one even though the count of selectors is two.

### Options Considered

| Option | Verdict |
|--------|---------|
| Two selectors on disjoint inputs, one decision point | ✓ Chosen — neither selector reads the other's input, neither can override the other, and there is no path on which both run |
| Keep one selector by passing the URI scheme into `storage_block` | ✗ Rejected — the scheme is known only after `loadTable`, which runs later, once per table, and never at all on the `createVirtualSchema` path |
| Have the vended arm mutate the payload `storage_block` returned | ✗ Rejected by the feature's own existing clause: reaching into the payload to finish construction is exactly the knowledge this feature removes |
| Declare the scheme switch "not really a selection" and leave the one-site clause intact | ✗ Rejected — leaving a fence textually intact and factually false is worse on a credentials path than superseding it |

### Consequences

`vs-adapter/storage-backend-enum`'s "ONLY place a backend is SELECTED FROM INPUT" clause is superseded by an "EXACTLY TWO selectors on disjoint inputs, ONE decision point" clause. Naming both selectors and bounding each is honest; collapsing them would require deferring CONNECTION parsing past table load for every request.

---

## ADR: Delete the `base: &StorageBackend` parameter

**ID:** resolve-vended-storage-drops-base-backend-parameter
**Plan:** `change-vended-storage-resolution-scheme-driven`
**Status:** Accepted

### Context

`resolve_vended_storage` took a `base: &StorageBackend` and overlaid vended values field-by-field onto the payload `storage_block` had already chosen — a second selector reading the first's output, and the source of six per-field absence-and-preservation conventions.

### Decision

`resolve_vended_storage(result, anchor, allow_http) -> Result<StorageBackend, UdfError>`. It takes no `StorageBackend`, no `ConnectionCreds`, and no other CONNECTION-derived value. The `allow_http` parameter is a virtual-schema property, not a CONNECTION field, so it does not reopen what this deletion closes.

### Options Considered

| Option | Verdict |
|--------|---------|
| Delete the `base` parameter entirely | ✓ Chosen — "no CONNECTION storage field is read under vending" becomes a property of the signature, not a rule an auditor verifies by reading the body |
| Keep `base` and read only `allow_http` from it | ✗ Rejected — leaves one CONNECTION-derived read under vending, the exact coupling the rule removes |
| Add a `StorageBackend::allow_http()` accessor and keep `base` | ✗ Rejected — has to answer `allow_http` for an `Adls` base whose account credentials are irrelevant, a question with no meaningful answer |

### Consequences

The interface gets narrower while absorbing more work: a whole `StorageBackend` parameter is exchanged for one boolean, six per-field absence conventions drop to none, and variant selection previously made by the caller's caller moves inside. No future edit can quietly reintroduce a per-field preservation rule.

---

## ADR: `ALLOW_HTTP` stays the operator's consent gate for plaintext transport

**ID:** allow-http-threaded-as-vended-selector-parameter
**Plan:** `change-vended-storage-resolution-scheme-driven`
**Status:** Accepted

### Context

An earlier draft derived `allow_http` from the vended endpoint's scheme, on the theory that this was "strictly narrower" than the shipped behaviour. `crates/lakehouse-engine/src/adapter/mod.rs:190` defaults `allow_http` to false when the `ALLOW_HTTP` property is absent, so the shipped rule permits plaintext to NO endpoint; a scheme-derived value would have permitted it to any endpoint a catalog names as `http://`. A misconfigured or compromised catalog could then vend an `http://` endpoint and put STS credentials in cleartext with no operator control and no error.

### Decision

Thread the resolved `ALLOW_HTTP` virtual-schema property into `resolve_vended_storage` as its own `bool` parameter. A vended plain-`http://` `s3.endpoint`, and an `abfs://` anchor, are honoured only when it is true; otherwise return a `UdfError::User` naming the plaintext scheme and the `ALLOW_HTTP` property.

### Options Considered

| Option | Verdict |
|--------|---------|
| Thread `ALLOW_HTTP` as its own resolved parameter | ✓ Chosen — resolved once outside the catalog crate, consumed only there, and does not reopen the CONNECTION-read rule since it is a VS property, not a CONNECTION field |
| Derive `allow_http` from the vended endpoint's scheme | ✗ Rejected — a security regression in the default configuration; the claim it was "strictly narrower" held only when `ALLOW_HTTP` was already true, and is withdrawn |
| Keep reading it off the `base` backend, or add a `StorageBackend::allow_http()` accessor | ✗ Rejected — both reopen the exact CONNECTION-derived read decision [2] closes |

### Consequences

Threading the value costs a 4-tuple on `resolve_connection_config` (2 call sites) and one `bool` parameter on four functions, following the convention already established for `s3_max_connections` and the DataFusion tuning knobs. Removing `ALLOW_HTTP` from the vended path in the earlier draft was a planner decision, not an interview outcome — the interview's subject was the CONNECTION, and `allow_http` is not a CONNECTION field.

---

## ADR: A vended payload naming neither a region nor an endpoint is an error

**ID:** vended-s3-requires-region-or-endpoint-else-error
**Plan:** `change-vended-storage-resolution-scheme-driven`
**Status:** Accepted

### Context

Once the CONNECTION can no longer backfill an absent vended value, `client.region` and `s3.endpoint` are the only two values that can place an S3 store. Leaving both empty would address an AWS store as a region-less URL, silently.

### Decision

The S3 arm requires a non-empty `client.region` OR a non-empty `s3.endpoint`, else it returns a `UdfError::User` naming both keys.

### Options Considered

| Option | Verdict |
|--------|---------|
| Require at least one of region or endpoint | ✓ Chosen — a property of the payload alone; the live-verified Lakekeeper vended config satisfies it through the endpoint alone and needs no region |
| Leave an absent region empty and let the object-store builder do whatever it does | ✗ Rejected — the silent failure this whole change exists to remove |
| Require `client.region` unconditionally | ✗ Rejected — would break the Lakekeeper vended path, which places its store by endpoint, not region |
| Condition the requirement on `s3.path-style-access`, mirroring what `register_side_store` consumes | ✗ Rejected — encodes the engine's builder logic inside the catalog crate |
| Narrow the error to only an AWS-hosted `s3://` URI | ✗ Rejected — turns the rule on recognising "an AWS-hosted URI", a host-pattern test the catalog crate cannot state without encoding AWS endpoint conventions, and a wrong answer fails silently in exactly the direction the rule exists to prevent |

### Consequences

This is a rule the interview did not ask for; it extends the interview's "requested but not satisfied is an error" principle from the keys to the address. It is the reason task 4.2 (the Glue vended-payload assertions) exists — whether AWS Glue's vended response actually carries `client.region` was unverified in the planning environment and is carried as a blocking verification obligation.

---

## ADR: Per-side scheme selection needs a plan-time join guard scoped to variant and account, not full backend equality

**ID:** join-backend-guard-scoped-to-variant-and-account
**Plan:** `change-vended-storage-resolution-scheme-driven`
**Status:** Accepted

### Context

`resolve_one_join_side` resolves each side's own `effective_storage`, but `join_fan_out_scan_spec` keeps only `primary.effective_storage` as the spec's single `CommonScanSpec.storage`, and `StoreRegistration.backend` is a whole-spec value shared by every side's registration call. That collapse was variant-safe only because both sides previously took their variant from one `storage_block` output. Once the variant depends on each side's own table location, an `s3://` fact joined to an `abfss://` dimension would run the S3 arm over the dimension's Azure files, and two `abfss://` sides on different accounts would read one through the other's account and SAS. `validate_sides_share_one_store` cannot catch either case, because it fires only when two sides share a registry key, and different schemes produce different keys.

### Decision

Add `validate_sides_share_one_backend(sides: &[ResolvedJoinSide]) -> Result<(), UdfError>`, a pure function in `joins/planning.rs` beside `select_broadcast_sides`, called from `plan_join` immediately after the per-side resolution loop and before the empty-side shortcut. It compares every side's `effective_storage` against the first side's — the variant, and for `Adls` the `account_name` — and errors naming the differing variants and accounts, with no credential value. It is deliberately scoped to variant and account only, not full backend equality.

### Options Considered

| Option | Verdict |
|--------|---------|
| Pure function scoped to variant + account, called from `plan_join` | ✓ Chosen — unit-testable without a live catalog, using the existing `resolved_side`/`sample_storage` fixtures, matching `select_broadcast_sides`'s own established pattern |
| Guard inline in `plan_join` | ✗ Rejected — `plan_join` reaches the divergent backends only by awaiting `resolve_one_join_side`'s live catalog I/O per side, so the two backends are outputs of that I/O and no unit test could supply them; the guard would ship with no falsifiable gate |
| Guard on full backend equality (including per-prefix vended credentials) | ✗ Rejected — could break every vended join against a catalog minting per-table STS keys, which is unverified either way; the pre-existing per-prefix credential collapse is named as a separate, pre-existing defect and recommended as its own tracked issue rather than folded in here |
| Leave the collapse unguarded | ✗ Rejected — reads one side's files through another side's storage backend with no error |

### Consequences

A join whose sides all select the same backend is unaffected, keeping its current plan and byte-identical scan-driving SQL. The pre-existing per-prefix vended-credential collapse in `join_fan_out_scan_spec` is deliberately left alone and filed as issue #294, since widening the guard to full equality needs its own live verification.

---

## ADR: Deriving `allow_http` from the vended endpoint was a security regression, reversed

**ID:** allow-http-derivation-from-vended-scheme-reversed-as-regression
**Plan:** `change-vended-storage-resolution-scheme-driven`
**Status:** Accepted

### Context

The plan's original design derived `allow_http` from the vended endpoint's own scheme and described that as "strictly narrower" than the shipped behaviour. Plan review verified this false against `crates/lakehouse-engine/src/adapter/mod.rs:190-192`, which defaults `allow_http` to false when `ALLOW_HTTP` is absent — the secure default. Under the shipped rule that default permits plaintext to no endpoint; the derivation would have permitted it to any endpoint a catalog names as `http://`, letting a misconfigured or compromised catalog put vended STS credentials in cleartext with no operator control and no error. The `abfs://` half of the scheme mapping compounded this, since the Azure backend carries no HTTP-scheme knob to gate it at all.

### Decision

Reverse the derivation. `ALLOW_HTTP` is threaded in as its own `bool` parameter — a virtual-schema property, not a CONNECTION field — gating both a vended plain-`http://` endpoint and an `abfs://` anchor, erroring otherwise.

### Options Considered

| Option | Verdict |
|--------|---------|
| Thread `ALLOW_HTTP` in as its own resolved parameter | ✓ Chosen — restores operator consent as the gate, in both configurations |
| Derive `allow_http` from the vended endpoint's scheme (original plan) | ✗ Rejected — a security regression in the default configuration; the "strictly narrower" claim held only when `ALLOW_HTTP` was already true |

### Consequences

Every consequence of the original derivation had to be corrected, not only the central decision: the spec Background, the § Test Disposition table, and § Impact all previously described or prescribed the derivation and were rewritten to match the reversal, since a fence left textually intact and factually false is worse on a credentials path than one openly superseded.
