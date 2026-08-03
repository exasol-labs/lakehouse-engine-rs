# Decisions: add-azure-static-storage-backend

## ADR: A CONNECTION mixing Azure and static S3 credential fields is rejected

**ID:** azure-s3-mixed-credential-fields-rejected
**Plan:** `add-azure-static-storage-backend`
**Status:** Accepted

### Context

The Azure backend is selected purely from which credential fields a CONNECTION supplies — there is no explicit `backend` field. That leaves open what happens when a CONNECTION supplies fields from both credential shapes at once: at least one of `account_name`/`account_key`/`sas_token` together with at least one of `endpoint`/`region`/`access_key`/`secret_key`/`session_token`. A credentials path must not resolve an ambiguous input silently.

### Decision

`validate_creds` errors when any Azure field is supplied together with any static S3 field. The error names the supplied field names on both sides and no values.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reject the mixed CONNECTION | ✓ Chosen — an ambiguous credentials input is never resolved silently |
| Declare a precedence (Azure wins, or S3 wins) | ✗ Rejected — an undeclared precedence between two credential sets is exactly the silent misconfiguration this feature exists to prevent |
| Accept and silently ignore the unused field set | ✗ Rejected — same defect as an undeclared precedence, with no error at all |

### Consequences

An S3-only deployment can never trigger this error, since it supplies no Azure field. The rule is the one case issue #275's text does not name, and it settles the credential-shape ambiguity that any-of-three field triggering leaves open.

---

## ADR: `AdlsCred` makes "exactly one credential" unrepresentable rather than merely validated

**ID:** adlscred-exactly-one-credential-type
**Plan:** `add-azure-static-storage-backend`
**Status:** Accepted

### Context

`object_store`'s Azure builder silently prefers an access key over a SAS token when both are configured (`object_store-0.13.2/src/azure/builder.rs:990` vs `:1021`). A credential type that can hold both values would make that silent precedence reachable from a contradictory CONNECTION.

### Decision

`AdlsCred` is an enum with exactly two states — an account-key state and a SAS-token state — and no "both" or "neither" state.

### Options Considered

| Option | Verdict |
|--------|---------|
| Two-state enum, no "both" state | ✓ Chosen — a contradictory credential set is unrepresentable rather than merely rejected at a boundary check |
| Two `Option<String>` fields, validated at the boundary | ✗ Rejected — leaves `object_store`'s silent access-key-beats-SAS precedence reachable if the boundary check is ever bypassed or missed at a new call site |

### Consequences

`validate_creds` is the single place a contradictory credential set is reported; `object_store`'s precedence between the two credential kinds can never be reached because the type cannot hold both.

---

## ADR: `AdlsCred` implements a manual redacting `Debug`

**ID:** adlscred-manual-redacting-debug
**Plan:** `add-azure-static-storage-backend`
**Status:** Accepted

### Context

`ConnectionCreds` already hand-implements `Debug` to mask `secret_key`, `session_token`, `token`, and `client_secret`. `StorageProps`, by contrast, derives `Debug` and prints `secret_key` in the clear — a known gap tracked as issue #135. `AdlsCred` carries a new secret (an account key or a SAS token) and needs a `Debug` decision of its own.

### Decision

`AdlsCred` implements `Debug` manually so the wrapped secret is replaced by a redaction marker in both states; `account_name` stays visible since it is not a secret.

### Options Considered

| Option | Verdict |
|--------|---------|
| Manual redacting `Debug` | ✓ Chosen — six lines on a security path; leaves issue #135 strictly less to fix |
| Derive `Debug`, matching `StorageProps`' plaintext `secret_key` | ✗ Rejected — matching an existing leak would add a new leak on the grounds that an old one exists |

### Consequences

The asymmetry with `StorageProps` (which still derives `Debug` plaintext) is deliberate and named in the spec rather than left for a future reader to discover and question.

---

## ADR: The container collision is closed by a backend-agnostic whole-spec precondition

**ID:** azure-container-collision-whole-spec-precondition
**Plan:** `add-azure-static-storage-backend`
**Status:** Accepted

### Context

DataFusion's object-store registry key (`get_url_key`, scheme + `host:port`, excluding userinfo) names an Azure storage ACCOUNT, while `MicrosoftAzureBuilder::with_url` builds a store scoped to one CONTAINER (the userinfo). Two containers of one account collapse onto one registered store; without a guard, a broadcast join's dimension side could be read out of the fact side's container with no error.

### Decision

`validate_sides_share_one_store(spec)` runs once in `build_session_context`, before any registration. Per non-empty side it compares DataFusion's registry key against the store URL the side actually needs (scheme + userinfo + `host:port`), and errors naming both conflicting store URLs when two sides share the former and differ in the latter. It matches on no storage-backend variant.

### Options Considered

| Option | Verdict |
|--------|---------|
| Whole-spec precondition, checked once, backend-agnostic | ✓ Chosen — states the actual invariant (a property of DataFusion's registry-key formula) once, where both sides of a pair are visible |
| Check inside the Azure arm of `register_side_store` against the already-registered store | ✗ Rejected — the arm sees only one side; recovering a container from a registered `Arc<dyn ObjectStore>` means string-matching its `Display` impl, which is fragile |
| Carry the other sides on `StoreRegistration`, or compare the two returned `Url`s at the call site | ✗ Rejected — would make `build_session_context` name a container, which `vs-adapter/storage-backend-enum` forbids |

### Consequences

The check can never fire for an S3 spec, since an `s3://` URI carries no userinfo and the two notions coincide. Stating the invariant as a property of DataFusion's registry-key formula rather than of Azure specifically keeps it true for any future backend whose store scope is finer than its registry key.

---

## ADR: `storage_block` stays total — no panic, no `Result`, on the unreachable both-absent Azure state

**ID:** storage-block-total-fallback-no-panic
**Plan:** `add-azure-static-storage-backend`
**Status:** Accepted

### Context

`storage_block` selects the Azure branch when an Azure field is present, requiring both an account name and a resolvable `AdlsCred`. `validate_creds` runs before `storage_block` and already rejects every malformed shape, so the both-absent case cannot occur in production. CLAUDE.md records that a panic inside a UDF is an abnormal VM exit, and that the engine SIGKILLs every sibling VM of the statement part when one VM dies abnormally.

### Decision

When the Azure branch's required fields are unexpectedly absent, `storage_block` falls through to the S3 branch rather than panicking; the function's return type stays a plain `StorageBackend`, not a `Result`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Deterministic fall-through to S3 | ✓ Chosen — costs nothing, and a state that cannot occur in production never risks a cluster-wide SIGKILL fan-out from a defensive assertion |
| `unreachable!()`, justified by `validate_creds` running first | ✗ Rejected — a panic here is an abnormal VM exit; CLAUDE.md records that this kills every sibling VM of the statement part, not just this one |
| Change the signature to `Result` | ✗ Rejected — pushes a new error path through the one caller for a state that cannot occur |

### Consequences

`storage_block` remains a total function. The unreachable branch is defensive dead code with a guaranteed-safe behavior rather than a landmine, at zero cost to the reachable paths.
