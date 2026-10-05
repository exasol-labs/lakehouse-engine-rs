# Decisions: add-azure-static-storage-backend

## ADR: A CONNECTION mixing Azure and static S3 credential fields is rejected

**ID:** azure-s3-mixed-credential-fields-rejected
**Plan:** `add-azure-static-storage-backend`
**Status:** Accepted

### Context

A CONNECTION selects the Azure backend only by which credential fields it supplies, with no explicit `backend` field. A CONNECTION could supply fields from both shapes, and a credentials path must not resolve that silently.

### Decision

Credential validation errors when any Azure field (`account_name`, `account_key`, `sas_token`) appears with any static S3 field (`endpoint`, `region`, `access_key`, `secret_key`, `session_token`). The error names the supplied fields on both sides and no values.

### Options Considered

| Option | Verdict |
|--------|---------|
| Declare a precedence between the two sets | Rejected: an undeclared precedence is the silent misconfiguration this feature prevents |
| Accept and ignore the unused set | Rejected: the same defect with no error |

### Consequences

An S3-only deployment never triggers the error. The rule covers the one case issue #275 does not name.

## ADR: `AdlsCred` makes "exactly one credential" unrepresentable rather than merely validated

**ID:** adlscred-exactly-one-credential-type
**Plan:** `add-azure-static-storage-backend`
**Status:** Accepted

### Context

The `object_store` Azure builder silently prefers an access key over a SAS token when both are set. A credential type that can hold both makes that precedence reachable.

### Decision

`AdlsCred` is an enum with two states, account key and SAS token, and no "both" or "neither" state.

### Options Considered

| Option | Verdict |
|--------|---------|
| Two `Option<String>` fields checked at the boundary | Rejected: the silent key-beats-SAS precedence stays reachable if the check is bypassed or missed at a new call site |

### Consequences

Credential validation is the only place a contradictory set is reported, and the builder's precedence cannot be reached.

## ADR: `AdlsCred` implements a manual redacting `Debug`

**ID:** adlscred-manual-redacting-debug
**Plan:** `add-azure-static-storage-backend`
**Status:** Accepted

### Context

`ConnectionCreds` hand-implements `Debug` to mask its secrets. `StorageProps` derives `Debug` and prints `secret_key` in the clear (issue #135). `AdlsCred` carries a new secret and needs its own decision.

### Decision

`AdlsCred` implements `Debug` manually and replaces the secret with a redaction marker in both states. `account_name` stays visible.

### Options Considered

| Option | Verdict |
|--------|---------|
| Derive `Debug`, as `StorageProps` does | Rejected: would add a new leak because an old one exists |

### Consequences

The asymmetry with `StorageProps` is deliberate and named in the spec.

## ADR: The container collision is closed by a backend-agnostic whole-spec precondition

**ID:** azure-container-collision-whole-spec-precondition
**Plan:** `add-azure-static-storage-backend`
**Status:** Accepted

### Context

DataFusion's object-store registry key (scheme plus `host:port`, excluding userinfo) names an Azure storage account, while the Azure store is scoped to one container (the userinfo). Two containers of one account share one registered store, so a broadcast join could read its dimension side from the fact side's container with no error.

### Decision

A check runs once before any registration. For each non-empty side, it compares the registry key with the store URL the side needs, and errors naming both URLs when two sides share the key but differ in the URL. It matches on no backend variant.

### Options Considered

| Option | Verdict |
|--------|---------|
| Check inside the Azure arm of store registration | Rejected: the arm sees one side, and recovering a container from a registered store means string-matching its `Display` output |
| Carry the other sides on `StoreRegistration`, or compare returned URLs at the call site | Rejected: makes `build_session_context` name a container, which `vs-adapter/storage-backend-enum` forbids |

### Consequences

The check never fires for S3, since an `s3://` URI has no userinfo. Stating it as a property of the registry-key formula keeps it valid for any future backend whose store scope is finer than its registry key.
