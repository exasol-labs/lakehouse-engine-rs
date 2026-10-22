# Decisions: add-azure-static-storage-backend

## ADR: A CONNECTION mixing Azure and static S3 credential fields is rejected

**ID:** azure-s3-mixed-credential-fields-rejected
**Plan:** `add-azure-static-storage-backend`
**Status:** Accepted

### Context

A CONNECTION selects the Azure backend only by which credential fields it supplies. A CONNECTION that supplies fields from both Azure and S3 must not resolve silently.

### Decision

Credential validation errors when any Azure field appears with any static S3 field. The error names the supplied fields on both sides and no values.

### Options Considered

| Option | Verdict |
|--------|---------|
| Declare a precedence between the two sets | Rejected: a hidden precedence is the silent misconfiguration this rule prevents |
| Accept and ignore the unused set | Rejected: same silent misconfiguration |

### Consequences

- An S3-only deployment never triggers the error.

## ADR: `AdlsCred` makes "exactly one credential" unrepresentable rather than merely validated

**ID:** adlscred-exactly-one-credential-type
**Plan:** `add-azure-static-storage-backend`
**Status:** Accepted

### Context

The Azure store builder silently prefers an account key over a SAS token when both are set. A credential type that can hold both makes that precedence reachable.

### Decision

The Azure credential type is an enum with two states, account key and SAS token, and no "both" or "neither" state.

### Options Considered

| Option | Verdict |
|--------|---------|
| Two `Option<String>` fields checked at the boundary | Rejected: the silent key-over-SAS precedence stays reachable if a new call site skips the check |

### Consequences

- A contradictory credential set cannot be built, so the builder's precedence is unreachable.

## ADR: The container collision is closed by a backend-agnostic whole-spec precondition

**ID:** azure-container-collision-whole-spec-precondition
**Plan:** `add-azure-static-storage-backend`
**Status:** Accepted

### Context

DataFusion's object-store registry key (scheme plus host and port, excluding userinfo) names an Azure storage account, while the Azure store is scoped to one container. Two containers of one account share one registered store, so a broadcast join could read one side from the other side's container with no error.

### Decision

A check runs once before any store registration. It errors, naming both URLs, when two sides share a registry key but need different store URLs. It matches on no backend variant.

### Options Considered

| Option | Verdict |
|--------|---------|
| Check inside the Azure arm of store registration | Rejected: the arm sees one side, and recovering a container from a registered store means string-matching its display output |
| Carry the other sides on `StoreRegistration`, or compare returned URLs at the call site | Rejected: makes session setup name a container, which the storage-backend spec forbids |

### Consequences

- The check never fires for S3, since an `s3://` URI has no userinfo.
- It stays valid for any future backend whose store scope is finer than its registry key.
