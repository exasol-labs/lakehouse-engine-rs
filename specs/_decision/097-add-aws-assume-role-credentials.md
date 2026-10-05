# Decisions: add-aws-assume-role-credentials

## ADR: The assumed role becomes the request's single AWS identity, substituted into the credential set once per request

**ID:** assumed-role-single-request-identity-substituted-into-creds
**Plan:** add-aws-assume-role-credentials
**Status:** Accepted

### Context

After validation, every reader of the key triple needs the identity that AWS requests are made as, and none needs the base identity once STS returns. The SigV4 signing region is separate, because `region` has a second reader for store placement (`derived-signing-region-separate-from-connection-region`). Validation and the sealing key read the CONNECTION before the substitution.

### Decision

Both adapter entry points call one `lakehouse-catalog` function, `resolve_aws_identity`, once per request inside the request's async runtime. It sends the STS `AssumeRole` request and returns the `ConnectionCreds` with the key pair and session token replaced by the session's. The adapter rebuilds the static storage backend from that set with `storage_block`, and every downstream reader reads the session without naming the role.

### Options Considered

| Option | Verdict |
|--------|---------|
| A parallel effective-identity type threaded to every reader | Rejected: every reader would learn about role assumption |
| The scan UDF calls STS per shard | Rejected: up to 300 calls per query |
| Resolve the identity only where a request needs AWS access | Rejected: two call patterns, and CREATE would no longer surface an STS misconfiguration |

### Consequences

SigV4 catalog signing, static storage, Iceberg manifest reads, Delta log reads, the direct-storage store, and value-based redaction need no change. `createVirtualSchema`, `refresh`, and `setProperties` also call STS, so an STS misconfiguration fails at CREATE. The scan UDF never calls STS. The STS module redacts its own errors against the base `secret_key`, base `session_token`, and external ID, because the rebuilt backend no longer holds them.

## ADR: The assumed role replaces the CONNECTION's key pair only where that pair is read

**ID:** assumed-role-replaces-key-pair-only-leaves-vending-unchanged
**Plan:** add-aws-assume-role-credentials
**Status:** Accepted

### Context

Vending never reads the key pair for storage, so a role has nothing to replace there. Whether AWS Glue vends credentials is unverified. If it does, vending supplies storage and the session signs the request that asks for it.

### Decision

The session replaces the key pair for SigV4 catalog signing and for object storage when `use_vended_credentials` is false. Every site that chooses between vending and static storage keeps branching on `use_vended_credentials`. The one changed site is `scan_storage_for`, which returns the CONNECTION reference only when the CONNECTION neither vends nor names a role, and the sealed envelope otherwise. The scenarios of `vs-adapter/connection-credentials-assume-role` state the resulting behavior.

### Options Considered

| Option | Verdict |
|--------|---------|
| An assumed role wins over vending, through a `StorageCredentialSource` precedence enum matched at five sites | Rejected: changes how vending works and edits four sites that never read the key pair for storage |
| Reject a CONNECTION that names a role and sets `use_vended_credentials` | Rejected: the combination is valid, because the session signs catalog requests and the catalog vends storage |
| A three-variant source enum used only by `scan_storage_for` | Rejected: that function already owns the wire variant, so the enum adds a public type with one caller |

### Consequences

A role CONNECTION without vending is sealed, which applies `seal-vended-storage-block-hkdf-aes-gcm-refuse-when-no-key-material` to a second credential. A role CONNECTION always has key material, because it requires `secret_key`. It reads S3 only, so an `abfss://` table location fails, as for any static-S3 CONNECTION.
