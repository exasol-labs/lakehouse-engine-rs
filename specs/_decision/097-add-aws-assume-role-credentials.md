# Decisions: add-aws-assume-role-credentials

## ADR: The assumed role becomes the request's single AWS identity, substituted into the credential set once per request

**ID:** assumed-role-single-request-identity-substituted-into-creds
**Plan:** add-aws-assume-role-credentials
**Status:** Accepted

### Context

After validation, every reader of the key triple wants the identity that AWS requests are made as. The SigV4 signing region is different. ADR `derived-signing-region-separate-from-connection-region` keeps a derived signing region out of `region`, because `region` has a second reader with a different purpose, store placement. No reader of the key triple needs the base identity after STS returns. Validation and the sealing key read the CONNECTION before the substitution.

### Decision

Both adapter entry points call one `lakehouse-catalog` function, `resolve_aws_identity`, once per request inside the request's async runtime. It sends the STS `AssumeRole` request and returns the `ConnectionCreds` with `access_key`, `secret_key`, and `session_token` replaced by the session's. The adapter rebuilds the static storage backend from that set with `storage_block`. Every downstream reader then reads the session without naming the role.

### Options Considered

| Option | Verdict |
|--------|---------|
| Substitute the session into the credential set once per request | ✓ Chosen: no reader learns about role assumption |
| A parallel effective-identity type threaded to every reader | ✗ Rejected: the catalog session, both format readers, both catalog clients, and redaction would each learn about role assumption |
| The scan UDF calls STS per shard | ✗ Rejected by the interview: up to 300 calls per query |
| Resolve the identity only where a request needs AWS access, skipping CREATE on an unsigned Iceberg catalog | ✗ Rejected: two call patterns instead of one, and CREATE no longer surfaces an STS misconfiguration |

### Consequences

SigV4 catalog signing, static storage, Iceberg manifest reads, Delta log reads, the direct-storage store, and value-based redaction need no change. `createVirtualSchema`, `refresh`, and `setProperties` also call STS, so an STS misconfiguration fails at CREATE. The scan UDF never calls STS. The STS module redacts its own errors against the base `secret_key`, base `session_token`, and external id, because the rebuilt backend no longer holds them.

## ADR: The assumed role replaces the CONNECTION's key pair only where that pair is read, and leaves credential vending unchanged

**ID:** assumed-role-replaces-key-pair-only-leaves-vending-unchanged
**Plan:** add-aws-assume-role-credentials
**Status:** Accepted

### Context

The interview scopes the session to SigV4 signing and the non-vended storage path. Vending never reads the key pair for storage, so a role has nothing to replace there. The substitution of the single-identity decision already reaches both reads, so no reader learns about roles. Whether AWS Glue vends credentials is unverified. If it does, vending supplies storage and the session signs the request that asks for it, with no special case.

### Decision

The session replaces the key pair for its two reads: SigV4 catalog signing, and object storage when `use_vended_credentials` is false. Every site that chooses between vending and static storage keeps branching on `use_vended_credentials`: the Iceberg and Delta readers' split, the `X-Iceberg-Access-Delegation` header, the Unity Catalog temporary-credentials request, and the `path_style` guard. The one changed site is `scan_storage_for`. It returns the CONNECTION reference only when the CONNECTION neither vends nor names a role, and the sealed envelope otherwise.

### Options Considered

| Option | Verdict |
|--------|---------|
| Replace the key pair only where it is read; leave vending unchanged | ✓ Chosen: no reader learns about roles |
| An assumed role wins over vending for storage, through a `StorageCredentialSource` precedence enum matched at five sites | ✗ Rejected: it changes how vending works, it drops vended credentials the operator asked for, and it edits four sites that never read the key pair for storage |
| Reject a CONNECTION that names a role and sets `use_vended_credentials` | ✗ Rejected: the combination is valid, because the session signs the catalog requests and the catalog vends storage |
| A three-variant source enum consumed only by `scan_storage_for` | ✗ Rejected: that function already owns the wire variant (`vs-adapter/scan-spec-credential-reference`), so the enum adds a public type with one caller |

### Consequences

A role CONNECTION with `use_vended_credentials` sends the access-delegation header, requests Unity Catalog temporary credentials, and skips the `path_style` guard, exactly as without the role. A role CONNECTION without vending carries session credentials that the CONNECTION does not state, so its storage block is sealed. This applies ADR `seal-vended-storage-block-hkdf-aes-gcm-refuse-when-no-key-material` to a second credential. A role CONNECTION always has key material, because it requires `secret_key`. A role CONNECTION without vending reads S3 only, so an `abfss://` table location fails as it fails for any static-S3 CONNECTION today.

Known open gap, left unresolved per PR #441 review: `pushdown-planning-cloud-credentials` and `delta-table-planning` still say that a non-vended CONNECTION's scan spec carries a bare CONNECTION reference. That is false for a non-vended role CONNECTION, which is sealed. The code stays correct, because `scan_storage_for` seals it. The recorded prose of those two specs contradicts `connection-credentials-assume-role` for that one input until someone edits them.
