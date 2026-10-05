# Decisions: change-sigv4-region-inference

## ADR: The derived region signs catalog requests only and never becomes the CONNECTION's `region`

**ID:** derived-signing-region-separate-from-connection-region
**Plan:** change-sigv4-region-inference
**Status:** Accepted

### Context

An AWS Glue catalog and its tables' S3 buckets can sit in different regions. `ConnectionCreds.region` is already the CONNECTION's storage-address field, so a signing region derived from the Glue endpoint hostname must not share it, or storage rules would read a catalog region as a bucket region.

### Decision

The SigV4 signing region is computed on demand from `ConnectionCreds` and the catalog URI. Only the adapter's SigV4 guard and the two catalog signing paths read it. `parse_creds` does not compute it, and `ConnectionCreds.region` holds exactly what the CONNECTION states.

### Options Considered

| Option | Verdict |
|--------|---------|
| Write the derived region into `ConnectionCreds.region` at parse time | Rejected: a plain string cannot tell a derived value from a stated one |
| Add a `signing_region` field to `ConnectionCreds` | Rejected: mixes parsed input with a derived value and touches every struct literal in both crates and the E2E suites |

### Consequences

Storage addressing is unchanged. A non-vended Glue CONNECTION that omits `region` passes validation and `CREATE VIRTUAL SCHEMA`, then fails at scan time because `storage_block` passes an empty region to `AmazonS3Builder::with_region`. Operators must keep stating `region` whenever the scan reads with static S3 keys. Under vending, the store region is the CONNECTION's stated region, else the vended `client.region`, which stays unverified for Glue.

## ADR: A standard AWS Glue endpoint's derived region always signs; a stated `region` places the S3 store, independently

**ID:** glue-endpoint-region-always-signs-over-stated-region
**Plan:** change-sigv4-region-inference
**Status:** Accepted

### Context

A Glue catalog and its tables' S3 buckets are commonly in different regions. If a stated `region` always won for signing, an operator who states the bucket's region would force Glue's signature to that region, and Glue would reject it.

### Decision

For a standard AWS Glue endpoint, `sigv4_signing_region` returns the region the host names, even when `region` is also stated and differs. It falls back to the stated `region` only when the address is not a standard Glue endpoint. `ConnectionCreds.region` is untouched, so a stated `region` still places the S3 store.

### Options Considered

| Option | Verdict |
|--------|---------|
| A stated `region` always wins for signing | Rejected: makes the cross-region Glue and S3 shape unsupportable |
| A mismatch check that rejects a differing stated region | Rejected: forbids the configuration this change supports |
| A second field for the signing region | Rejected: duplicates the separation that on-demand signing already gives, and touches the wire schema |

### Consequences

A CONNECTION whose stated `region` differs from the Glue endpoint's region now works. This is not a breaking change, because Glue rejected that CONNECTION before. Reverting to "stated always wins" would silently reintroduce the cross-region bug.
