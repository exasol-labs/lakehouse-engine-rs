# Decisions: change-sigv4-region-inference

## ADR: The derived region signs catalog requests only and never becomes the CONNECTION's `region`

**ID:** derived-signing-region-separate-from-connection-region
**Plan:** change-sigv4-region-inference
**Status:** Accepted

### Context

An AWS Glue catalog and the S3 buckets of its tables can sit in different regions.
`ConnectionCreds.region` already serves as the CONNECTION's storage-address field,
read by `StaticStoreAddress::from`, `StorageCreds::from`, and `supplied_s3_fields`. A
signing region derived from the Glue endpoint's hostname must not collide with that
field, or storage rules would read a catalog region as a bucket region.

### Decision

The SigV4 signing region is a value computed on demand from `ConnectionCreds` and the
catalog URI. Only the adapter's SigV4 guard and the two catalog signing paths read it.
`parse_creds` does not compute it, and `ConnectionCreds.region` holds exactly what the
CONNECTION states.

### Options Considered

| Option | Verdict |
|--------|---------|
| Compute the signing region on demand, kept out of `ConnectionCreds.region` | ✓ Chosen — keeps storage addressing and catalog signing from reading the same field for two different concepts |
| Write the derived region into `ConnectionCreds.region` at parse time | ✗ Rejected — a plain `String` cannot distinguish a derived value from a stated one; storage rules would read a catalog region as a storage region |
| Add a separate `signing_region` field to `ConnectionCreds` | ✗ Rejected — conflates parsed input with a derived value and touches every `ConnectionCreds` struct literal across both crates and the E2E suites |

### Consequences

Storage addressing is unchanged: a CONNECTION that omits `region` reaches every
storage rule with `region` unstated. A non-vended Glue CONNECTION that omits `region`
passes validation and `CREATE VIRTUAL SCHEMA`, then fails at scan time, because
`storage_block` hands `AmazonS3Builder::with_region("")` an empty region; operator
documentation tells operators to keep stating `region` whenever the scan reads with
static S3 keys. Under vending, the store region follows
`vs-adapter/pushdown-planning-cloud-credentials`: the CONNECTION's stated region, else
the vended `client.region`, which stays unverified for Glue.

## ADR: A standard AWS Glue endpoint's derived region always signs; a stated `region` places the S3 store, independently

**ID:** glue-endpoint-region-always-signs-over-stated-region
**Plan:** change-sigv4-region-inference
**Status:** Accepted

### Context

A Glue catalog and its tables' S3 buckets are commonly in different AWS regions. This
plan's original design made a stated `region` always win for signing, with inference
only a fallback when `region` was empty. Under that design, an operator who states the
bucket's region to place the store also forces Glue's signature to that same wrong
region, and Glue rejects it — making the cross-region deployment shape unsupportable.

### Decision

`sigv4_signing_region` checks the address first: when it is a standard AWS Glue
endpoint, the method returns the region the host names, even when `region` is also
stated and even when the two differ. Only when the address is not a standard Glue
endpoint does the method fall back to the stated `region`. `ConnectionCreds.region` is
untouched either way, so a stated `region` still places the S3 store regardless of
which value signed the catalog request.

### Options Considered

| Option | Verdict |
|--------|---------|
| The endpoint's own derived region always signs; a stated `region` places the store, independently | ✓ Chosen — supports a Glue catalog and its tables' S3 bucket sitting in different regions |
| The stated `region` always wins for signing (this plan's original design) | ✗ Rejected — makes the cross-region Glue-and-S3 deployment shape unsupportable, since Glue rejects a signature computed for the bucket's region |
| A mismatch check that rejects a differing stated region | ✗ Rejected — forbids the cross-region configuration this change exists to support |
| A second field carrying the signing region separately from `region` | ✗ Rejected — duplicates the separation the signing-only design already provides, for no benefit, and touches the wire/JSON schema for no reason |

### Consequences

A CONNECTION whose stated `region` differs from a standard Glue endpoint's own region
now works: Glue signing uses the endpoint's region, and the S3 store places at the
stated region. A CONNECTION whose stated `region` matches the endpoint's region is
unaffected. Not a breaking change: a CONNECTION with a differing stated `region` was
already rejected by Glue before this change (signed for the wrong region); it now
succeeds instead. Getting this precedence wrong again later (for example
"simplifying" back to "stated always wins") would silently reintroduce the
cross-region bug this change exists to fix.
