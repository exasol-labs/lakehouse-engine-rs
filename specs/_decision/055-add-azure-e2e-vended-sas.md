# Decisions: add-azure-e2e-vended-sas

## ADR: Both credential arms live in one fixture and one test function

**ID:** azure-vended-arm-single-fixture-one-test
**Plan:** add-azure-e2e-vended-sas
**Status:** Accepted

### Context

`AzureFixture::_container`'s `Drop` guard deletes the per-run blob container only while the
provisioning test still owns it on its stack frame. Adding a second, vended-credential ADLS
warehouse needed a fixture shape that could hold both arms without doubling live-Azure
container provisioning, the dominant cost and the suite's only orphan surface.

### Decision

One per-run blob container holds two Lakekeeper warehouses (`<container>-static`,
`sas-enabled: false`; `<container>-vended`, `sas-enabled: true`), each with `key-prefix`
equal to its own warehouse name, seeded and queried through two Virtual Schemas from a single
`AzureFixture` held as a local in one `#[test]`.

### Options Considered

| Option | Verdict |
|--------|---------|
| One container, two warehouses, one test function | ✓ Chosen — halves live-Azure provisioning and keeps one orphan surface |
| A separate `#[test]` owning its own container | ✗ Rejected — two containers, two orphan surfaces, roughly double the live-Azure provisioning cost |
| Per-arm `catch_unwind` inside one test | ✗ Rejected — harness complexity for a masking risk assertion order already mitigates |
| Both arms in the shared `OnceLock` `setup()` the MinIO suite uses | ✗ Rejected as impossible — a `Drop` guard parked in a static is never dropped |

### Consequences

Cross-arm row equality rests on the deterministic 20-row seed shape, not on the shared
container, so the topology choice does not itself strengthen that comparison. The trade-off
does introduce a masking risk: a static-arm regression can obscure the vended proof, addressed
by a separate accepted decision on assertion and provisioning order.

## ADR: Single-test-function masking is a knowingly accepted residual risk

**ID:** azure-vended-masking-residual-accepted
**Plan:** add-azure-e2e-vended-sas
**Status:** Accepted

### Context

Housing both credential arms in one fixture and one test function (see the sibling ADR on
fixture shape) means a static-arm failure could mask the new vended-SAS proof this plan adds.
`AzureFixture::provision()` panics on every failure and performs all live provisioning for both
arms before the first assertion runs.

### Decision

Accept masking in two phases and mitigate each by order. Assertion phase: every vended-arm
assertion except the cross-arm row comparison runs BEFORE the static arm's assertions.
Provisioning phase: `provision()` creates the container, then the VENDED arm's warehouse, seed,
and virtual schema, then the STATIC arm's. Both orders are normative.

### Options Considered

| Option | Verdict |
|--------|---------|
| Order assertions and provisioning vended-first | ✓ Chosen — reduces, without removing, the masking window |
| Per-arm `catch_unwind` | ✗ Rejected in the interview as harness complexity |
| A separate test per arm | ✗ Rejected — needs a second live-Azure container |
| Interleaving assertions by cost rather than by arm | ✗ Rejected — no rule a reviewer could check |

### Consequences

A static-arm QUERY or ASSERTION regression cannot mask the vended proof. A static-arm
PROVISIONING failure, or a container-create failure, still aborts the shared fixture before any
assertion runs and does mask it — the accepted residual cost of one fixture. No ordering
mechanism removes that residual; only a second container or per-arm `catch_unwind` would, and
both were declined.

## ADR: The ADLS seed is immune to vended-config clobbering by key shape, not by flag

**ID:** azure-adls-seed-immune-by-key-shape
**Plan:** add-azure-e2e-vended-sas
**Status:** Accepted

### Context

`RestCatalog::load_file_io` merges a table's `loadTable` `config` OVER the builder props, which
is why the MinIO E2E arm installs a `CustomAwsCredentialLoader` to stop vended STS keys from
reaching its seed writes. The existing `SeedStorage::Adls` comment attributed the ADLS arm's
immunity to that same override being unnecessary because the warehouse was `sas-enabled: false`
— a premise this plan makes false by adding a `sas-enabled: true` sibling warehouse sharing the
same seed path.

### Decision

Seed both warehouses through the unchanged shared seed-catalog configuration with the account
key, and correct the `SeedStorage::Adls` comment to state the real reason no override is
needed: Lakekeeper vends the host-suffixed `adls.sas-token.<host>`, while iceberg-rust
(`iceberg-0.10.0/src/io/storage/config/azdls.rs:34-38`) reads only the flat `adls.sas-token` and
`adls.account-key`, so a vended key cannot reach `AzdlsConfig` and cannot displace the seed's
account key.

### Options Considered

| Option | Verdict |
|--------|---------|
| Seed unchanged; correct the comment's stated reason | ✓ Chosen — the immunity is real, the prior rationale was not |
| Install an ADLS credential override mirroring the MinIO arm's | ✗ Rejected — opendal's Azdls exposes no equivalent hook, and nothing needs defending |
| Create the vended warehouse `sas-enabled: false`, seed, then flip via the management API | ✗ Rejected — adds a second credential-bearing management call to solve a non-problem |

### Consequences

The seed path needs no per-arm branching. The iceberg-rust half of the immunity argument is
verifiable from source; the Lakekeeper half — that it never vends a flat `adls.sas-token` — is
settled only by the live suite run, so a vended-arm seed write failure signals that premise
broke, not that the account key is wrong.

## ADR: The Iceberg table spec does not bind this slice; the REST OpenAPI spec is the governing source and is silent on the ADLS key

**ID:** azure-vended-key-shape-unspecified-live-run-evidence
**Plan:** add-azure-e2e-vended-sas
**Status:** Accepted

### Context

`CLAUDE.md` requires any plan touching scanning, pushdown, or schema/type handling to be
checked against the Apache Iceberg table spec, with a deviation either fixed or recorded as a
tracked exception. This plan adds test coverage over an already-shipped vended-credential
extraction path and touches no scan planning, pushdown, or schema/type handling, so the
question is whether — and against which specification — that rule applies.

### Decision

Record that the table spec has no clause to check this slice against. The governing source is
the Iceberg REST Catalog OpenAPI specification (`open-api/rest-catalog-open-api.yaml`,
`apache/iceberg` `main`), fetched and quoted rather than recalled. It enumerates `config` keys
under `## AWS Configurations` only and names no ADLS key anywhere; `LoadTableResult` states
storage credentials arrive via `storage-credentials` first, `config` only as fallback. The
host-suffixed `adls.sas-token.<host>` key the adapter parses is a reference-implementation
convention (`ADLS_SAS_TOKEN_PREFIX`, `azure/src/main/java/org/apache/iceberg/azure/AzureProperties.java:43`)
that Lakekeeper follows, not a specified contract.

### Options Considered

| Option | Verdict |
|--------|---------|
| Name the REST OpenAPI spec as governing, quote it, and record the key shape as unspecified | ✓ Chosen — answers applicability explicitly with a fetched source |
| Declare the compliance rule inapplicable with no governing source named | ✗ Rejected — silence is exactly what the rule forbids |
| Cite the table spec anyway | ✗ Rejected — it specifies table format, not catalog credential delegation |

### Consequences

There is no spec deviation to fix or track as an exception, because the specification is
silent rather than contradicted. The dependency on an unspecified key convention is real and
unverifiable by spec reading alone, which is the strongest argument for this suite's live run
being the only available conformance evidence.

## ADR: Vended-arm provisioning order is a mandated mitigation for provisioning-phase masking

**ID:** azure-vended-arm-provisioning-order-mitigates-masking
**Plan:** add-azure-e2e-vended-sas
**Status:** Accepted

### Context

Plan review found that the vended scenario, as first drafted, claimed a static-arm regression
"cannot mask the vended proof" while the stated mitigation covered assertion order only.
`AzureFixture::provision()` panics on every failure and performs all live provisioning for both
arms — including `create_virtual_schema_with_password`, which enumerates table metadata through
the credential path and is the vended SAS's first real exercise — before the first assertion
runs, so the original claim was false as written.

### Decision

Task 3.1 mandates arm order in provisioning: the container, then the VENDED arm's warehouse,
seed, and virtual schema, then the STATIC arm's. The vended scenario's ordering clause now
claims only assertion- and query-phase protection, and names provisioning-phase masking as the
residual cost of one fixture — reduced, not removed, by vended-first provisioning.

### Options Considered

| Option | Verdict |
|--------|---------|
| Mandate vended-first provisioning order and restate the scenario's claim as partial | ✓ Chosen — closes the gap between claimed and actual protection |
| Leave provisioning order unspecified, keep the original "cannot mask" claim | ✗ Rejected — the claim was false: a static-arm provisioning failure still masks the vended proof |
| Add per-arm `catch_unwind` to remove the residual entirely | ✗ Rejected — declined in the interview as harness complexity |

### Consequences

A static-arm provisioning failure leaves the vended arm fully provisioned and its own
assertions still reachable in principle, but the shared fixture aborts before any assertion
runs, so the residual masking case (container-create failure, or vended-arm failure) is the
only one this plan cannot close.
