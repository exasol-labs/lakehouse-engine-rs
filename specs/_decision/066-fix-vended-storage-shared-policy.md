# Decisions: fix-vended-storage-shared-policy

## ADR: CONNECTION-wins-when-set addressing, resolved per field

**ID:** connection-wins-when-set-vended-addressing
**Plan:** fix-vended-storage-shared-policy
**Status:** Accepted

### Context

The Iceberg vended selector rejected a legal Databricks AWS response that carries credentials but no endpoint or region. A rule is needed for which source wins when both the vended response and the CONNECTION carry an endpoint or region.

### Decision

The vended S3 store address is resolved per field: the CONNECTION value wins when non-empty, else the vended value, else empty. Empty endpoint and region are valid and resolve through the AWS default chain. The plaintext-transport consent gate applies to the resolved endpoint, whichever source supplied it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Vended value wins when present | Rejected: the CONNECTION is the operator's explicit setting |
| One source wins both fields together | Rejected: discards a usable value when each source states only one field |

### Consequences

- The consent gate describes the transport the store will use.
- A non-empty CONNECTION endpoint or region overrides the vended one.

## ADR: Extraction stays forked; policy does not

**ID:** vended-extraction-forked-policy-shared
**Plan:** fix-vended-storage-shared-policy
**Status:** Accepted

### Context

The Iceberg REST and Unity Catalog vended selectors shared only scheme classification, and their copied policy steps diverged: the Unity ADLS arm accepted plaintext `abfs://`, and only the Iceberg selector enforced the store-address rule.

### Decision

A catalog kind may differ in how it reads a value off the wire, never in what makes the value acceptable. Consent gates, address rules, and target-variant decisions live in one place that both kinds call, and wire extraction stays per catalog.

### Options Considered

| Option | Verdict |
|--------|---------|
| Fix both defects in place in the Unity selector | Rejected: keeps the duplicated policy that caused the divergence |
| Unify wire extraction behind a trait | Rejected: the two wire formats share nothing above the neutral values they produce |

### Consequences

- The next catalog kind calls the shared rule.
- A divergence fails a build or test instead of leaving a silent gap.

## ADR: Addressing arrives as a capability-narrowed type, and the credential guarantee moves from the signature to a probe

**ID:** static-store-address-capability-narrowed-type
**Plan:** fix-vended-storage-shared-policy
**Status:** Accepted

### Context

The vended selectors took no CONNECTION-derived value, so no static credential could leak in under vending. CONNECTION-wins addressing requires passing one, which reopens the risk of a vended credential falling back to a static one.

### Decision

Both vended selectors take a narrow address type that carries only endpoint and region, with private fields and one conversion from the CONNECTION credentials. Two source probes assert that the type names no credential-like field and keeps both fields private.

### Options Considered

| Option | Verdict |
|--------|---------|
| Pass the full CONNECTION credentials | Rejected: makes the no-static-fallback guarantee unenforceable |
| Pass two bare string parameters | Rejected: an endpoint/region transposition compiles silently |
| Build the value at each call site | Rejected: spreads the rule over every caller |
| Public fields, with a probe forbidding literals outside `storage.rs` | Rejected: a text probe can be defeated by formatting, while the compiler checks every call site |

### Consequences

- The compiler enforces that no credential reaches the selectors.
