# Decisions: fix-vended-storage-shared-policy

## ADR: CONNECTION-wins-when-set addressing, resolved per field

**ID:** connection-wins-when-set-vended-addressing
**Plan:** fix-vended-storage-shared-policy
**Status:** Accepted

### Context

The Iceberg vended selector rejected a legal Databricks AWS response that carries credentials but no endpoint or region. Fixing that requires a rule for which source wins when both the vended response and the CONNECTION carry an endpoint or region.

### Decision

The vended S3 store address is resolved per field. The CONNECTION value wins when non-empty, else the vended value, else empty. A backend with both fields empty is valid and resolves through the AWS default chain. The plaintext-transport consent gate applies to the resolved endpoint, whichever source supplied it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Vended value wins when present | Rejected: the interview overruled this reading of the issue |
| One source wins both fields together | Rejected: discards a usable value when the sources each state only one field |

### Consequences

The consent gate describes the transport the store will use. A deployment that sets vended credentials plus a non-empty CONNECTION endpoint or region now uses the CONNECTION value, where the vended one used to win.

## ADR: path_style does not read the CONNECTION, because the field cannot express "unstated"

**ID:** path-style-connection-read-excluded-type-limitation
**Plan:** fix-vended-storage-shared-policy
**Status:** Accepted

### Context

The CONNECTION-wins rule for endpoint and region left open whether the CONNECTION's `path_style` field participates, and how it composes with the vended `s3.path-style-access` key.

### Decision

`path_style` is the vended `s3.path-style-access` value when the response states a parseable boolean, else whether an endpoint was resolved. The CONNECTION's `path_style` does not participate and is not passed to the vended selectors.

### Options Considered

| Option | Verdict |
|--------|---------|
| Apply the CONNECTION-wins rule to `path_style` | Rejected: it is a plain boolean defaulting to true, so it cannot distinguish "set false" from "unset", and the vended override would be unreachable whenever the key is omitted |
| Widen the CONNECTION field to an optional boolean | Rejected: changes the shipped default on the static storage path and every CONNECTION literal in the tests, for a value the vended derivation already answers |

### Consequences

The exclusion follows from a type limitation, not a preference. The non-vended static path is unaffected.

## ADR: Extraction stays forked; policy does not

**ID:** vended-extraction-forked-policy-shared
**Plan:** fix-vended-storage-shared-policy
**Status:** Accepted

### Context

The Iceberg REST and Unity Catalog vended selectors share only scheme classification. Their copied policy steps diverged in two places: the Unity ADLS arm silently accepted plaintext `abfs://`, and only the Iceberg selector enforced the store-address rule.

### Decision

A catalog kind may fork on how a value is read off the wire. It must not fork on what makes the value acceptable. Consent gates, address rules, and target-variant decisions have one home that both kinds call, and per-catalog wire extraction stays separate.

### Options Considered

| Option | Verdict |
|--------|---------|
| Fix both defects in place in the Unity selector | Rejected: re-copies the gate instead of removing the seam that lost it |
| Unify wire extraction behind a trait | Rejected: a flat map and a typed three-family response share nothing above the neutral values they produce |

### Consequences

The next catalog kind reads the rule where it lives. A future divergence becomes a build or test failure instead of a silent gap.

## ADR: Addressing arrives as a capability-narrowed type, and the credential guarantee moves from the signature to a probe

**ID:** static-store-address-capability-narrowed-type
**Plan:** fix-vended-storage-shared-policy
**Status:** Accepted

### Context

The vended selectors took no CONNECTION-derived value, so "no CONNECTION storage field is read under vending" held by signature. CONNECTION-wins addressing requires passing a CONNECTION-derived value, which would reopen the risk of a vended credential falling back to a static one.

### Decision

Both vended selectors take a narrow `StaticStoreAddress` type that carries only endpoint and region, with private fields, accessor reads, a default, and one conversion from the CONNECTION credentials. Two source probes assert that the declaration names no credential-like field and keeps both fields private.

### Options Considered

| Option | Verdict |
|--------|---------|
| Pass the full CONNECTION credentials | Rejected: makes the no-static-fallback guarantee unenforceable |
| Pass two bare string parameters | Rejected: an endpoint/region transposition compiles silently |
| Build the value at each call site | Rejected: spreads the rule over every caller |
| Public fields, with a probe forbidding literals outside `storage.rs` | Rejected: a text probe can be defeated by formatting, and the compiler checks every call site |

### Consequences

Field privacy makes the compiler enforce the no-credential half of the guarantee, and the accessors keep the read side honest.
