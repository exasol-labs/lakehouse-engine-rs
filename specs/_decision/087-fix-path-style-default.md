# Decisions: fix-path-style-default

## ADR: Admit the CONNECTION's stated path_style into the vended CONNECTION-wins rule

**ID:** admit-connection-path-style-into-vended-wins-rule
**Plan:** fix-path-style-default
**Status:** Accepted
**Supersedes:** path-style-connection-read-excluded-type-limitation

### Context

`ConnectionCreds.path_style` was a plain boolean defaulting to `true`, so it could not distinguish "set false" from "unstated". That kept it out of the vended CONNECTION-wins rule and shipped an unstated value as `true` on the non-vended path, which is the defect in issue #130, because AWS S3 addresses buckets virtual-hosted by default.

### Decision

`path_style` becomes an optional boolean in both `ConnectionCreds` and `StorageCreds`. On the non-vended path, an unstated value resolves to `false` in the one selector both readers call. On the vended path, it resolves to the CONNECTION's stated value, else the vended `s3.path-style-access`, else whether a store endpoint was resolved. The endpoint-presence derivation stays as the last resort, because `register_side_store` gates the endpoint on `path_style`. `validate_creds` rejects a non-vended CONNECTION that supplies an `endpoint`, states no `path_style`, and does not enable vending, because a resolved `false` discards the endpoint and reaches the wrong host.

### Options Considered

| Option | Verdict |
|--------|---------|
| Resolve an unstated non-vended value to whether an endpoint exists | Rejected: issue #130 and the interview specify a flat `false`, and the derivation would tie `path_style` to a second field |
| Keep a boolean and flip only the default | Rejected: leaves the type limitation that excluded the vended rule |
| Accept the silent breakage for an endpoint-configured CONNECTION | Rejected: the store discards the endpoint, which causes a wrong-host read and not a failed connection |

### Consequences

An unstated `path_style` on the non-vended path now follows the AWS S3 convention, closing issue #130. A non-vended CONNECTION that configures an `endpoint` without `path_style` fails at plan time with a named error, and the operator adds one field to recover the old behavior. A vended CONNECTION's stated `path_style` now wins over the response's value. `StorageProps`, the resolved wire type, keeps its `true` serde default, because the adapter always serializes the field and about thirty scan fixtures rely on that default.
