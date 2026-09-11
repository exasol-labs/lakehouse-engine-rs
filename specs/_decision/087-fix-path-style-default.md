# Decisions: fix-path-style-default

## ADR: Admit the CONNECTION's stated path_style into the vended CONNECTION-wins rule

**ID:** admit-connection-path-style-into-vended-wins-rule
**Plan:** fix-path-style-default
**Status:** Accepted
**Supersedes:** path-style-connection-read-excluded-type-limitation

### Context

`ConnectionCreds.path_style` was a plain `bool` defaulting to `true`, so it could not distinguish
"the operator set false" from "the operator said nothing." That type limitation kept it out of the
vended CONNECTION-wins addressing rule that already applies to `endpoint` and `region`, and it also
meant an unstated `path_style` shipped as `true` on the non-vended path — the defect issue #130
reports, since AWS S3 and every AWS SDK address a bucket virtual-hosted by default. Widening the
field to `Option<bool>` removes both premises of the exclusion at once.

### Decision

`ConnectionCreds.path_style` and `StorageCreds.path_style` become `Option<bool>`. On the non-vended
path, an unstated value resolves to `false` inside the one selector both readers call. On the vended
path, `s3_backend` resolves `path_style` in three ordered steps: the CONNECTION's stated value, else
the vended `s3.path-style-access`, else whether a store endpoint was resolved at all. The
endpoint-presence derivation stays as the last resort rather than being deleted, because
`register_side_store` gates `endpoint` on `path_style` inside `AmazonS3Builder`, so a vended endpoint
nobody stated a style for must still reach the store. `validate_creds` gains a guard rejecting a
non-vended CONNECTION that supplies an `endpoint`, states no `path_style`, and does not enable
vending, because a resolved `false` silently discards the configured endpoint and reaches the wrong
host.

### Options Considered

| Option | Verdict |
|--------|---------|
| Widen `path_style` to `Option<bool>`; resolve unstated to `false` on the non-vended path and admit a stated value into the vended CONNECTION-wins rule | Chosen — root-cause fix; discharges the type limitation that motivated the prior exclusion and repairs the AWS-default defect in one change |
| Resolve an unstated non-vended value to `!endpoint.is_empty()`, mirroring the vended derivation | Rejected — issue #130 and the interview both specify a flat `false`; a derivation would couple `path_style`'s meaning to a second field, so adding an endpoint later would silently change addressing mode |
| Keep `path_style` a `bool` and flip only its default | Rejected by the interview — leaves the vended exclusion's type limitation standing and would need reopening later |
| Accept the silent breakage the default flip introduces for an endpoint-configured, non-vended CONNECTION | Rejected — `build_undecorated_store` discards the endpoint when `path_style` resolves `false`, turning a silent flip into a wrong-host read rather than a failed connection |

### Consequences

An unstated `path_style` on the non-vended path now matches AWS S3 client convention, closing issue
#130. A non-vended CONNECTION that configures an `endpoint` and omits `path_style` now fails at plan
time with a named error instead of silently reaching the wrong host; the operator adds one field to
recover the prior behavior. A vended CONNECTION that states `path_style` now has that value win over
the response's `s3.path-style-access`, where it was previously discarded. `StorageProps`, the
resolved wire type, is unchanged and keeps its `true` serde default, because the adapter always
serializes the field and about thirty scan fixtures rely on that default only through
`..Default::default()`.
