# Decisions: fix-absent-table-location-error-consistency

## ADR: An absent table location is a hard error on every path, and the REST `warehouse` is never a storage anchor

**ID:** absent-table-location-hard-error-every-path
**Plan:** fix-absent-table-location-error-consistency
**Status:** Accepted

### Context

Commit `6d08c8a` sited the absent-table-location check inside the `if creds.use_vended_credentials`
arm of `resolve_file_list`, so the non-vended path still tolerated an absent `location` silently
and resolved an empty `table_root`. The Apache Iceberg table spec marks `location` `_required_` in
the v1, v2, and v3 columns of `format/spec.md`'s Table Metadata field table, so an absent location
is a malformed catalog response independently of how credentials are obtained. The REST `warehouse`
builds only the `loadTable` URL prefix — a bare AWS account id under Glue, a warehouse name or
per-warehouse UUID under Lakekeeper — and denotes no object store, so it cannot substitute for a
missing location.

### Decision

Reject a `loadTable` response carrying an empty table metadata `location` with a `UdfError::User`,
from one check that runs before the vended/static storage split. No CONNECTION-derived value —
`warehouse`, `endpoint`, or any other — may be substituted for it, with or without vended
credentials.

### Options Considered

| Option | Verdict |
|--------|---------|
| Hoist the guard above the vended/static split, unconditional on every path | ✓ Chosen — matches the Iceberg spec's required-`location` guarantee and closes the non-vended gap the shipped state left open |
| Keep the check vended-only (the shipped state after commit `6d08c8a`) | ✗ Rejected — leaves the non-vended path to resolve an empty `table_root` silently, changing the wire encoding of every file path |
| Warning-and-continue on an absent location | ✗ Rejected — not considered viable; an empty root silently changes the wire encoding of every file path |

### Consequences

A spec-conformant catalog is unaffected: every Iceberg v1/v2/v3 `loadTable` response carries a
`location`, so no existing query reaches the new error. A malformed response now fails at plan
time on the non-vended path too, instead of resolving an empty table root and emitting every file
path as an absolute URI. The error text becomes path-independent, so an operator is never
misdirected toward a credentials fault when the actual cause is a malformed catalog response.

## ADR: The guard sits at the resolve-once seam, not at the catalog-load seam

**ID:** absent-table-location-guard-resolve-once-seam
**Plan:** fix-absent-table-location-error-consistency
**Status:** Accepted

### Context

Every `loadTable` response enters the system through `load_table_any_auth`
(`crates/lakehouse-catalog/src/session.rs`), which also serves `resolve_table_schema` on the
`createVirtualSchema` path. `resolve_table_schema` reads only `result.metadata.current_schema()`
and never a table location, so a guard sited at the catalog-load seam would fail an entire
virtual-schema creation over a field that path never uses. `resolve_file_list`
(`crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs`) is the narrowest site every
location-dependent path passes exactly once, and `resolve_one_join_side` already delegates to it
by overriding only `table`, so join sides are covered without a second check.

### Decision

Site the check in `resolve_file_list`, immediately after reading `result.metadata.location()`.

### Options Considered

| Option | Verdict |
|--------|---------|
| `resolve_file_list`, above the vended/static split | ✓ Chosen — narrowest site every location-dependent path passes exactly once; join sides inherit it for free; `createVirtualSchema` stays untouched |
| `load_table_any_auth` (`lakehouse-catalog`), the catalog-load seam | ✗ Rejected — would also reject the response on the `createVirtualSchema` path, which reads no location, failing a whole virtual-schema creation over a field it never uses |

### Consequences

The rule stays path-independent by placement rather than by a second guard: the join path and the
non-vended path inherit it through the same call, while `createVirtualSchema` needs no exemption
because it never reaches the check.

## ADR: No helper is extracted to make the guard unit-testable; the test drives the real entry point over a loopback catalog fake

**ID:** absent-table-location-loopback-catalog-fake-test
**Plan:** fix-absent-table-location-error-consistency
**Status:** Accepted

### Context

There was no automated test asserting the absent-location error before this plan — the only
coverage was a live-credential assertion in `cloud_e2e_test.rs` that runs solely when cloud env
vars are set. The rule needed host-level coverage, with no network and no DB, that proves both
values of `use_vended_credentials` reach the identical error, and that cannot silently regress.

### Decision

Add no function. Test `resolve_file_list` itself from `file_resolution.rs`'s own `mod tests`,
serving a synthetic `loadTable` JSON body from a `tokio::net::TcpListener` bound on
`127.0.0.1:0` — the pattern `crates/lakehouse-catalog/src/session.rs` tests already use — with
`use_sigv4 = true` so each call issues exactly one HTTP request.

### Options Considered

| Option | Verdict |
|--------|---------|
| Drive the real `resolve_file_list` over a loopback HTTP fake | ✓ Chosen — proves both `use_vended_credentials` values reach the same error through the real production entry point, with no network, no DB, and no new dependency |
| Extract a pure `require_table_location(&str) -> Result<&str, UdfError>` | ✗ Rejected — a one-line `is_empty` guard behind its own function is classitis, and after the hoist there is exactly one path through the guard, so a passing helper test would keep passing if the guard were moved back inside the vended arm |
| Extract `resolve_effective_storage(result, static_storage, use_vended, allow_http)` into `lakehouse-catalog` | ✗ Rejected — reintroduces the storage-backend parameter and the do-the-work/return-the-input boolean that `vs-adapter/pushdown-planning-cloud-credentials` forbids on the vended entry point |
| Add an HTTP mock crate (`wiremock`/`httpmock`) | ✗ Rejected — a new dev-dependency for what the repo already does in 20 lines |

### Consequences

The test runs on every `cargo test` with no DB, no cloud, and no new dependency, and it exercises
the real production entry point rather than a stand-in, so a regression that moved the guard back
inside the vended arm would be caught immediately.
