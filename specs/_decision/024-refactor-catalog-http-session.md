# Decisions: refactor-catalog-http-session

## ADR: CatalogSession Bundles Client, Auth, and Prefix, Resolved Once Per Query

**ID:** catalog-session-bundles-client-auth-prefix-once
**Plan:** `refactor-catalog-http-session`
**Status:** Accepted

### Context

`resolve_file_list` ran once per table, and each run independently resolved the catalog-auth
strategy, resolved the `/v1/config` prefix, and issued the `loadTable` GET on a freshly constructed
`reqwest::Client`. The catalog-auth token and the `/v1/config` prefix are catalog-scoped, not
table-scoped — confirmed against the Apache Iceberg REST Catalog OpenAPI spec, where `GET
/v1/config` configures "the catalog and its HTTP client" and is keyed by `warehouse`, and the
OAuth2 client-credentials grant returns a catalog bearer token. An N-table join therefore ran N
grants, N config lookups, and up to 3N cold HTTP clients on the OAuth2 path.

### Decision

Introduce `CatalogSession { client, catalog_uri, auth, prefix }`, built once per query by
`CatalogSession::resolve(catalog_uri, warehouse, creds)` (one client, one OAuth grant, one
`/v1/config` lookup), and thread it by `&CatalogSession` into the `pub(crate)` file-resolution
core `resolve_file_list_with_session`, `resolve_one_join_side`, and `plan_join`.
`load_table_any_auth(&CatalogSession, &CatalogProps, &ConnectionCreds)` issues only the per-table
`loadTable` GET.

### Options Considered

| Option | Verdict |
|--------|---------|
| `CatalogSession` bundling client + auth + prefix, threaded by shared reference | ✓ Chosen — makes "resolve once, reuse everywhere" a type-level property |
| Memoize only the client, keep `load_table_any_auth` re-deriving auth and prefix | ✗ Rejected — partial win, leaves the grant and config lookup re-run per table |
| Pass raw `(client, auth, prefix)` tuples instead of a named struct | ✗ Rejected — loses the one-`(catalog_uri, warehouse)`-per-session invariant binding |

### Consequences

The OAuth2 grant and `/v1/config` lookup each run at most once per query regardless of table
count, and one pooled `reqwest::Client` is shared across every catalog request. The per-table
`loadTable` GET is preserved because only that response carries per-table vended storage
credentials.

## ADR: Session Built Per-Path at the handle_pushdown Seam, Not Once Before detect_join

**ID:** catalog-session-built-per-path-not-before-detect-join
**Plan:** `refactor-catalog-http-session`
**Status:** Accepted

### Context

The single-table and join pushdown paths have different pre-existing error orderings. Building
`CatalogSession` once before `detect_join` and threading it into both arms would run a catalog
HTTP call (the OAuth grant and `/v1/config` lookup) ahead of validation that today runs with no
network contact at all: an `Ineligible` join decline and a malformed single-table projection both
fail today before any request is issued.

### Decision

Build the session inside the `handle_pushdown` `JoinShape::Join` arm, threaded into `plan_join`.
On the single-table fall-through, the public `resolve_file_list` builds a single-use session
internally. The `Ineligible` arm declines with no session build. Table-identifier validation
(`parse_table_ident`) runs before the session's `/v1/config` lookup on both paths, so a
malformed-table request still issues zero catalog HTTP requests.

### Options Considered

| Option | Verdict |
|--------|---------|
| Build the session per-path, at the point each path is confirmed to need catalog contact | ✓ Chosen — preserves both paths' pre-existing error orderings while still building exactly once per executed path |
| Build once at the top of `handle_pushdown`, before `detect_join` | ✗ Rejected — contacts the catalog before an `Ineligible` join declines, and before single-table projection validation runs, regressing today's no-network-on-fast-reject behavior |

### Consequences

Exactly one path executes per request, so exactly one session is built per query, with no change
to which requests fail fast without touching the network.

## ADR: CatalogSession Stays Internal; Public resolve_file_list Keeps Its Signature via a Wrapper Plus pub(crate) Core

**ID:** catalog-session-internal-public-wrapper-plus-core
**Plan:** `refactor-catalog-http-session`
**Status:** Accepted

### Context

`resolve_file_list` has external callers: the `exasol-e2e` integration tests
(`tests/common/e2e_harness.rs`, `tests/e2e_scan_test.rs`) call it directly with a `catalog_uri:
&str` and cannot construct a `pub(crate) CatalogSession`. The public signature must therefore stay
`catalog_uri: &str`, while join legs still need to share one session across every leg's file
resolution.

### Decision

Declare `CatalogSession` `pub(crate)` in `credentials.rs` with no re-export via `pushdown/mod.rs`.
Keep the public `resolve_file_list(catalog_uri: &str, …)` and `resolve_table_schema` signatures
unchanged. Carry the shared session through a new `pub(crate)` core
`resolve_file_list_with_session(&CatalogSession, …)`; the public `resolve_file_list` builds a
single-use session and delegates to it. `plan_join` and `resolve_one_join_side` become
`pub(crate)`/`pub(super)` internals taking `&CatalogSession`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Public wrapper delegating to a `pub(crate)` session-taking core | ✓ Chosen — keeps the public entry point's name, visibility, AND signature identical; the `exasol-e2e` integration tests compile unedited |
| Re-export `CatalogSession` on the `crate::adapter::pushdown::<name>` façade and give `resolve_file_list` a `&CatalogSession` parameter | ✗ Rejected — widens the frozen public surface and still forces edits to every external caller |
| Extract a `lakehouse-catalog` crate exposing a genuinely-`pub` `CatalogSession` | ✗ Rejected — deferred to its own plan (tracked issue #204); drags `ConnectionCreds`/`CatalogProps`/`StorageProps` across a new crate boundary, beyond this refactor's scope |

### Consequences

The `vs-adapter/pushdown-module-structure` reachability probe compiles unchanged with an empty
diff against its frozen baseline. Session reuse across join legs flows through the `pub(crate)`
core, not the public entry point. A follow-up crate extraction (issue #204) is needed before
`CatalogSession` can become genuinely public.

## ADR: Iceberg REST Spec Confirms the Catalog-Scoped Premise

**ID:** iceberg-rest-spec-confirms-catalog-scoped-session
**Plan:** `refactor-catalog-http-session`
**Status:** Accepted

### Context

CLAUDE.md requires checking any pushdown-touching plan against the Apache Iceberg table/REST spec
rather than relying on memory. This refactor's premise — that catalog auth and the `/v1/config`
prefix are catalog-scoped, not table-scoped — needed normative confirmation before the code change.

### Decision

Record the normative basis, quoted from the Apache Iceberg REST Catalog OpenAPI spec
(`apache/iceberg` `open-api/rest-catalog-open-api.yaml`, main): `GET /v1/config` is the route "All
REST clients should first call ... to get catalog configuration properties from the server to
configure the catalog and its HTTP client," keyed by the `warehouse` parameter. The OAuth2
`POST /v1/oauth/tokens` client-credentials grant returns a catalog bearer token authenticating
catalog requests session-wide (the endpoint is marked "DEPRECATED for REMOVAL" in favor of an
external `oauth2-server-uri`, an existing accommodation rather than a gap introduced here). The
`loadTable` response, by contrast, "may contain credentials that should be used for subsequent
requests for the table" — per-table vended `storage_credentials` — so the per-table `loadTable` GET
must stay.

### Options Considered

| Option | Verdict |
|--------|---------|
| Quote the normative REST spec sections before implementing | ✓ Chosen — required by CLAUDE.md's Iceberg-compliance rule; confirms the refactor is spec-compliant with no gap |
| Rely on memory of the spec | ✗ Rejected — prohibited by CLAUDE.md |

### Consequences

The refactor has a cited normative basis: catalog auth and `/v1/config` are computed once per
query, and the per-table `loadTable` GET is preserved because only it carries per-table vended
credentials. No spec deviation was introduced or found.
