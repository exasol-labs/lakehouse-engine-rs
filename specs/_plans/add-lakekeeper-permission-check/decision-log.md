# Decision Log: add-lakekeeper-permission-check

## Interview

**Q:** How does the adapter locate `/management/v1/...`?
**A:** Derive it from the catalog URI by default, plus an optional override property (for example `LAKEKEEPER_MANAGEMENT_URL`) for path-rewriting gateways. Fail closed when derivation cannot be done. Decision [4] adds a same-origin rule for the override.

**Q:** Where does batch-check get the warehouse identity per table?
**A:** From the session's `/v1/config` response already on the `CatalogSession`, with no extra HTTP call. Fail closed when it is absent. Verify it against the real `/v1/config` shape and the #414 fixtures, and record an open question instead of guessing if the response does not expose the warehouse id. Decision [3] records the live verification.

**Q:** Which request shapes are covered?
**A:** Iceberg REST pushdown, single-table and join legs. Everything else is refused fail-closed while `PERMISSION_CHECK` is on: the Unity/Delta catalog kind, the direct-storage kind, and the non-pushdown paths (createVirtualSchema/refresh) as applicable. Verify what is reachable and spec each refusal. #416 expands coverage. Decisions [1], [6], and [7] record the result.

**Q:** Is `USER_MAPPING` required when `PERMISSION_CHECK` is on, and what can it contain?
**A:** Required: an absent mapping is rejected at CREATE/SET time. The grammar is literal text plus `$1` or `$lower($1)`. The substituted user name has to match a strict allowed charset (reject `~`, `@`, whitespace, control characters, and similar), so it cannot forge another principal. An invalid template is rejected at create time, and an invalid user name is refused at query time. Decision [5] fixes the charset.

## Design Decisions

### [1] The check runs once per request inside `TableScanResolver::for_request`, and the resolver refuses unchecked tables

- **Decision:** The Iceberg arm of `TableScanResolver::for_request` runs the batch-check right after `CatalogSession::resolve` and before it returns. The check list is the distinct `table_identifiers` that the call receives. While a gate is present, the resolver records the checked identifiers, and `resolve` refuses any other identifier. The Unity and direct-storage arms refuse a gated request before they build anything. Both pushdown paths (`handle_pushdown` and `plan_join`) build their resolver through this one function, so every Iceberg REST shape (row scan, aggregates, COUNT(DISTINCT), top-N, the qualified fallback wrappers, the empty-result path, and joins) is checked before shape routing. The gate travels as the `pub(crate)` field `permission_gate` of `ResolvedConnectionConfig`. `handle_pushdown` and `plan_join` already receive that struct as `conn`, and they pass `conn.permission_gate.as_ref()` to `for_request`.
- **Alternatives:** A gate parameter on `handle_pushdown_request`, `handle_pushdown`, and `plan_join` (rejected: `handle_pushdown` is `pub` at `lakehouse_engine::adapter::pushdown::handle_pushdown`, so a `pub(crate)` gate type in its signature trips rustc's `private_interfaces` lint, which `-D warnings` makes an error. A `pub` gate type widens the façade that `vs-adapter/pushdown-module-structure` § "Public pushdown façade resolves at every pre-refactor path" fixes). A per-shape check in each SQL builder (rejected: the shapes consume the shards of one resolver, so per-shape checks add sites without adding coverage, and a new shape could forget one). Refusing every shape except the row scan until #416 (rejected: no shape bypasses the resolver, so the refusal would remove function without removing risk). Sourcing the check list from `involvedTables` (rejected: a second source of truth for the table set, and a table the planner does not resolve yields no data).
- **Rationale:** One chokepoint at the point where table metadata is first read makes "check before load" structural. The `resolve` guard turns a future bypass into a refusal instead of an unenforced read, which is the fail-closed rule of #415.
- **Consequences:** #416 keeps its bypass audit, its per-shape end-to-end test with two Exasol users, and its documentation. Its audit starts from this seam. A fourth catalog kind gets a compile error in `for_request`, so its author decides its permission support there.
- **Promotes to ADR:** yes

### [2] Lakekeeper wire knowledge lives in `lakehouse_catalog::lakekeeper`, and the Exasol user mapping lives in `adapter/permission.rs`

- **Decision:** `crates/lakehouse-catalog/src/lakekeeper.rs` owns the batch-check wire types, the management-base derivation and origin rule, the warehouse-id reading of the session prefix, check-id mapping, and error mapping. It exposes three items: `LakekeeperManagementBase`, the free function `batch_check(&CatalogSession, ...)`, and `TableReadDecision`. `crates/lakehouse-engine/src/adapter/permission.rs` owns the three virtual-schema properties, the `USER_MAPPING` grammar and charset, the principal, and the refusal messages. `CatalogSession` gains `pub(crate)` accessors only, so its fields stay private and it gains no public method.
- **Alternatives:** A `batch_check` method on `CatalogSession` (rejected by #415: it puts a Lakekeeper-only concern on a session that Glue and plain REST deployments share). The whole client in the adapter (rejected: the adapter cannot read the session's client, token, or prefix without widening `CatalogSession`). The mapping in the catalog crate (rejected: an Exasol user name is a delivery-mechanism concept, which `vs-adapter/catalog-crate-public-surface-extensions` forbids the crate to name).
- **Rationale:** Each decision has one owner. The adapter sees only per-table decisions, so a Lakekeeper wire change stays inside one module.
- **Consequences:** The request type has no optional identity, so the totality rule of #415 holds by construction. Every failure inside the client is an error, and the adapter turns every error into a refusal.
- **Promotes to ADR:** yes

### [3] The warehouse id is the session's `/v1/config` prefix, and only a UUID is accepted

- **Decision:** `batch_check` takes `warehouse-id` from the prefix that `CatalogSession::resolve` already holds. An empty prefix or a prefix that is not a UUID is an error, and the query is refused.
- **Alternatives:** A call to `/management/v1/warehouse` to look up the id by name (rejected: an extra HTTP call, and #415 asks for none). A `LAKEKEEPER_WAREHOUSE_ID` property (rejected: it can disagree with the warehouse that the session resolves).
- **Rationale:** A live check on 2026-10-02 against Lakekeeper `v0.13.1` showed `GET /catalog/v1/config?warehouse=lakehouse_authz` answer `defaults.prefix = 5c25f9c4-be40-11f1-a87d-17102559a460`, which is the id that `/management/v1/warehouse` lists for `lakehouse_authz`. `prefix_from_config` already reads `defaults.prefix`. The UUID rule rejects a SigV4 Glue prefix (`catalogs/<id>`) and an empty prefix from a failed lookup.
- **Promotes to ADR:** no

### [4] The management base is derived from a catalog URI that ends in `/catalog`, and the override keeps the catalog's origin

- **Decision:** Without `LAKEKEEPER_MANAGEMENT_URL`, the last path segment `catalog` of the catalog URI is replaced by `management`. A catalog URI with any other last segment, a query, or a fragment cannot be derived from, and the adapter rejects it with an error that names `LAKEKEEPER_MANAGEMENT_URL`. The adapter accepts an override only as an absolute `http` or `https` URL with the scheme, host, and port of the catalog URI. `batch_check` re-checks that origin against the session's catalog URI.
- **Alternatives:** An override to any host (rejected: the request carries the catalog session's bearer token, and a virtual-schema property could send it to another host). The full batch-check URL as the property value (rejected: a base is what an operator reads in a gateway configuration).
- **Rationale:** #414 found that Lakekeeper builds `{base}/catalog` and `{base}/management` from one base URL under the same host and token. Only a path-rewriting gateway breaks the derivation, and such a gateway keeps the origin.
- **Promotes to ADR:** no

### [5] `USER_MAPPING` takes exactly one placeholder, and each placeholder accepts only names on which it is injective

- **Decision:** Grammar: `<idp>~<subject>`, with a literal `<idp>` and exactly one `$1` or `$lower($1)` in `<subject>`. Literal text is printable ASCII without whitespace, `$`, or `~`. `$1` accepts `[A-Za-z0-9_]+`, and `$lower($1)` accepts `[A-Z0-9_]+`. Any other user name, an absent one, or an empty one is refused before any catalog request.
- **Alternatives:** One charset for both placeholders (rejected: under `$lower($1)` the delimited Exasol user `"alice"` and the user `ALICE` would share one principal). Several placeholders (rejected: concatenations of two substitutions are not injective). A template with no placeholder (rejected: every user gets one principal, which removes per-user enforcement). A placeholder in `<idp>` (rejected: a user name would choose the identity provider).
- **Rationale:** With fixed literal text and one injective substitution, two Exasol users never map to one principal, so a crafted user name cannot take another user's principal. The charset is stricter than the interview's minimum, and it also excludes `~` and `@`.
- **Consequences:** An Exasol user whose name holds a non-ASCII letter, or a delimited name with other characters, is refused. That outcome is fail-closed.
- **Promotes to ADR:** no

### [6] An unsupported catalog kind is rejected at every request type, and the resolver refuses it again

- **Decision:** With the check on, `CATALOG_KIND = UNITY_CATALOG` or `DIRECT_STORAGE` fails createVirtualSchema, refresh, setProperties, and pushdown before the CONNECTION is read. `CatalogKind::supports_lakekeeper_permission_check` in `adapter/catalog_kind.rs` owns the rule as an exhaustive match. `adapter/mod.rs` calls it for both entry points, and `adapter/permission.rs` takes no `CatalogKind`. The resolver arms of decision [1] repeat the refusal for a gated request.
- **Alternatives:** Refusal at pushdown only (rejected: the management-base validation would then reject the Unity Catalog URI at create time with a misleading message, and the operator learns of the limitation only at query time). A `CatalogKind` parameter on the permission resolver (rejected: `vs-adapter/catalog-kind-selection` permits only credential validation and the pushdown construction site to take the kind as an input).
- **Rationale:** The rule has one owner in `adapter/catalog_kind.rs`, and its only callers are files that `vs-adapter/catalog-kind-selection` already permits to name the kind. Rejecting early names the real cause.
- **Promotes to ADR:** no

### [7] Listing requests are not checked

- **Decision:** createVirtualSchema, refresh, and setProperties validate the permission properties and then list as before, as the CONNECTION's identity, with no batch-check.
- **Alternatives:** Refuse listing requests while the check is on (rejected: no virtual schema with the check on could be created). Check the listing for the user who runs the DDL (rejected: #416 scopes listing as the deliberate metadata limitation it asserts).
- **Rationale:** Listing is a DDL operation by the owner or an `ALTER` holder, not a data read by the querying user.
- **Promotes to ADR:** no

### [8] With the check off, `USER_MAPPING` and `LAKEKEEPER_MANAGEMENT_URL` are ignored

- **Decision:** An absent `PERMISSION_CHECK` skips all permission validation, and the adapter does not read the current user.
- **Alternatives:** Reject a mapping without the check (rejected: today the adapter ignores unknown properties, and #415 requires the off state to be identical to today).
- **Rationale:** Off means identical to today, and disabling the check keeps working when the operator unsets only `PERMISSION_CHECK`.
- **Promotes to ADR:** no

### [9] The end-to-end tests cover the row scan through three Exasol users and one non-inspecting CONNECTION

- **Decision:** `e2e_lakekeeper_test.rs` seeds the events table into `lakehouse_authz` and creates three Exasol users mapped by `oidc~$lower($1)@lakehouse.test`. One principal holds `select` on the events table, one holds `select` on `authz_alpha` only, and one holds no grant. The granted user runs first, and the two others then run the byte-identical statement. A second virtual schema is created through the operator CONNECTION, and that CONNECTION is then replaced with the client credentials of `lakehouse-reader-b`, which reads `/v1/config` but cannot inspect permissions.
- **Alternatives:** Every pushdown shape end to end (rejected: #416 owns that test). A `lakehouse-reader-b` CONNECTION from the start (rejected: that identity cannot list the namespace, so createVirtualSchema would expose no table).
- **Rationale:** Verification discipline requires a live run for three claims: the adapter receives the querying user through `ctx.current_user()`, a grant for a template-derived id authorizes that user, and Exasol does not serve one user's virtual-schema result to another. Exasol documents that the query cache is not used for statements that reference virtual schemas. The repeated statement checks that live.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] A gate parameter on `handle_pushdown` fails the plan's own clippy gate

- **Finding:** Task 2.3 threaded `Option<&PermissionGate>` through `handle_pushdown`, which is `pub` and reachable as `lakehouse_engine::adapter::pushdown::handle_pushdown`. A `pub(crate)` type in that signature trips rustc's `private_interfaces` lint, and the Checklist runs `cargo clippy --all-targets -- -D warnings`. A `pub` gate type would widen the recorded pushdown façade.
- **Direction change:** The gate travels as the `pub(crate)` field `permission_gate` of `ResolvedConnectionConfig` (decision [1]). The signatures of `handle_pushdown_request`, `handle_pushdown`, and `plan_join` stay unchanged. `for_request` keeps a gate parameter, because it is `pub(super)` and the lint does not apply to it. Task 2.3 adds `permission_gate: None` to the six test struct literals, and task 2.4 passes `None` from the 11 existing `for_request` test calls.
- **Promotes to ADR:** no

### [plan-review] Background facts of two deltas backed no scenario step

- **Finding:** Three Background passages backed no step of their own spec: the Exasol property-privilege sentence and the subject-claim and direct-login sentences of `vs-adapter/lakekeeper-permission-check`, and the live-check sentence of `vs-adapter/lakekeeper-permission-client`.
- **Direction change:** The three passages are deleted. plan.md § Context keeps the property-privilege fact, plan.md § Impact keeps the subject-claim guidance, and decision [3] keeps the live-check evidence. The client spec keeps the UUID rule and the Iceberg-spec sentence on `prefix`.
- **Promotes to ADR:** no

### [plan-review] No scenario step stated the mapping's injectivity

- **Finding:** #415 requires that a crafted user name cannot produce another user's principal. The check delta stated injectivity only in Background, so `each_placeholder_maps_its_accepted_names_injectively` traced to a scenario that did not state what the test checks.
- **Direction change:** "A user name the mapping cannot accept is refused before any catalog request" gains the step that two distinct accepted user names map to two distinct principals, for example `ALICE` and `alice` under `$1`. The Background injectivity sentence is deleted. The spec keeps 10 scenarios, and the test row keeps its trace.
- **Promotes to ADR:** no
