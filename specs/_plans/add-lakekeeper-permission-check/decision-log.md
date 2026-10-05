# Decision Log: add-lakekeeper-permission-check

## Interview

**Q:** How does the adapter locate `/management/v1/...`?
**A:** Derive it from the catalog URI by default, plus an optional override property (for example `LAKEKEEPER_MANAGEMENT_URL`) for path-rewriting gateways. Fail closed when derivation cannot be done. Superseded by decision [4] after review.

**Q:** Where does batch-check get the warehouse identity per table?
**A:** From the session's `/v1/config` response already on the `CatalogSession`, with no extra HTTP call. Fail closed when it is absent. Verify it against the real `/v1/config` shape and the #414 fixtures, and record an open question instead of guessing if the response does not expose the warehouse id. Decision [3] records the live verification.

**Q:** Which request shapes are covered?
**A:** Iceberg REST pushdown, single-table and join legs. Everything else is refused fail-closed while `PERMISSION_CHECK` is on: the Unity/Delta catalog kind, the direct-storage kind, and the non-pushdown paths (createVirtualSchema/refresh) as applicable. Verify what is reachable and spec each refusal. #416 expands coverage. Decisions [1], [6], and [7] record the result.

**Q:** Is `USER_MAPPING` required when `PERMISSION_CHECK` is on, and what can it contain?
**A:** Required: an absent mapping is rejected at CREATE/SET time. The grammar is literal text plus `$1` or `$lower($1)`. The substituted user name has to match a strict allowed charset (reject `~`, `@`, whitespace, control characters, and similar), so it cannot forge another principal. An invalid template is rejected at create time, and an invalid user name is refused at query time. Superseded by decision [5] after review.

## Design Decisions

### [1] The permission check runs where a pushdown first reads table metadata, and unchecked tables are refused

- **Decision:** The permission check runs at the one point where a pushdown request first reads table metadata, before any table is loaded. That point refuses to load a table that the check did not cover.
- **Alternatives:** A check in each query shape (rejected: every shape already reads its tables through that one point, so per-shape checks add sites without adding coverage, and a new shape could miss its check). Refusing every shape except the row scan until #416 (rejected: no shape bypasses that point, so the refusal removes function and no risk). Taking the table list from the request's `involvedTables` (rejected: a second source of truth for which tables a query reads).
- **Rationale:** Every query shape loads its tables through that point: row scan, aggregates, COUNT(DISTINCT), top-N, fallback wrappers, and joins. A new shape therefore cannot skip the check. Refusing unchecked tables turns a future bypass into a refusal instead of an unchecked read.
- **Consequences:** #416's bypass audit starts from this point. While the check is on, a catalog kind whose resolution runs no check gets every table refused.
- **Promotes to ADR:** yes

### [2] Lakekeeper calls are free functions that take the shared catalog session

- **Decision:** Each Lakekeeper management API call is a free function in the catalog crate that takes the request's shared catalog session. The session gains no Lakekeeper method. The adapter maps the Exasol user to a principal and passes the principal in.
- **Alternatives:** A method on the catalog session (rejected: it puts a Lakekeeper-only concern on a session that every Iceberg REST catalog shares, Lakekeeper or not). The whole client in the adapter (rejected: the adapter would need the session's HTTP client, token, and prefix, so the session would have to expose its internals).
- **Rationale:** A free function reuses the session's grant, connection pool, and prefix without widening the session's surface. The catalog crate already names no Exasol concept (`vs-adapter/catalog-crate-public-surface-extensions`), so that existing rule keeps the user mapping in the adapter. Only the free-function rule is new.
- **Consequences:** A Lakekeeper wire change stays inside the catalog crate. The adapter sees per-table decisions and errors only.
- **Promotes to ADR:** yes

### [3] The warehouse id is the session's `/v1/config` prefix, sent unchanged, and no call detects Lakekeeper in advance

- **Decision:** The batch-check names the warehouse by the prefix that the session's `/v1/config` lookup returned, unchanged. The adapter does not detect Lakekeeper before the check. A server that is not Lakekeeper fails the batch-check, and the refusal names the batch-check URL and the HTTP status and says that the catalog must be a Lakekeeper server.
- **Alternatives:** Accepting only a UUID prefix (the prior design, rejected: Lakekeeper already rejects a bad warehouse id, and the rule added a second error path for one cause). A detection call before the check (rejected: one more request per query, and the failed batch-check already reports the same fact). A management lookup of the warehouse id by name (rejected: one more request). A warehouse-id property (rejected: it can disagree with the warehouse that the session resolves).
- **Rationale:** A live check on 2026-10-02 against Lakekeeper `v0.13.1` showed `GET /catalog/v1/config?warehouse=lakehouse_authz` answer `defaults.prefix = 5c25f9c4-be40-11f1-a87d-17102559a460`. `/management/v1/warehouse` lists the same id for `lakehouse_authz`. A server without the batch-check route answers 404, 405, or a body that is not JSON.
- **Consequences:** A failed `/v1/config` lookup leaves the prefix empty (`resolve_load_table_prefix`). The check then fails or denies, and the query is refused.
- **Promotes to ADR:** no

### [4] The management API URL is derived from a catalog URI that ends in `/catalog`, with no override

- **Decision:** The management API URL is the catalog URI with its trailing `/catalog` replaced by `/management`, after one trailing `/` is dropped. createVirtualSchema, refresh, setProperties, and pushdown reject any other catalog URI. No property overrides the URL.
- **Alternatives:** A `LAKEKEEPER_MANAGEMENT_URL` override for path-rewriting gateways (the prior design, rejected for now: no deployment needs it yet, and it needs a same-origin rule so that a property cannot send the session's bearer token to another host). Reading the URL from the `/v1/config` answer (rejected: #414 found that no config value names the management API).
- **Rationale:** #414 found that Lakekeeper mounts `/catalog` and `/management` side by side under one base URL. The derivation changes only the path, so the bearer token goes only to the catalog's own origin.
- **Consequences:** A catalog behind a gateway that rewrites paths cannot use the check. An override property can be added later without breaking an existing configuration.
- **Promotes to ADR:** no

### [5] `USER_MAPPING` is an ordered rule list, and the mapping is injective over every user it accepts

- **Decision:** `USER_MAPPING` holds `<pattern> -> <template>` rules separated by `;`, and the first match wins. A pattern is an exact user name or a name with one `*`. A `*` rule's template holds one placeholder `{*}` with the optional functions `lower` and `replace(a,b)`. An exact rule's template is a fixed principal. An unmatched user is refused. createVirtualSchema, refresh, and setProperties reject two rules that can produce one principal, judged from the rules' literal text. Each function refuses an input that it would merge with another: `lower` refuses a lowercase ASCII letter, and `replace(a,b)` refuses `b`.
- **Alternatives:** One template with `$1` or `$lower($1)` (the prior design, rejected: it cannot express `ALICE_COOPER` to `oidc~alice.cooper@corp.net`, a stripped prefix, a fixed service-account id, or a per-group domain). Regular expressions with capture groups (rejected: the adapter cannot decide at create time whether an arbitrary replacement is injective). A plain user-to-principal table (rejected: every new user needs a property change). A Lakekeeper user search (rejected by #415: the principal comes from the mapping alone). A clash check against the existing Exasol users (rejected: it misses users created later, and listing users needs connect-back credentials).
- **Rationale:** Fixed literal text plus one injective substitution makes each rule injective. The literal-text comparison makes the rules' outputs disjoint. Together the mapping never gives two accepted users one principal. Only admins create Exasol users, so the checks target mapping mistakes, and refusing an unusual delimited name is fail-closed.
- **Consequences:** The literal-text comparison can reject a mapping that no existing pair of users would make clash. The adapter cannot verify that a principal belongs to the intended person.
- **Promotes to ADR:** no

### [6] Every catalog kind other than Iceberg REST gets one error, at every request type

- **Decision:** With the check on, every catalog kind other than Iceberg REST fails createVirtualSchema, refresh, setProperties, and pushdown with `PERMISSION_CHECK = 'LAKEKEEPER' requires the Iceberg REST catalog kind`, before the CONNECTION is read. The rule names only the supported kind.
- **Alternatives:** One error per unsupported kind (the prior design, rejected: it missed `GLUE` (#455), and every new kind would need an edit). Refusal at pushdown only (rejected: the operator learns of the limitation only at query time). Validation at createVirtualSchema, refresh, and setProperties only (rejected: a virtual schema that carried the property before this feature would be queried unchecked).
- **Rationale:** A comparison with the one supported kind rejects a new kind by default. The comparison sits where the kind is resolved, so it adds no exhaustive match site to the ones that `vs-adapter/catalog-kind-selection` counts.
- **Promotes to ADR:** no

### [7] Listing requests are not checked

- **Decision:** createVirtualSchema, refresh, and setProperties validate the permission properties and then list as before, as the CONNECTION's identity, with no batch-check.
- **Alternatives:** Refuse listing requests while the check is on (rejected: no virtual schema with the check on could be created). Check the listing for the user who runs the DDL (rejected: #416 scopes listing as the deliberate metadata limitation it asserts).
- **Rationale:** Listing is a DDL operation by the owner or an `ALTER` holder, not a data read by the querying user.
- **Promotes to ADR:** no

### [8] With the check off, `USER_MAPPING` is ignored

- **Decision:** An absent `PERMISSION_CHECK` skips all permission validation, and the adapter does not read the current user.
- **Alternatives:** Reject a mapping without the check (rejected: today the adapter ignores unknown properties, and #415 requires the off state to be identical to today).
- **Rationale:** Off means identical to today, and disabling the check keeps working when the operator unsets only `PERMISSION_CHECK`.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] A gate parameter on `handle_pushdown` fails the plan's own clippy gate

- **Finding:** Task 2.3 threaded `Option<&PermissionGate>` through `handle_pushdown`, which is `pub` and reachable as `lakehouse_engine::adapter::pushdown::handle_pushdown`. A `pub(crate)` type in that signature trips rustc's `private_interfaces` lint, and the Checklist runs `cargo clippy --all-targets -- -D warnings`. A `pub` gate type would widen the recorded pushdown façade.
- **Direction change:** The gate travels as a `pub(crate)` field of `ResolvedConnectionConfig`. The signatures of `handle_pushdown_request`, `handle_pushdown`, and `plan_join` stay unchanged. `for_request` keeps a gate parameter, because it is `pub(super)` and the lint does not apply to it.
- **Promotes to ADR:** no

### [plan-review] Background facts of two deltas backed no scenario step

- **Finding:** Three Background passages backed no step of their own spec: the Exasol property-privilege sentence and the subject-claim and direct-login sentences of `vs-adapter/lakekeeper-permission-check`, and the live-check sentence of the former client delta.
- **Direction change:** The three passages are deleted. plan.md § Context keeps the property-privilege fact, plan.md § Impact keeps the subject-claim guidance, and decision [3] keeps the live-check evidence.
- **Promotes to ADR:** no

### [plan-review] No scenario step stated the mapping's injectivity

- **Finding:** #415 requires that a crafted user name cannot produce another user's principal. The check delta stated injectivity only in Background, so the injectivity test traced to a scenario that did not state what the test checks.
- **Direction change:** A scenario step states that two distinct accepted user names never get one principal. Decision [5] carries the rule into the rule-list mapping, and "USER_MAPPING never gives two Exasol users one principal" now holds that step.
- **Promotes to ADR:** no
