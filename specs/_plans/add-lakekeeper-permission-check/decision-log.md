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
- **Rationale:** A live check on 2026-10-02 against Lakekeeper `v0.13.1` showed `GET /catalog/v1/config?warehouse=lakehouse_authz` answer `defaults.prefix = 5c25f9c4-be40-11f1-a87d-17102559a460`. `/management/v1/warehouse` lists the same id for `lakehouse_authz`. A read-only probe on 2026-10-05 recorded two failure answers. The repo's Iceberg REST reference server answered `POST /management/v1/action/batch-check` with 400 `application/json`, a `BadRequestException` "No route for request". Lakekeeper `v0.13.1` answered an empty or non-UUID `warehouse-id` with 422 `text/plain`, a JSON-body deserialization error that names `TabularIdentOrUuid`.
- **Consequences:** A failed `/v1/config` lookup leaves the prefix empty (`resolve_load_table_prefix`). The check then fails with 422, and the query is refused.
- **Promotes to ADR:** no

### [4] The management API URL is derived from a catalog URI that ends in `/catalog`, with no override

- **Decision:** The management API URL is the catalog URI with its trailing `/catalog` replaced by `/management`, after one trailing `/` is dropped. createVirtualSchema, refresh, setProperties, and pushdown reject any other catalog URI. No property overrides the URL.
- **Alternatives:** A `LAKEKEEPER_MANAGEMENT_URL` override for path-rewriting gateways (the prior design, rejected for now: no deployment needs it yet, and it needs a same-origin rule so that a property cannot send the session's bearer token to another host). Reading the URL from the `/v1/config` answer (rejected: #414 found that no config value names the management API).
- **Rationale:** #414 found that Lakekeeper mounts `/catalog` and `/management` side by side under one base URL. The derivation changes only the path, so the bearer token goes only to the catalog's own origin.
- **Consequences:** A catalog behind a gateway that rewrites paths cannot use the check. An override property can be added later without breaking an existing configuration.
- **Promotes to ADR:** no

### [5] `USER_MAPPING` is a MiniJinja template, the adapter trusts its author, and the checks guard only against injection

- **Decision:** `USER_MAPPING` is a MiniJinja template with one variable, `user`, the querying user's name as Exasol reports it. The trimmed output is the principal. The engine runs with strict undefined values, no template loader, no auto-escaping, a fuel limit of 100,000, `trim_blocks`, and `lstrip_blocks`. The query is refused on an absent or empty current user, a render failure, an empty principal, or a principal that holds a whitespace or control character. createVirtualSchema, refresh, and setProperties compile the template and do not render it. The adapter checks neither the uniqueness nor the ownership of a principal.
- **Alternatives:** An ordered `<pattern> -> <template>` rule list with injective `lower` and `replace(a,b)` functions and a create-time clash check (the prior design, rejected: it forbids a shared principal such as one reporting principal for every BI account, it bakes the `<idp>~<subject>` id format into the grammar, and it cannot express a lookup table, a deny list, or an allow list). CEL (rejected: it has no template mode, so each principal becomes a string concatenation inside a SQL literal, and the released `cel` 0.14.5 ships no `lowerAscii` or `replace`). One template with `$1` or `$lower($1)` (rejected: it cannot strip a prefix or give a service account a fixed id). A Lakekeeper user search (rejected by #415: the principal comes from the mapping alone).
- **Rationale:** Setting `USER_MAPPING` requires `ALTER` on the virtual schema, so the template author is already trusted. The adapter cannot know who owns a principal. The checks therefore stop injection only: the user name is a template value and is never compiled, the principal enters the batch-check body only through `serde_json`, and a principal with inner whitespace or a control character is refused. Strict undefined values turn a missing lookup into a refusal instead of a partial principal such as `oidc~@corp.net`. #415's rule that "a crafted user name cannot produce another user's principal" holds for template code, JSON, and whitespace. Whether two names share a principal is the author's choice, and `{% if user == user|upper %}...{% endif %}` restores the strict rule for delimited lowercase names. A scratch crate with these settings rendered every spec example as stated on 2026-10-05. The largest example used 19 fuel units, and a 5,000-entry lookup table used 8, because the engine folds a constant map at compile time. `{% include %}` and `{% macro %}` failed to compile, and an unknown filter or a missing lookup failed the render.
- **Consequences:** The engine crate gains `minijinja` 2.24.0 (`default-features = false`, features `builtins`, `fuel`, `serde`) and the one new transitive crate `memo-map` 0.3.4, both Apache-2.0. An unknown filter or an undefined variable passes createVirtualSchema and refuses every query. Exasol's property-length limit bounds the template, and task 3.7 measures it live.
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
- **Direction change:** A scenario step states that two distinct accepted user names never get one principal. Decision [5] carries the rule into the rule-list mapping, and "USER_MAPPING never gives two Exasol users one principal" now holds that step. Superseded by "[plan-review] The rule-list USER_MAPPING forbade shared principals and checked what the adapter cannot know".
- **Promotes to ADR:** no

### [plan-review] The kind scenario and the unmappable-user scenario demanded different errors for one pushdown

- **Finding:** A pushdown under `GLUE`, or under a catalog URI without `/catalog`, with a valid mapping and an absent current user matched both scenarios. The kind scenario required the kind or URI error. The user scenario required the user error, without a CONNECTION read. The user scenario's GIVEN also overlapped the invalid-property and clash scenarios.
- **Direction change:** The kind scenario's WHEN covers a pushdown only when `USER_MAPPING` maps its current user. The user scenario's GIVEN requires a valid `USER_MAPPING` whose rules do not clash. Task 2.4 keeps its order: parse the settings, map the user, then resolve the kind and the CONNECTION.
- **Promotes to ADR:** no

### [plan-review] A non-Lakekeeper server that answers 400 got no Lakekeeper statement

- **Finding:** The failure scenario gave the Lakekeeper statement only to a 404, a 405, or a non-batch-check body. A probe on 2026-10-05 showed that the Iceberg REST reference server answers 400 `BadRequestException`. Lakekeeper answers an empty or non-UUID warehouse id with 422 `text/plain`.
- **Direction change:** Every non-2xx answer other than a 403 `CannotInspectPermissions`, and every 2xx body that is not a batch-check answer, gets the Lakekeeper statement. Every failure error carries the redacted answer body, so a 422 names its cause. Decision [3] records the two probe results. Task 1.4 covers a 400 and a 422 answer.
- **Promotes to ADR:** no

### [plan-review] The rule-list USER_MAPPING forbade shared principals and checked what the adapter cannot know

- **Finding:** The PR #457 review of 2026-10-05 rejected the rule grammar, its functions and their refusals, and the clash check. The guarantee that no two users share a principal blocks real setups, such as one reporting principal for every BI account. The grammar baked in the `<idp>~<subject>` id format and could not express a lookup table, a deny list, or a fixed service-account id. The review also tested the template path: on Exasol Personal 2026.2, a multi-line `USER_MAPPING` with `{% %}`, `{{ }}`, double quotes, and tabs reaches the adapter byte for byte through `CREATE VIRTUAL SCHEMA` and `ALTER VIRTUAL SCHEMA ... SET`, and `EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES` stores it unchanged.
- **Direction change:** Decision [5] makes `USER_MAPPING` a MiniJinja template. The check delta replaces the grammar and clash bullets with the template bullets, replaces "USER_MAPPING never gives two Exasol users one principal" with "The adapter trusts the template author and evaluates no user name as template text", and puts the review's examples into the mapping and refusal scenarios. The e2e mapping becomes a three-line template with a tab and double quotes. Task 2.1 shrinks and loses `[expert]`. Task 2.10 verifies the dependency footprint and licenses, and task 3.7 measures Exasol's property-length limit live.
- **Promotes to ADR:** no

### [plan-review] The spec delta described implementation instead of behavior

- **Finding:** The PR #457 review of 2026-10-05 found that `vs-adapter/lakekeeper-permission-check` carried the template engine's version and settings, the management URL and warehouse-id derivation, the adapter's order of internal steps, the e2e test setup, and test-table lists of `USER_MAPPING` examples. A spec says only what users and operators see.
- **Direction change:** Background now states what an admin sees: which template features work, that a template that runs too long is refused, and that a multi-line template works. Scenarios state outcomes, such as a refused query never contacting the catalog, with one or two examples each. The engine settings stay in decision [5] and task 2.1, the URL and warehouse-id derivation in decisions [3] and [4] and task 1.3, the exhaustive mapping and refusal examples in the task 2.1 unit tests, and the e2e setup in tasks 3.3 and 3.4.
- **Promotes to ADR:** no
