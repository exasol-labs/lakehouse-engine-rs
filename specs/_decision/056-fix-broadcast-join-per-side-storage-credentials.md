# Decisions: fix-broadcast-join-per-side-storage-credentials

## ADR: Route by path inside one registered store per bucket, not by a custom object-store registry

**ID:** join-route-by-path-inside-one-store-per-bucket
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

Issue #294: a pushed-down broadcast join read both tables through the fact side's storage credential. DataFusion selects an object store by bucket only — `get_url_key` keys the registry on `scheme://host[:port]` and `ObjectStoreUrl::parse` rejects any URL carrying a path — so one bucket is served by exactly one registered store, and Databricks makes one shared metastore bucket the normal case for two tables of one catalog.

### Decision

Register one `PrefixRoutingObjectStore` per bucket, holding one inner store per join side, each built from that side's own storage backend. Every `ObjectStore` trait call carrying a `Path` routes to the owning side's inner store.

### Options Considered

| Option | Verdict |
|--------|---------|
| Prefix-routing decorator per bucket | ✓ Chosen — object-store trait methods receive the full path, so the store layer is the only place the two sides can be told apart |
| A custom DataFusion `ObjectStoreRegistry` keyed with userinfo | ✗ Rejected — the registry never receives a path, so it cannot separate two S3 tables in one bucket, the Databricks-normal case; `object_store` 0.13.2 ships a userinfo-retaining registry DataFusion 54.1.0 does not use, but adopting it is a strictly larger change against a pluggable DataFusion seam |
| Refuse credential-divergent joins | ✗ Rejected — forfeits broadcast for the common Databricks case |
| Fall back to the N-scan renderer | ✗ Rejected — forfeits broadcast for the same case |

### Consequences

One code path serves every join, with no credential comparison. Two credentials can live behind one registered store, satisfying DataFusion's one-store-per-bucket constraint while each side reads through its own credential.

## ADR: Route on each side's enumerated file paths first, table root only as a fallback

**ID:** join-route-by-enumerated-paths-then-table-root
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

The router needs a rule for matching a requested path to the owning side. A root-prefix-only match was the original prescription, but the Apache Iceberg table specification explicitly permits a table's files to live outside its `location` (Appendix E, Version 4: "Absolute paths must be used for files that do not share a common prefix with the table location").

### Decision

The router's per-side routing table is the set of that side's own object-store paths — every data file and every one of its positional-delete files — matched exactly first. Only a path no side enumerated falls through to a longest-table-root-prefix match. Anything matching neither is an error.

### Options Considered

| Option | Verdict |
|--------|---------|
| Exact file-path match first, table-root prefix as fallback | ✓ Chosen — the scan discovers no files, so each side's spec already names every path it will request; exact membership is complete for every access the scan issues |
| Longest table-root prefix match only, as originally prescribed | ✗ Rejected — misroutes a spec-legal table whose files sit outside its `location`, which the Iceberg spec permits |

### Consequences

The root-prefix fallback is retained as the interviewed prescription but is UNREACHABLE through the current scan on a well-formed spec — every access the scan issues carries a path a side's spec enumerates exactly. It is honestly labelled as an unreachable fallback rather than implied to be load-bearing, resolving a completeness-vs-fallback contradiction a plan review round flagged.

## ADR: Layer the router OUTSIDE the spec-sized store, one size index per side

**ID:** join-router-outside-spec-sized-store-per-side-index
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

The scan's spec-sized store answers `head` calls from a size index without a network call. With one store per bucket previously serving both join sides, that index was whole-spec, so one side's store could answer the other side's metadata lookup.

### Decision

Layer as `Router[ SpecSized(inner_fact, sizes_fact), SpecSized(inner_dim, sizes_dim) ]` — the router outside, one size index per side inside.

### Options Considered

| Option | Verdict |
|--------|---------|
| Router outside, per-side sized index inside | ✓ Chosen — routing happens before the sized-`head` shortcut can answer it, so an unroutable `head` surfaces as the plan defect it is rather than as a later credential-shaped access denial |
| `SpecSized(Router[inner_fact, inner_dim], sizes_whole_spec)` — one sized layer outside a router, keeping the whole-spec index | ✗ Rejected — an unroutable `head` would be answered from the index and the routing failure would defer to the range read, surfacing as an access denial instead of a plan defect |

### Consequences

One side's store can no longer satisfy the other side's metadata lookup, restoring the information-hiding property that was lost when one store had to answer both sides' `head` calls.

## ADR: `validate_sides_share_one_store` stays; prefix routing does not subsume it

**ID:** join-keep-validate-sides-share-one-store-guard
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

With path-based routing now separating two sides behind one bucket-keyed store, whether the existing `validate_sides_share_one_store` guard is still needed came into question.

### Decision

Keep the scan-time guard that refuses a spec whose sides collapse onto one DataFusion registry key while needing different stores. Rewrite only its doc comment.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the guard | ✓ Chosen — verified against the tree: DataFusion's `get_url_key` drops URL userinfo, and on `abfss://` that userinfo IS the container, so an Azure store built from a container-qualified URL is container-scoped and its `Path` values are container-relative; two tables in two containers of one storage account can produce identical paths that no path-based routing can distinguish |
| Delete it as subsumed by the new router | ✗ Rejected — the router cannot distinguish two ADLS containers producing identical relative paths, so deleting the guard would leave that case unguarded |

### Consequences

The ADLS different-container case keeps its own plan-time and scan-time guards, unchanged by the prefix-routing decorator.

## ADR: Per-side scheme selection needs a plan-time join guard scoped to variant and account, not full backend equality

**ID:** join-backend-guard-scoped-to-variant-and-credential-served
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted
**Supersedes:** join-backend-guard-scoped-to-variant-and-account

### Context

`validate_sides_share_one_backend` compares join sides' storage backends at plan time. Its prior justification (recorded in ADR `join-backend-guard-scoped-to-variant-and-account`) scoped the comparison to backend variant and, for ADLS, `account_name`, citing a per-prefix vended-credential collapse as a deferred, separately-tracked defect (`#294`). This plan fixes that collapse, removing the justification's premise.

### Decision

Keep the plan-time comparison scoped to the backend variant and, for ADLS, `account_name`. Remove the "(tracked separately as `#294`)" justification and replace it with the new rationale.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the variant + ADLS-account scope, justified by unserveability | ✓ Chosen — a variant difference is unserveable (an `AmazonS3Builder` cannot address an `abfss://` URI) and an ADLS account difference is unserveable (two containers of one account collapse onto one DataFusion registry key); a credential difference is now served, so the guard needs no wider scope |
| Widen to full backend equality | ✗ Rejected — would reject exactly the credential divergence this plan now serves |
| Delete the guard | ✗ Rejected — variant and ADLS-account divergence remain genuinely unserveable by the scan |

### Consequences

The guard's scope is unchanged, but its justification no longer rests on a deferred defect — the per-prefix credential collapse it once deferred is fixed, so the narrow scope is now a statement of what is unserveable, not what is unverified.

## ADR: The reproduction gate is a hard stop, not a fallback ladder

**ID:** join-credential-repro-gate-hard-stop-no-fallback
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

Reproducing issue #294 as a genuine read error (not merely as differing credential values) requires two vended credentials whose SCOPE diverges, which is an empirical property of the target catalog/fixture that could not be assumed during planning.

### Decision

The investigation task stops the plan if it shows the two vended credentials do not diverge in scope, and escalates the substitute-proof question to the user rather than choosing a fallback proof strategy itself.

### Options Considered

| Option | Verdict |
|--------|---------|
| Hard stop and escalate on a non-diverging-scope finding | ✓ Chosen — decided explicitly in the planning interview: a passing join test over two whole-bucket credentials would evidence nothing about the defect, since reading the dimension through the fact's credential would simply succeed both before and after the fix |
| Pre-declare a fallback — unit-level-only proof of carriage, or a stub catalog vending two prefix-scoped credentials | ✗ Rejected — silently substituting the carriage assertion for the defect reproduction would ship a fix whose proof cannot fail |

### Consequences

A green suite proves the fix corrects the read outcome the defect broke, not merely that the wire format now carries two backends. Any future plan reproducing a credential-scoping defect against a new catalog should apply the same discipline: verify scope divergence before treating a passing test as reproduction.

## ADR: Strip Exasol's native `tableAlias` in `render_broadcast_join`, render everything bare

**ID:** join-strip-table-alias-render-bare-in-broadcast-renderer
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

Investigating the credential-defect reproduction (issue #294) surfaced a second, independent, pre-existing defect (issue #303): `render_broadcast_join` preserves Exasol's native `tableAlias` when rendering the join condition, filter, and projection, but the scan's derived sub-SELECTs are unaliased, so any aliased join query fails at scan time with a DataFusion schema error before the credential path is even reached. Without fixing this, the reproduction gate for #294 cannot observe the credential defect using realistic (aliased) client SQL.

### Decision

`render_broadcast_join` strips `tableAlias` from the join condition, the WHERE filter, and the whole `pushdown_req` passed to `extract_join_projection`, using the existing `strip_table_alias` helper, immediately after the disjoint-schema guard passes. `build_join_sql` is unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Strip `tableAlias` and render everything bare | ✓ Chosen — `build_side_fan_out_sql`, the N-scan fallback's own per-side scan renderer, already strips `tableAlias` for the identical reason (bare uppercase column names with no outer alias); `build_join_sql`'s derived sub-SELECTs have the identical shape, so the same fix applies for the same reason; `disjoint_schema_guard`, checked immediately before, already proves bare-name resolution is unambiguous |
| Thread each side's native alias through the wire (new `JoinSpec` fields, `build_join_sql` aliases its derived sub-SELECTs) | ✗ Rejected — needs a home for the fact side's alias too, coupling into the credential fix's own wire-format work; must handle asymmetric aliasing (one side aliased, one not); needs live-Exasol verification of alias-omission behavior — solvable, but for a problem bare rendering already discharges for free |
| Recover the alias by parsing it out of the already-rendered condition string inside `build_join_sql`, at scan time | ✗ Rejected — string-surgery on an opaque, already-rendered SQL fragment to recover structured information the JSON form already carried cleanly is fragile and cannot tell which alias belongs to which side without wire metadata |
| Leave `render_broadcast_join` as-is; declare an aliased join ineligible for broadcast | ✗ Rejected — would silently forfeit broadcast for the common case (every aliased query) rather than fix the actual defect |

### Consequences

A broadcast join now resolves correctly for aliased SQL (the common real-world case), with no wire-format change and no change to `build_join_sql` or `register_join_tables`. `render_broadcast_join`'s and `vs-adapter/pushdown-planning-join`'s recorded claims that broadcast rendering is "side-agnostic bare-name" become true by construction rather than describing an unstated assumption that Exasol never sends `tableAlias`.
