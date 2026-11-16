# Decision Log: fix-order-by-constant-select-item

## Interview

- Q: How to handle a literal sort key? → A: drop it at parse time, before any path renders it
- Q: Every key literal plus LIMIT n? → A: treat as no ORDER BY (plain row scan with LIMIT)
- Q: E2E scope? → A: local Docker suite only, plus a unit test for the key filter

## Design Decisions

### [1] Constant keys with a non-zero offset collapse to one FALSE key

- **Decision:** When every sort key is a constant and the request carries a non-zero offset, the parse step replaces the keys with one `FALSE` key instead of removing the `orderBy`.
- **Rationale:** Exasol rejects `LIMIT … OFFSET` without `ORDER BY` ("OFFSET not allowed in LIMIT without ORDER BY", verified on the Docker Exasol) and accepts `ORDER BY FALSE`. A boolean constant is never a select-list position. Each renderer then serves the window through its existing ordered path, and the invariant that a non-zero offset arrives with a non-empty `orderBy`, which `build_dispatch_sql` and the N-scan join wrapper assert, still holds. The result matches native Exasol, because a constant ordering admits any row order (ADR `no-exasol-side-fallback-for-an-advertised-capability`: the adapter returns equivalent results for what it advertises). The interview answer covered `LIMIT n` without an offset. With a non-zero offset, the `FALSE` key keeps the per-shard limit withheld, so each shard returns every matching row, as on main.
- **Alternatives:** Remove the `orderBy` and let each of the four renderers (row-scan wrapper, grouped merge, broadcast join window, N-scan join wrapper) order by a visible column when an offset has no ordering. Rejected, because four modules would each learn the same rule.
- **Promotes to ADR:** no

### [2] A sort key is constant when its expression is any literal node

- **Decision:** The parse step drops an `orderBy` element whose expression node type starts with `literal_`. It does not reuse the literal list that `project_columns` and grouped detection share.
- **Rationale:** That list names the literal types the scan can project. The sort-key rule asks only whether a key is constant, and every literal type is constant, including types the list omits such as `literal_interval`.
- **Promotes to ADR:** no

## Review Findings
