# Decisions: fix-concat-null-semantics

## ADR: Render pushed-down CONCAT as nullif(concat(...), '')

**ID:** concat-null-as-empty-string-wrapped-in-nullif
**Plan:** fix-concat-null-semantics
**Status:** Accepted

### Context

Exasol treats a NULL operand of `||` and `CONCAT` as the empty string, and its VARCHAR has no empty string, so an all-NULL concatenation is NULL. DataFusion's `||` returns NULL for any NULL operand, and its `concat()` returns `''` for an all-NULL list. The translator's chained `||` therefore returned NULL where Exasol returns the non-NULL parts (issue #374).

### Decision

The DataFusion dialect renders `nullif(concat(<a1>, ...), '')`. `concat` reproduces NULL-as-empty-string, and `nullif` reproduces the absence of an empty string. The Exasol dialect keeps chained `||`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Bare `concat(...)`, as issue #374 proposed | Rejected: returns `''` for an all-NULL list, so `WHERE <concat> IS NULL` matches no rows where Exasol matches all, and it regresses the NULL-boolean group label of issue #200 |
| `coalesce(<arg>, '')` per operand with chained `\|\|` | Rejected: one wrapper per argument, and it still needs the empty-result `nullif` |
| Custom DataFusion UDF implementing Exasol's `\|\|` | Rejected: adds a scan-side registration and a second home for the contract |

### Consequences

Pushed-down `CONCAT` over a nullable operand now returns the joined non-NULL parts, and NULL only when every operand is NULL, so filters over it match native Exasol. `FN_CONCAT` stays advertised and Exasol-dialect SQL is unchanged. This follows the `GREATEST`/`LEAST` precedent (issue #202).
