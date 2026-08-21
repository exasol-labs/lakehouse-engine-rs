# Decisions: fix-concat-null-semantics

## ADR: Render pushed-down CONCAT as nullif(concat(...), '')

**ID:** concat-null-as-empty-string-wrapped-in-nullif
**Plan:** fix-concat-null-semantics
**Status:** Accepted

### Context

Exasol's `||` operator and `CONCAT` function treat a NULL operand as the empty string, but Exasol's
VARCHAR domain has no empty string — `'' IS NULL` is TRUE, `LENGTH('')` is NULL, and
`CONCAT(NULL, NULL) IS NULL` is TRUE, all captured live on the pinned Exasol 2025.2.1 container.
DataFusion's `||` propagates NULL for any NULL operand, while DataFusion's `concat()` ignores a NULL
argument but returns a non-NULL `''` for an all-NULL argument list (`datafusion-functions-54.1.0/
src/string/concat.rs:106`). The translator rendered `CONCAT` as chained `||` in both dialects, so a
pushed-down concatenation over a nullable operand returned NULL where Exasol returns the
concatenated non-NULL parts (issue #374): `C_NAME || NULLIF(C_MKTSEGMENT, C_MKTSEGMENT) ||
'-suffix'` returned `Customer#000000001-suffix` natively and NULL through the virtual schema.
Neither half of Exasol's contract — NULL-as-empty-string, and no-empty-string-so-all-NULL-is-NULL —
was reproduced by a bare `concat()` alone: `WHERE concat(nullif("NAME","NAME"),
nullif("NAME","NAME")) IS NULL` matched 0 rows against DataFusion 54.1.0 where Exasol matches every
row, and a bare call would additionally regress issue #200's NULL-boolean group label
(`concat_group_by_key_uses_exasol_uppercase_labels` asserts NULL where a bare call yields `''`).

### Decision

Render the DataFusion dialect as `nullif(concat(<a1>, ...), '')`, not the bare `concat(a, b, c)`
that issue #374's fix direction proposed. `concat(...)` reproduces "a NULL operand is the empty
string"; `nullif(..., '')` reproduces "Exasol's VARCHAR domain has no empty string, so an empty
result is NULL". The Exasol dialect stays byte-identical chained `||`, since Exasol's own `||`
already has both halves of this contract.

### Options Considered

| Option | Verdict |
|--------|---------|
| `nullif(concat(...), '')` | ✓ Chosen — reproduces both halves of Exasol's contract and preserves issue #200's NULL-boolean group label |
| Bare `concat(...)`, as issue #374's fix direction proposed | ✗ Rejected — returns a non-NULL `''` for an all-NULL argument list, so `WHERE <concat> IS NULL` would match 0 rows against Exasol's every row, and regresses `concat_group_by_key_uses_exasol_uppercase_labels` |
| `coalesce(<arg>, '')` per operand, then chained `\|\|` | ✗ Rejected — equivalent in result but adds one wrapper per argument instead of one per node, and still needs the empty-result `nullif` |
| A custom DataFusion UDF implementing Exasol's `\|\|` | ✗ Rejected — adds a scan-side registration and a second home for the contract |

### Consequences

Pushed-down `CONCAT` results change wherever any operand can be NULL — a nullable column, a
`NULLIF`, an outer-join output — returning the non-NULL parts joined instead of NULL, and NULL only
when every operand is NULL. Filters over such an expression return different row sets, matching what
native Exasol returns for the same query. `FN_CONCAT` stays advertised and the Exasol-dialect
rendering is unchanged, so no outer wrapper SQL moves. This generalizes the precedent
`GREATEST`/`LEAST` (issue #202, `080-fix-greatest-least-null-semantics.md`) already set: an
advertised capability's NULL contract is the adapter's to reproduce at the rendering site, not to
withdraw.
