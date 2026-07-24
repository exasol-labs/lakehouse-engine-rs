# Decisions: fix-207-like-non-string-column

## ADR: Type-aware LIKE decision lives in the adapter, not vs-expression

**ID:** like-guard-in-adapter-not-vs-expression
**Plan:** fix-207-like-non-string-column
**Status:** Accepted

### Context

A pushed-down `LIKE`/`REGEXP_LIKE` predicate over a non-string column (DATE, DECIMAL, integer)
hard-failed the DataFusion scan at execution time, because DataFusion performs no implicit
non-string-to-VARCHAR coercion the way Exasol does (issue #207). A LIKE predicate's `column`
subject never carries a `dataType` on the wire — column Exasol types exist only in
`involvedTables[0].columns`, exposed via `extract_all_column_types(request)` in the adapter layer.
`crates/vs-expression` is a pure syntactic JSON-to-SQL translator with zero external state,
reused by a sibling VS-adapter project, so it has no access to column-type context and cannot make
this decision itself.

### Decision

A new `like_subject_type_guard` function in `pushdown/support.rs` preprocesses the filter JSON
before `render_df_filter_safe`, using the column-type map already produced by
`extract_all_column_types`. The type-aware dispatch — and the resulting rewrite or decline —
happens one layer above `vs-expression`, in the adapter.

### Options Considered

| Option | Verdict |
|--------|---------|
| Guard in the adapter (`pushdown/support.rs`) | ✓ Chosen — the adapter already resolves column types per query; `vs-expression` stays a pure, stateless, shared translator |
| Add type handling inside `vs-expression`'s `predicate_like` arm | ✗ Rejected — no column-type context available there, and the crate is a shared stateless translator used by a sibling project |

### Consequences

Keeping `vs-expression` type-blind preserves its reuse by the sibling VS-adapter project and keeps
its own LIKE/REGEXP_LIKE rendering scenarios unchanged. The adapter now owns one more
preprocessing pass over the filter JSON before rendering, and any future non-string-subject
pushdown gap (e.g. the join per-leg path, or the SELECT-list/projection path) must be fixed at the
same adapter layer, not inside `vs-expression`.

## ADR: CAST DATE to VARCHAR, decline every other non-string LIKE subject

**ID:** like-cast-date-decline-other-nonstring
**Plan:** fix-207-like-non-string-column
**Status:** Accepted

### Context

Once the type-aware guard exists, it must decide what to do with each non-string LIKE subject
type it encounters: DATE, DECIMAL (including integer, carried on the wire as `DECIMAL(p,0)`),
DOUBLE, BOOLEAN, TIMESTAMP, and others. Uniformly casting every non-string type to VARCHAR before
matching was considered, since Exasol's own implicit LIKE coercion casts any subject type to
VARCHAR.

### Decision

Only DATE subjects are rewrapped as `CAST(<col> AS VARCHAR)` (`function_scalar_cast` targeting
`{"type":"VARCHAR"}`); every other non-string subject (DECIMAL, integer, DOUBLE, BOOLEAN,
TIMESTAMP, and any bare-column subject whose type cannot be resolved from
`involvedTables[0].columns`) declines pushdown of the whole top-level filter, so Exasol evaluates
the predicate natively instead.

### Options Considered

| Option | Verdict |
|--------|---------|
| CAST DATE only; decline all other non-string types | ✓ Chosen — DataFusion's `Date32`→`Utf8` cast is ISO `YYYY-MM-DD`, matching Exasol's default `NLS_DATE_FORMAT`; other types' DataFusion-to-string formatting diverges from Exasol's (e.g. decimal trailing-zero scale) and would silently change which rows match |
| CAST every non-string type uniformly | ✗ Rejected — DataFusion's decimal/double/timestamp-to-string formatting diverges from Exasol's native formatting, silently changing results — strictly worse than a native-evaluation fallback |

### Consequences

The DATE CAST is Exasol-faithful only under the default `NLS_DATE_FORMAT`; a session that has
altered that format is an accepted, tracked exception (issue #216), not a silent gap. Correct
trimmed-decimal-to-string formatting remains a separate, already-tracked follow-up (issue #211) —
this decision explicitly defers it rather than shipping a lossy approximation. A non-string LIKE
anywhere in the filter tree declines the WHOLE top-level filter, mirroring the existing
all-or-nothing untranslatable-predicate backstop, so partial-filter rewriting never risks changing
result semantics.
