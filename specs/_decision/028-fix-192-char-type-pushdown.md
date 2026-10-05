# Decisions: fix-192-char-type-pushdown

## ADR: Keep the DataFusion-dialect CAST rendering as bare VARCHAR for CHAR targets

**ID:** char-cast-datafusion-dialect-stays-varchar
**Plan:** `fix-192-char-type-pushdown`
**Status:** Accepted

### Context

`vs-expression` renders CAST targets in two dialects that feed different parsers. The Exasol dialect now renders CHAR as `CHAR(n)`. The DataFusion dialect needs its own decision.

### Decision

The DataFusion dialect renders a CHAR target as bare `VARCHAR`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Render CHAR in the DataFusion CAST | Rejected: Arrow has only `Utf8`, and datafusion-sql rejects a length-qualified character target without `support_varchar_with_length` |

### Consequences

Width normalization for a CHAR-declared grouping key is a blank pad on the DataFusion side (see `char-group-key-blank-pad-before-grouping`), not a CAST target. Exasol pads the emitted value into the CHAR output column on read, verified live.

## ADR: Extend the CHAR fix to vs-expression's Exasol-dialect CAST renderer

**ID:** char-cast-exasol-dialect-renders-char
**Plan:** `fix-192-char-type-pushdown`
**Status:** Accepted

### Context

Three reachable paths take their select-list column types from the Exasol-dialect CAST renderer, not from the adapter seam: the N-scan unaccelerated join wrapper, the qualified single-table aggregate fallback, and the grouped-merge scalar-over-aggregate wrapper. That renderer emitted `VARCHAR(n)` for a CHAR target.

### Decision

The Exasol dialect renders a CHAR target as `CHAR(n)`, with an `ASCII` suffix when the character set is ASCII. The DataFusion dialect and Exasol VARCHAR rendering are unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Narrow the claims and track an exception for the three wrapper paths | Rejected: all three are reachable and fail with a type-check rejection and lost padding |

### Consequences

This supersedes the clause "`CHAR(n)` also → `VARCHAR(n)`" in the follow-up entry "Exasol-dialect CAST for the qualified wrapper" in `specs/_decision/011-fix-count-distinct-shard-cap.md`. That entry has no slug. Its dialect split and length-qualified character targets stand. Five tests that asserted the old rendering were retargeted, not deleted.

## ADR: Blank-pad a CHAR-declared group key on the DataFusion side before grouping

**ID:** char-group-key-blank-pad-before-grouping
**Plan:** `fix-192-char-type-pushdown`
**Status:** Accepted

### Context

The grouped-aggregate merge groups on the unpadded staging string, while Exasol groups on the CHAR value. Values such as `'ab'` and `'ab   '` would yield two split rows where Exasol returns one.

### Decision

For a CHAR(n) group key, the adapter renders a blank-padded fragment to width n and uses it only in the group-key list. The unpadded fragment stays the identity key for select-item and `ORDER BY` matching.

### Options Considered

| Option | Verdict |
|--------|---------|
| Leave grouping unpadded | Rejected: turns a clean type-mismatch rejection into a silently wrong answer |
| Restrict to provably fixed-length group keys and decline the rest | Rejected: leaves `CAST(<col> AS CHAR(n))` GROUP BY unpushable and needs a fixed-length prover |

### Consequences

Only the grouped-aggregate group key needs the pad: group keys are populated at one site, the `COUNT(DISTINCT)` fan-out carries only base-column types, and constant projections already have width n. The pad is `CASE WHEN character_length(x) < n THEN rpad(x, n) ELSE x END`, so values at or above n pass through unchanged and Exasol's own error still fires.
