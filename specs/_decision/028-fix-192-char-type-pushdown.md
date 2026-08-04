# Decisions: fix-192-char-type-pushdown

## ADR: Keep the DataFusion-dialect CAST rendering as bare VARCHAR for CHAR targets

**ID:** char-cast-datafusion-dialect-stays-varchar
**Plan:** `fix-192-char-type-pushdown`
**Status:** Accepted

### Context

`vs-expression`'s `render_cast_target` has two dialect arms. The Exasol arm now renders a
`CHAR` target as `CHAR(n)` (see the `char-cast-exasol-dialect-renders-char` ADR). Whether the
DataFusion arm should do the same needed a separate decision, because the two arms feed
different parsers.

### Decision

The `Dialect::DataFusion` arm keeps rendering a CHAR target as a bare, length-less
`VARCHAR`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Bare, length-less `VARCHAR` on the DataFusion side | ✓ Chosen — Arrow has only `Utf8`, no CHAR type, and datafusion-sql rejects a length-qualified character target without `support_varchar_with_length` |
| Render CHAR in the DataFusion-side CAST fragment | ✗ Rejected — no target type exists to render it as; the value must be computed as a string, the declared output type is a separate boundary concern |

### Consequences

Width normalization for a CHAR-declared value in a grouping-equality position still needs a
blank pad on the DataFusion side, but as a `Utf8`-value transformation, not a CAST target — see
the `char-group-key-blank-pad-before-grouping` ADR. Exasol space-pads the emitted value into
the CHAR output column on read, verified live: a 15-character value emitted into
`EMITS (P CHAR(20))` returned padded to 20, matching native `CAST(c_phone AS CHAR(20))`.

## ADR: Keep CHAR pushdown type mapping separate from datafusion-scan/type-mapping

**ID:** char-pushdown-type-separate-from-scan-type-mapping
**Plan:** `fix-192-char-type-pushdown`
**Status:** Accepted

### Context

Two distinct type mappings exist in this codebase. `datafusion-scan/type-mapping` governs
Iceberg/Arrow source columns mapping into the `createVirtualSchema` schema declaration and the
Arrow-to-`Value` conversion; no Arrow type maps to Exasol CHAR there. This fix instead governs
Exasol-echoed pushdown-request `dataType` JSON mapping into the EMITS declaration, where CHAR
appears only as an Exasol-computed expression result type.

### Decision

`specs/datafusion-scan/type-mapping/spec.md` is not touched by this plan.

### Options Considered

| Option | Verdict |
|--------|---------|
| Leave `type-mapping` untouched; carve out a separate feature for the pushdown-request mapping | ✓ Chosen — the two mappings answer different questions and conflating them would misdescribe both |
| Extend `type-mapping`'s type table with a CHAR row | ✗ Rejected — CHAR never appears as a source-column type; adding it there would document a mapping that never fires |

### Consequences

The pushdown-request-to-EMITS-type mapping lives in the new
`vs-adapter/pushdown-planning-char-type-declaration` feature. Anyone extending
`type-mapping` in the future should not expect CHAR to appear there.

## ADR: Verify Exasol CHAR semantics live before planning the fix

**ID:** char-pushdown-live-verification-before-planning
**Plan:** `fix-192-char-type-pushdown`
**Status:** Accepted

### Context

The fix's correctness depends on undocumented Exasol runtime behavior: whether `CHAR(n)` and
`CHAR(n) ASCII` are valid dynamic UDF `EMITS` output types, whether Exasol space-pads a shorter
emitted value into a CHAR output column, whether `CAST(<expr> AS CHAR(n) ASCII)` parses, and
which declared type each of the four #192 query shapes actually produces.

### Decision

Probe the running Exasol 2025.2.1 container during planning for all four facts and record the
results in the spec Background, rather than plan from the issue text and precedent alone.

### Options Considered

| Option | Verdict |
|--------|---------|
| Probe live against the running container during planning | ✓ Chosen — each probe was one SQL statement against an already-running container, and the fix is worthless if any assumption is wrong |
| Plan from the issue text and the VARCHAR arm's precedent alone | ✗ Rejected — neither the EMITS validity nor the padding semantics were documented anywhere in the codebase |
| Defer all verification to implementation | ✗ Rejected — would let an unverifiable design ship past adversarial review |

### Consequences

The probe surfaced a fact the issue did not report: the declared type depends on whether CASE
branches have equal length (`'NEG'`/`'POS'` yields `CHAR(3) ASCII`; `'high'`/`'low'` yields
`VARCHAR(4) ASCII`), which explains why the pre-existing `'high'`/`'low'` E2E projection test
already passed and is now stated in the spec Background.

## ADR: Extend the CHAR fix to vs-expression's Exasol-dialect CAST renderer

**ID:** char-cast-exasol-dialect-renders-char
**Plan:** `fix-192-char-type-pushdown`
**Status:** Accepted

### Context

Adversarial plan review found that the original plan's claim — that one adapter-side seam
(`exasol_type_from_json`) accounts for every pushdown path's declared CHAR type — was false.
Three reachable paths derive their select-list column types from `vs-expression`'s Exasol
dialect instead: the N-scan unaccelerated join wrapper, the qualified single-table aggregate
fallback, and the grouped-merge scalar-over-aggregate wrapper. All three reach
`render_cast_target`'s `Dialect::Exasol` arm, which rendered a CHAR target as
`VARCHAR({size})` — a defect this plan's original scope would have left unfixed.

### Decision

Add a `CHAR` case to `render_cast_target`'s `Dialect::Exasol` arm, rendering `CHAR({size})`
plus an ` ASCII` suffix when the node's `dataType.characterSet` is `ASCII` case-insensitively.
The `Dialect::DataFusion` arm and the Exasol `VARCHAR` rendering stay unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Add the CHAR case to the Exasol dialect arm, fixing all three wrapper paths at once | ✓ Chosen — the defect and the fix share one seam; a single additive dialect case covers every consumer |
| Narrow the plan's claims and file a tracked exception for the three wrapper paths | ✗ Rejected — all three paths are reachable today; the failure is a type-check rejection plus lost padding, not a cosmetic gap a tracked exception should paper over |

### Consequences

This decision supersedes the clause "`CHAR(n)` also → `VARCHAR(n)` per the mission data-type
table" in the `specs/_decision/011-fix-count-distinct-shard-cap.md` follow-up entry
"Exasol-dialect CAST for the qualified wrapper" (that entry predates this project's ADR
`ID:`/slug convention and carries no slug to reference formally). That follow-up's core
decision — the dialect split itself, and length-qualifying character targets on the Exasol
side — stands unchanged; only the CHAR-collapses-to-VARCHAR clause is superseded. Five existing
tests that asserted the old VARCHAR-collapsing behavior were retargeted, not deleted, to keep
guarding the same invariants under the corrected rendering.

## ADR: Blank-pad a CHAR-declared group key on the DataFusion side before grouping

**ID:** char-group-key-blank-pad-before-grouping
**Plan:** `fix-192-char-type-pushdown`
**Status:** Accepted

### Context

Adversarial plan review found that making a CHAR-declared group key pushable was not
sufficient by itself. The grouped-aggregate merge groups on the unpadded staging string: the
inner EMITS always declares `"GK_i" VARCHAR(2000000)`, and `CAST("GK_i" AS CHAR(n))` applies
only in the SELECT list, never in the `GROUP BY`. Source values differing only in trailing
whitespace — `'ab'` and `'ab   '` — would therefore yield two output rows with split counts
that render identically, where native Exasol merges them into one. Declaring CHAR without
padding would convert a clean type-checker rejection into a silently wrong answer.

### Decision

When a grouped-aggregate group key's declared type is `CHAR(n)`, render its DataFusion-side
fragment as a blank pad to width `n` and use that padded fragment only in
`ScanSpec.common.group_keys`. The unpadded fragment stays the identity key for select-item and
`ORDER BY` slot matching, because `build_grouped_order_by_clause` and
`detect_group_by_aggregates` both match group keys by unpadded rendered-SQL equality.

### Options Considered

| Option | Verdict |
|--------|---------|
| Pad the DataFusion-side CHAR group-key fragment, kept as a separate list from the identity key | ✓ Chosen — makes the staging value equal the CHAR value Exasol would group on, without disturbing `ORDER BY` resolution |
| Leave grouping unpadded | ✗ Rejected — turns a clean `Data type mismatch` rejection into a silently wrong answer (split duplicate groups) |
| Restrict the CHAR declared type to provably fixed-length group keys and decline the rest | ✗ Rejected — leaves `CAST(<col> AS CHAR(n))` GROUP BY unpushable and needs a fixed-length prover |

### Consequences

The exact pad expression needed a second look — see the
`char-group-key-pad-must-not-truncate` ADR, which found and fixed a truncation defect in this
decision's first-round pad expression. The exposure is bounded: `ScanSpec.common.group_keys` is
populated at exactly one site, the `COUNT(DISTINCT)` fan-out only ever carries a base-column
type, and `constant_projection_sql` renders a literal already exactly `n` characters wide — so a
CHAR-declared grouped-aggregate group key is the one position that needed this pad.

## ADR: Use a non-truncating CASE-guarded pad instead of bare rpad for CHAR group keys

**ID:** char-group-key-pad-must-not-truncate
**Plan:** `fix-192-char-type-pushdown`
**Status:** Accepted

### Context

The first-round fix for the grouping-equality gap (`char-group-key-blank-pad-before-grouping`)
chose bare `rpad(<fragment>, n)` as the pad expression. Round-2 adversarial review found that
`rpad` truncates an over-length value, while Exasol raises an error rather than truncating:
`CAST('abcdefghij' AS CHAR(3))` fails live with SQL state 22001,
`data exception - string data, right truncation`, but `rpad('abcdefghij', 3)` in DataFusion
54.1 returns `'abc'`. A truncating pad would silently shorten an over-length value, turn the
outer `CAST("GK_0" AS CHAR(n))` into a no-op, and return a wrongly-merged group where native
Exasol fails outright — reintroducing the exact silent-wrong-answer failure class this plan
exists to prevent, in the opposite direction.

### Decision

Render the CHAR group-key pad as
`CASE WHEN character_length(<fragment>) < n THEN rpad(<fragment>, n) ELSE <fragment> END`,
which pads a short value to exactly `n` and leaves a value at or above `n` characters
unmodified.

### Options Considered

| Option | Verdict |
|--------|---------|
| `CASE WHEN character_length(<frag>) < n THEN rpad(<frag>, n) ELSE <frag> END` | ✓ Chosen — measured against DataFusion 54.1 for NULL, short, exact-length, over-length, multibyte, and nested-CASE fragments; NULL stays NULL, short values pad, over-length values pass through unmodified so Exasol's own 22001 still fires |
| Bare `rpad(<fragment>, n)` | ✗ Rejected — truncates an over-length value instead of raising Exasol's error |
| `concat(<frag>, repeat(' ', greatest(n - character_length(<frag>), 0)))` | ✗ Rejected — DataFusion's `concat` skips NULL arguments, so a NULL group key measures as `''` and merges with a genuine all-blanks group |

### Consequences

Every "pads to n" / "exactly Exasol's CHAR(n) blank-padding semantics" claim in the plan and
spec was narrowed to the accurate statement: short values pad to `n`, values at or above `n`
pass through unchanged. A dedicated seed table carrying a trailing-space pair and a
25-character over-length value was added to E2E-cover both the merge case and the truncation
case from one table.
