# Plan: fix-192-char-type-pushdown

## Summary

Make a result column Exasol declared `CHAR(n)` come back as `CHAR(n)` instead of `VARCHAR(n)`, and
make a `CHAR`-declared GROUP BY key group on the blank-padded value, so the three query shapes issue
#192 reports stop failing the type check and a pushed-down grouping merges exactly the rows native
Exasol merges. Three changes in two crates: a `"char"` arm in the adapter's shared type-derivation
seam, a `CHAR` case in `vs-expression`'s Exasol-dialect CAST-target renderer, and a non-truncating
blank pad on the DataFusion-side group-key expression when its declared type is `CHAR(n)`.

## Design

### Context

There are **two** seams that decide a pushed-down column's declared Exasol type, not one. The
original plan assumed one; a plan review found the second.

**Seam 1 — the adapter.** `exasol_type_from_json`
(`crates/lakehouse-engine/src/adapter/pushdown/support.rs:778`) maps an Exasol `dataType` JSON object
to the Exasol type string the adapter puts in its pushdown response. It has explicit arms for
`boolean`, `decimal`, `double`, `date`, and `timestamp`, then a catch-all arm commented "VARCHAR,
CHAR, and all others" that renders every string-family type — including a genuine `"char"` — as
`VARCHAR(size)`. There is no `"char"` arm, so the adapter never emits a genuine CHAR type. It has 8
non-test call sites:

| Call site | File | Reached by |
|---|---|---|
| `extract_all_column_types` | `support.rs:429` | base-column types — **inert for CHAR** |
| `project_columns` | `support.rs:663` | row-scan + broadcast-join EMITS types (facet B) |
| `aggregate_exasol_types` | `support.rs:841` | single-group aggregate CASTs |
| `constant_projection_sql` | `grouped_agg.rs:121` | literal select item (facet C) |
| `detect_group_by_aggregates` | `grouped_agg.rs:203` | per-plan + scalar-over-aggregate types |
| `group_key_exasol_types` | `grouped_agg.rs:458` | `GK_*` outer-wrapper CASTs (facet A) |
| `involved_table_columns` | `joins/planning.rs:374` | base-column types — **inert for CHAR** |
| `empty_select_list_typed_sql` | `file_resolution.rs:716` | empty-result `CAST(NULL AS …)` |

The two inert sites both read `involvedTables[].columns`, which can never carry CHAR: no Iceberg or
Arrow source type maps to Exasol CHAR (Iceberg `string` → VARCHAR), a fact already recorded at
`e2e_count_distinct_test.rs:511`. That is why the bug surfaces exactly on computed select-list
ordinals and group keys.

**Seam 2 — `vs-expression`.** **Three** Exasol-parsed wrapper paths never consult
`exasol_type_from_json` for their select-list column types:

| Consumer | File | Renderer entry point |
|---|---|---|
| N-scan unaccelerated join wrapper (`n_scan_join_select_items`) | `joins/sql_builders.rs:191` | `render_selectlist_item_qualified` → `render_expression_exasol_safe` |
| Qualified single-table aggregate fallback | `joins/sql_builders.rs:773`, via `outer_wrapper_clauses` at `:814` | `render_selectlist_item_qualified` → `render_expression_exasol_safe` |
| **Grouped-merge scalar-over-aggregate wrapper (`render_scalar_over_merge`)** | `grouped_agg.rs:415`, reached from `build_grouped_aggregate_scan_sql`'s `ScalarOverAggregate` arm at `:640-644` | `render_expression_exasol` (raising variant) |

All three reach `render_cast_target`, whose Exasol-dialect character arm
(`crates/vs-expression/src/lib.rs:116-131`) renders a `CHAR` target as `VARCHAR({size})`. So
`SELECT CAST(c AS CHAR(20)), COUNT(DISTINCT x), COUNT(DISTINCT y) FROM t` still fails the type check
and loses its blank padding, and so does a `CAST(SUM(x) AS CHAR(20))` item on the grouped-merge path.
Because the defect and the fix both live in the one shared `render_cast_target` Exasol arm, change 2
fixes all three consumers at once — but each has its own test that asserts the OLD collapsing
behavior, so the test inventory spans all three (Task 5). The broadcast join path is not affected —
it resolves EMITS types through `project_columns` (`joins/mod.rs:138`).

**The grouping-equality hole.** Declaring a group key `CHAR(n)` is not sufficient. The grouped
merge groups on the **unpadded staging string**: the inner EMITS is always `"GK_i" VARCHAR(2000000)`
(`grouped_agg.rs:591`), the outer wrapper is `GROUP BY "GK_0"` over that raw column
(`grouped_agg.rs:652`), and `CAST("GK_0" AS CHAR(20))` appears only in the SELECT list
(`grouped_agg.rs:608`). The DataFusion-side key renders as a bare unpadded `CAST(c AS VARCHAR)`. So
source values `'ab'` and `'ab   '` would yield two output rows with split counts that both render
identically as `'ab'` + 18 spaces, where native Exasol yields one merged row. Today the type checker
rejects that shape and Exasol computes it correctly natively — declaring CHAR without padding would
convert a clean rejection into a silently wrong answer.

The exposure is bounded and verified: `ScanSpec.common.group_keys` is populated at exactly one site
(`mod.rs:390`, the grouped arm); every other construction sets it `None`. The `COUNT(DISTINCT)`
fan-out takes only a lone **bare-column** argument (`is_lone_count_distinct` requires
`dc.column.is_some()`; an expression argument declines to the qualified wrapper — `support.rs:314`),
so its `"V"` column always carries a base-column type, never CHAR. `constant_projection_sql` renders
an Exasol-side expression with no DataFusion counterpart, over a literal already exactly `n` wide.
All three wrapper paths let Exasol group natively over the padded CHAR value, so they are correct once
seam 2 renders `CHAR(n)`. Facets A and C are fixed-length by construction and carry no padding risk.
**The one exposed position is a `CHAR`-declared grouped-aggregate group key.**

Ground truth captured live this session against the running Exasol 2025.2.1 container, using a
native probe schema and `SYS.EXA_ALL_COLUMNS`:

| Expression | Exasol declared type |
|---|---|
| `CASE WHEN c_acctbal<0 THEN 'NEG' ELSE 'POS' END` | `CHAR(3) ASCII` |
| `CAST(c_phone AS CHAR(20))` | `CHAR(20) UTF8` |
| `'X'` | `CHAR(1) ASCII` |
| `c_mktsegment` (VARCHAR base column, control) | `VARCHAR(10) UTF8` |
| `CASE WHEN id>10 THEN 'high' ELSE 'low' END` (unequal branch lengths) | `VARCHAR(4) ASCII` |

The last row explains why this bug went unnoticed: the existing `'high'`/`'low'` E2E projection
test (`e2e_capability_test.rs:978`) is declared VARCHAR because its branches differ in length. A
one-character change to either literal flips the declared type to CHAR and the test would fail.

Four further Exasol-side facts were verified live, because the fix depends on all of them:

- `CHAR(n)` and `CHAR(n) ASCII` are valid dynamic UDF `EMITS` output types — a LUA probe script
  emitting into `EMITS (G CHAR(3) ASCII)` and `EMITS (P CHAR(20))` both succeeded.
- Exasol space-pads a shorter emitted value into a `CHAR(n)` output column: emitting the
  15-character `25-989-741-2988` into `EMITS (P CHAR(20))` returned `25-989-741-2988     `, so a
  pushed-down `CAST(<col> AS CHAR(20))` matches native Exasol semantics.
- `CAST(<expr> AS CHAR(n) ASCII)` parses, which the grouped-aggregate outer wrapper requires for
  `CAST("GK_i" AS <declared type>)`, which a constant select-list item requires, and which the two
  Exasol-parsed wrapper paths require for a `CAST(… AS CHAR(n))` select item.
- `CAST('a' AS CHAR(2001))` fails with `specified length too long for char type - maximum is 2000`,
  fixing the adapter CHAR branch's size cap at 2,000.

Three code-side facts were verified in this repo, because they retire the risks the fix would
otherwise carry:

- The raw-scan strict `emit_batch` path already tolerates a `CHAR(n)` declared type.
  `coerce_batch_to_exa_types` (`scan/emit.rs:107-113`) routes any type for which
  `exasol_type_to_arrow` returns `None` — explicitly "VARCHAR / CHAR" (`types/mapping.rs:124-126`) —
  to the `Utf8` string path. No scan-side change is needed for facet B.
- **The pad MUST NOT truncate, and bare `rpad` does.** Exasol does not truncate an over-length value
  into a `CHAR(n)` — it raises an error. Verified live on the running 2025.2.1 container:
  `CAST('abcdefghij' AS CHAR(3))` fails with `data exception - string data, right truncation;
  Valuelength: 10 Maxlength: 3` (SQL state 22001). A pad that truncated would therefore reintroduce
  the exact failure class this plan exists to prevent: it would silently shorten the over-length
  value, the outer `CAST("GK_0" AS CHAR(n))` would become a no-op on an already-`n`-character input,
  and the query would return a wrongly-merged group where native Exasol fails outright.
  `rpad(str, n)` truncates — verified in source (`datafusion-functions-54.1.0/src/unicode/rpad.rs`:
  `if target_len <= str_len { builder.append_value(&string[..target_len]) }`; its own doc says "If
  the input string is longer than this length, it is truncated") and confirmed by executing it:
  `rpad('abcdefghij', 3)` → `'abc'`. So bare `rpad` is NOT usable as the pad.
- **The pad is `CASE WHEN character_length(<frag>) < n THEN rpad(<frag>, n) ELSE <frag> END`** —
  chosen after executing every candidate against DataFusion 54.1 in this workspace. It pads short
  values to exactly `n` with trailing spaces and leaves values of `n` characters or more UNCHANGED,
  so an over-length value reaches the outer `CAST("GK_0" AS CHAR(n))` unmodified and Exasol's own
  22001 truncation error still fires exactly as it would natively. Measured results, `n = 5`, inputs
  `NULL` / `'ab'` / `'abc'` / `'abcdefghij'`:

  | Candidate | NULL | `'ab'` | `'abcdefghij'` | Verdict |
  |---|---|---|---|---|
  | `rpad(<frag>, n)` | NULL | `'ab   '` | **`'abcde'` (truncated)** | REJECTED — silently truncates |
  | `concat(<frag>, repeat(' ', greatest(n - character_length(<frag>), 0)))` | **`''` (NULL destroyed)** | `'ab   '` | `'abcdefghij'` | REJECTED — DataFusion `concat` skips NULL arguments, so a NULL group key becomes a blank string and merges with a genuine all-blanks group |
  | `CASE WHEN character_length(<frag>) < n THEN rpad(<frag>, n) ELSE <frag> END` | NULL | `'ab   '` | `'abcdefghij'` | **CHOSEN** |

  Three further properties of the chosen form were measured, not assumed: a NULL `WHEN` condition
  falls to `ELSE`, so NULL passes through as NULL (no `'     '` group); `character_length` and `rpad`
  both count CHARACTERS, not bytes (`'äö'` padded to 4 → `'äö  '`), matching Exasol's character-based
  `CHAR(n)`; and the form parses and evaluates when `<frag>` is itself a `CASE` expression — the #192
  primary shape — spliced into all three of its positions.
- Over-length values are safe on the **projection** facet too, by erroring rather than truncating. A
  LUA probe emitting a 10-character value into `EMITS (P CHAR(5))` failed with `Lua Error "string too
  long"` (SQL state 22001), while a 3-character value padded cleanly to `'abc  '`. Exasol enforces
  the declared CHAR width at emit. Confirming the same on the Rust SLC's `emit_batch` Arrow IPC path
  is an E2E assertion (Task 8), not an assumption.
- The UDF preserves the padding: `value_to_gk_string` (`scan/partial_agg.rs:243`) passes strings
  through unchanged into `GK_i VARCHAR(2000000)`, and `build_grouped_partial_agg_sql` splices the
  same group-key fragment verbatim into both the DataFusion SELECT list and its GROUP BY
  (`partial_agg.rs:210,233`), so one padded fragment covers both.

- **Goals** — a select-list ordinal or group key Exasol declared `CHAR(n)` is declared `CHAR(n)` on
  every path (with the ` ASCII` suffix when Exasol declared ASCII), so the pushdown is accepted and
  returns the same rows as native Exasol including CHAR blank padding AND CHAR grouping equality;
  the VARCHAR path and every non-string branch are unchanged. Where a value is too long for its
  declared `CHAR(n)`, the pushdown MUST surface Exasol's own 22001 truncation error rather than
  silently truncate — a clean failure is the goal, not a returned row.
- **Non-Goals** — the **DataFusion** dialect arm of `render_cast_target` stays a bare, length-less
  `VARCHAR` for a CHAR target: Arrow has only `Utf8` and no CHAR type, and datafusion-sql rejects a
  length-qualified character target without `support_varchar_with_length`. (The **Exasol** dialect
  arm is now IN scope — see Decision below.) Padding a `CHAR`-declared **projection** ordinal that
  carries no grouping semantics: a bare per-row `CHAR(n)` EMITS declaration is already correct there,
  because Exasol pads on read and no equality merge happens. `specs/datafusion-scan/type-mapping/spec.md`,
  which governs the Iceberg/Arrow source-column mapping — a different type-mapping concern from this
  pushdown-request-JSON-to-EMITS-type mapping. Advertised capabilities, and any new Exasol type
  support beyond CHAR.

### Decision

Three narrow changes, one per seam plus the grouping fix.

1. **Adapter seam.** Add one `"char"` match arm to `exasol_type_from_json`, ahead of the catch-all,
   mirroring the existing VARCHAR arm's `characterSet` handling and capping `size` at 2,000 instead
   of 2,000,000. All 6 live consumers (of 8 call sites; 2 are inert) pick it up with no call-site
   change.
2. **`vs-expression` seam.** Add a `CHAR` case to `render_cast_target`'s `Dialect::Exasol` arm
   rendering `CHAR({size})`, plus ` ASCII` when the node's own `dataType` declares `characterSet`
   `ASCII`. This is a separate, purely additive dialect arm — the `Dialect::DataFusion` arm is not
   touched, and the Exasol `VARCHAR` rendering is not touched. The suffix is not cosmetic: without
   it an ASCII-declared CHAR would trade a `VARCHAR(3) ASCII` mismatch for a `CHAR(3) UTF8` one.
   `crates/vs-expression` is shared with a sibling VS-adapter project (mission.md), so the change
   stays additive and the crate's "trust the size Exasol sent, do not clamp" convention is kept.
3. **Grouping equality.** When a grouped-aggregate group key's declared type is `CHAR(n)`, render
   its DataFusion-side fragment as the non-truncating pad
   `CASE WHEN character_length(<fragment>) < n THEN rpad(<fragment>, n) ELSE <fragment> END` so the
   staging value IS the CHAR value and the outer `GROUP BY "GK_i"` merges what Exasol merges. Short
   values are padded to exactly `n`; a value of `n` characters or more passes through untouched, so
   Exasol's own `CAST("GK_i" AS CHAR(n))` still raises 22001 on an over-length value instead of the
   pad hiding it. The padded list is a **separate** list used only for
   `ScanSpec.common.group_keys`; the unpadded list stays the identity key for select-item and
   `ORDER BY` slot matching.

#### Architecture

```
Exasol pushdown request JSON
        │
        ├── selectListDataTypes[i] / groupBy dataType / involvedTables[].columns[].dataType
        │       ▼
        │   exasol_type_from_json                      ← change 1: add a "char" arm
        │       │
        │  ┌────┴────────┬────────────────┬──────────────┬─────────────┬──────────────┐
        │  ▼             ▼                ▼              ▼             ▼              ▼
        │ project_    group_key_      constant_      aggregate_   joins/          file_
        │ columns     exasol_types    projection_    exasol_      planning.rs     resolution.rs
        │ (EMITS)     (GK_* CAST)     sql (literal)  types        (inert)         (CAST(NULL AS …))
        │ facet B     facet A         facet C
        │                 │
        │                 └──→ declared CHAR(n)? ──→ non-truncating blank pad   ← change 3
        │                        CASE WHEN character_length(frag) < n
        │                             THEN rpad(frag, n) ELSE frag END
        │                        (short → padded to n; >= n → UNCHANGED, so Exasol's
        │                         own CAST("GK_i" AS CHAR(n)) still raises 22001;
        │                         padded list → ScanSpec.common.group_keys only;
        │                         unpadded list stays the ORDER BY / select-item identity key)
        │
        └── selectList item node (Exasol-parsed wrappers only)
                ▼
            render_expression_exasol[_safe]  (via render_selectlist_item_qualified
                ▼                            on the two join/fallback paths)
            render_cast_target(Dialect::Exasol)         ← change 2: add a CHAR case
                │
           ┌────┴─────────────────────┬──────────────────────────┐
           ▼                          ▼                          ▼
      n_scan_join_select_items   build_qualified_single_    render_scalar_over_merge
      (join fallback)            table_fallback_sql         (grouped-merge wrapper,
                                 (undecomposable grouped     e.g. CAST(SUM(x) AS CHAR(20)))
                                  / multi COUNT(DISTINCT))
```

#### Type dispatch, string family

| `dataType` JSON | `exasol_type_from_json` | `render_cast_target(Exasol)` |
|---|---|---|
| `{"type":"CHAR","size":3,"characterSet":"ASCII"}` | `CHAR(3) ASCII` | `CHAR(3) ASCII` |
| `{"type":"CHAR","size":20,"characterSet":"UTF8"}` | `CHAR(20)` | `CHAR(20)` |
| `{"type":"CHAR","size":20}` | `CHAR(20)` | `CHAR(20)` |
| `{"type":"CHAR","size":9999}` | `CHAR(2000)` (Exasol's CHAR maximum) | `CHAR(9999)` (trust Exasol's size) |
| `{"type":"VARCHAR","size":10,"characterSet":"UTF8"}` | `VARCHAR(10)` (unchanged) | `VARCHAR(10)` (unchanged) |
| anything else | unchanged | unchanged |

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Fix each seam at its own seam, not at the call sites | `exasol_type_from_json`; `render_cast_target` | 6 live consumers behind seam 1 and 3 wrapper paths behind seam 2 (`n_scan_join_select_items`, `build_qualified_single_table_fallback_sql`, `render_scalar_over_merge`); per-call-site fixes would duplicate the charset rule and drift, exactly the failure mode issue #52 recorded |
| Mirror the VARCHAR charset rule | both new CHAR cases | The ` ASCII` suffix convention is already live-proven (issue #136 follow-up), and the two seams must agree or the wrapper column type mismatches on character set |
| Type-specific size cap in the adapter only | `min(2000)` on `exasol_type_from_json`'s CHAR branch | Exasol rejects CHAR above 2,000. The adapter synthesizes a declaration and caps defensively; `vs-expression` echoes a width Exasol just sent and keeps its documented no-clamp rule |
| Pad only where equality is evaluated | the pad on group keys; NOT on projection ordinals | A projection ordinal has no grouping semantics — Exasol pads on read, and errors on an over-length value at emit. A group key's staging value IS the equality key, so it must carry the padding |
| Pad short values only; never truncate | the `CASE`/`character_length` guard around `rpad` | Exasol raises 22001 on an over-length `CHAR(n)` cast rather than truncating. A truncating pad would swap that clean error for a silently merged wrong group — the very failure class this plan prevents. Leaving values `>= n` untouched keeps Exasol's error on Exasol's side of the seam |
| Padded list separate from the identity list | grouped arm of `adapter/pushdown/mod.rs` | `build_grouped_order_by_clause` and `detect_group_by_aggregates` match group keys by unpadded rendered-SQL equality; padding in place would decline every `ORDER BY` on a CHAR group key |
| Declared type only, never the value expression — except for equality | `vs-expression` DataFusion arm untouched | Arrow has no CHAR type; DataFusion computes a `Utf8` value. The pad is a width normalization on that `Utf8` value, not a CHAR type |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Fix `render_cast_target`'s Exasol arm too | Narrow the plan's claims and file a tracked exception for the three wrapper paths (the repo's `(#27)` pattern) | All three wrapper paths are reachable today and the failure is a type-check rejection plus lost padding, not a cosmetic gap. The change is one additive dialect case that fixes all three at once; a tracked exception would leave a known-broken shape shipping. Supersedes ADR 011's follow-up clause "CHAR(n) also → VARCHAR(n) per the mission data-type table" |
| Pad the DataFusion-side CHAR group key with a non-truncating blank pad | Restrict the CHAR declared type to provably fixed-length group keys and decline the rest; leave grouping unpadded; pad with bare `rpad`; pad with `concat` + `repeat` + `greatest` | Leaving it unpadded turns a clean `Data type mismatch` rejection into a silently wrong answer (split duplicate groups). Restricting would leave `CAST(<col> AS CHAR(n))` GROUP BY unpushable and needs a fixed-length prover. Bare `rpad` truncates over-length values, recreating the same silent-wrong-answer class in the other direction (Exasol raises 22001 there, verified live). `concat` + `repeat` was measured to destroy NULL — DataFusion's `concat` skips NULL arguments, so a NULL key becomes a blank string. The chosen `CASE`-guarded form was executed against DataFusion 54.1 and is correct on NULL, short, exact-length, over-length, multibyte, and nested-`CASE` fragments |
| Cap the adapter CHAR branch at 2,000 | Reuse VARCHAR's 2,000,000 cap; leave the size uncapped | `CAST('a' AS CHAR(2001))` fails live with `specified length too long for char type - maximum is 2000`; an uncapped or 2,000,000 cap would emit an Exasol-invalid declaration. Defensive only — Exasol cannot declare a CHAR above 2,000 in the first place |
| New feature spec plus one `vs-expression` delta | Delta on `vs-adapter/pushdown-planning` only; deltas on every affected pushdown feature | The adapter behavior spans the row-scan, grouped-aggregate, single-group, join, join-fallback, and empty-result paths, so it belongs to none of them exclusively — mirroring how `pushdown-planning-like-type-coercion` was carved out. The CAST-rendering change belongs to the feature that owns CAST rendering |
| Add an E2E regression test alongside unit tests | Unit tests only, as in `fix-207-like-non-string-column` | Unit tests can only assert the rendered type string. Only an E2E run proves Exasol's type checker accepts the emitted `CHAR(n)`, that CHAR padding matches native results, that the padded group key merges trailing-whitespace variants, and that an over-length value raises Exasol's own 22001 rather than being silently truncated — and this bug reached production precisely because no E2E covered an equal-length CASE |
| Add a small dedicated seed table carrying both a trailing-space pair and an over-length value | Extend the `events` or `labels` seed with those rows; use two separate new tables | The `events` seed's `name` values are `event-NN`, exactly 8 characters, so the existing data cannot exhibit padding divergence. Adding rows to a shared seed shifts `SEED_TOTAL_ROWS`/`SEED_LABELS_ROWS` and the join and pruning assertions built on them; a new table via the existing `create_and_append_files` helper is additive and perturbs nothing. One table suffices for both cases because the declared CAST width selects which case is exercised — `CHAR(30)` fits every value and isolates the merge, `CHAR(20)` makes the 25-character row over-length |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-char-type-declaration | NEW | `vs-adapter/pushdown-planning-char-type-declaration/spec.md` |
| sql-comprehension/vs-expression-translator-scalar-ops | CHANGED | `sql-comprehension/vs-expression-translator-scalar-ops/spec.md` |

`datafusion-scan/type-mapping` is intentionally NOT changed — see decision-log entry [4].

## Dependencies

- GitHub issue #192 is the tracking issue; cite it in the implementing commit (`Closes #192`).
- The local Docker stack (`exasol`, `minio`, `iceberg-rest`) must be up for the E2E test. It is
  already running and healthy in this worktree's compose project.
- ADR `specs/_decision/011-fix-count-distinct-shard-cap.md`'s follow-up "Exasol-dialect CAST for the
  qualified wrapper" states `CHAR(n)` also renders as `VARCHAR(n)`. Change 2 supersedes that clause;
  `recorder-agent` must supersede it rather than leave both statements standing.
- Iceberg specification compliance: this change touches neither scanning, pushdown predicate
  semantics, nor Iceberg schema/type handling. It maps an Exasol-echoed pushdown-request type to an
  Exasol EMITS type and normalizes a group key's width; no Apache Iceberg spec section governs
  either, and it introduces no deviation.

## Implementation Tasks

1. *(Advisory, best-effort — NOT a gate on any later task.)* Capture the real pushdown payload for
   the four #192 shapes with `scripts/capture-pushdown-payload.sh` against the running Docker stack.
   Record for each the `selectListDataTypes` / `groupBy` JSON, which adapter path handles it
   (`project_columns`, `group_key_exasol_types`, `constant_projection_sql`), and the exact error. Use
   `c_varchar` as the string column and `c_decimal_a` as the CASE subject. If a captured shape routes
   to a path this plan did not name, note it and cover that path's type resolution too. This needs a
   full `.so` release build inside Docker; if that build is slow or unavailable, record what was
   captured and proceed — the declared types are already established from the live native probe
   (Design table above), and Tasks 2-8 do not depend on this task's output.
2. Add failing unit tests for the new CHAR arm to `exasol_type_from_json`'s test module in
   `crates/lakehouse-engine/src/adapter/pushdown/support.rs`, next to
   `exasol_type_from_json_propagates_ascii_character_set` and in its style: `CHAR` with an ASCII
   `characterSet` → `CHAR(3) ASCII`, with `UTF8` and with no `characterSet` → bare `CHAR(20)`, and
   an over-maximum `size` → `CHAR(2000)`. Each MUST fail on current code.
3. Add failing unit tests through the real request paths, exercising pushdown-request JSON rather
   than the bare function: in `support.rs`, a `selectList` with a `function_scalar_cast` to CHAR(20)
   plus a `selectListDataTypes` CHAR entry resolving to a `CHAR(20)` EMITS type through
   `project_columns` (facet B, asserting the item stays a rendered `_LH_PROJ_*` expression and does
   not fall back to the full base row); in `grouped_agg.rs`, an equal-length CASE group key
   resolving to `CHAR(3) ASCII` through `group_key_exasol_types` (facet A), a `literal_string`
   select item rendering `CAST('X' AS CHAR(1) ASCII)` through `constant_projection_sql` (facet C),
   and a `MIN(CAST(<col> AS CHAR(20)))` grouped item asserting the partial EMITS declares
   `"PARTIAL_min_0" CHAR(20)` and the merge item casts to `CHAR(20)`; a VARCHAR control group key
   still resolving to `VARCHAR(10)`; and in `support.rs` a `predicate_like` over a `CHAR(n)`-typed
   column still pushing down unchanged through `like_subject_type_guard`.
4. Add the `"char"` match arm to `exasol_type_from_json`
   (`crates/lakehouse-engine/src/adapter/pushdown/support.rs:778`) ahead of the catch-all: read
   `size` (defaulting within CHAR's valid range), cap it at 2000, read `characterSet` and append
   `" ASCII"` case-insensitively exactly as the VARCHAR arm does, and update the catch-all's
   "VARCHAR, CHAR, and all others" comment so it no longer claims CHAR. Then confirm each live
   consumer tolerates a `CHAR(...)` string: `project_columns` (`is_valid_emits_output_type` rejects
   only `TIMESTAMP WITH LOCAL TIME ZONE`, so `CHAR(n)` passes — `support.rs:592`),
   `group_key_exasol_types` and `constant_projection_sql` (`CAST(… AS CHAR(n) ASCII)` is valid
   Exasol syntax — live-verified), `detect_group_by_aggregates` / `partial_emits_items` (a
   `CHAR(20)` partial EMITS column and merge cast for `MIN`/`MAX` over an expression argument — see
   Task 3), and `file_resolution.rs`'s `CAST(NULL AS <type>)`. Note that `like_subject_type_guard`
   already classifies a `CHAR`-prefixed type as a string subject (`support.rs:546`), so the LIKE path
   is unaffected, and that `coerce_batch_to_exa_types` already routes CHAR to the `Utf8` path
   (`scan/emit.rs:107-113`), so the raw-scan emit path needs no change. [expert]
5. Add a `CHAR` case to `render_cast_target`'s `Dialect::Exasol` arm
   (`crates/vs-expression/src/lib.rs:116-131`): split the current `"VARCHAR" | "CHAR"` arm so the
   Exasol side renders `CHAR({size})` for a CHAR target, appending `" ASCII"` when the node's
   `dataType.characterSet` equals `ASCII` case-insensitively; keep the `size`-absent fallback and the
   documented no-clamp rule; leave the `Dialect::DataFusion` side and the Exasol `VARCHAR` rendering
   byte-identical.

   The one shared arm fixes all THREE seam-2 consumers at once, but **five** existing tests assert
   the OLD CHAR→VARCHAR collapsing behavior and one doc comment cites a renamed test. Retarget every
   one — preserve each test's original guarding intent, do not delete or weaken any assertion:

   | # | Location | Currently asserts | Retarget to |
   |---|---|---|---|
   | 1 | `vs-expression/src/lib.rs:1610` `renders_cast_char_as_varchar` | `CAST("X" AS VARCHAR)` (DataFusion dialect — still CORRECT) | Rename to name the DataFusion dialect explicitly (`renders_cast_char_as_datafusion_varchar`); correct its "maps CHAR to VARCHAR everywhere" comment; KEEP the assertion; add an Exasol-dialect twin asserting `CAST("X" AS CHAR(3) ASCII)` |
   | 2 | `joins/sql_builders.rs:1719` `qualified_count_distinct_cast_char_renders_length_qualified_exasol_varchar` | `COUNT(DISTINCT CAST("LHS_T0"."C_VARCHAR" AS VARCHAR(20)))` | `CHAR(20) ASCII`; keep the never-bare/never-length-less assertions and the full qualified shape |
   | 3 | `vs-expression/src/lib.rs:2869` `renders_cast_char_exasol_dialect_includes_length` | `render_expression_exasol` → `CAST("X" AS VARCHAR(3))` for `{"type":"CHAR","size":3,"characterSet":"ASCII"}` | `CAST("X" AS CHAR(3) ASCII)`; keep the length-qualification intent the name states |
   | 4 | `vs-expression/src/lib.rs:2889` `cast_char_target_diverges_between_dialects` | DataFusion `CAST("C_VARCHAR" AS VARCHAR)` **and** Exasol `CAST("C_VARCHAR" AS VARCHAR(20))` | Retarget the Exasol-side assertion to `CAST("C_VARCHAR" AS CHAR(20) ASCII)`; KEEP the DataFusion-side assertion and KEEP the test as a divergence guard — do NOT delete it. Its purpose (the two dialects must diverge) is now stronger, not weaker |
   | 5 | `grouped_agg.rs:1134` `scalar_over_merge_casts_to_length_qualified_exasol_varchar` | `render_scalar_over_merge` output `sql.contains("VARCHAR(20)")` for `CAST(SUM(x) AS CHAR(20) ASCII)` | `CHAR(20) ASCII`; rename accordingly. Do NOT weaken or drop the length-qualification assertion — it guards exactly the invariant ADR 011 established. This is the third seam-2 consumer's only guard |
   | 6 | `support.rs:891` (doc comment on `exasol_type_from_json_propagates_ascii_character_set`) | Cites `vs-expression`'s `renders_cast_char_as_varchar` **by name** as the confirming test | Update the citation to the test's new name (row 1), since that test is being renamed |

   Then add two NEW tests: the N-scan join wrapper's select list carrying the same CAST item, and a
   nested-CAST shape `CAST(CAST(SUM(x) AS CHAR(20) ASCII) AS CHAR(20) ASCII)` through
   `render_scalar_over_merge`, proving the CHAR case renders correctly when it recurses into itself.
   [expert]
6. Pad the DataFusion-side group-key expression when its declared type is `CHAR(n)`, in the grouped
   arm of `crates/lakehouse-engine/src/adapter/pushdown/mod.rs`. Move the
   `group_key_exasol_types(...)` call (currently `mod.rs:395`) ABOVE the `spec_template` construction
   (`mod.rs:385`), derive a padded copy of `group_keys` where each slot whose declared type starts
   with `CHAR(` becomes

   ```
   CASE WHEN character_length(<fragment>) < n THEN rpad(<fragment>, n) ELSE <fragment> END
   ```

   and pass ONLY that padded copy into `spec_template.common.group_keys`. The fragment is spliced
   into all three positions; that is verified to parse and evaluate even when the fragment is itself
   a `CASE` expression. Parse `n` out of the declared type so BOTH forms work — `CHAR(3)` and
   `CHAR(3) ASCII` — by reading the digits between `(` and `)` rather than trimming a trailing `)`
   off the whole string, which would fail on the ` ASCII` suffix and silently skip padding on every
   ASCII-declared CHAR key (the #192 primary shape). Keep the unpadded `group_keys` as the argument
   to `build_grouped_order_by_clause`, `group_key_exasol_types`, and
   `build_grouped_aggregate_scan_sql` — `build_grouped_order_by_clause` matches `orderBy` elements
   against unpadded rendered SQL and would otherwise return `Unresolvable` and decline the pushdown.
   Add unit tests asserting: the emitted `ScanSpec.common.group_keys` carries the pad wrapper for a
   `CHAR(20)`-declared key; a `CHAR(3) ASCII`-declared key is padded to 3 (proving the suffix-tolerant
   `n` parse); the pad expression preserves the over-length value unmodified — assert the rendered
   fragment contains no truncating construct and keeps the `ELSE <fragment>` branch, so an over-length
   value reaches Exasol's `CAST("GK_0" AS CHAR(n))` intact and its 22001 error still fires (the error
   path itself is only observable live, covered by Task 8); a `VARCHAR`-declared key is unpadded; the
   outer wrapper still emits `CAST("GK_0" AS CHAR(20))`; and an `ORDER BY` on a `CHAR(20)`-declared
   group key still resolves to its output ordinal instead of declining. [expert]
7. Add a minimal trailing-space seed table for the padding-equality E2E test, using the existing
   `create_and_append_files` helper in `crates/lakehouse-engine/tests/common/seed.rs` and the same
   pattern as the `labels` seed: one string column holding FOUR values — `'ab'`, `'ab   '`, `'cd'`,
   and one value **longer than 20 characters** (e.g. the 25-character
   `'over-length-value-abcdefg'`) — registered from `seed_events`. The over-length row is what makes
   the truncation-error scenario testable; the other three carry the merge case. Sizing it at 25
   characters lets one table serve both Task 8 queries: a `CHAR(30)` cast fits every value and
   exercises the merge, while a `CHAR(20)` cast makes exactly this row over-length and must raise
   Exasol's 22001. Do NOT add rows to `events`, `labels`, or `regions` — `SEED_TOTAL_ROWS`,
   `SEED_LABELS_ROWS`, and the partition-pruning constants are asserted by existing tests.
8. Add the E2E regression tests to `crates/lakehouse-engine/tests/e2e_capability_test.rs` (gated by
   the existing `exasol-e2e` feature). Over the seeded `events` table, run the four #192 shapes
   through the Virtual Schema — the equal-length CASE GROUP BY, `CAST(name AS CHAR(20))`, the
   bare-literal GROUP BY key, and the VARCHAR control — asserting rows return rather than a
   `Data type mismatch` error and that the `CHAR(20)` values are space-padded to exactly 20
   characters. Over the Task 7 table, run three queries:
   - `SELECT CAST(<col> AS CHAR(30)) g, COUNT(*) ... GROUP BY 1` — assert the `'ab'`/`'ab   '` pair
     merges into exactly ONE row with count 2 and that three rows come back in total, matching what
     Exasol computes natively over the same values. `CHAR(30)` fits every seeded value, so this
     isolates the merge behavior from the truncation behavior.
   - `SELECT CAST(<col> AS CHAR(20)) g, COUNT(*) ... GROUP BY 1` — the over-length group key. Assert
     the statement FAILS with Exasol's truncation error (SQL state 22001, `data exception - string
     data, right truncation`), exactly as the same statement fails on a native Exasol table over the
     same values. Assert specifically that it does NOT return rows — a returned result set would mean
     the pad silently truncated the 25-character value into a wrong, merged group. Compare against the
     native control to prove equivalence rather than merely asserting "some error".
   - `SELECT CAST(<col> AS CHAR(20)) FROM ...` with no GROUP BY — the projection facet's over-length
     path, where the width is enforced at UDF emit rather than by the outer cast. Assert a clean
     failure rather than a silently truncated value. This is the assertion that confirms the LUA-probe
     result carries over to the Rust SLC's `emit_batch` Arrow IPC path; if the Rust path is found to
     truncate instead of erroring, that is a divergence to record as a cited tracked exception in the
     spec, never to leave silent.
9. Run the gates: `cargo test`, `cargo clippy --all-targets`, `cargo fmt`.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 1, Task 2, Task 3, Task 7 |
| Group B | Task 4, Task 5 |
| Group C | Task 8 |

Sequential dependencies:
- Group A → Group B (the fixes land once the failing tests exist; Task 1 is advisory and gates
  nothing)
- Group B → Task 6 (padding reads the declared type the Task 4 arm produces)
- Task 6 + Task 7 → Group C → Task 9
- Task 4 and Task 5 are independent of each other (different crates, different seams) and may run
  concurrently.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Comment | `crates/lakehouse-engine/src/adapter/pushdown/support.rs:805` | The catch-all's "VARCHAR, CHAR, and all others" comment is wrong once CHAR has its own arm; corrected in Task 4 |
| Comment | `crates/vs-expression/src/lib.rs:1611-1613` | The `renders_cast_char_as_varchar` test's "maps CHAR to VARCHAR everywhere" comment is wrong once the Exasol arm renders CHAR; corrected in Task 5 |
| Comment | `crates/lakehouse-engine/src/adapter/pushdown/support.rs:891` | The doc comment on `exasol_type_from_json_propagates_ascii_character_set` cites `vs-expression`'s `renders_cast_char_as_varchar` by name; that test is renamed in Task 5, so the citation goes stale. Updated in Task 5 (row 6 of its table) |
| Test name + assertions | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs:1719` | `qualified_count_distinct_cast_char_renders_length_qualified_exasol_varchar` asserts the buggy `VARCHAR(20)` rendering for a CHAR target; retargeted (not deleted) in Task 5, preserving its never-bare intent |
| Test name + assertions | `crates/vs-expression/src/lib.rs:2869` | `renders_cast_char_exasol_dialect_includes_length` asserts the buggy Exasol-dialect `CAST("X" AS VARCHAR(3))` for a CHAR dataType; retargeted (not deleted) in Task 5 |
| Assertion | `crates/vs-expression/src/lib.rs:2889` | `cast_char_target_diverges_between_dialects`'s Exasol-side assertion expects `VARCHAR(20)`; retargeted in Task 5. The TEST is retained deliberately — it is the divergence guard between the two dialect arms |
| Test name + assertions | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs:1134` | `scalar_over_merge_casts_to_length_qualified_exasol_varchar` asserts `VARCHAR(20)` for the third seam-2 consumer; retargeted (not deleted) in Task 5, keeping the length-qualification guard ADR 011 established |
| — | — | No function or module is obsoleted |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A CHAR-declared type renders as CHAR | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `exasol_type_from_json_renders_char_type` |
| A CHAR-declared ASCII type carries the ASCII suffix | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `exasol_type_from_json_propagates_char_ascii_character_set` |
| A CHAR size above Exasol's maximum is capped at 2,000 | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `exasol_type_from_json_caps_char_size_at_exasol_maximum` |
| An explicit CAST-to-CHAR select-list item projects with a CHAR EMITS type | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `project_columns_emits_char_type_for_cast_to_char_item` |
| An equal-length CASE group key resolves to a CHAR group-key type | Unit | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs` | `group_key_exasol_types_resolves_char_case_key` |
| A CHAR group key is blank-padded to its declared width before grouping | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `char_group_key_is_blank_padded_in_scan_spec_only` |
| The CHAR group-key pad preserves NULL, parses the declared width, and leaves the identity list unpadded | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `char_ascii_group_key_pad_width_parses_past_suffix`, `char_group_key_pad_keeps_unpadded_identity_list` |
| A CHAR group key over trailing-space data groups identically to native Exasol | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `char_group_key_merges_trailing_space_variants_like_native` |
| An over-length CHAR group-key value reaches Exasol unmodified | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `char_group_key_pad_leaves_over_length_value_unmodified` |
| An over-length CHAR group-key value raises Exasol's truncation error instead of merging a truncated group | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `over_length_char_group_key_raises_truncation_error_like_native` |
| An over-length CHAR projection value fails cleanly rather than truncating | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `over_length_char_projection_fails_cleanly` |
| A bare string-literal group-key projection casts to CHAR | Unit | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs` | `constant_projection_casts_literal_to_char` |
| A CAST-to-CHAR item inside an Exasol-parsed wrapper declares a CHAR column (all three seam-2 consumers) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`; `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs` | `qualified_count_distinct_cast_char_renders_exasol_char_target`, `n_scan_join_select_list_renders_exasol_char_target`, `scalar_over_merge_casts_to_exasol_char_target` |
| A nested CAST-to-CHAR over a merged aggregate renders CHAR at both levels | Unit | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs` | `scalar_over_merge_nested_char_cast_renders_char_at_both_levels` |
| The two dialects still diverge on a CHAR target | Unit | `crates/vs-expression/src/lib.rs` | `cast_char_target_diverges_between_dialects` (retained divergence guard, Exasol side retargeted) |
| A MIN or MAX over a CHAR-typed expression declares a CHAR partial column | Unit | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs` | `min_over_char_expression_declares_char_partial_and_merge_cast` |
| A VARCHAR-declared type is unaffected | Unit | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs` | `group_key_exasol_types_resolves_varchar_key_unchanged` |
| A CHAR-typed LIKE subject keeps pushing down unchanged | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `like_guard_char_subject_unchanged` |
| The four #192 query shapes execute end to end | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `char_declared_pushdown_shapes_match_native` |
| The Exasol dialect renders a CHAR CAST target as CHAR, not VARCHAR | Unit | `crates/vs-expression/src/lib.rs` | `renders_cast_char_as_exasol_char`, `renders_cast_char_exasol_dialect_includes_length` (retargeted from `VARCHAR(3)` to `CHAR(3) ASCII`) |
| CAST translates to DataFusion CAST syntax | Unit | `crates/vs-expression/src/lib.rs` | `renders_cast_char_as_datafusion_varchar` (control, renamed from `renders_cast_char_as_varchar`) |

Unit tests are the right form for every scenario except the end-to-end ones: the type derivation, the
CAST rendering, and the projection and group-key resolution are pure computation over JSON with no
I/O. Scenarios for facets A, B, and C exercise pushdown-request JSON through the real resolution entry
points, not the bare `exasol_type_from_json` function, so they prove the wiring and not just the arm.
Four scenarios must be E2E runs: only Exasol's own type checker can confirm the emitted `CHAR(n)`
declaration is accepted and that CHAR blank padding matches native results; only a real grouped query
over trailing-whitespace data can prove the padded group key merges the rows Exasol merges; and only a
live run can prove that an over-length value raises Exasol's 22001 truncation error on both the
grouped and the projection path rather than being silently truncated. The over-length UNIT tests
deliberately assert a weaker property — that the rendered pad expression carries the over-length value
through unmodified — because a plan-level test can inspect only the SQL text, not Exasol's evaluation
of it; the error path itself is E2E-only, and the coverage table pairs each unit test with its E2E
counterpart accordingly. Every test MUST fail on current code except the three controls
(`group_key_exasol_types_resolves_varchar_key_unchanged`, `like_guard_char_subject_unchanged`, and
`renders_cast_char_as_datafusion_varchar`), which MUST pass before and after.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-planning-char-type-declaration | `cargo test -p lakehouse-engine char` | All CHAR unit tests pass; the control tests still pass |
| sql-comprehension/vs-expression-translator-scalar-ops | `cargo test -p vs-expression cast_char` | The DataFusion-dialect test asserts bare `VARCHAR`; every Exasol-dialect test asserts `CHAR(n) ASCII`; `cast_char_target_diverges_between_dialects` still passes as a divergence guard |
| sql-comprehension/vs-expression-translator-scalar-ops | `cargo test -p lakehouse-engine scalar_over_merge` | The grouped-merge wrapper (third seam-2 consumer) renders `CHAR(20) ASCII`, including the nested-CAST shape |
| vs-adapter/pushdown-planning-char-type-declaration | `scripts/capture-pushdown-payload.sh 'SELECT CAST(c_varchar AS CHAR(3)) G, COUNT(*) FROM {table} GROUP BY 1'` | The over-length shape, where `c_varchar` values exceed 3 characters. Both before and after the fix the statement MUST fail — after the fix with Exasol's own `data exception - string data, right truncation` (SQL state 22001), the same error a native table raises. It MUST NOT return rows |
| vs-adapter/pushdown-planning-char-type-declaration | `scripts/capture-pushdown-payload.sh 'SELECT CASE WHEN c_decimal_a < 0 THEN '"'"'NEG'"'"' ELSE '"'"'POS'"'"' END G, COUNT(*) FROM {table} GROUP BY 1'` | Before the fix: `Data type mismatch ... Expected CHAR(3) ASCII, but got VARCHAR(3) ASCII`. After: two rows, and the `EXPLAIN VIRTUAL` output shows `CHAR(3) ASCII` in the EMITS clause and `CAST("GK_0" AS CHAR(3) ASCII)` in the outer wrapper |
| vs-adapter/pushdown-planning-char-type-declaration | `scripts/capture-pushdown-payload.sh 'SELECT id, CAST(c_varchar AS CHAR(20)) FROM {table} WHERE id <= 2'` | Before the fix: `Data type mismatch ... Expected CHAR(20) UTF8, but got VARCHAR(20) UTF8`. After: two rows whose second column is space-padded to 20 characters |
| vs-adapter/pushdown-planning-char-type-declaration | `scripts/capture-pushdown-payload.sh 'SELECT '"'"'X'"'"' G, COUNT(*) FROM {table} GROUP BY 1'` | Before the fix: `Data type mismatch ... Expected CHAR(1) ASCII, but got VARCHAR(1) ASCII`. After: one row `X, 12` |
| vs-adapter/pushdown-planning-char-type-declaration | `scripts/capture-pushdown-payload.sh 'SELECT CAST(c_varchar AS CHAR(20)) G, COUNT(DISTINCT id), COUNT(DISTINCT c_varchar) FROM {table} GROUP BY 1'` | The qualified-fallback shape. Before the fix: `Data type mismatch ... Expected CHAR(20)`. After: rows return, and the `EXPLAIN VIRTUAL` wrapper SELECT shows `CAST("LHS_T0"."C_VARCHAR" AS CHAR(20))` |
| vs-adapter/pushdown-planning-char-type-declaration | `scripts/capture-pushdown-payload.sh 'SELECT c_varchar, COUNT(*) FROM {table} GROUP BY 1'` | Unchanged before and after — the VARCHAR control keeps returning its grouped counts |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures against the local Exasol Docker stack |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt` | No changes |

Build note: the `.so` build is `make cross-musl-udf-build` (inside `rust:1.94-bookworm`), which the
capture script and the E2E harness invoke themselves. Never run host `cargo build --release`.
