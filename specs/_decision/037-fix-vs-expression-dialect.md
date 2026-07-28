# Decisions: fix-vs-expression-dialect

## ADR: One gated dispatch owns the Exasol-dialect rendering rule

**ID:** exasol-dialect-render-what-exasol-sent
**Plan:** fix-vs-expression-dialect
**Status:** Accepted

### Context

`crates/vs-expression` threads a `Dialect` parameter through every node of one recursive walker,
but only `render_cast_target` read it; every other arm rendered the DataFusion form
unconditionally. The same "which parser reads this fragment" question had been answered
independently in `render_cast_target`, in the `MOD` arm (#197), and in the string-function arm
(#210), and left unanswered everywhere else — the failure mode behind issue #209.

### Decision

State one rule, "in the Exasol dialect, render what Exasol sent", and give it a single owner: a
guarded `function_scalar` arm, promoted to a gate ahead of the whole dispatch and driven by one
declaration. The math family, the field-shortcut date functions, `WEEK`, the `*_BETWEEN` family,
`DATE_TRUNC`, `TO_DATE`, `TO_TIMESTAMP`, `GREATEST`, `LEAST`, `NULLIF`, `NULLIFZERO`, and
`ZEROIFNULL` are declared there instead of gaining private dialect branches. Constructs the
declaration marks `Shaped`, and node types outside `function_scalar` branch inline with
`match dialect`, the shape #197's `MOD` fix already used.

### Options Considered

| Option | Verdict |
|--------|---------|
| One gated declaration owning the rule | ✓ Chosen — a reader finds the whole rule and its exceptions in one place |
| A per-dialect name lookup table keyed by function name | ✗ Rejected — a second mechanism, and cannot express the `EXTRACT`/`REGEXP_LIKE`/timestamp-literal shape changes |
| A `Dialect` trait with two implementations | ✗ Rejected — the dialects differ at roughly a dozen of some fifty arms; two full implementations would duplicate the rest and invite the same silent drift |
| A private `match dialect` inside each affected arm, following #197 everywhere | ✗ Rejected — scatters one decision across ten arms with no obvious inheritance point for the next arm |

### Consequences

A reader finds every translated name and its Exasol-dialect form in one declaration instead of
inferring it from arm order. The exclusions are stated where the names are declared, not implied
by which arm sits higher in a 500-line `match` — the exact ordering assumption that left `SIGN`
rendering `signum(...)` in the Exasol dialect.

## ADR: The verbatim rule applies uniformly, not only to arms that currently fail

**ID:** verbatim-rule-applies-uniformly-not-minimal-diff
**Plan:** fix-vs-expression-dialect
**Status:** Accepted

### Context

Some Exasol-native scalar functions (`NULLIFZERO`, `ZEROIFNULL`, `GREATEST`, `LEAST`,
`DATE_TRUNC`, `TO_DATE`, `TO_TIMESTAMP`, `DAYS_BETWEEN`, most of the math family) already parsed
in Exasol despite rendering their DataFusion form, because their DataFusion name happened to be
lowercase-compatible or the rendering happened not to diverge syntactically.

### Decision

Apply the verbatim rule to every Exasol-native scalar function, including the ones whose current
rendering already parses in Exasol, rather than changing only the arms that currently produce a
compilation error.

### Options Considered

| Option | Verdict |
|--------|---------|
| Apply the rule uniformly to every Exasol-native function | ✓ Chosen — needs no per-function verification once applied |
| Change only the arms that currently hard-fail (smaller diff) | ✗ Rejected — a rule applied to some arms and not others is not one a future reader can apply; they would have to test each name against live Exasol to learn whether its current rendering is principled or merely lucky, which is how the `*_BETWEEN` family shipped broken |

### Consequences

Every math name in the arm was verified live on Exasol 2025.2.1 to exist natively (`ABS`,
`FLOOR`, `CEIL`, `SQRT`, `EXP`, `LN`, `DEGREES`, `RADIANS`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`,
`ATAN`, `SINH`, `COSH`, `TANH`, `COT`, `ROUND`, `TRUNC`, `LOG`, `POWER`, `ATAN2`), so folding them
into the verbatim rule changes no result while removing the exception list a reader would
otherwise have to maintain.

## ADR: The Exasol dialect imposes no arity check

**ID:** exasol-dialect-no-arity-check
**Plan:** fix-vs-expression-dialect
**Status:** Accepted

### Context

Several DataFusion-dialect arms validate argument count before rendering. Whether the
Exasol-dialect verbatim arm should repeat that validation was an open question extending the
existing precedent from the #210 string-function fix.

### Decision

The Exasol-dialect verbatim arm forwards the argument list unchanged and does not validate
argument count, even where the DataFusion-dialect arm does.

### Options Considered

| Option | Verdict |
|--------|---------|
| No arity check on the Exasol path | ✓ Chosen — Exasol's compiler emitted a call its own engine accepts, so a translator-side arity check can only reject valid input |
| Keep each family's existing arity check in both dialects | ✗ Rejected — would reject calls Exasol itself already validated and can evaluate |

### Consequences

Extends the rule `vs-adapter/pushdown-planning-string-fn-type-coercion` already depends on: that
feature declines a three-argument `INSTR(s, sub, start)` from the DataFusion scan precisely
because the Exasol wrapper can still evaluate it verbatim. Applying the same rule to every other
family keeps one behavior instead of two.

## ADR: A declaration-derived sweep test enforces the rendering rule structurally

**ID:** sweep-test-enforces-declaration-derived-coverage
**Plan:** fix-vs-expression-dialect
**Status:** Accepted

### Context

A per-family paired-dialect test only covers arms someone remembered to test — the exact failure
mode that shipped the `*_BETWEEN` family broken despite per-function E2E parity tests, because
every one of those tests exercised only the DataFusion path.

### Decision

Add `exasol_dialect_renders_declared_verbatim_surface`, a unit test whose `function_scalar` rows
are iterated from the crate's one name declaration rather than hand-written. The test fails
naming any declared name with no fixture, and any fixture whose name is not declared. Six
constructs outside the `<NAME>(<args>)` shape (operator wire names, `MOD`, `CONCAT`, `CAST`, the
`REGEXP_LIKE` alternate encoding, `function_scalar` named `CASE`) each assert the expected string
their fixture declares; every other declared name asserts `<NAME>(<rendered args>)` derived from
the node's own uppercased name.

### Options Considered

| Option | Verdict |
|--------|---------|
| Derive sweep rows from the one declaration | ✓ Chosen — coverage is structural: a declared name without a fixture fails by name, and a declaration cannot fall behind the dispatch it gates |
| Rely on per-family paired-dialect tests alone | ✗ Rejected — only covers arms someone remembered to test |
| A hand-written deny-list of DataFusion-only tokens as the primary assertion | ✗ Rejected — can only catch arms already known today; "every DataFusion-only token" is an unbounded, untestable set |
| Keep the table hand-written and assert declared-set membership over its own rows | ✗ Rejected — only checks names someone already listed; a name absent from both the table and the set still passes |

### Consequences

The test enumerates what the crate translates rather than what someone remembered to list, so a
name cannot be forgotten twice (once from the declaration, once from the sweep). The guarantee is
bounded and stated precisely: it cannot enforce that every node type outside `function_scalar` has
a row, because those are matched on the `type` string in the outer walker and are not declared
anywhere — five such rows stay reviewed, not derived.

## ADR: One declaration gates the function_scalar dispatch and drives the sweep table

**ID:** declaration-gates-dispatch-and-sweep
**Plan:** fix-vs-expression-dialect
**Status:** Accepted
**Supersedes:** exasol-dialect-render-what-exasol-sent

### Context

An inline pattern list in the guarded arm (the shape #210 shipped, and the mechanism the original
gate decision assumed) is itself a second copy of the translated-name set, alongside the
DataFusion arms' own `match fn_name.as_str()`. Nothing enforced agreement between the two, so a
name present in a DataFusion arm but absent from the inline list would fall through to DataFusion
rendering on the Exasol path with no error — reproducing the exact defect class the plan exists to
close.

### Decision

Declare the translated `function_scalar` surface exactly once:
`const TRANSLATED_SCALAR_FNS: &[(&str, ExasolForm)]`, one row per translated name, where
`enum ExasolForm { VerbatimCall, Shaped }` states what the Exasol dialect does with that name. The
declaration does three jobs: it gates the dispatch (a name absent from it is declined in both
dialects before any per-name arm is reached), it renders the Exasol dialect directly for
`VerbatimCall` names (so arm order stops carrying dialect precedence), and it drives the sweep
test that iterates the declared names.

### Options Considered

| Option | Verdict |
|--------|---------|
| One declaration serving as gate, renderer, and sweep source | ✓ Chosen — disagreement between dispatch and declaration becomes structurally impossible in both directions |
| Spell the eligible names inline in the guarded arm's pattern list | ✗ Rejected — a second copy of the translated-name set with nothing to keep it in sync |
| A flat `const EXASOL_VERBATIM_FNS: &[&str]` read by the guard and a hand-written sweep table | ✗ Rejected — removes the duplicate name list but leaves two independent ways to forget a name (omit it from the set, or omit its sweep row) |
| Replace string dispatch with a per-name enum, making every link an exhaustive `match` (compile-enforced) | ✗ Rejected on cost — roughly 160 lines of mechanical boilerplate and a rewrite of all ~80 arm patterns in a 3,351-line file, to convert one test failure into one compile failure, when the gate already delivers the stronger half |

### Consequences

A `function_scalar` name added to a DataFusion arm without a declaration row cannot be translated
in either dialect, so the author's own test for the new function fails immediately. A declared
`VerbatimCall` name cannot diverge from what Exasol sent, because its Exasol rendering comes from
the declaration's own branch, which no per-name arm can reach. What remains reviewed rather than
derived is narrow: the five non-`function_scalar` node types, matched on the `type` string in the
outer walker.

## ADR: Withdraw the four now-family capabilities rather than re-render them

**ID:** withdraw-now-family-capabilities
**Plan:** fix-vs-expression-dialect
**Status:** Accepted

### Context

`CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, and `SYSTIMESTAMP` parse as valid SQL in both
dialects today, but the node-local scan cannot produce Exasol's value for any of them. Exasol's
four names are three distinct semantics over one instant (session zone, database zone, and
`TO_DATE` of each), which needs `SESSIONTIMEZONE` and `DBTIMEZONE` to render correctly. Neither
value reaches the scan UDF: the pushdown request carries no zone, `CommonScanSpec` carries no
temporal field, the scan opens no connect-back session, and the SDK's `UdfContext` exposes no
clock or zone — so the scan reads its own container clock in UTC, once independently per shard,
while Exasol's now-family is statement-constant. Measured live on the pinned
`exasol/docker-db:2025.2.1` container (`DBTIMEZONE`/`SESSIONTIMEZONE` both `EUROPE/BERLIN` over a
UTC container clock): a pushed `SYSTIMESTAMP` returned `15:02:02.716` against `17:02:03.141` from
Exasol's own `SYSTIMESTAMP`, and a `GROUP BY SYSTIMESTAMP` over a two-file table returned two
distinct timestamps against one statement-constant native value.

### Decision

Withdraw `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, and `FN_SYSTIMESTAMP` from the
advertised capability set, so Exasol evaluates all four itself, once, in its own zones. Update
`docs/capabilities.md` to move the four names into the "Handled by Exasol" section.

### Options Considered

| Option | Verdict |
|--------|---------|
| Withdraw all four capabilities | ✓ Chosen — Exasol never delegates a capability the adapter does not advertise, so withdrawal is the safe direction and the only one requiring no further verification |
| Accept the divergence and file an issue | ✗ Rejected by the human — leaves an advertised capability returning a wrong value; a silent wrong answer is worse than a lost optimization |
| Plumb `SESSIONTIMEZONE`, `DBTIMEZONE`, and a statement-level anchor into the scan spec over a new connect-back call | ✗ Not rejected on merit — the only route to correct pushdown, but a scan-spec and connect-back change far outside a rendering fix; recorded as tracked future work (issue #263) |
| Withdraw only the two collapsed names (`SYSDATE`, `SYSTIMESTAMP`), keeping `CURRENT_DATE`/`CURRENT_TIMESTAMP` | ✗ Rejected — all four are wrong on the scan path, not only the collapsed pair, because the scan reads a UTC container clock in no Exasol zone |

### Consequences

The filter position loses real pushdown for all four names; a predicate containing one of them is
no longer pushed and is applied by Exasol over the returned rows instead. The select-list position
loses pushdown for three of the four (only a select-list `CURRENT_TIMESTAMP` already widened to
the full-row fallback via a pre-existing type check). Every other date/time capability stays
advertised, because each takes its datetime from its own arguments rather than from a clock. The
pushdown being given up was producing wrong values, so the cost is a lost wrong optimization, not
a lost correct one. Restoring now-family pushdown with full time-zone fidelity is tracked in issue
#263, cited in `plan.md` § Non-Goals.

## ADR: Delete the now-family's translator arms rather than leave them unreachable

**ID:** delete-now-family-translations-not-unreachable-arms
**Plan:** fix-vs-expression-dialect
**Status:** Accepted

### Context

Once the now-family capabilities are withdrawn (see the withdrawal ADR above), the DataFusion arms
that rendered `current_date()`/`now()` for these four names become unreachable: every production
call site of all six translator entry points is fed raw pushdown-request JSON, and every tree
transformer in the codebase is structure-preserving and cannot introduce a function name — so once
the capability is gone, nothing can deliver such a node to the arm.

### Decision

Remove `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, and `SYSTIMESTAMP` from the crate's name
declaration and delete their two DataFusion-dialect rendering arms, so the gate declines all four
in both dialects with the same `unsupported scalar function: <name>` error every other
untranslated name produces.

### Options Considered

| Option | Verdict |
|--------|---------|
| Delete the arms | ✓ Chosen — converts a hypothetical stray node's failure mode from a silently wrong timestamp to a loud decline, and removes dead code the gate already makes unreachable |
| Keep a `BareKeyword` declared form and withdraw only the capability | ✗ Rejected — the Exasol-dialect rendering would be unreachable the moment the capability is gone, the same situation already ruled against for the `decimal_to_varchar_exasol` node |
| Declare the four names `Shaped` and keep the existing DataFusion arm, withdrawing only the capability | ✗ Rejected — leaves an advertised-set-exceeds-translated-set inversion inverted the wrong way, and forces the sweep test to assert `current_date()`/`now()` as Exasol-dialect renderings, contradicting the same test's DataFusion-only token deny-list |
| Leave the arms in place with a comment marking them unreachable | ✗ Rejected — inconsistent with deleting the #210 string guard for the same reason once the gate lands |

### Consequences

`ExasolForm` ships with only two variants, `VerbatimCall` and `Shaped` — no `BareKeyword` variant
is built. `crates/vs-expression` is a standalone crate shared with a sibling project; this removes
a translation a future consumer with different clock context might want, but restoring a
`VerbatimCall` row is a one-line change if that context is ever available, following the same
precedent ADR `014-add-date-arithmetic-pushdown` already set for `ADD_HOURS`/`ADD_MINUTES`.

## ADR: One declared name set closes the gate/dispatch mapping gap review found

**ID:** one-declared-name-set-for-gate-and-sweep-test
**Plan:** fix-vs-expression-dialect
**Status:** Accepted

### Context

Round-1 plan review found the design claimed "there is no mapping left that can drift" while the
translated-name set actually existed twice: once as the guarded arm's inline pattern list and once
across the DataFusion arms, with nothing enforcing agreement. Because the guarded arm precedes the
DataFusion arms, a name present in a DataFusion arm but missing from the inline list would fall
through to DataFusion rendering silently — the plan's original design rejected a name table for
"adding a second mechanism" without noticing the guarded arm was itself the second copy.

### Decision

Declare the eligible names once, in a flat set (`EXASOL_VERBATIM_FNS`) plus an `is_exasol_verbatim`
guard helper, read by both the guarded arm and the sweep assertion, so drift between dispatch and
declaration fails a test instead of passing silently.

### Options Considered

| Option | Verdict |
|--------|---------|
| One declared name set read by both the gate and the sweep | ✓ Chosen — removes the duplicate copy the original design had not noticed it introduced |
| Leave the inline pattern list as the sole source, add only a comment | ✗ Rejected — does not close the drift risk the finding identified |

### Consequences

Both dialects read one declared name set, so a name missing from it is undeclared everywhere
rather than merely undocumented. This flat-set shape was itself superseded by a later round of
review into a richer per-name declaration (see the superseding ADR), once a second completeness
gap was found in the flat set's coverage guarantee.

## ADR: The declaration is rewritten into a structural, per-name form after a second review round

**ID:** translated-scalar-fns-declaration-gates-dispatch-and-sweep
**Plan:** fix-vs-expression-dialect
**Status:** Accepted
**Supersedes:** one-declared-name-set-for-gate-and-sweep-test

### Context

Round-2 plan review found that the flat `EXASOL_VERBATIM_FNS` set and its hand-written sweep table
still let a future arm escape both: a new `SUBSTRING`, `NVL`, or `DATE_BIN` arm added with neither
a sweep row nor a set entry would pass every existing test and render DataFusion SQL on the Exasol
path, because nothing derived the table or the set from the DataFusion arms themselves. Several
plan sections had claimed an enforcement guarantee ("no divergence is possible") the specified test
could not actually deliver.

### Decision

Escalated to a human, who chose the structural fix over softening the claims and filing an issue.
The flat name set is rewritten into one declaration, `TRANSLATED_SCALAR_FNS`, carrying a per-name
`ExasolForm` that does three jobs at once: gates the `function_scalar` dispatch so an undeclared
name cannot be translated in either dialect, renders the Exasol dialect for declared names ahead of
every per-name arm, and is iterated by the sweep test so a declared name with no fixture fails by
name.

### Options Considered

| Option | Verdict |
|--------|---------|
| Rewrite into one gating, rendering, and sweep-driving declaration | ✓ Chosen by the human — closes both directions of drift structurally rather than by convention |
| Soften the plan's claims to match what the flat-set-plus-hand-written-table design actually delivered | ✗ Rejected — leaves the underlying gap (a new arm can still escape both the set and the table) unresolved |
| The compile-enforced per-name-enum variant considered in the same review round | ✗ Rejected on cost — recorded as considered in the gate-declaration ADR above |

### Consequences

The claims this plan makes are now scoped to exactly what the mechanism delivers: an undeclared
name is unreachable, a declared name cannot escape the sweep, a `VerbatimCall` name cannot diverge
from what Exasol sent, and a `Shaped` name rests on the sweep row the test forces it to have. The
declaration task was split out as its own no-op-refactor step, proved by the unchanged test suite
passing before any behavior change landed on top of it.
