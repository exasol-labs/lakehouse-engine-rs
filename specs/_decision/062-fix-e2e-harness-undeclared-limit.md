# Decisions: fix-e2e-harness-undeclared-limit

## ADR: Replace the opt-out row-cap knob with an opt-in one

**ID:** e2e-harness-opt-in-row-cap
**Plan:** fix-e2e-harness-undeclared-limit
**Status:** Accepted

### Context

The shared E2E WebSocket test client attached an invented `resultSetMaxRows: 10000` to every
statement by default, requiring callers to opt OUT via `ExaConn::unbounded_result_sets()` when a
test needed no cap. Three present-day callers need a declarable cap instead: the live
capped-vs-uncapped measurement and its `CAPTURE_RESULT_SET_MAX_ROWS` capture knob, a regression
test pinning the measured injection surface, and the per-test remediation rule that re-caps an
individual failing test against a filed issue.

### Decision

Delete `ExaConn::unbounded_result_sets()` and add `ExaConn::capped_result_sets(max_rows: u32)`.
`connect_inner` defaults `result_set_max_rows` to `0` — Exasol's own documented "no limit" default
— and both `execute` and `try_execute` keep sending the `resultSetMaxRows` attribute
unconditionally.

### Options Considered

| Option | Verdict |
|--------|---------|
| Opt-in `capped_result_sets(n)`, default uncapped | ✓ Chosen — the cap becomes visible where it is used and absent where it is not |
| Keep `unbounded_result_sets` as a no-op for source compatibility | ✗ Rejected — dead code that invites belief a cap still exists |
| Delete the knob entirely (YAGNI) | ✗ Rejected — three present-day callers need a declarable cap |

### Consequences

A test that declares no cap now visibly means "no limit," and a test that needs a cap must say so
explicitly at its own call site instead of relying on a silent harness-wide default.

## ADR: Phase 4 remediates every default-flip-unmasked test through one repeated per-binary procedure

**ID:** e2e-harness-phase4-per-binary-procedure
**Plan:** fix-e2e-harness-undeclared-limit
**Status:** Accepted

### Context

Flipping the harness's default `resultSetMaxRows` from `10000` to `0` was expected to unmask
currently-passing E2E tests that secretly depended on the injected cap, with the total remediation
size unknown until the flip actually ran. The user's interview answer was to fix everything
unmasked rather than bound the work to a pre-enumerated list of shapes.

### Decision

Phase 4 carries one task per E2E binary, each running the same loop: run the binary under the
flipped default, then close every newly-red test by filing a GitHub issue and declaring
`capped_result_sets(n)` at that test's own call site. It enumerates no individual assertion fix,
classifies no test, and makes no production-code fix.

### Options Considered

| Option | Verdict |
|--------|---------|
| One repeated per-binary procedure, no pre-enumerated fix list | ✓ Chosen — matches "fix everything unmasked," avoids presenting a fabricated enumeration as authoritative |
| Enumerate expected failures in the plan up front | ✗ Rejected — the actual set is unknown until the flip runs |
| Bound remediation to the seven measured shapes, defer the rest | ✗ Rejected — contradicts "fix everything unmasked" |

### Consequences

Every binary gets an identical remediation pass with a single tracking-issue-per-recap paper
trail, but the plan's own size stays unknown until Phase 4 executes.

## ADR: Phase 4 remediation splits into an in-scope real-fix branch and an out-of-scope issue-plus-recap branch

**ID:** e2e-harness-phase4-two-branch-membership-test
**Plan:** fix-e2e-harness-undeclared-limit
**Status:** Accepted

### Context

A plan-review round found that the original Phase 4 loop step — "record it, fix it if it is in
this plan's scope, and raise an issue if it is not" — was decorative and subvertible: it let an
implementer reclassify any newly-red test as out of scope and defer it, contradicting the
interview answer to fix everything unmasked rather than defer to a follow-up issue.

### Decision

Tests inside this plan's deliberately-scoped remediation work (the seven measured statement
shapes' own assertions, the broadcast-join workaround pair
`e2e_broadcast_join_pushdown_shape`/`e2e_broadcast_join_result_correct`, and anything Phase 1-3's
design work directly targets) must be fixed properly per the plan's actual design and must NOT be
closed by re-adding a cap. Any other test that turns red under the flipped default gets a filed
GitHub issue referencing #312 plus an explicit `capped_result_sets(n)` call at that test's own
connection setup, with no production-code fix attempted for it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Explicit membership test naming the in-scope set, two branches with distinct permitted outcomes | ✓ Chosen — closes the "reclassify and defer" escape the reviewer found |
| Leave step 3(c)'s discretionary wording in place | ✗ Rejected — let an implementer defer any test unilaterally |

### Consequences

Every unmasked test's fate is determined by an explicit membership test rather than
implementer discretion, but the membership test's own wording ("that shape's own assertion") later
proved ambiguous enough to require a further correction — see the superseding ADR below.

## ADR: One flat remediation rule replaces the two-branch scope classification

**ID:** e2e-harness-phase4-flat-remediation-rule
**Plan:** fix-e2e-harness-undeclared-limit
**Status:** Accepted
**Supersedes:** e2e-harness-phase4-two-branch-membership-test

### Context

A second plan-review round attacked the two-branch membership test's wording: clause (i) —
"a test exercising one of the seven statement shapes measured in Phase 1, for that shape's own
assertion" — covered substantially every statement any E2E test in the repository could issue, so
all the narrowing rested on the undefined phrase "for that shape's own assertion." Two lawful
readings of that phrase prescribed opposite actions on whether production code changes and whether
an issue gets filed. The user rejected the whole two-branch structure as overcomplicated.

### Decision

Phase 4 runs one flat loop applied uniformly to every test in every binary: run the binary under
the flipped default; for each newly-failing test, file a GitHub issue referencing #312, add
`capped_result_sets(n)` to that one test's connection, and move on. No production-code fix is made
in Phase 4; the filed issue owns that work. Verified against the code that the broadcast-join pair
needs no new opt-in call — both already called the now-deleted `unbounded_result_sets()`, which set
`result_set_max_rows = 0`, the same value a plain `exa_conn()` sends after the flip — so their
existing calls become dead code removed alongside the flip, not evidence the two-branch rule was
still needed.

### Options Considered

| Option | Verdict |
|--------|---------|
| One flat rule: file an issue and re-cap every newly-red test, no branch, no production-code fix in Phase 4 | ✓ Chosen by the user — no undefined membership phrase, no judgment call |
| Anchor the two-branch membership test to the Phase 1 affected-assertion list | ✗ Rejected — still required interpreting which assertions counted as "that shape's own" |

### Consequences

Phase 4 needs no per-test judgment call, but no test unmasked by the flip receives a
production-code fix within this plan — every such defect is tracked by a filed issue instead,
including the broadcast-join disqualification mechanism this plan's own second correction later
confirmed is real.
