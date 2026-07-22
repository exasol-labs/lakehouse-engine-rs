# Decision Log: add-fn-div-pushdown

## Interview

Headless mode — no live interview. The orchestrator's discovery and the planner's independent
verification stand in for the interview.

**Q:** Does issue #105 have remaining scope, given `FN_NEG` shipped (PR #115) and `FN_DIV` was
declined by ADR `exclude-fn-div-no-faithful-datafusion-floor-division`?
**A:** `FN_NEG` is complete — advertised and locked in by test and spec; nothing to do. `FN_DIV`
stays declined, but independent live verification found the ADR's premise false, so the record needs
correcting. That correction is the residual scope.

**Q:** Is DataFusion 54's floor/integer-division story still as the ADR describes, and does any
verified rendering close the previously-open `DIV` gaps?
**A:** No verified-safe rendering exists, so the decline holds — but the ADR's premise is wrong.
Live Exasol shows `DIV` truncates toward zero (`DIV(-7,2) = -3`, `DIV(15.7,6.2) = 2`), not floor
division, and raises error 22012 on divide-by-zero. DataFusion 54 integer `/` also truncates toward
zero (docs verified), so the two match for integer operands — the divergence the ADR cited does not
exist. The real blockers: DataFusion 54 has no `div` builtin, a `TRUNC(m/n)` emulation diverges on
DOUBLE division by zero (infinity vs. error), and per-function advertisement cannot restrict `DIV`
to integer operands.

**Q:** Close #105, or leave it open for a future DataFusion upgrade?
**A:** Close it after this correction records. FN_NEG and FN_DIV both resolved; no residual scope
remains unless DataFusion adds a faithful `div`.

## Design Decisions

### [1] Correct the FN_DIV decline rationale to verified truncation semantics

- **Decision:** Supersede ADR "Exclude FN_DIV — No Faithful DataFusion Floor Division" with a
  corrected ADR. Exasol `DIV` truncates the quotient toward zero (live-verified, not floor
  division) and matches DataFusion integer `/`; the decline stands because DataFusion 54 has no
  `div` builtin, a `TRUNC(m/n)` emulation diverges from Exasol on DOUBLE-operand division by zero
  (Exasol raises SQL state 22012; DataFusion float division yields infinity), and per-function
  capability advertisement cannot restrict `DIV` pushdown to the integer-operand case where `/`
  would match.
- **Alternatives:** (a) Advertise `FN_DIV` via `TRUNC(m/n)` — rejected: the DOUBLE division-by-zero
  divergence is a silent wrong result, and advertisement is per function, not per operand type.
  (b) Close #105 with no spec change — rejected: the permanent spec and ADR assert a live-refuted
  premise, which could lead a future planner to ship `FN_DIV` via `FLOOR` and introduce a real
  divergence.
- **Rationale:** The decline outcome is correct, but its recorded reason is factually wrong.
  Correcting it upholds the project's verified-semantics bar and removes a future-error trap.
- **Supersedes:** Exclude FN_DIV — No Faithful DataFusion Floor Division
- **Promotes to ADR:** yes

### [2] Scope the change to record-correction, not a capability change

- **Decision:** Change only the spec Background paragraph, the `DIV` scenario rationale, and one
  stale test doc comment. Advertise nothing; change no test assertion or runtime behavior.
- **Alternatives:** Build a full `FN_DIV` pushdown feature — rejected: no verified-safe rendering
  exists (see [1]).
- **Rationale:** The translator already declines `DIV` correctly; the defect is documentation
  accuracy, not behavior.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated in Revision Mode after plan-reviewer blockers, and by speq-implement after code review. -->
