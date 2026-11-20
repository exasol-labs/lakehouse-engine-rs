# Intent: add-multi-way-join-broadcast

## Interview

- Q: One plan for the plan model and the hybrid, or split? → A: One plan.
- Q: Should a pair whose smaller leg has no filter be broadcast? → A: Yes. One eligibility rule for every pair, 2 legs or more. A filter only ranks candidates.
- Q: Does the width rule apply in the hybrid when the request does not aggregate? → A: Yes. It applies whenever Exasol processes the fan-out's rows further: it aggregates them or joins them with another leg.
- Q: How is the benchmark criterion covered? → A: Manual run of the existing multi-way bench queries before and after. No bench change.
- Q: A filter the scan cannot run on a broadcast pair? → A: It runs in Exasol's WHERE and the broadcast stays, for 2-leg joins too.
- Q: Width rule when only a filter is left for Exasol? → A: Applies too: aggregation, another join, or a filter in Exasol.
- Q: Chosen pair cannot be rendered? → A: Try the next eligible pair in rank order.
- Q: Select-list item the scan cannot compute on a broadcast? → A: Exasol computes it over the fan-out; the broadcast stays.
- Q: Per-shard sort by an expression (`ORDER BY UPPER(c.name) LIMIT n`)? → A: Not in this plan. Opened #675. A 2-leg join with such a sort keeps today's N-scan.

## Directions

- [D3] The pair's fan-out takes its fact leg's relation alias `LHS_T{fact}` in Exasol's join chain, and its columns keep the `LHS_T{leg}_{COLUMN}` output names, so a 2-leg wrapper's SQL stays byte-identical.
- [D4] Condition placement keeps the accepted renderability-screen ADR: the structural split picks the fan-out that holds all of a conjunct's legs, and the scan translator screen at the renderer moves a declined conjunct to Exasol's WHERE.
- [D5] The plan model is recorded as an architecture constraint, not an ADR, because `specs/architecture.md` states it once recorded.
- [D6] "The broadcast contract" names the two-leg join whose pair is eligible and whose every WHERE conjunct the scan renders, so the recorded per-shard ordering, cap, and bare-fan-out rules keep their meaning unchanged.
- [D7] A two-leg request whose window the broadcast cannot serve keeps the N-scan, whatever else runs in Exasol; sorting by an expression stays with #675.
- [D8] The pair's fan-out takes the position of its lowest leg in Exasol's `INNER JOIN … ON` chain, so each condition comes into scope no later than in the N-scan.
