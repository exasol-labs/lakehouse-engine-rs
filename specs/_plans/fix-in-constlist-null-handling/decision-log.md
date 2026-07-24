# Decision Log: fix-in-constlist-null-handling

## Interview

Headless plan (no live interview). The orchestrator supplied a fully-diagnosed bug report and a decided fix approach in place of a Q&A exchange.

**Q:** What is the defect and its root cause?
**A:** `predicate_in_constlist` renders NULL entries verbatim into the `IN (...)` list. DataFusion three-valued logic makes `NOT IN (v, NULL)` evaluate to UNKNOWN (row filtered) for every non-match, while Exasol ignores NULL entries; the pushed-down `NOT IN` therefore returns a silently empty result. Confirmed in `gh issue view 206`.

**Q:** What is the fix?
**A:** Strip rendered entries equal to the bare token `NULL` from the const list before the emptiness check. This reproduces Exasol semantics in both polarities. If the list is empty after stripping, keep the existing `FALSE` rendering.

**Q:** What tests are required?
**A:** A regression unit test in the `crates/vs-expression` suite that fails on current code and passes after the fix — a mixed real-plus-NULL list renders `IN (...)` with NULL stripped, and an all-NULL list renders `FALSE`.

**Q:** Does the Iceberg-spec compliance check apply?
**A:** No. This is a SQL-dialect translation bug in predicate rendering, not scanning, pushdown-planning, or schema/type handling.

**Q:** Is an E2E test planned locally?
**A:** No. Local Exasol Docker bandwidth is reserved; CI runs E2E once the PR is pushed. Plan only host-runnable unit tests.

## Design Decisions

### [1] Strip NULL by inspecting the argument node before rendering

- **Decision:** Skip any argument whose JSON node is a NULL-valued literal — `type == "literal_null"`, or any `literal_*` node whose `value` is JSON `null` (or absent) — before calling the renderer, so it never enters the `rendered` Vec. The check keys on the pre-render node, not the rendered string.
- **Alternatives:** Filter rendered strings equal to the bare token `"NULL"`. **Rejected — superseded (this was the original decision):** typed nulls do not render to the bare token, so string matching misses them. A null `literal_date` renders `DATE NULL` (`lib.rs:216`) and a null `literal_timestamp` renders `arrow_cast(NULL, 'Timestamp(Microsecond, None)')` (`lib.rs:222-225`); both would survive a `"NULL"` string filter and leave the reported 3VL bug alive for DATE/TIMESTAMP entries. The node-level check strips every typed null uniformly regardless of how its renderer stringifies a null. A non-NULL literal cannot be falsely stripped: only literals with an explicitly null/absent `value` match, and const-list arguments are always literal nodes.
- **Rationale:** Keying on the node's null-ness (not its rendered form) is the only mechanism that catches all NULL const-list shapes uniformly; a small `is_null_literal` helper keeps the arguments loop readable. Semantically correct because a NULL-valued literal of any type is exactly what Exasol ignores in an IN list.
- **Promotes to ADR:** no

### [2] All-NULL list reuses the existing empty-list FALSE branch

- **Decision:** After stripping, an all-NULL list is empty and falls through to the current `FALSE` rendering; no new branch is added.
- **Alternatives:** A dedicated all-NULL branch. Rejected as redundant — the post-strip empty list is indistinguishable from an originally-empty list and both correctly yield `FALSE`.
- **Rationale:** Minimises the change surface and preserves unchanged behavior for currently-empty lists.
- **Promotes to ADR:** no

### [3] Unit tests, not E2E; no expert tagging

- **Decision:** Cover the fix with unit tests in `crates/vs-expression/src/lib.rs` and route both tasks to a standard implementer.
- **Alternatives:** An E2E test against Exasol Docker. Deferred to CI per the reserved-bandwidth constraint. Expert tagging considered and rejected — the change is a one-line filter plus copy-pattern tests.
- **Rationale:** `render_expression` is pure computation with no I/O, so unit tests are the correct proof form and run on the host.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] Node-level NULL stripping, not rendered-string matching

- **Finding:** [COMPLETENESS_GAP] The rendered-string mechanism (filter entries equal to the bare token `"NULL"`) misses typed-null const-list shapes: a null `literal_date` renders `DATE NULL` and a null `literal_timestamp` renders `arrow_cast(NULL, 'Timestamp(Microsecond, None)')`, neither of which equals `"NULL"`, so they pass through unstripped — leaving the reported 3VL bug alive for null-valued DATE/TIMESTAMP const-list entries. Decision-log claim [1] ("String matching catches every NULL-valued entry uniformly") was false.
- **Direction change:** Reworked the mechanism to key on the argument node before rendering — skip `literal_null` and any `literal_*` node with a JSON-null/absent `value`. Corrected decision [1] (marked the old string-matching approach as superseded and explained why it was incomplete), rewrote plan.md §Context and Task 1 to describe the node-level check, and updated the spec THEN clause to require omission of "any argument whose node is a NULL-valued literal, of any type" keyed on the node rather than the rendered string.
- **Promotes to ADR:** no

### [plan-review] Direct `NOT IN` regression coverage

- **Finding:** [COMPLETENESS_GAP] Neither planned regression test wrapped the constlist node in `predicate_not`, so neither rendered the actually-reported `NOT IN` scenario (issue #206 is specifically about `NOT IN` diverging under 3VL). The fix covers `NOT IN` transitively (shared renderer, `predicate_not` is an outer wrapper), but the suite did not demonstrate it directly.
- **Direction change:** Added Task 3 and a new unit test `renders_not_in_constlist_strips_null` that wraps `predicate_in_constlist` in `predicate_not` over a mixed real + `literal_null` + null-valued `literal_date` list, asserting `(NOT (<target> IN (<real value only>)))` with no surviving NULL shape. Added a matching spec `*AND*` clause for the `predicate_not` wrapper, and updated the Scenario Coverage and Manual Testing tables.
- **Promotes to ADR:** no

<!-- Populated in Revision Mode after plan-reviewer findings, and by speq-implement after code review. -->
