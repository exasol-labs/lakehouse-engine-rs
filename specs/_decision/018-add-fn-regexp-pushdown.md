# Decisions: add-fn-regexp-pushdown

## ADR: Affirm the Regexp Scalar Function Decline — Re-Verified Against Pinned Versions

**ID:** affirm-regexp-scalar-function-decline-re-verified-pinned-versions
**Plan:** `add-fn-regexp-pushdown`
**Status:** Accepted
**Supersedes:** exclude-regexp-scalar-functions-rust-regex-dialect-divergence

### Context

Issue #106 reopened the recorded decline of `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`,
`FN_REGEXP_INSTR`, and `FN_REGEXP_COUNT`, asking whether a literal-pattern validation gate changes
the calculus. Re-verification against the pinned DataFusion 54.0.0 and `regex` 1.12.4 found: the
Rust `regex` crate rejects backreferences (`\1`-`\9`) and named captures (`\g<name>`) that Exasol's
documented `REGEXP_REPLACE` dialect supports; `datafusion-functions` 54.0.0 registers no
`regexp_substr`; and `regexp_replace`/`regexp_instr` omit Exasol's position/occurrence/return-option
arguments, while `regexp_count`'s argument shape aligns with Exasol's and is blocked only by the
dialect gap.

### Decision

Affirm the decline. Keep `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, and
`FN_REGEXP_COUNT` unadvertised. The pre-existing `FN_PRED_REGEXP_LIKE` predicate advertisement is
unaffected and stays out of scope.

### Options Considered

| Option | Verdict |
|--------|---------|
| Affirm the decline; document and cite #106 | ✓ Chosen — all three blockers hold at the pinned versions; a compile-success check does not prove match parity with Exasol's PCRE |
| Advertise the three functions DataFusion provides, gated on a literal-pattern compile check | ✗ Rejected — a `Regex::new` compile check certifies pattern syntax, not semantic match parity with Exasol's PCRE, so it fails the project's backing-path bar; `regexp_substr` remains absent and the `REGEXP_REPLACE`/`REGEXP_INSTR` argument shapes still diverge regardless of the pattern |

### Consequences

All four regexp scalar functions continue to evaluate in Exasol. The decline now carries a
re-verified evidence trail against the pinned dependency versions and an inline citation to issue
#106 in both governing specs, so the gap reads as investigated-and-declined rather than a silent
omission. `FN_PRED_REGEXP_LIKE` pushdown is unchanged.
