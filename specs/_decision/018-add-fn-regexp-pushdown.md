# Decisions: add-fn-regexp-pushdown

## ADR: Affirm the Regexp Scalar Function Decline — Re-Verified Against Pinned Versions

**ID:** affirm-regexp-scalar-function-decline-re-verified-pinned-versions
**Plan:** `add-fn-regexp-pushdown`
**Status:** Accepted
**Supersedes:** exclude-regexp-scalar-functions-rust-regex-dialect-divergence

### Context

The Rust `regex` crate rejects backreferences and named captures that Exasol's `REGEXP_REPLACE` supports. DataFusion has no `regexp_substr`, and its `regexp_replace` and `regexp_instr` omit Exasol's position, occurrence, and return-option arguments.

### Decision

`FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, and `FN_REGEXP_COUNT` stay unadvertised, and Exasol evaluates them. The `FN_PRED_REGEXP_LIKE` predicate is unaffected.

### Options Considered

| Option | Verdict |
|--------|---------|
| Advertise the functions DataFusion provides, gated on a literal-pattern compile check | Rejected: a compile check proves pattern syntax, not match parity with Exasol's PCRE, and argument shapes still diverge |

### Consequences

The decline cites issue #106 in both governing specs, so it reads as investigated and not as an omission.
