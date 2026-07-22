# Decision Log: add-fn-regexp-pushdown

## Interview

Headless plan (`speq-plan-pr`); no live interview occurred. The questions resolved below are the
"Open questions" from issue #106, answered by re-verifying against the pinned dependency versions.

**Q:** Is the safe move to push regexp scalar functions only when the pattern is a literal we can
validate, or to gate on a dialect-compatibility check?
**A:** Neither. A `regex::Regex` compile check certifies pattern syntax, not match parity with
Exasol's PCRE (POSIX leftmost-longest vs PCRE leftmost-first, Unicode vs ASCII classes, dot-newline
all diverge on patterns both engines accept). It fails the project's backing-path bar, and it does
not address the missing `regexp_substr` or the argument-shape divergence. Affirm the decline.

**Q:** Do Exasol's position/occurrence/return-option arguments have matching DataFusion argument
shapes?
**A:** No. `regexp_replace(str, pattern, replacement[, flags])` has no position/occurrence;
`regexp_instr(str, regexp[, start[, N[, flags[, subexpr]]]])` carries `subexpr`, not Exasol's
return-option. Confirmed against `datafusion-functions` 54.0.0 source.

## Design Decisions

### [1] Affirm the recorded decline; do not advertise the regexp scalar functions

- **Decision:** Keep `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, and
  `FN_REGEXP_COUNT` unadvertised, re-verified against pinned DataFusion 54.0.0 and `regex` 1.12.4.
- **Alternatives:** Overturn decision `034` entry [5] and advertise the three functions DataFusion
  provides, gated on a literal-pattern compile check. Rejected — compile-success does not prove
  match parity with Exasol's PCRE, so it fails the backing-path bar; `regexp_substr` is absent and
  the argument shapes diverge regardless of the pattern.
- **Rationale:** All three original blockers hold at the pinned versions: the Rust `regex` crate
  rejects backreferences and lookaround by design; `datafusion-functions` 54.0.0 registers no
  `regexp_substr`; and `regexp_replace`/`regexp_instr`/`regexp_count` omit Exasol's
  position/occurrence/return-option arguments.
- **Supersedes:** none — reaffirms "Exclude the regexp scalar functions — Rust regex dialect
  divergence" (decision `034` entry [5]) with a stronger backing-path-bar argument.
- **Promotes to ADR:** yes

### [2] Strengthen the citation trail; scope the change to spec prose and one code comment

- **Decision:** Cite issue #106 inline in both governing scenarios (the `(#N)` convention this repo
  uses for tracked exceptions), add the same citation to the two backing-code comments, and close
  #106 as investigated-and-declined. No translator or capability behavior changes.
- **Alternatives:** Close #106 with no spec change. Rejected — CLAUDE.md requires a known gap to be
  a cited, tracked exception in the spec, never a silent omission.
- **Rationale:** The decline behavior is already implemented and tested; the missing element was a
  traceable link between issue #106 and the specs and code that encode the decision.
- **Promotes to ADR:** no

### [3] The Iceberg-spec compliance gate does not apply to this plan

- **Decision:** Skip the CLAUDE.md Iceberg-spec check, documenting the reason rather than silently
  omitting it.
- **Alternatives:** Quote an Iceberg-spec section anyway. Rejected — no normative section governs
  SQL-expression pushdown.
- **Rationale:** These are Exasol SQL-expression-pushdown capabilities (VS-layer function
  translation), not Iceberg file-format or schema/type handling; carries forward decision `034`
  entry [7].
- **Promotes to ADR:** no

## Review Findings

<!-- Populated in Revision Mode after plan-reviewer blockers, and by speq-implement after code review. -->
