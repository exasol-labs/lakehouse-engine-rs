# Decision Log: fix-regexp-like-pushdown

## Interview

- Q: How extend ADR 018's decline to REGEXP_LIKE? → A: new superseding ADR, not an in-place edit
- Q: Shape of the docs-vs-CAPABILITIES drift test? → A: unit test parses backticked names from docs/capabilities.md, asserts set equality with CAPABILITIES
- Q: Remove REGEXP_LIKE rendering from shared vs-expression? → A: yes, remove (no code handles `predicate_like_regexp` or a REGEXP_LIKE scalar)
- Context: separate from the ongoing add-delta-pruning-e2e plan; issue #471; related #106, ADR 018

## Design Decisions

### [1] Exasol evaluates every regular-expression function and predicate

- **Decision:** The adapter advertises no regular-expression capability. `FN_PRED_REGEXP_LIKE`, `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, and `FN_REGEXP_COUNT` stay unadvertised, Exasol evaluates them, and the expression translator renders none of them.
- **Rationale:** DataFusion's Rust `regex` engine cannot reproduce Exasol's PCRE match. DataFusion's `regexp_like` matches anywhere in a value. Exasol accepts a value only when the first PCRE match from the value's start ends at the value's end. The `regex` crate also rejects look-around and backreferences, and its `\d`, `\w`, `\s`, and `\b` classes match Unicode characters where Exasol's match ASCII only. DataFusion has no `regexp_substr`, and Exasol's position, occurrence, and return-option arguments have no matching DataFusion argument shape. This entry meets `/speq:adr-rules` rule-2 criterion 4, because it rejects anchoring, a plausible fix whose failure a later reader must know, and criterion 2, because it binds future capability work. ADR search: `affirm-regexp-scalar-function-decline-re-verified-pinned-versions` declines the four scalar functions for the same reason but keeps `FN_PRED_REGEXP_LIKE`. This entry supersedes it. No other accepted ADR covers regular expressions.
- **Alternatives:** Keep `FN_PRED_REGEXP_LIKE` and anchor the pushed pattern as `^(?:<pat>)$`. Rejected: an anchored pattern accepts any whole-string match, so `'ARGENTINA' REGEXP_LIKE '(A|AR)(G|GENTINA)?'` stays true in DataFusion and false in Exasol, and the look-around, backreference, and character-class differences remain. Keep it and decline a pattern that fails a compile check. Rejected: a compile check proves pattern syntax, not match parity.
- **Consequences:** A filter with a `REGEXP_LIKE` conjunct still pushes its other `AND` conjuncts and the projection. An `OR` that contains one pushes no filter, and an aggregate over a query with one runs in Exasol. No benchmark query uses `REGEXP_LIKE`.
- **Supersedes:** affirm-regexp-scalar-function-decline-re-verified-pinned-versions
- **Architecture:** no change: only the advertised capability list and the translator's arms change, inside the existing adapter and `vs-expression` components
- **Promotes to ADR:** yes

### [2] E2E patterns reproduce the reported failures on the seeded `EVENTS` names

- **Decision:** The E2E tests run patterns of the three reported failure classes against the existing `EVENTS.NAME` fixture (`event-01` to `event-20`): `'event-1'` (partial match), `'(e|ev)(e|ent-01)?'` (the same alternation shape as `'(A|AR)(G|GENTINA)?'`), and `'(?=event-0[1-3]).*'` (look-around). The issue's literal patterns `'AN'` and `'(A|AR)(G|GENTINA)?'` match nothing in any seeded table, so they cannot discriminate the defect there.
- **Rationale:** Measured on the Docker Exasol, the current virtual schema returns 10, 20, and a scan error for the three patterns, while native Exasol returns 0, 0, and 3 rows. Each test therefore fails against the defect (`specs/testing.md` § Correctness oracles).
- **Alternatives:** Seed a TPC-H `NATION` table to run the issue's literal patterns. Rejected: a new table in the shared `e2e_lakehouse` namespace changes the table map and the enumeration that other suites assert, for no added proof.
- **Promotes to ADR:** no

### [3] Touched Background sections state the current rule only

- **Decision:** Where a delta must replace a whole `## Background` or scenario to remove a `REGEXP_LIKE` mention, it restates each copied "This delta SUPERSEDES" or "REPLACING the recorded" passage as the rule in force today. Evidence stays verbatim: Iceberg spec quotes, issue numbers, SQL error texts, and live measurements.
- **Rationale:** A `DELTA:CHANGED` Background block replaces the whole section. A copied "This delta" bullet would then claim that this plan made a change it did not make. The acceptance criteria require the specs to describe current behavior only.
- **Promotes to ADR:** no

## Review Findings
