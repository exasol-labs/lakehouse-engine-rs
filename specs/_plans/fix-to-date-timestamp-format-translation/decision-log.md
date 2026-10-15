# Decision Log: fix-to-date-timestamp-format-translation

## Interview

- Q: Elements to translate → A: issue core set (YYYY, MM, DD, HH24, MI, SS, FF1-FF9, literal separators); rest declines
- Q: Exasol leniency vs chrono strictness → A: measure live per element; decline or document diverging ones
- Q: Fallback scope → A: verify live that a render error reaches native Exasol on every path (single-table, aggregate, join)

## Design Decisions

### [1] Translate a closed set of format elements and decline the rest

- **Decision:** `FN_TO_DATE` and `FN_TO_TIMESTAMP` stay advertised. The DataFusion dialect translates a string-literal format built from the core elements and declines every other format, and the caller self-applies the declined call in Exasol. Input strings on which the two parsers differ are listed in `docs/semantic-differences.md`, not declined.
- **Rationale:** A render decline of an advertised call already reaches native Exasol: measured on the deployed 0.52.2 build with a DataFusion-declined `SECOND(ts, 2)` over 16 query shapes (select list, filter, LIMIT, single-group and grouped aggregates, aggregate argument, HAVING, top-N, `COUNT(DISTINCT)`, join filter, join select list, join `ON`, join aggregate, `OR` filter), with join pushdown on and off, every result matched the native table. The listed differences depend on the data, not on the format, so no plan-time check can decline them. Each one either fails the query or accepts a string Exasol rejects. None returns a different value for a string both engines accept.
- **Alternatives:** Withdraw both capabilities, as ADR `exclude-fn-to-char-and-fn-to-number-format-model-incompatibility` does for `TO_CHAR` and `TO_NUMBER`. Rejected: that ADR's reason, that partial support cannot be expressed, no longer holds once a declined call is self-applied (ADR `no-exasol-side-fallback-for-an-advertised-capability`), and withdrawal would also stop pushing the one-argument forms. Decline every format with a space. Rejected: the reported `TO_TIMESTAMP` format has a space. Rewrite the source with `rtrim` or `nullif` to close the trailing-space and empty-string differences. Rejected for this fix: it changes the one-argument renderings, and the interview limited the first cut to the core elements.
- **Consequences:** The format-model knowledge lives in one new private module of `vs-expression`. Its one function maps a format to a strftime pattern or reports that none exists, so the `TO_DATE`/`TO_TIMESTAMP` arm knows no element. A declined filter routes to the qualified single-table wrapper and pushes no filter into the scan, so that query reads more rows.
- **Promotes to ADR:** no

### [2] Each structural rule removes a measured wrong value

- **Decision:** A format declines unless it names `YYYY`, `MM`, and `DD` once each. A `TO_DATE` format declines with any time element. A `TO_TIMESTAMP` format declines unless its time elements are none, `HH24`+`MI`, `HH24`+`MI`+`SS`, or those three plus one `FFn`.
- **Rationale:** Measured with DataFusion 54.1 (chrono 0.4.45) against Exasol 2025.1.16. `to_date` divides epoch milliseconds by the day length and truncates toward zero, so `'1969-12-31 10:11:12'` under `%Y-%m-%d %H:%M:%S` yields `1970-01-01` and `'0001-01-01 00:00:01'` yields `0001-01-02`, where Exasol returns the date. `to_timestamp` builds a time only from an hour and a minute and otherwise falls back to the date, so `'1996-03-13 10'` under `%Y-%m-%d %H` yields midnight where Exasol returns 10:00. A format without `YYYY`, `MM`, or `DD` raises on every row in DataFusion, while Exasol defaults the part to `0001`, `01`, or `01`. Exasol raises 22124 on a repeated element, so declining it returns Exasol's own error.
- **Promotes to ADR:** no

### [3] Only FF3, FF6, and FF9 translate, at an exact width

- **Decision:** `FF3`, `FF6`, and `FF9` map to a fraction of exactly that many digits. `FF1`, `FF2`, `FF4`, `FF5`, `FF7`, `FF8`, and a bare `FF` decline.
- **Rationale:** Exasol's `FFn` keeps the first n digits (`'.1234'` under `FF3` is `.123`). chrono's exact-width forms raise on any other digit count, so they never return a different value. chrono has no 1-, 2-, 4-, 5-, 7-, or 8-digit fraction form (`%1f` is a bad format string).
- **Alternatives:** Map every `FFn` to the variable-width `%.f`. Rejected: it keeps up to nine digits, so `'.1234'` under `FF3` returns `.1234` where Exasol returns `.123`.
- **Promotes to ADR:** no

## Review Findings
