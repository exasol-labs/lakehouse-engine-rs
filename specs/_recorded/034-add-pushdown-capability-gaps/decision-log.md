# Decision Log: add-pushdown-capability-gaps

## Interview

No live interview — this plan ran in headless mode. Requirements were taken from GitHub issues #104, #105, #106, #107 and the project's backing-path bar (`crates/lakehouse-engine/src/adapter/capabilities.rs`). Where an issue flagged a risk (cast-target exclusions, format-model strings, regex dialect parity, calendar semantics), the risk was resolved by static analysis of the translator source, DataFusion 54 documentation and source, and Exasol built-in-function metadata, then recorded as a decision below rather than escalated.

**Q (from issue #104):** Which Exasol cast targets have a faithful DataFusion mapping, and which must be excluded?
**A:** VARCHAR, CHAR, DECIMAL(p,s), DOUBLE, BOOLEAN, DATE, TIMESTAMP are faithful and already rendered by the existing `render_cast_target` arm. INTERVAL, GEOMETRY, HASHTYPE, and TIMESTAMP WITH LOCAL TIME ZONE are excluded — the arm returns an error and the adapter falls back. `FN_CAST` is advertised; `FN_TO_CHAR`/`FN_TO_NUMBER` are not (see decision [4]).

**Q (from issue #105):** Do `FN_NEG` and `FN_DIV` have exact DataFusion reproductions?
**A:** `FN_NEG` yes — the unary-negation arm already renders `(-x)`; it was rendered but unadvertised, a latent coherence gap this plan closes. `FN_DIV` no — see decision [3].

**Q (from issue #106):** Push only literal regex patterns, or gate on a dialect check?
**A:** Neither — exclude all four regexp scalar functions (decision [5]).

**Q (from issue #107):** Advertise the date group as a block or split by parity?
**A:** Split. Only `FN_WEEK` holds parity (ISO-8601); the rest are excluded (decision [6]).

## Design Decisions

### [1] Advertise FN_CAST backed by the existing CAST arm

- **Decision:** Advertise `FN_CAST`. The `render_cast_target` arm already renders the faithful target set and returns an error for unsupported targets, so the adapter falls back safely.
- **Alternatives:** Keep `FN_CAST` unadvertised. Rejected — it is the highest-impact gap; a CAST currently blocks pushdown of its whole containing expression.
- **Rationale:** Arm exists, faithful targets are confirmed, unsupported targets fall back. Meets the backing-path bar today.
- **Promotes to ADR:** no

### [2] Advertise FN_NEG (closing a latent coherence gap)

- **Decision:** Advertise `FN_NEG`. The unary-negation arm already renders `(-x)` and composes inside aggregate arguments.
- **Alternatives:** Keep unadvertised. Rejected — the translator already renders NEG, so leaving it unadvertised violated the scalar-ops coherence clause ("no rendered operator is left unadvertised").
- **Rationale:** Exact reproduction; composes with the arithmetic-aggregate decomposition path.
- **Promotes to ADR:** no

### [3] Exclude FN_DIV — no faithful DataFusion floor division

- **Decision:** Do not advertise `FN_DIV`; the translator declines a `DIV` node and Exasol post-processes it.
- **Alternatives:** Emulate via `CAST(FLOOR(a / CAST(b AS DOUBLE)) AS …)`. Rejected — div-by-zero and negative-operand and decimal-rounding parity are unverified and the safe fallback is already correct.
- **Rationale:** Exasol `DIV` is floor division; DataFusion 54 `/` truncates integer division toward zero (diverges for negatives) and has no `div` function. Advertising would yield silently wrong results.
- **Promotes to ADR:** yes

### [4] Exclude FN_TO_CHAR and FN_TO_NUMBER — format-model incompatibility

- **Decision:** Do not advertise `FN_TO_CHAR` or `FN_TO_NUMBER`.
- **Alternatives:** Advertise the no-format case only. Rejected — capability advertisement is per function, not per argument shape; Exasol would push format-argument variants the translator cannot render faithfully.
- **Rationale:** DataFusion 54 `to_char` uses strftime masks (not Exasol Oracle-style models) and rejects numeric formatting; DataFusion 54 has no `to_number`. The no-format string-to-number path is already reachable via `FN_CAST`.
- **Promotes to ADR:** yes

### [5] Exclude the regexp scalar functions — Rust regex dialect divergence

- **Decision:** Do not advertise `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, or `FN_REGEXP_COUNT`.
- **Alternatives:** Advertise `REGEXP_REPLACE`/`INSTR`/`COUNT` (which exist in DataFusion 54) and pre-validate patterns. Rejected — the translator cannot compile a pattern to detect Rust-regex incompatibility without embedding a regex engine, and an incompatible pushed pattern would fail the node-local scan rather than fall back.
- **Rationale:** DataFusion 54 runs the Rust `regex` crate (no pattern backreferences, no lookaround), lacks `regexp_substr`, and its argument shapes differ from Exasol's position/occurrence/return-option arguments.
- **Promotes to ADR:** yes

### [6] Advertise only FN_WEEK from #107; exclude the rest — calendar-semantic divergence

- **Decision:** Advertise `FN_WEEK` (ISO-8601, `date_part('week', …)`), gated on a year-boundary parity test. Exclude `FN_ADD_*`, `FN_*_BETWEEN`, `FN_ADD_MONTHS`, `FN_ADD_YEARS`, `FN_MONTHS_BETWEEN`, `FN_YEARS_BETWEEN`, `FN_DAYOFWEEK`, `FN_LAST_DAY`, `FN_CONVERT_TZ`.
- **Alternatives:** Advertise the date group as a block. Rejected — only `WEEK` holds parity.
- **Rationale:** DataFusion 54 has no `add_days`/`add_months`/`last_day`/`convert_tz` and its variable×INTERVAL scaling is unverified; date-diff needs divergent multi-step emulation; month/year arithmetic needs Oracle end-of-month clamping DataFusion lacks; `date_part('dow')` numbers Sunday = 0. Exasol `WEEK` and DataFusion `date_part('week')` are both documented ISO-8601.
- **Promotes to ADR:** yes

### [7] The Iceberg-spec compliance gate does not apply to this plan

- **Decision:** Skip the `CLAUDE.md` Iceberg-spec check for this plan, documenting the reason rather than silently omitting it.
- **Alternatives:** Quote an Iceberg-spec section anyway. Rejected — no normative section governs SQL-expression pushdown.
- **Rationale:** These are Exasol SQL-expression-pushdown capabilities (VS-layer function translation), not Iceberg file-format or schema/type handling.
- **Promotes to ADR:** no

### [8] CAST dispatched on the wrong node type — real wire format is top-level `function_scalar_cast`

- **Decision:** Add a top-level `"function_scalar_cast"` dispatch arm to the `crates/vs-expression` walker as the real-traffic CAST path, sharing the CAST body (`render_cast` → `render_cast_target`) with the pre-existing nested `function_scalar`+name=CAST arm, which is retained defensively (mirroring the retained REGEXP_LIKE alternate encoding). Task-2.1 unit fixtures were corrected to the real `{"type":"function_scalar_cast","name":"CAST","dataType":{...},"arguments":[...]}` shape; a single defensive test still covers the nested form.
- **Discovery:** Found by the Group C live E2E run (task 2.9), not by unit tests. Once `FN_CAST` was advertised (task 2.8), `EXPLAIN VIRTUAL SELECT id, score FROM MY_LAKEHOUSE.EVENTS WHERE CAST(id AS VARCHAR(2000000)) = '15'` showed the pushed scan spec with **no `"filter"` key at all** — Exasol fell back to raw-row scan + client-side filter despite `FN_CAST` being advertised. Root cause: the walker only handled CAST nested inside a generic `"function_scalar"` node (dispatching on `name == "CAST"`), but real Exasol emits CAST as its own top-level node type `"function_scalar_cast"` — following the same family pattern as `"function_scalar_extract"` and `"function_scalar_case"`. The unrecognized node hit the "unsupported expression node type" error, which `render_df_filter_safe` swallows via `.ok()??` — a silent decline-to-push (the correctness-safe fallback), which is exactly why the query still ran but unaccelerated.
- **Evidence:** Confirmed against the Exasol engine source (not memory): `exasol-db/Compiler/src/querygraph/scalar/qecast.cpp:101` (`QECastBasis::toJson`) sets `root["type"] = "function_scalar_cast"`, `root["name"] = "CAST"`, `root["dataType"] = …`, `root["arguments"] = [<src>]`, and is the **sole** CAST emitter. Sibling nodes emit `"function_scalar_case"` (qecase.cpp:441) and `"function_scalar_extract"` (qedatefunctions.cpp:2685) the same way. REGEXP_LIKE is emitted only as `predicate_like_regexp` (qemethods_vschema.cpp:729), confirming the `function_scalar`+REGEXP_LIKE arm is likewise a defensive-only alternate encoding — the precedent followed here for keeping the nested CAST arm.
- **Audit gap noted for future work:** The task-2.1 CAST audit verified target-type-mapping correctness but constructed its fixtures with the wrong top-level node type (`"function_scalar"`), so the unit suite passed green while never exercising the real dispatch shape. The scalar-ops spec scenario for CAST states the node "carries a `dataType` field" but did not pin the top-level `type` value — a wire-format under-specification that let the mismatch hide. Future capability audits MUST verify dispatch node-type shape against the engine serializer, not only the semantic mapping, and specs SHOULD pin the exact top-level `type` string for any node with a dedicated family encoding.
- **Verification:** `cargo test -p vs-expression` green (74 tests, 9 CAST); live E2E `e2e_capability_test` green — 11/11 including `e2e_cast_in_filter` (the pushed scan spec now carries the `filter` field).
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
