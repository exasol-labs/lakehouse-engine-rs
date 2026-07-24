# Decision Log: fix-207-like-non-string-column

## Interview

Headless mode — no live interview. The orchestrator supplied captured ground truth in place of a live Q&A. Key exchanges reconstructed from that brief:

**Q:** What exactly fails, and is the issue text confirmed against the running system?
**A:** Confirmed. Against the live Exasol+MinIO+Iceberg-REST docker stack, all three issue-#207 repros produce identical DataFusion planning errors: `c_date LIKE '2024%'` → "coerce Date32 and Utf8"; `c_decimal_a LIKE '9%'` → "coerce Decimal128(9, 2) and Utf8"; `id LIKE '1%'` (Int64/DECIMAL(20,0)) → "coerce Int64 and Utf8". A `predicate_like` `column` node carries no `dataType` on the wire; column types exist only in `involvedTables[0].columns`, extracted by `extract_all_column_types(request)`.

**Q:** Where should the type-aware decision live?
**A:** In the adapter layer, not `vs-expression`. `vs-expression` is a pure syntactic translator with no external state, shared with a sibling VS-adapter project; column type is genuinely external context. The #206 NULL-in-IN-list precedent (self-contained in `vs-expression`) does not generalize.

**Q:** How should each non-string subject type be handled?
**A:** VARCHAR/CHAR unchanged; DATE cast to VARCHAR; every other non-string type declines the whole filter (Exasol evaluates natively). Do not attempt decimal formatting — that is issue #211.

**Q:** What is in and out of scope, and how is verification bounded?
**A:** Single-table pushdown path only; the broadcast-join per-leg path has the same latent bug and becomes a tracked follow-up issue. Regression tests must fail on current code and pass after the fix. Do not re-run the full local E2E suite; host `cargo test` is the verification mechanism. Commit the three untracked capture-harness files as deliverables. PR base `fix/206-not-in-null-const-list`; PR title `fix(vs-expression): ...`; commit references `Closes #207`.

## Design Decisions

### [1] Type-aware LIKE decision lives in the adapter, not vs-expression

- **Decision:** A new `like_subject_type_guard` in `pushdown/support.rs` preprocesses the filter JSON before `render_df_filter_safe`, using the column-type map from `extract_all_column_types`.
- **Alternatives:** Add type handling inside `vs-expression`'s `predicate_like` arm (rejected: no column-type context there, and the crate is a shared stateless translator).
- **Rationale:** Column type is external context available only at the adapter layer; keeping `vs-expression` a pure JSON-to-SQL translator preserves its reuse by the sibling project.
- **Promotes to ADR:** yes

### [2] CAST DATE, decline every other non-string type

- **Decision:** DATE subjects are rewrapped as `CAST(<col> AS VARCHAR)`; DECIMAL (including integer `DECIMAL(p,0)`), DOUBLE, BOOLEAN, TIMESTAMP, and all other non-string subjects decline the whole filter.
- **Alternatives:** CAST every non-string type uniformly (rejected: DataFusion's decimal/double/timestamp-to-string formatting keeps trailing zeros and other artifacts that diverge from Exasol's native formatting, silently changing which rows match — strictly worse than a hard error, which at least fails loudly).
- **Rationale:** DATE-to-string is ISO-8601 `YYYY-MM-DD` in both DataFusion and Exasol (default `NLS_DATE_FORMAT`), so the CAST is faithful in the common case. The non-default-session caveat is entry [8] (tracked as #216). Correct trimmed-decimal formatting is the separate, already-tracked issue #211.
- **Promotes to ADR:** yes

### [3] Decline the whole top-level filter, not just the offending conjunct

- **Decision:** A non-string LIKE anywhere in the tree makes `like_subject_type_guard` return `None`, omitting the entire filter so Exasol evaluates it natively.
- **Alternatives:** Surgically drop only the offending conjunct (rejected: matches neither the existing all-or-nothing backstop nor `render_df_filter_safe`'s `None`-means-omit contract, and partial rewriting risks changing result semantics).
- **Rationale:** Mirrors the documented untranslatable-predicate correctness backstop (`mod.rs:14-15`); the DataFusion scan drops the filter and Exasol keeps the predicate, so results stay correct.
- **Promotes to ADR:** no

### [4] No delta to sql-comprehension/vs-expression-translator

- **Decision:** The `vs-expression-translator` feature's LIKE and REGEXP_LIKE scenarios are left unchanged.
- **Alternatives:** Amend those scenarios to describe type-aware behavior (rejected: `vs-expression`'s own rendering logic is untouched — the injected CAST node uses the shape `render_cast`/`render_cast_target` already handle).
- **Rationale:** The fix rewrites the JSON tree one layer up; `vs-expression` still renders whatever tree it receives verbatim, so its spec remains accurate.
- **Promotes to ADR:** no

### [5] No delta to pushdown-planning-capability-extensions

- **Decision:** The advertised `LIKE` and `FN_PRED_REGEXP_LIKE` capabilities are unchanged.
- **Alternatives:** Withdraw or condition the capability (rejected: capabilities are static and type-blind; Exasol still pushes the predicate down for all types).
- **Rationale:** The capability stays advertised; the adapter now sometimes declines to render at plan time (filter omitted → Exasol post-processes), which the capability contract already permits.
- **Promotes to ADR:** no

### [6] Join per-leg LIKE type-awareness deferred to a tracked follow-up

- **Decision:** Scope this fix to the single-table `handle_pushdown` path; the broadcast-join per-leg `render_df_filter_safe` calls (`joins/sql_builders.rs` ~line 71 and ~506) carry the same latent bug and are deferred to a new GitHub issue.
- **Alternatives:** Fix both paths now (rejected: the join path needs per-side column-type threading, materially enlarging the change; the issue-#207 repro exercises only the single-table path).
- **Rationale:** Keeps the fix minimal and focused on the reproduced bug. The gap is named explicitly, not silent; issue #215 is the filed follow-up, and its number is cited when the join path is addressed.
- **Promotes to ADR:** no

### [7] Iceberg-spec compliance check

- **Decision:** The fix is consistent with the Apache Iceberg table spec and changes no type mapping.
- **Alternatives:** None.
- **Rationale:** The DATE column is stored as the Iceberg `date` primitive (calendar date without timezone, days from 1970-01-01), mapped Iceberg `date` → Arrow `Date32` → Exasol `DATE` by the existing `type-mapping` feature, all unchanged. The CAST-to-VARCHAR is a DataFusion query-expression operation for LIKE matching, orthogonal to Iceberg storage/type semantics. No spec deviation is introduced.
- **Promotes to ADR:** no

### [8] DATE LIKE CAST is faithful only under the default NLS_DATE_FORMAT — accepted, tracked (#216)

- **Decision:** The DATE-subject CAST-to-VARCHAR is Exasol-faithful only when the session's `NLS_DATE_FORMAT` is the Exasol default (`YYYY-MM-DD`). A session that altered `NLS_DATE_FORMAT` sees DataFusion's unconditional ISO `YYYY-MM-DD` form, which may diverge from what native Exasol returns for the same LIKE predicate. This is accepted for now and tracked as GitHub issue #216. The code-level decision is unchanged (DATE still casts; other non-string types still decline).
- **Alternatives:** Thread the session NLS format through the pushdown path — via connect-back querying of `SYS.EXA_PARAMETERS`, or a pushdown-protocol extension carrying the session format (rejected: materially larger scope than issue #207's non-string-LIKE bug, disproportionate to the severity of an uncommon non-default-session case). Decline all DATE LIKE pushdown (rejected: penalizes the correct common case — Exasol's default `NLS_DATE_FORMAT` is genuinely `YYYY-MM-DD`, which is what issue #207's repro validated against).
- **Rationale:** The pushdown request carries no session/NLS field anywhere in the adapter path (no `nlsDateFormat`/`dateFormat` in `pushdownRequest` or `schemaMetadataInfo`), so the adapter cannot detect a non-default session format. The common case is correct; the uncommon non-default-session case is explicitly accepted and tracked, not silently dropped. The spec.md DATE scenario names it inline as a tracked exception (#216), following this repo's `(#27)`/`(#83)` inline-exception convention.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] DATE CAST NLS_DATE_FORMAT assumption unstated (BLOCKER)

- **Finding:** The DATE→CAST path is Exasol-faithful only under the default `NLS_DATE_FORMAT` (`YYYY-MM-DD`). Exasol's implicit DATE-to-VARCHAR cast uses the session format, which a client can alter; DataFusion's cast is unconditionally ISO. The pushdown request carries no session/NLS field, so a non-default session would silently return a different row set — the exact silent-wrong-match hazard decision [2] cites to decline CAST for DECIMAL, reintroduced for DATE undetected and unrecorded.
- **Direction change:** Added decision-log entry [8] naming the limitation exactly and marking it accepted + tracked; filed GitHub issue #216; updated the spec.md DATE scenario with an explicit tracked-exception clause citing #216 (matching this repo's `(#27)`/`(#83)` inline convention); cross-referenced the caveat from the plan.md Consequences DATE row. The code-level decision is unchanged — DATE still casts (correct in the common default-session case), other non-string types still decline.
- **Promotes to ADR:** no

### [plan-review] Task 2 wrong-format-cast contingency (ADVISORY)

- **Finding:** Task 2 left an implicit assumption that DataFusion's `Date32`→`Utf8` cast is `YYYY-MM-DD`, with no stated fallback if that were ever untrue.
- **Direction change:** Added a contingency sentence to Task 2: if the cast format is ever confirmed NOT to be `YYYY-MM-DD`, DATE MUST fall back to the DECLINE branch (same as DECIMAL), never ship a wrong-format cast.
- **Promotes to ADR:** no

### [plan-review] Unresolvable bare-column subject undefined (ADVISORY)

- **Finding:** The type-dispatch table and scenarios did not define the case where a bare-`column` subject's name is not found in `involvedTables[0].columns` (lookup miss / unresolvable type).
- **Direction change:** Added a type-dispatch table row and a spec.md scenario: an unresolvable bare-column subject declines the whole filter (fail-safe). Stated that the name lookup is case-normalized (uppercased) to match `extract_all_column_types`'s uppercasing (`support.rs:411`), so a case-mismatched name resolves rather than spuriously declining. Threaded the rule into Task 1, Task 4, and added a scenario-coverage row.
- **Promotes to ADR:** no

### [plan-review] Stale #215 placeholder wording (ADVISORY)

- **Finding:** plan.md described follow-up issue #215 as a placeholder the orchestrator would create; #215 has already been filed (OPEN, join per-leg LIKE gap).
- **Direction change:** Updated plan.md Dependencies and decision-log entry [6] to state #215 is the real, already-filed follow-up issue, removing the placeholder/orchestrator-creates language.
- **Promotes to ADR:** no

### [plan-review] Backstop citation line numbers (ADVISORY — verified, no change)

- **Finding:** The advisory reported the `mod.rs:14-15` backstop citation as off by one and asked to change it to `mod.rs:15-16`.
- **Direction change:** Verified against `crates/lakehouse-engine/src/adapter/pushdown/mod.rs`: the untranslatable-predicate backstop doc-comment is on lines 14-15 (line 12 is file-list-resolved-once, line 13 is the UDF-never-discovers-files invariant). The existing `mod.rs:14-15` citation is accurate; `15-16` would point at the backstop continuation plus the unrelated LIMIT invariant. Retained `14-15` per the "verify by reading the file yourself before citing" directive.
- **Promotes to ADR:** no
