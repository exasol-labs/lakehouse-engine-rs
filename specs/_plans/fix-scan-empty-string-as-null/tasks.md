# Tasks: fix-scan-empty-string-as-null

- [x] 1. Live capture: native Exasol results of each text function with a NULL in each argument, and what Exasol sends for `''` and omitted arguments (Requirement "Live capture first")
- [x] 2. Typed pass: the empty-string node for string columns and text function results, the null test form, and the bare-column filter form; remove the `CONCAT` arm's own `nullif` (pushdown-planning-scan-empty-string, vs-expression-translator-scalar-fns, vs-expression-translator-predicates, Dead Code Removal)
- [x] 3. Text functions with a NULL argument: `REPLACE` rendering, and a guard or a decline per function from the capture (pushdown-planning-scan-empty-string)
- [x] 4. Adapter: join conditions and filters render at the filter root; update golden SQL and unit expectations (pushdown-planning-scan-empty-string, Dead Code Removal)
- [x] 5. Plan-time null test over string and variable-length binary columns (pruning-predicate-translation)
- [x] 6. Per-shard sort shares the join top-N ranking; remove `render_join_sort_target` (pushdown-planning-topn, Dead Code Removal)
- [x] 7. Host scan test `guarded_string_filter_skips_row_groups`
- [x] 8. E2E fixture and `empty_string.rs` tests
- [x] 9. Docs: `docs/capabilities.md`, `docs/semantic-differences.md` where they name the changed forms
- [x] 10. Verification checklist: build, unit tests, E2E, lint, format
