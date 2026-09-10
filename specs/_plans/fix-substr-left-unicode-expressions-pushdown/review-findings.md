# Code Review Findings: fix-substr-left-unicode-expressions-pushdown

## Summary
- Files reviewed: 4
- Total findings: 1 (standard: 1, expert: 0)

## Standard fixes

### crates/lakehouse-engine/tests/scan_substr_expression.rs

#### [REDUNDANT_COMMENT] Provenance tag on dummy_storage doc comment
- Location: line 34
- Issue: The doc comment on `dummy_storage` ends with "(copied from `scan_column_binding.rs`)". The module-level doc (line 2) already establishes "mirroring the harness in `scan_column_binding.rs`", making this function-level provenance tag redundant. The user instruction for this PR asks to flag anything that adds unnecessary verbosity. The useful first clause ("Storage props are never dialed for a local `file://` scan; a placeholder keeps the spec well-formed") stands on its own.
- Fix: In `crates/lakehouse-engine/tests/scan_substr_expression.rs`, trim the parenthetical from the `dummy_storage` doc comment so it reads `/// Storage props are never dialed for a local `file://` scan; a placeholder\n/// keeps the spec well-formed.`

## Expert fixes
[none]
