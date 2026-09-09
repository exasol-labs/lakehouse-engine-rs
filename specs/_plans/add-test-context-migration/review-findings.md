# Code Review Findings: add-test-context-migration

## Summary
- Files reviewed: 20
- Total findings: 1 (standard: 1, expert: 0)

## Standard fixes

### crates/lakehouse-engine/src/adapter/adapter_tests.rs

#### [OUTPUT_PARAMETER] collect_rust_files accumulates through a mutable reference
- Location: line 1851
- Issue: The nested function `collect_rust_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>)` returns its results through a mutated `out` parameter instead of returning the collected value. Per guardrails, a function must not use output parameters: return the value instead.
- Fix: In `crates/lakehouse-engine/src/adapter/adapter_tests.rs`, refactor `collect_rust_files` inside `no_hand_rolled_udf_context_in_adapter_tests` to return `Vec<std::path::PathBuf>` directly. Move the `Vec::new()` into the function body, extend from recursive calls with `out.extend(collect_rust_files(&path))`, and return `out`. Update the call site to `let files = collect_rust_files(&adapter_dir);`.

## Expert fixes
[none]
