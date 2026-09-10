# Decision Log: fix-substr-left-unicode-expressions-pushdown

## Interview

**Q:** Which fix direction: enable `unicode_expressions` or withdraw `FN_SUBSTR`/`FN_LEFT`?
**A:** Enable `unicode_expressions`. Withdrawal rejected.

**Q:** Broader capability audit or scoped fix + regression test only?
**A:** Fix + regression test only. No broader audit.

## Design Decisions

### [1] Declare `unicode_expressions` on the member manifest, not `[workspace.dependencies]`

- **Decision:** Add `features = ["unicode_expressions"]` in `crates/lakehouse-engine/Cargo.toml`.
- **Rationale:** rust-cache hashes member manifests but skips the root (no `[package]`). Member placement rotates all three cache keys without a hand bump. Matches existing convention for `arrow`, `object_store`, `tokio`, `delta_kernel`.

### [2] No CI `shared-key` bump needed

- **Decision:** No bump. The member-manifest edit rotates keys on its own.
- **Rationale:** rust-cache restores a warm `target/`; cargo recomputes per-unit fingerprints including features, so the artifact is always correct. The stale-cache hazard is recompilation cost, not correctness.

### [3] Root cause: missing `ExprPlanner`, not missing UDFs

- **Decision:** Scope the fix to the `Substring` AST node and `UnicodeFunctionPlanner`.
- **Rationale:** Probed the workspace: `left`, `right`, `character_length`, `strpos`, `lpad` all plan today. Only `substr`/`substring` fail because `sqlparser` parses them into the `Substring` node, handled exclusively by `UnicodeFunctionPlanner` (gated behind `unicode_expressions` at `session_state_defaults.rs:96`). The reported `LEFT` failure originates in Exasol normalizing `LEFT` to `SUBSTR` in the pushdown request, not in the translator.

### [4] `Cargo.lock` unchanged, no new dependency

- **Decision:** Confirmed `Cargo.lock` is byte-identical with the feature applied.
- **Rationale:** `unicode_expressions` forwards to feature flags already enabled transitively. No crate added, `cargo deny` license gate unaffected.

## Review Findings

### [5] Live capture confirms: Exasol rewrites `LEFT` to `SUBSTR` before the pushdown request

- **Method:** `scripts/capture-pushdown-payload.sh 'SELECT id, LEFT(c_varchar, 3) FROM {table} WHERE id <= 5 ORDER BY 1'` against the local Docker stack (Exasol 2025.2.1 + MinIO + Iceberg REST), `typed_distinct_probe` seed table.
- **Finding:** The captured `EXPLAIN VIRTUAL` pushdown request's `selectList` contains no `LEFT` node at all. Exasol sends:
  ```json
  {
    "arguments": [
      {"columnNr": 4, "name": "C_VARCHAR", "tableName": "TYPED_DISTINCT_PROBE", "type": "column"},
      {"type": "literal_exactnumeric", "value": "1"},
      {"type": "literal_exactnumeric", "value": "3"}
    ],
    "name": "SUBSTR",
    "type": "function_scalar"
  }
  ```
  i.e. `LEFT(c_varchar, 3)` arrives at the adapter already normalized to `SUBSTR(c_varchar, 1, 3)` — a `function_scalar` node named `SUBSTR`, not `LEFT`. The real-execution result set's own column header confirms the same rewrite: `"SUBSTR(TYPED_DISTINCT_PROBE.C_VARCHAR,1,3)"`.
- **Conclusion:** This confirms [3]'s hypothesis directly: the adapter's `SUBSTR -> substr` translation (`vs-expression/src/lib.rs:627`) is the ONLY code path a Exasol-side `LEFT(...)` call ever reaches. There is no separate `LEFT` function_scalar node to translate or plan. Enabling `unicode_expressions` (which fixes `substr`'s `UnicodeFunctionPlanner` gap) therefore fixes both the reported `SUBSTR(...)` and `LEFT(...)` failures from #187 with the same one-line change — task 2.9's E2E test exercises both forms directly against the live stack for this reason.
