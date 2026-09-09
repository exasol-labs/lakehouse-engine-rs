//! Surface-probe gate: asserts that no hand-rolled `UdfContext` implementation
//! remains under `crates/lakehouse-engine/tests/` (the shared `BatchCapturingCtx`
//! in `scan_fixture/` is the sole approved double), and that exactly 2 retained
//! in-crate doubles exist under `crates/lakehouse-engine/src/scan/` (in
//! `emit_tests.rs` and `test_support_tests.rs`).

use std::process::Command;

const PATTERN: &str = r"impl\s+(exasol_udf_sdk::context::)?UdfContext\s+for\s";

fn grep_matches(dir: &str, excludes: &[&str]) -> Vec<String> {
    let crate_dir = env!("CARGO_MANIFEST_DIR");
    let search_dir = format!("{crate_dir}/{dir}");
    let mut cmd = Command::new("grep");
    cmd.args(["-rn", "-E", PATTERN, &search_dir]);
    for pattern in excludes {
        cmd.args(["--exclude", pattern]);
    }
    let output = cmd.output().expect("grep must run");
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect()
}

#[test]
fn no_hand_rolled_udf_context_in_integration_tests() {
    // The only `impl UdfContext` under tests/ is the shared `BatchCapturingCtx`
    // in scan_fixture/mod.rs. Every other file must be clean.
    let hits = grep_matches("tests", &["mod.rs", "scan_udf_context_surface.rs"]);
    assert!(
        hits.is_empty(),
        "no test file outside scan_fixture/ may declare its own `impl UdfContext`:\n{}",
        hits.join("\n")
    );
}

#[test]
fn exactly_two_retained_doubles_under_src_scan() {
    let hits = grep_matches("src/scan", &[]);
    assert_eq!(
        hits.len(),
        2,
        "expected exactly 2 `impl UdfContext` under src/scan/ \
         (emit_tests.rs and test_support_tests.rs), found {}:\n{}",
        hits.len(),
        hits.join("\n")
    );
    let joined = hits.join("\n");
    assert!(
        joined.contains("emit_tests.rs"),
        "one retained double must be in emit_tests.rs:\n{joined}"
    );
    assert!(
        joined.contains("test_support_tests.rs"),
        "one retained double must be in test_support_tests.rs:\n{joined}"
    );
}
