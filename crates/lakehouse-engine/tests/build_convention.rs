//! Pure convention test (no I/O): asserts the build documentation states that a
//! host `cargo build --release` of the UDF crate produces an artifact that
//! cannot be loaded inside Exasol.
//!
//! Covers packaging/single-so-two-entry-points scenario:
//! "Host release build of the .so is rejected by convention" — the documentation
//! MUST state that host `cargo build --release` produces an unloadable artifact.
//!
//! The documentation is embedded at compile time via `include_str!`, so this is
//! a pure test: it reads no files at runtime and needs no live services.

/// Workspace build-convention documentation, embedded at compile time.
/// Path is relative to this source file: crates/lakehouse-engine/tests -> workspace root.
const WORKSPACE_CLAUDE_MD: &str = include_str!("../../../CLAUDE.md");

#[test]
fn host_release_build_documented_unloadable() {
    let doc = WORKSPACE_CLAUDE_MD.to_lowercase();

    assert!(
        doc.contains("cargo build --release"),
        "build documentation must mention the host `cargo build --release` path"
    );
    assert!(
        doc.contains("host"),
        "build documentation must call out the host build specifically"
    );
    assert!(
        doc.contains("fails to load") || doc.contains("unloadable"),
        "build documentation must state the host-built .so does not load in Exasol"
    );
}

#[test]
fn the_type_relaxation_suite_and_fixture_are_wired_into_run_fixtures_and_make_test_e2e() {
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");

    let run_fixtures =
        std::fs::read_to_string(workspace_root.join("scripts/spark-fixtures/run_fixtures.sh"))
            .expect("scripts/spark-fixtures/run_fixtures.sh must be readable");
    assert!(
        run_fixtures.contains("create_iceberg_type_promotion_fixture.sql"),
        "run_fixtures.sh must invoke create_iceberg_type_promotion_fixture.sql"
    );

    let makefile = std::fs::read_to_string(workspace_root.join("Makefile"))
        .expect("Makefile must be readable");
    let mut lines = makefile.lines();
    let recipe_line = lines
        .find(|line| line.starts_with("test-e2e:"))
        .and_then(|_| lines.next())
        .expect("Makefile must have a test-e2e target followed by a recipe line");
    assert!(
        recipe_line.contains("--test e2e_type_relaxation_test"),
        "test-e2e target must run --test e2e_type_relaxation_test"
    );

    let fixture_sql =
        workspace_root.join("scripts/spark-fixtures/create_iceberg_type_promotion_fixture.sql");
    assert!(
        fixture_sql.exists(),
        "scripts/spark-fixtures/create_iceberg_type_promotion_fixture.sql must exist on disk"
    );
}
