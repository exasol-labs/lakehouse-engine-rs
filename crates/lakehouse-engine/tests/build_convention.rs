//! Asserts the workspace's build and suite-wiring conventions from `CLAUDE.md` (embedded at
//! compile time) against `Makefile`/`.github/workflows/ci.yml` (read at runtime); no live service needed.

const WORKSPACE_CLAUDE_MD: &str = include_str!("../../../CLAUDE.md");

/// Scenario: Host release build of the .so is rejected by convention.
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
    let run_fixtures = workspace_file("scripts/spark-fixtures/run_fixtures.sh");
    assert!(
        run_fixtures.contains("create_iceberg_type_promotion_fixture.sql"),
        "run_fixtures.sh must invoke create_iceberg_type_promotion_fixture.sql"
    );

    let recipe = makefile_recipe(&workspace_file("Makefile"), "test-e2e:");
    assert!(
        recipe.contains("--test e2e_type_relaxation_test"),
        "test-e2e target must run --test e2e_type_relaxation_test"
    );
    let fixture_sql = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/spark-fixtures/create_iceberg_type_promotion_fixture.sql");
    assert!(
        fixture_sql.exists(),
        "scripts/spark-fixtures/create_iceberg_type_promotion_fixture.sql must exist on disk"
    );
}

#[test]
fn make_test_e2e_runs_the_direct_storage_binary() {
    let recipe = makefile_recipe(&workspace_file("Makefile"), "test-e2e:");

    assert!(
        recipe.contains("--test e2e_direct_storage_test"),
        "test-e2e target must run --test e2e_direct_storage_test"
    );
}

#[test]
fn make_test_e2e_runs_the_assume_role_binary() {
    let recipe = makefile_recipe(&workspace_file("Makefile"), "test-e2e:");

    assert!(
        recipe.contains("--test e2e_assume_role_test"),
        "test-e2e target must run --test e2e_assume_role_test"
    );
}

fn workspace_file(relative: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(relative);
    std::fs::read_to_string(path).unwrap_or_else(|_| panic!("{relative} must be readable"))
}

fn makefile_recipe(makefile: &str, target_line_prefix: &str) -> String {
    let mut lines = makefile
        .lines()
        .skip_while(|line| !line.starts_with(target_line_prefix));
    lines.next().expect("target line must exist");
    lines
        .take_while(|line| line.starts_with('\t'))
        .collect::<Vec<_>>()
        .join("\n")
}
