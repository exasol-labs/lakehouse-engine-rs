const WORKSPACE_AGENTS_MD: &str = include_str!("../../../../AGENTS.md");

#[test]
fn host_release_build_documented_unloadable() {
    let doc = WORKSPACE_AGENTS_MD.to_lowercase();

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

    let suite_main = std::fs::read_to_string(
        workspace_root.join("crates/lakehouse-e2e/tests/e2e_local/main.rs"),
    )
    .expect("tests/e2e_local/main.rs must be readable");
    assert!(
        suite_main
            .lines()
            .any(|line| line == "mod type_relaxation;"),
        "the e2e_local suite must declare its type_relaxation module"
    );
    let fixture_sql =
        workspace_root.join("scripts/spark-fixtures/create_iceberg_type_promotion_fixture.sql");
    assert!(
        fixture_sql.exists(),
        "scripts/spark-fixtures/create_iceberg_type_promotion_fixture.sql must exist on disk"
    );
}

#[test]
fn make_test_e2e_runs_every_e2e_local_module() {
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let makefile = std::fs::read_to_string(workspace_root.join("Makefile"))
        .expect("Makefile must be readable");
    let mut lines = makefile.lines();
    let recipe_line = lines
        .find(|line| line.starts_with("test-e2e:"))
        .and_then(|_| lines.next())
        .expect("Makefile must have a test-e2e target followed by a recipe line");
    assert!(
        recipe_line.contains("--test e2e_local"),
        "test-e2e target must run --test e2e_local"
    );

    let suite_dir = workspace_root.join("crates/lakehouse-e2e/tests/e2e_local");
    let main_rs = std::fs::read_to_string(suite_dir.join("main.rs"))
        .expect("tests/e2e_local/main.rs must be readable");
    for entry in std::fs::read_dir(&suite_dir).expect("tests/e2e_local must be listable") {
        let path = entry.expect("tests/e2e_local entry").path();
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default();
        if stem != "main" && path.extension().is_some_and(|e| e == "rs") {
            assert!(
                main_rs.lines().any(|line| line == format!("mod {stem};")),
                "tests/e2e_local/main.rs must declare `mod {stem};`"
            );
        }
    }
}

/// Cargo builds each top-level `tests/*.rs` and `tests/*/main.rs` as its own binary (none under
/// `autotests = false`) and a `tests/it/` file only when declared, so both mistakes fail here (#523).
#[test]
fn every_host_test_file_belongs_to_a_listed_test_target() {
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for krate in ["lakehouse-engine", "lakehouse-catalog", "lakehouse-e2e"] {
        let tests_dir = workspace_root.join("crates").join(krate).join("tests");
        let manifest = std::fs::read_to_string(tests_dir.join("../Cargo.toml"))
            .expect("crate Cargo.toml must be readable");
        let lines: Vec<&str> = manifest.lines().map(str::trim).collect();
        let mut listed: Vec<&str> = lines
            .windows(2)
            .filter(|pair| pair[0] == "[[test]]")
            .filter_map(|pair| pair[1].strip_prefix("name = \"")?.strip_suffix('"'))
            .collect();
        listed.push("it");

        for entry in std::fs::read_dir(&tests_dir).expect("tests/ must be listable") {
            let path = entry.expect("tests/ entry").path();
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            let is_target = if path.is_dir() {
                path.join("main.rs").exists()
            } else {
                path.extension().is_some_and(|e| e == "rs")
            };
            assert!(
                !is_target || listed.contains(&name),
                "crates/{krate}/tests/{name} is a test target: move it into an existing binary, \
                 or list it as a [[test]] target in crates/{krate}/Cargo.toml"
            );
        }

        let it_dir = tests_dir.join("it");
        if !it_dir.exists() {
            continue;
        }
        let it_main = std::fs::read_to_string(it_dir.join("main.rs"))
            .expect("tests/it/main.rs must be readable");
        for entry in std::fs::read_dir(&it_dir).expect("tests/it must be listable") {
            let path = entry.expect("tests/it entry").path();
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            let is_module = if path.is_dir() {
                path.join("mod.rs").exists()
            } else {
                name != "main" && path.extension().is_some_and(|e| e == "rs")
            };
            assert!(
                !is_module || it_main.lines().any(|line| line == format!("mod {name};")),
                "crates/{krate}/tests/it/main.rs must declare `mod {name};`, or its tests never run"
            );
        }
    }
}
