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
        workspace_root.join("crates/lakehouse-engine/tests/e2e_local/main.rs"),
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

    let suite_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/e2e_local");
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

/// Each separate test binary statically links the whole engine (#523), so a top-level test file is
/// a deliberate `[[test]]` target, and a module file the `it` binary does not declare never runs.
#[test]
fn integration_tests_build_into_one_binary_per_crate() {
    let crates_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    for crate_name in ["lakehouse-engine", "lakehouse-catalog"] {
        let tests_dir = crates_dir.join(crate_name).join("tests");
        let manifest = std::fs::read_to_string(crates_dir.join(crate_name).join("Cargo.toml"))
            .expect("crate manifest must be readable");
        let mut lines = manifest.lines();
        let declared_targets: Vec<String> = std::iter::from_fn(|| {
            lines.find(|line| *line == "[[test]]")?;
            lines.next().map(str::to_owned)
        })
        .collect();
        for entry in std::fs::read_dir(&tests_dir).expect("tests dir must be listable") {
            let path = entry.expect("tests dir entry").path();
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            if path.extension().is_some_and(|e| e == "rs") {
                assert!(
                    declared_targets.contains(&format!("name = \"{stem}\"")),
                    "{crate_name}/tests/{stem}.rs builds its own binary: move it into tests/it/ \
                     or declare it as a [[test]] target"
                );
            }
        }

        let it_dir = tests_dir.join("it");
        let main_rs = std::fs::read_to_string(it_dir.join("main.rs"))
            .expect("tests/it/main.rs must be readable");
        for entry in std::fs::read_dir(&it_dir).expect("tests/it must be listable") {
            let path = entry.expect("tests/it entry").path();
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            if stem != "main" && path.extension().is_some_and(|e| e == "rs") {
                assert!(
                    main_rs.lines().any(|line| line == format!("mod {stem};")),
                    "{crate_name}/tests/it/main.rs must declare `mod {stem};`"
                );
            }
        }
    }
}
