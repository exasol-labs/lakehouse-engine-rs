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

/// Each separate test binary statically links the whole engine (#523), so only `it` and the
/// deliberate targets build one, and a module file the `it` binary does not declare never runs.
#[test]
fn integration_tests_build_into_one_binary_per_crate() {
    let output = std::process::Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--offline",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("cargo metadata must run");
    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("cargo metadata must print JSON");
    let mut extra_binaries = Vec::new();
    for package in metadata["packages"].as_array().into_iter().flatten() {
        let package_name = package["name"].as_str().unwrap_or_default();
        for target in package["targets"].as_array().into_iter().flatten() {
            let name = target["name"].as_str().unwrap_or_default();
            let ungated = target["required-features"]
                .as_array()
                .is_none_or(Vec::is_empty);
            if target["kind"] == serde_json::json!(["test"])
                && ungated
                && !["it", "scan_footer_refetch_observable"].contains(&name)
            {
                extra_binaries.push(format!("{package_name}/{name}"));
            }
        }

        let manifest_path = package["manifest_path"].as_str().unwrap_or_default();
        let it_dir = std::path::Path::new(manifest_path).with_file_name("tests/it");
        let Ok(main_rs) = std::fs::read_to_string(it_dir.join("main.rs")) else {
            continue;
        };
        for entry in std::fs::read_dir(&it_dir).expect("tests/it must be listable") {
            let path = entry.expect("tests/it entry").path();
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            if stem != "main" && path.extension().is_some_and(|e| e == "rs") {
                assert!(
                    main_rs.lines().any(|line| line == format!("mod {stem};")),
                    "{package_name}/tests/it/main.rs must declare `mod {stem};`"
                );
            }
        }
    }
    assert!(
        extra_binaries.is_empty(),
        "each of these builds its own binary, move it into tests/it/: {extra_binaries:?}"
    );
}
