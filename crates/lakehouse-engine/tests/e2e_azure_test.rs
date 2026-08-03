//! End-to-end integration tests for the lakehouse-engine Virtual Schema against
//! a **real Azure Data Lake Storage Gen2** account, catalogued by a local
//! Lakekeeper (OpenID-secured via Keycloak).
//!
//! Azure has no working local substitute — Azurite's `dfs` endpoint is incomplete
//! and Lakekeeper v0.13.1's `adls` profile addresses `https://<account>.<host>`
//! with a bare hostname, so an Azurite endpoint is not expressible through it at
//! all. Storage is therefore real cloud; the catalog, Keycloak, and Exasol stay
//! local Docker. The suite FAILS (never skips) when the stack, any of the five
//! credential variables, or the Azure account is unavailable.
//!
//! All tests share one Exasol provisioning, so they must run serially
//! (`--test-threads=1`); the `make test-e2e-azure` target passes the flag.
//!
//! # Two credentials, two purposes, never conflated
//!
//! The harness creates and deletes its own blob container under an **Entra ID
//! service principal**, because the official Azure blob crate accepts no account
//! key. Everything under test — the warehouse storage credential, the seed
//! `FileIO`, and the Exasol CONNECTION the scan reads through — carries the
//! **account key**, which is the `AdlsCred::AccountKey` path this suite exists to
//! verify. A service principal reaching the CONNECTION would let the suite pass
//! while exercising nothing the production read path ships.
//!
//! # Provisioning is split by whether it needs cleaning up
//!
//! [`setup`] holds only what is idempotent and leaves nothing behind: the
//! readiness waits, the Lakekeeper bootstrap, and the shared SLC / `.so` /
//! script provisioning. Everything with a lifecycle lives on [`AzureFixture`],
//! which a test holds as a local so `Drop` deletes the run's container at scope
//! end — including while unwinding from a panic. A guard parked in the `OnceLock`
//! would never clean up at all: statics are not dropped at process exit.
#![cfg(feature = "azure-e2e")]

mod common;

use common::azure::{self, AzureContainer};
use common::e2e_harness::{
    ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME, SCHEMA_NAME, SO_UDF_OBJECT_PATH, SYS_PASSWORD, VsProps,
    create_schema_and_scripts, create_virtual_schema_with_password, exa_conn, install_slc,
    parse_int, upload_so,
};
use common::exasol_ws::ExaConn;
use common::lakekeeper::{
    self, AdlsWarehouseProfile, lakekeeper_adls_connection_password,
    lakekeeper_create_adls_warehouse, lakekeeper_warehouse_storage_profile,
};
use common::seed::{
    E2E_NAMESPACE, E2E_TABLE, SEED_ROWS_SCORE_GT_15, SEED_TOTAL_ROWS, SeedCatalogAuth, SeedStorage,
    seed_events_table_with_auth,
};
use common::stack::{
    self, CatalogConnectionPassword, build_create_connection_sql, exasol_host, exasol_sql_port,
    wait_for_exasol, wait_for_url,
};

use futures::FutureExt;
use std::sync::OnceLock;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Constants — one Virtual Schema over the run's own ADLS warehouse.
// ---------------------------------------------------------------------------

/// Virtual Schema over the static-credential (delegation-off) ADLS warehouse.
const VS_STATIC: &str = "AZ_STATIC_LAKEHOUSE";
/// Catalog CONNECTION for that warehouse.
const CONN_STATIC: &str = "AZ_STATIC_CATALOG_CREDS";

/// Lakekeeper catalog base URL as reached from inside the Exasol UDF container —
/// the Docker-network name plus the `/catalog` base-path segment.
const LAKEKEEPER_CATALOG_URI_INTERNAL: &str = "http://lakekeeper:8181/catalog";

/// Lakekeeper catalog base URL as reached from the host (mapped port), used only
/// for host-side seeding.
fn lakekeeper_catalog_url_host() -> String {
    format!("http://localhost:{}/catalog", lakekeeper::lakekeeper_port())
}

// ---------------------------------------------------------------------------
// One-time, cleanup-free setup (shared across the serial binary).
// ---------------------------------------------------------------------------

static SETUP_DONE: OnceLock<()> = OnceLock::new();

/// Provision everything this binary shares and nothing that needs cleaning up.
///
/// MinIO is deliberately absent from the readiness waits: this suite's storage is
/// Azure, and waiting on a service it never reads would make an unrelated MinIO
/// outage fail it. The scan path comes from the SHARED `common::e2e_harness`
/// definition, so the script DDL is byte-identical to every other E2E binary.
fn setup() {
    SETUP_DONE.get_or_init(|| {
        // 1. Readiness — fail loud, never skip.
        wait_for_exasol();
        lakekeeper::wait_for_keycloak();
        lakekeeper::wait_for_lakekeeper();

        // 2. Bootstrap the catalog server (idempotent). The warehouse is NOT
        //    created here: it is per-run and its container has a lifecycle.
        lakekeeper::lakekeeper_bootstrap();

        // 3. Shared-harness provisioning (SLC + .so + scripts) — REUSED, never
        //    redeclared.
        install_slc();
        upload_so();
        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
    });
}

// ---------------------------------------------------------------------------
// Per-test Azure fixture — the only thing in this binary that owns a lifecycle.
// ---------------------------------------------------------------------------

/// One run's Azure container, ADLS warehouse, seeded table, CONNECTION, and
/// Virtual Schema.
///
/// **Hold this as a local in the test that provisions it.** Its container is
/// deleted by `_container`'s `Drop`, which runs on a normal return and while
/// unwinding from a panic — but only if the value is still owned by the test's
/// stack frame. Do not move `_container` out and do not park a fixture in a
/// static.
struct AzureFixture {
    /// Storage account under test — the `abfss://` authority the scan reads.
    account_name: String,
    /// The run's blob container, which is also the warehouse's `filesystem`.
    container_name: String,
    /// Lakekeeper's name for the run's warehouse, which is also its `key-prefix`.
    warehouse: String,
    /// Data-file paths the seed committed, as the catalog recorded them.
    data_file_paths: Vec<String>,
    /// Existence-only: dropping it deletes the container. Declared last so it
    /// outlives every field describing it.
    _container: AzureContainer,
}

impl AzureFixture {
    /// Create the run's container, warehouse, seeded table, CONNECTION, and
    /// Virtual Schema, in that order.
    ///
    /// The order is a requirement, not a preference. Lakekeeper creates no ADLS
    /// filesystem and validates physical access at warehouse-creation time by
    /// writing and deleting a probe object, so the container must exist first — a
    /// missing container or a wrong account key then fails warehouse creation
    /// immediately instead of surfacing later as a scan error.
    ///
    /// Every blocking HTTP or WebSocket call stays outside `rt.block_on`: issuing
    /// one from inside a runtime context panics, and a panic here would unwind
    /// through the container guard rather than reaching a test assertion.
    fn provision() -> Self {
        setup();

        // The data path under test, read before anything exists in Azure, so an
        // absent variable fails the run with no container to clean up. The three
        // Entra ID values are read by `AzureContainer::create`, which validates
        // all of them before it creates anything.
        let account_name = azure::account_name();
        let account_key = azure::account_key();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");

        let container_name = azure::per_run_container_name();
        let container = rt
            .block_on(AzureContainer::create(&container_name))
            .unwrap_or_else(|e| panic!("create per-run Azure container '{container_name}': {e:#}"));

        // The warehouse name is derived from the container by the profile, so it
        // carries the same per-run suffix: a repeated local run can never bind to
        // a surviving warehouse whose container has already been deleted.
        let profile = AdlsWarehouseProfile::new(&container_name, &account_name, &account_key);
        let warehouse = profile.name().to_string();
        lakekeeper_create_adls_warehouse(&profile);

        // Seeding authenticates to the catalog with a host-side Keycloak bearer
        // token (fetched here, so a short token lifetime cannot go stale
        // mid-write) and writes its data files through `abfss://` under the
        // account key — the same credential the CONNECTION below carries.
        let token = lakekeeper::keycloak_client_credentials_token();
        let seeded = rt
            .block_on(seed_events_table_with_auth(
                &lakekeeper_catalog_url_host(),
                &warehouse,
                SeedCatalogAuth {
                    token: Some(token),
                    storage: SeedStorage::Adls {
                        account_name: account_name.clone(),
                        account_key: account_key.clone(),
                    },
                },
            ))
            .unwrap_or_else(|e| panic!("seed events into ADLS warehouse '{warehouse}': {e:#}"));

        // The CONNECTION and the Virtual Schema, from the shared harness helper.
        // The password names the per-run warehouse and carries the account key;
        // it leaves every static S3 field empty, which is what makes the adapter
        // read it as an Azure CONNECTION rather than an ambiguous one.
        let password = lakekeeper_adls_connection_password(&warehouse, &account_name, &account_key);
        let mut conn = exa_conn();
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_STATIC, E2E_NAMESPACE).with_catalog_conn_name(CONN_STATIC),
            LAKEKEEPER_CATALOG_URI_INTERNAL,
            &password,
        );

        Self {
            account_name,
            container_name,
            warehouse,
            data_file_paths: seeded.data_file_paths,
            _container: container,
        }
    }

    /// The `abfss://<container>@<account>.dfs.core.windows.net/` prefix every one
    /// of this fixture's data files must carry.
    fn abfss_prefix(&self) -> String {
        format!(
            "abfss://{}@{}.dfs.core.windows.net/",
            self.container_name, self.account_name
        )
    }
}

// ---------------------------------------------------------------------------
// End-to-end scan over the static-credential ADLS warehouse.
// ---------------------------------------------------------------------------

/// The Azure fixture provisions a per-run container, a delegation-disabled ADLS
/// warehouse seeded over `abfss://`, and a Virtual Schema over it. This single
/// test carries three sets of assertions over that one fixture — the storage
/// profile Lakekeeper reports, the seeded `abfss://` paths, projection/filter/
/// LIMIT correctness, and the shared-harness script DDL — because splitting them
/// would triple the live-Azure cost and the orphan surface for no added coverage.
///
/// The Virtual Schema existing is the downstream proof that the whole chain ran:
/// a VS cannot be created over a warehouse whose container is missing (Lakekeeper
/// would have rejected the warehouse), whose account key is wrong (the seed write
/// would have failed), or whose table was never seeded (enumeration would find
/// nothing).
#[test]
fn azure_static_creds_end_to_end() {
    let fixture = AzureFixture::provision();
    let mut conn = exa_conn();

    let cols = conn.query_columns(&format!(
        "SELECT SCHEMA_NAME FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = '{VS_STATIC}'"
    ));
    assert_eq!(
        cols.first().map(|c| c.len()).unwrap_or(0),
        1,
        "virtual schema {VS_STATIC} must exist — its per-run ADLS warehouse '{}' must have been \
         created, seeded over the account key, and enumerated",
        fixture.warehouse
    );

    // 1. The warehouse's storage profile, as Lakekeeper itself reports it back —
    //    not merely as this harness constructed it.
    let profile = lakekeeper_warehouse_storage_profile(&fixture.warehouse);
    assert_eq!(
        profile["type"].as_str(),
        Some("adls"),
        "storage profile type must be adls: {profile}"
    );
    assert_eq!(
        profile["account-name"].as_str(),
        Some(fixture.account_name.as_str()),
        "storage profile account-name must be the configured account: {profile}"
    );
    assert_eq!(
        profile["filesystem"].as_str(),
        Some(fixture.container_name.as_str()),
        "storage profile filesystem must be the run's own container: {profile}"
    );
    assert_eq!(
        profile["key-prefix"].as_str(),
        Some(fixture.warehouse.as_str()),
        "storage profile key-prefix must be the warehouse name: {profile}"
    );
    assert_eq!(
        profile["sas-enabled"].as_bool(),
        Some(false),
        "storage profile sas-enabled must be false — a SAS-vending warehouse would let a scan \
         succeed without ever exercising the account key under test: {profile}"
    );

    // 2. Every seeded data-file path is a real Azure location.
    let prefix = fixture.abfss_prefix();
    assert!(
        !fixture.data_file_paths.is_empty(),
        "the seed must have committed at least one data file to warehouse '{}'",
        fixture.warehouse
    );
    for path in &fixture.data_file_paths {
        assert!(
            path.starts_with(&prefix),
            "every seeded data file must live in real Azure storage under {prefix}, got: {path}"
        );
    }

    // 3. Projection + filter + LIMIT correctness over the real scan. Same seed
    //    shape as every other E2E suite: id 1..20, score = 5.0 * id.
    let table = format!("{VS_STATIC}.{}", E2E_TABLE.to_uppercase());
    let cols = conn.query_columns(&format!(
        "SELECT id, name, score FROM {table} WHERE score > 15.0 LIMIT 5"
    ));
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (id, name, score): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        5,
        "LIMIT 5 must return exactly 5 rows: {cols:?}"
    );
    for score in &cols[2] {
        let s = score
            .as_f64()
            .or_else(|| score.as_str().and_then(|v| v.parse().ok()))
            .unwrap_or_else(|| panic!("score not numeric: {score:?}"));
        assert!(s > 15.0, "filter violated: score {s} <= 15.0");
    }
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    assert!(
        ids.iter().all(|&id| id >= 4),
        "id < 4 appeared (score would be <= 15): {ids:?}"
    );
    let filtered = conn.query_row_count(&format!("SELECT id FROM {table} WHERE score > 15.0"));
    assert_eq!(
        filtered, SEED_ROWS_SCORE_GT_15 as i64,
        "WHERE score > 15.0 must return {SEED_ROWS_SCORE_GT_15} rows, got {filtered}"
    );
    let total = conn.query_row_count(&format!("SELECT id FROM {table}"));
    assert_eq!(
        total, SEED_TOTAL_ROWS as i64,
        "the static ADLS warehouse must hold {SEED_TOTAL_ROWS} seeded rows, got {total}"
    );

    // 4. The script DDL came from the shared harness definition, not a
    //    duplicated local one.
    for script in [ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME] {
        let resp = conn.execute(&format!(
            "SELECT SCRIPT_TEXT FROM EXA_ALL_SCRIPTS \
             WHERE SCRIPT_NAME = '{script}' AND SCRIPT_SCHEMA = '{SCHEMA_NAME}'"
        ));
        let body = resp["responseData"]["results"][0]["resultSet"]["data"][0][0]
            .as_str()
            .unwrap_or("")
            .to_string();
        assert!(
            body.contains(SO_UDF_OBJECT_PATH),
            "shared script {SCHEMA_NAME}.{script} must reference the shared BucketFS object \
             {SO_UDF_OBJECT_PATH}: {body}"
        );
    }
    let cols = conn.query_columns(&format!(
        "SELECT ADAPTER_SCRIPT_SCHEMA || '.' || ADAPTER_SCRIPT_NAME \
         FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = '{VS_STATIC}'"
    ));
    let adapter = cols
        .first()
        .and_then(|c| c.first())
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("virtual schema {VS_STATIC} must report its adapter script"));
    assert!(
        adapter.to_uppercase().contains(&SCHEMA_NAME.to_uppercase())
            && adapter
                .to_uppercase()
                .contains(&ADAPTER_SCRIPT_NAME.to_uppercase()),
        "VS must be created USING the shared adapter script {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME}, \
         got: {adapter}"
    );
}

// ---------------------------------------------------------------------------
// Container guard deletes on panic, even nested inside an active runtime.
// ---------------------------------------------------------------------------

/// The container guard deletes its container while unwinding from a panic,
/// including when that unwind crosses an active `rt.block_on` — the exact case
/// `AzureContainer::drop`'s own teardown thread exists to survive, since driving
/// the delete on the ambient runtime from `Drop` would re-enter "Cannot start a
/// runtime from within a runtime".
///
/// `std::panic::catch_unwind` cannot stand in for `futures::FutureExt::catch_unwind`
/// here: it takes a synchronous closure, but the guard's construction
/// (`AzureContainer::create`) is `async`, and driving it with a nested
/// `Handle::block_on` inside a synchronous closure would itself panic with
/// "Cannot start a runtime from within a runtime" — the very failure mode this
/// test exists to prove was fixed, not a way to avoid triggering it.
#[test]
fn azure_container_guard_deletes_on_panic() {
    let container_name = azure::per_run_container_name();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let outcome = rt.block_on(
        std::panic::AssertUnwindSafe(async {
            let _container = AzureContainer::create(&container_name)
                .await
                .unwrap_or_else(|e| {
                    panic!("create per-run Azure container '{container_name}': {e:#}")
                });
            panic!(
                "deliberate panic to exercise the container guard's Drop-while-unwinding path, \
                 nested inside rt.block_on"
            );
        })
        .catch_unwind(),
    );

    assert!(
        outcome.is_err(),
        "the inner async block must have panicked — that panic is what this test exercises \
         the guard's cleanup against"
    );

    let exists = rt
        .block_on(azure::container_exists(&container_name))
        .unwrap_or_else(|e| panic!("check container '{container_name}' existence: {e:#}"));
    assert!(
        !exists,
        "container '{container_name}' must have been deleted by the guard's Drop while \
         unwinding from the panic inside rt.block_on — it still exists"
    );
}

// ---------------------------------------------------------------------------
// Fail-not-skip when the stack is down.
// ---------------------------------------------------------------------------

/// The Azure suite's readiness contract is fail-loud, mirroring the Lakekeeper
/// suite's own `lakekeeper_suite_fails_when_stack_unavailable`: a readiness wait
/// against an unreachable stack PANICS (never returns cleanly), so a down stack
/// surfaces as a test failure, never a silent skip. This exercises the very
/// `wait_for_url` helper `setup`'s readiness waits are built on, pointed at a
/// closed local port with a short deadline.
#[test]
fn azure_suite_fails_when_stack_unavailable() {
    let result = std::panic::catch_unwind(|| {
        // 127.0.0.1:1 refuses immediately; the poll loop hits the short deadline
        // and panics rather than returning — the fail-not-skip contract.
        wait_for_url("http://127.0.0.1:1/health", Duration::from_secs(2));
    });
    assert!(
        result.is_err(),
        "a readiness wait against an unreachable stack must panic (fail), never return Ok (skip)"
    );
}

// ---------------------------------------------------------------------------
// No credential value ever appears in captured output / panic messages.
// ---------------------------------------------------------------------------

/// A failing, credential-bearing Azure CONNECTION DDL executed through a
/// redacting `ExaConn` must not surface the SQL text or either sentinel value in
/// the failure output.
///
/// Mirrors the Lakekeeper suite's own `lakekeeper_credentials_never_appear_in_output`:
/// obviously-fake sentinels carry `account_name`/`account_key`, an invalid
/// trailing token forces the DDL-failure path, and the captured panic message is
/// asserted to contain neither sentinel nor the SQL text.
#[test]
fn azure_credentials_never_appear_in_output() {
    const SENTINEL_ACCOUNT_NAME: &str = "azdummyaccountnamesentinel";
    const SENTINEL_ACCOUNT_KEY: &str = "AZ_DUMMY_ACCOUNT_KEY_SENTINEL";

    wait_for_exasol();
    let mut conn =
        ExaConn::connect_redacting(&exasol_host(), exasol_sql_port(), "sys", SYS_PASSWORD);

    let sentinel_password = CatalogConnectionPassword {
        warehouse: "az_redaction_probe".to_string(),
        use_vended_credentials: false,
        account_name: Some(SENTINEL_ACCOUNT_NAME.to_string()),
        account_key: Some(SENTINEL_ACCOUNT_KEY.to_string()),
        ..Default::default()
    };
    let base_sql = build_create_connection_sql(
        "AZ_REDACTION_PROBE",
        LAKEKEEPER_CATALOG_URI_INTERNAL,
        &sentinel_password,
    );
    let failing_sql = format!("{base_sql} THIS_TRAILING_TOKEN_MAKES_THE_STATEMENT_INVALID");

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        conn.execute(&failing_sql);
    }));
    let payload = match result {
        Ok(_) => panic!("expected execute() to fail on the malformed credential-bearing DDL"),
        Err(p) => p,
    };
    let panic_msg = stack::panic_payload_message(&*payload).unwrap_or_default();

    assert!(
        !panic_msg.is_empty(),
        "expected a string panic payload from the failed redacting execute()"
    );
    assert!(
        !panic_msg.contains(&failing_sql),
        "redacting execute() failure must not echo the SQL text: {panic_msg}"
    );
    assert!(
        !panic_msg.contains(SENTINEL_ACCOUNT_NAME) && !panic_msg.contains(SENTINEL_ACCOUNT_KEY),
        "redacting execute() failure must not leak either sentinel credential value: {panic_msg}"
    );
}

// ---------------------------------------------------------------------------
// Local credential file cannot be committed (pure, no I/O at runtime).
// ---------------------------------------------------------------------------

/// Workspace `.gitignore`, embedded at compile time.
/// Path is relative to this source file: crates/lakehouse-engine/tests -> workspace root.
const WORKSPACE_GITIGNORE: &str = include_str!("../../../.gitignore");

/// Committed `test.env.example`, embedded at compile time.
const TEST_ENV_EXAMPLE: &str = include_str!("../../../test.env.example");

/// `.gitignore` lists `test.env` (a filled-in credential file is never
/// committable) and the committed `test.env.example` names all five Azure
/// variables, each still carrying only the `placeholder` sentinel — never a real
/// credential value.
#[test]
fn azure_local_credential_file_is_gitignored() {
    assert!(
        WORKSPACE_GITIGNORE
            .lines()
            .any(|line| line.trim() == "test.env"),
        ".gitignore must list test.env, so a filled-in credential file is never committable"
    );

    for var in [
        "AZURE_STORAGE_ACCOUNT_NAME",
        "AZURE_STORAGE_ACCOUNT_KEY",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
    ] {
        assert!(
            TEST_ENV_EXAMPLE.contains(&format!("{var}=placeholder")),
            "test.env.example must name {var} and carry only the placeholder sentinel, never a \
             real credential value"
        );
    }
}

// ---------------------------------------------------------------------------
// The Make target rebuilds the .so and runs the suite serially.
// ---------------------------------------------------------------------------

/// Workspace `Makefile`, embedded at compile time.
const WORKSPACE_MAKEFILE: &str = include_str!("../../../Makefile");

/// `make test-e2e-azure`'s shape, asserted against the Makefile's text rather
/// than by actually running `make` (which would trigger the full Docker/build
/// pipeline inside a test): the target rebuilds the `.so` through
/// `cross-musl-udf-build` before running anything, its `test.env` sourcing and
/// its `cargo test` invocation share one recipe line, and that line passes
/// `--test-threads=1` — all tests in this binary share one Exasol provisioning.
#[test]
fn azure_make_target_rebuilds_so_and_runs_serially() {
    let target_start = WORKSPACE_MAKEFILE
        .find("test-e2e-azure:")
        .expect("Makefile must define a test-e2e-azure target");
    let after_target = &WORKSPACE_MAKEFILE[target_start..];
    let target_line_end = after_target.find('\n').unwrap_or(after_target.len());
    let target_line = &after_target[..target_line_end];
    assert!(
        target_line.contains("cross-musl-udf-build"),
        "test-e2e-azure must prerequisite cross-musl-udf-build, so a stale .so never gates the \
         suite: {target_line}"
    );

    let rest = &after_target[target_line_end..];
    let recipe_end = rest.find("\n\n").unwrap_or(rest.len());
    let recipe = &rest[..recipe_end];

    assert!(
        recipe
            .lines()
            .any(|line| line.contains("test.env") && line.contains("cargo test")),
        "the test.env sourcing and the cargo test invocation must share one recipe line: {recipe}"
    );
    assert!(
        recipe.contains("--test-threads=1"),
        "the recipe must pass --test-threads=1, because all azure-e2e tests share one Exasol \
         provisioning: {recipe}"
    );
}
