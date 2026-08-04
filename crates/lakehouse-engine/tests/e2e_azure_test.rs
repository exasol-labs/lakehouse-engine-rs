//! End-to-end integration tests for the lakehouse-engine Virtual Schema against a
//! **real Azure Data Lake Storage Gen2** account, catalogued by a local Lakekeeper
//! (OpenID-secured via Keycloak), over **both credential arms**: the static arm
//! (delegation off, account key carried in the Exasol CONNECTION) and the vended
//! arm (delegation on, a SAS minted per `loadTable` and carried by no CONNECTION
//! field).
//!
//! Azure has no working local substitute — Azurite's `dfs` endpoint is incomplete
//! and Lakekeeper's `adls` profile can't address it. Storage is therefore real
//! cloud; the catalog, Keycloak, and Exasol stay local Docker. The suite FAILS
//! (never skips) when the stack, any credential variable, or the Azure account is
//! unavailable.
//!
//! All tests share one Exasol provisioning, so they run serially
//! (`--test-threads=1`, set by the `make test-e2e-azure` target).
//!
//! Three credential roles, never conflated. The harness creates and deletes its
//! own blob container under an **Entra ID service principal** (the official Azure
//! blob crate accepts no account key). The **account key** carries both
//! warehouses' Lakekeeper storage credential, both arms' seed `FileIO`, and the
//! static arm's Exasol CONNECTION — the `AdlsCred::AccountKey` path. The **vended
//! SAS** carries the vended arm's scan and nothing else: Lakekeeper mints it from
//! that same account key and returns it per `loadTable`, and no CONNECTION field
//! holds it — the `AdlsCred::Sas` path.
//!
//! [`setup`] holds only what's idempotent and cleanup-free: readiness waits,
//! Lakekeeper bootstrap, shared SLC/`.so`/script provisioning. Everything with a
//! lifecycle lives on [`AzureFixture`], held as a test-function local so `Drop`
//! deletes the run's container at scope end — including while unwinding from a
//! panic, which a guard parked in a `OnceLock` never would.
//!
//! **One fixture and one container guard serve both arms**, which is why
//! [`azure_static_and_vended_creds_end_to_end`] is one test rather than two: the
//! guard cannot live in a `OnceLock`, so splitting the arms would mean a second
//! live-Azure container. Sharing costs masking, and assertion order is the
//! mitigation — every vended-arm assertion except the closing cross-arm
//! comparison runs BEFORE the static arm's. The vended CONNECTION carries no
//! account name and no account key, so a passing vended scan is reachable only
//! through the vended SAS and is the strongest proof in the file; running it
//! first stops an unrelated static-arm regression from aborting the test before
//! that proof has been made.
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
    lakekeeper_connection_password, lakekeeper_create_adls_warehouse,
    lakekeeper_warehouse_storage_profile,
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
// Constants — one Virtual Schema per credential mode, over one shared container.
// ---------------------------------------------------------------------------

/// Virtual Schema over the static-credential (delegation-off) ADLS warehouse.
const VS_STATIC: &str = "AZ_STATIC_LAKEHOUSE";
/// Catalog CONNECTION for that warehouse: OAuth2 catalog fields plus the account
/// name and key under test.
const CONN_STATIC: &str = "AZ_STATIC_CATALOG_CREDS";

/// Virtual Schema over the vended-credential (delegation-on) ADLS warehouse.
const VS_VENDED: &str = "AZ_VENDED_LAKEHOUSE";
/// Catalog CONNECTION for that warehouse: OAuth2 catalog fields and no storage
/// field of any kind, so only the SAS Lakekeeper vends can reach the data.
const CONN_VENDED: &str = "AZ_VENDED_CATALOG_CREDS";

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
/// MinIO is deliberately absent from the readiness waits: this suite's storage
/// is Azure, so waiting on MinIO would fail it on an unrelated outage.
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

/// One credential mode's warehouse: everything the suite needs to query it.
///
/// A three-field record earns its place here on leakage rather than on depth.
/// The flat alternative gives [`AzureFixture`] two same-typed `String` warehouse
/// names and two same-typed `Vec<String>` path vectors, where one cross-wired
/// pair would let the vended assertions read the static arm's data and still
/// pass. Pairing the three per-arm values in one value makes that mistake
/// unrepresentable.
///
/// `abfss_prefix()` deliberately does NOT live here: the container is shared, so
/// the prefix is a property of the container, not of an arm — and that placement
/// is what makes "both arms sit on the same container" assertable at all.
struct AzureArm {
    /// Lakekeeper's name for this arm's warehouse, which is also its `key-prefix`.
    warehouse: String,
    /// Virtual Schema created over that warehouse.
    vs: &'static str,
    /// Data-file paths this arm's seed committed, as the catalog recorded them.
    data_file_paths: Vec<String>,
}

impl AzureArm {
    /// The Exasol-qualified table name this arm's Virtual Schema exposes.
    fn table(&self) -> String {
        format!("{}.{}", self.vs, E2E_TABLE.to_uppercase())
    }
}

/// One run's Azure container, the two ADLS warehouses over it — one per
/// credential mode — and each one's seeded table, CONNECTION, and Virtual
/// Schema.
///
/// One container serves both arms: it halves the live-Azure provisioning cost of
/// a second credential mode and keeps the only cloud resource, and the only
/// orphan surface, at one. It buys nothing else — the arms are seeded
/// independently into two disjoint `key-prefix`es, so they are two Iceberg
/// tables over two sets of Parquet files, and cross-arm row equality rests on
/// the one deterministic seed shape rather than on shared bytes.
///
/// **Hold this as a local in the test that provisions it.** `_container`'s
/// `Drop` deletes the container on return or panic, but only while it's still
/// owned by the test's stack frame — never move it out or park it in a static.
/// That constraint is why both arms live in one test function: a `OnceLock`
/// setup shared across tests, as the MinIO suite uses, would leak the container.
struct AzureFixture {
    /// Storage account under test — the `abfss://` authority the scan reads.
    account_name: String,
    /// The run's blob container, which is also both warehouses' `filesystem`.
    container_name: String,
    /// The vended-SAS arm. Provisioned first, so a static-arm provisioning
    /// failure cannot leave the run with zero vended evidence.
    vended_arm: AzureArm,
    /// The static account-key arm.
    static_arm: AzureArm,
    /// The exact password [`AzureFixture::provision`] used to create
    /// `CONN_VENDED` — the installed artefact itself, not an equivalent value
    /// rebuilt later. Asserting on a second call of the password helper would
    /// only re-test that pure helper and would still pass if provisioning had
    /// installed a static-credential password here instead.
    vended_password: CatalogConnectionPassword,
    /// Existence-only: dropping it deletes the container, and with it both
    /// arms' data. Declared last so it outlives every field describing it.
    _container: AzureContainer,
}

impl AzureFixture {
    /// Create the run's container, then the VENDED arm's warehouse, seed, and
    /// Virtual Schema, then the STATIC arm's. Two independent reasons fix that
    /// order; neither is a preference.
    ///
    /// Container before either warehouse: Lakekeeper creates no filesystem and
    /// validates physical access at warehouse-creation time via a probe
    /// write/delete, so a missing container or a wrong key fails warehouse
    /// creation immediately instead of surfacing later as a scan error.
    ///
    /// Vended arm before static: every failure in here panics, so arm order is
    /// the only thing keeping a static-arm provisioning failure from leaving the
    /// run with zero evidence about the vended path. It reduces that masking
    /// rather than removing it — a vended-arm failure still aborts before the
    /// static arm — which is the accepted cost of one shared fixture.
    ///
    /// Blocking HTTP/WebSocket calls (`exa_conn`, DDL) stay outside
    /// `rt.block_on`: issuing one from inside a runtime context panics,
    /// unwinding through the container guard instead of reaching a test
    /// assertion.
    fn provision() -> Self {
        setup();

        // Read before anything exists in Azure, so an absent variable fails the
        // run with no container to clean up yet.
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

        // Both arms seed through this one closure, which is what makes the
        // Keycloak token necessarily fetched immediately before each write — a
        // short token lifetime cannot go stale mid-seed if there is no way to
        // reach the seed without minting one. Both arms also seed with the
        // account key and no per-arm override: Lakekeeper vends its SAS under
        // the host-suffixed `adls.sas-token.<host>` key, which cannot reach the
        // flat `adls.account-key` property the seed's FileIO reads.
        let seed_arm = |warehouse: &str| -> Vec<String> {
            let token = lakekeeper::keycloak_client_credentials_token();
            rt.block_on(seed_events_table_with_auth(
                &lakekeeper_catalog_url_host(),
                warehouse,
                SeedCatalogAuth {
                    token: Some(token),
                    storage: SeedStorage::Adls {
                        account_name: account_name.clone(),
                        account_key: account_key.clone(),
                    },
                },
            ))
            .unwrap_or_else(|e| panic!("seed events into ADLS warehouse '{warehouse}': {e:#}"))
            .data_file_paths
        };

        // Both warehouse names derive from the container, so they carry its
        // per-run suffix — a repeated run can't bind to a stale warehouse whose
        // container is already gone — and their differing tails keep the two
        // `key-prefix`es disjoint, which Lakekeeper requires of warehouses
        // sharing a filesystem.
        let vended_warehouse = create_warehouse_and_confirm(&AdlsWarehouseProfile::vended(
            &container_name,
            &account_name,
            &account_key,
        ));
        let vended_paths = seed_arm(&vended_warehouse);
        let mut conn = exa_conn();
        // No storage field at all: the vended branch of the shared password
        // helper returns the OAuth2 catalog fields alone, so nothing but the
        // SAS Lakekeeper mints can reach this arm's data.
        let vended_password = lakekeeper_connection_password(&vended_warehouse, true);
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_VENDED, E2E_NAMESPACE).with_catalog_conn_name(CONN_VENDED),
            LAKEKEEPER_CATALOG_URI_INTERNAL,
            &vended_password,
        );

        let static_warehouse = create_warehouse_and_confirm(&AdlsWarehouseProfile::static_creds(
            &container_name,
            &account_name,
            &account_key,
        ));
        let static_paths = seed_arm(&static_warehouse);
        // Leaving every static S3 field empty is what makes the adapter read
        // this as an Azure CONNECTION rather than an ambiguous one.
        let static_password =
            lakekeeper_adls_connection_password(&static_warehouse, &account_name, &account_key);
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_STATIC, E2E_NAMESPACE).with_catalog_conn_name(CONN_STATIC),
            LAKEKEEPER_CATALOG_URI_INTERNAL,
            &static_password,
        );

        Self {
            account_name,
            container_name,
            vended_arm: AzureArm {
                warehouse: vended_warehouse,
                vs: VS_VENDED,
                data_file_paths: vended_paths,
            },
            static_arm: AzureArm {
                warehouse: static_warehouse,
                vs: VS_STATIC,
                data_file_paths: static_paths,
            },
            vended_password,
            _container: container,
        }
    }

    /// The `abfss://<container>@<account>.dfs.core.windows.net/` prefix every one
    /// of this fixture's data files carries, on either arm.
    ///
    /// Container-level by design, because the container is shared: an arm's own
    /// prefix is this one followed by that arm's warehouse name.
    fn abfss_prefix(&self) -> String {
        format!(
            "abfss://{}@{}.dfs.core.windows.net/",
            self.container_name, self.account_name
        )
    }
}

/// Create one arm's warehouse and confirm Lakekeeper registered it, returning
/// the warehouse name.
///
/// The readback guards two distinct failure mechanisms:
///
/// (a) A create Lakekeeper rejected for overlapping an existing warehouse's
/// storage profile — now reachable since two warehouses share one
/// `filesystem` — leaves no warehouse registered under this name.
/// `lakekeeper_warehouse_storage_profile` panics first, here, with its own
/// "no warehouse named ..." error, instead of resurfacing several steps
/// later as an opaque seed error.
///
/// (b) The `key-prefix` equality assertion below covers the separate case of
/// Lakekeeper registering the warehouse under a prefix other than the one
/// requested. It cannot report the overlap case in (a): that case panics
/// earlier, inside `lakekeeper_warehouse_storage_profile`, before this
/// assertion runs.
fn create_warehouse_and_confirm(profile: &AdlsWarehouseProfile) -> String {
    let warehouse = profile.name().to_string();
    lakekeeper_create_adls_warehouse(profile);

    let registered = lakekeeper_warehouse_storage_profile(&warehouse);
    assert_eq!(
        registered["key-prefix"].as_str(),
        Some(warehouse.as_str()),
        "Lakekeeper registered warehouse '{warehouse}' under a different key-prefix than \
         requested: {registered}"
    );
    warehouse
}

// ---------------------------------------------------------------------------
// End-to-end scan over both credential arms — vended assertions first.
// ---------------------------------------------------------------------------

/// Assert `arm`'s Virtual Schema exists.
///
/// A Virtual Schema existing is itself proof that this arm's whole chain ran: it
/// could not have been created over a missing container, a warehouse Lakekeeper
/// rejected, a credential the catalog refuses, or an unseeded table.
fn assert_vs_exists(conn: &mut ExaConn, arm: &AzureArm) {
    let cols = conn.query_columns(&format!(
        "SELECT SCHEMA_NAME FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = '{}'",
        arm.vs
    ));
    assert_eq!(
        cols.first().map(|c| c.len()).unwrap_or(0),
        1,
        "virtual schema {} must exist — its per-run ADLS warehouse '{}' must have been created, \
         seeded, and enumerated",
        arm.vs,
        arm.warehouse
    );
}

/// Assert every data file `arm` seeded lives under `arm`'s own Lakekeeper
/// `key-prefix`, and that none of them sits under `sibling`'s.
///
/// The container-level `abfss://` prefix is identical for both arms, so matching
/// it proves only that storage is real Azure — never which warehouse a file
/// belongs to. The per-arm prefix (which equals the warehouse name) proves both.
/// That disjointness is what makes the cross-arm row comparison a comparison of
/// two distinct file sets rather than of one arm against itself.
fn assert_paths_under_own_prefix(fixture: &AzureFixture, arm: &AzureArm, sibling: &AzureArm) {
    assert!(
        !arm.data_file_paths.is_empty(),
        "the seed must have committed at least one data file to warehouse '{}'",
        arm.warehouse
    );

    let own_prefix = format!("{}{}/", fixture.abfss_prefix(), arm.warehouse);
    let sibling_prefix = format!("{}{}/", fixture.abfss_prefix(), sibling.warehouse);
    for path in &arm.data_file_paths {
        assert!(
            path.starts_with(&own_prefix),
            "every data file seeded into '{}' must live in real Azure storage under that \
             warehouse's own key-prefix {own_prefix}, got: {path}",
            arm.warehouse
        );
        assert!(
            !path.starts_with(&sibling_prefix),
            "no data file of '{}' may sit under sibling warehouse '{}'s prefix {sibling_prefix} \
             — overlapping prefixes would turn the cross-arm row comparison into one arm \
             compared against itself: {path}",
            arm.warehouse,
            sibling.warehouse
        );
    }
}

/// Projection + filter + LIMIT correctness and row counts over `arm`'s real
/// Azure scan. Same seed shape as every other E2E suite: id 1..20,
/// score = 5.0 * id.
fn assert_projection_filter_limit(conn: &mut ExaConn, arm: &AzureArm) {
    let table = arm.table();
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
        assert!(s > 15.0, "filter violated over {table}: score {s} <= 15.0");
    }
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    assert!(
        ids.iter().all(|&id| id >= 4),
        "id < 4 appeared over {table} (its score would be <= 15): {ids:?}"
    );

    let filtered = conn.query_row_count(&format!("SELECT id FROM {table} WHERE score > 15.0"));
    assert_eq!(
        filtered, SEED_ROWS_SCORE_GT_15 as i64,
        "WHERE score > 15.0 over {table} must return {SEED_ROWS_SCORE_GT_15} rows, got {filtered}"
    );
    let total = conn.query_row_count(&format!("SELECT id FROM {table}"));
    assert_eq!(
        total, SEED_TOTAL_ROWS as i64,
        "ADLS warehouse '{}' must hold {SEED_TOTAL_ROWS} seeded rows, got {total}",
        arm.warehouse
    );
}

/// The `(id, name, score)` rows of `table`, ordered by id, as comparable tuples.
///
/// The ordering is part of the contract, not presentation: comparing two
/// unordered result sets for equality is not an equality check.
fn projection_rows(conn: &mut ExaConn, table: &str) -> Vec<(i64, String, f64)> {
    let cols = conn.query_columns(&format!("SELECT id, name, score FROM {table} ORDER BY id"));
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (id, name, score): {cols:?}"
    );
    (0..cols[0].len())
        .map(|i| {
            let id = parse_int(&cols[0][i]);
            let name = cols[1][i]
                .as_str()
                .unwrap_or_else(|| panic!("name not string: {:?}", cols[1][i]))
                .to_string();
            let score = cols[2][i]
                .as_f64()
                .or_else(|| cols[2][i].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or_else(|| panic!("score not numeric: {:?}", cols[2][i]));
            (id, name, score)
        })
        .collect()
}

/// Both credential arms — static account key and vended SAS — end to end over
/// one per-run container, in one test because they share one fixture: the
/// container guard cannot live in a `OnceLock`, so splitting the arms would mean
/// a second live-Azure container.
///
/// **The assertion order below is normative, not style.** Every vended-arm
/// assertion except the final cross-arm comparison runs BEFORE the static arm's,
/// because the vended CONNECTION carries no account name and no account key —
/// which makes a passing vended scan reachable only through the SAS Lakekeeper
/// mints, and therefore the strongest and most specific proof in this file. A
/// static-arm query or assertion regression aborting the run after that proof
/// has already run is harmless; the reverse order would hide a vended regression
/// behind an unrelated static-arm failure.
#[test]
fn azure_static_and_vended_creds_end_to_end() {
    let fixture = AzureFixture::provision();
    let mut conn = exa_conn();

    // 1. The vended CONNECTION's REQUIRED shape, read off the very password
    //    `provision` installed as CONN_VENDED rather than off a re-derived
    //    equivalent: no storage field of any kind. Not merely a delegation hint
    //    — with nothing for scheme-driven resolution to fall back on, the
    //    vended SAS is the only credential that can make the scan below
    //    succeed.
    assert!(
        fixture.vended_password.use_vended_credentials,
        "the vended ADLS CONNECTION must request access delegation"
    );
    assert!(
        fixture.vended_password.account_name.is_none()
            && fixture.vended_password.account_key.is_none(),
        "a vended ADLS CONNECTION must carry NO Azure storage field: an account name or account \
         key present would let the scan read the container without ever exercising the vended SAS"
    );
    assert!(
        fixture.vended_password.endpoint.is_empty()
            && fixture.vended_password.region.is_empty()
            && fixture.vended_password.access_key.is_empty()
            && fixture.vended_password.secret_key.is_empty(),
        "a vended CONNECTION must carry no static S3 storage field either"
    );

    // 2. The vended warehouse as Lakekeeper itself reports it back — not as this
    //    harness constructed it.
    let vended_profile = lakekeeper_warehouse_storage_profile(&fixture.vended_arm.warehouse);
    assert_eq!(
        vended_profile["sas-enabled"].as_bool(),
        Some(true),
        "the vended warehouse's sas-enabled must be true — Lakekeeper vends a SAS only under \
         delegation, and this arm's scan has no other credential: {vended_profile}"
    );
    assert_eq!(
        vended_profile["filesystem"].as_str(),
        Some(fixture.container_name.as_str()),
        "the vended warehouse must sit on the same run container as its static sibling: \
         {vended_profile}"
    );

    // 3. The vended Virtual Schema exists, and its seed landed under the vended
    //    warehouse's own key-prefix rather than the static sibling's.
    assert_vs_exists(&mut conn, &fixture.vended_arm);
    assert_paths_under_own_prefix(&fixture, &fixture.vended_arm, &fixture.static_arm);

    // 4. The vended scan itself: projection, filter, LIMIT, and row counts.
    assert_projection_filter_limit(&mut conn, &fixture.vended_arm);

    // 5. The scan path came from the shared harness definition, not a
    //    duplicated local one: one schema-level script pair for the whole
    //    binary, and BOTH Virtual Schemas created USING the adapter script from
    //    that pair — checking only one arm would leave the other's provenance
    //    unverified. Covering both arms is why this group sits above the static
    //    arm's own groups rather than below them: the vended arm's provenance
    //    must be established before any static-arm assertion can abort the run.
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
    for arm in [&fixture.vended_arm, &fixture.static_arm] {
        let cols = conn.query_columns(&format!(
            "SELECT ADAPTER_SCRIPT_SCHEMA || '.' || ADAPTER_SCRIPT_NAME \
             FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = '{}'",
            arm.vs
        ));
        let adapter = cols
            .first()
            .and_then(|c| c.first())
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("virtual schema {} must report its adapter script", arm.vs));
        assert!(
            adapter.to_uppercase().contains(&SCHEMA_NAME.to_uppercase())
                && adapter
                    .to_uppercase()
                    .contains(&ADAPTER_SCRIPT_NAME.to_uppercase()),
            "VS {} must be created USING the shared adapter script \
             {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME}, got: {adapter}",
            arm.vs
        );
    }

    // 6. Only now the static arm, in the groups it has always run.
    assert_vs_exists(&mut conn, &fixture.static_arm);

    let static_profile = lakekeeper_warehouse_storage_profile(&fixture.static_arm.warehouse);
    assert_eq!(
        static_profile["type"].as_str(),
        Some("adls"),
        "storage profile type must be adls: {static_profile}"
    );
    assert_eq!(
        static_profile["account-name"].as_str(),
        Some(fixture.account_name.as_str()),
        "storage profile account-name must be the configured account: {static_profile}"
    );
    assert_eq!(
        static_profile["filesystem"].as_str(),
        Some(fixture.container_name.as_str()),
        "storage profile filesystem must be the run's own container: {static_profile}"
    );
    assert_eq!(
        static_profile["key-prefix"].as_str(),
        Some(fixture.static_arm.warehouse.as_str()),
        "storage profile key-prefix must be the warehouse name: {static_profile}"
    );
    assert_eq!(
        static_profile["sas-enabled"].as_bool(),
        Some(false),
        "storage profile sas-enabled must be false — a SAS-vending static warehouse would let \
         this arm's scan succeed without ever exercising the account key under test, even with \
         a SAS-vending sibling sharing the container: {static_profile}"
    );

    assert_paths_under_own_prefix(&fixture, &fixture.static_arm, &fixture.vended_arm);
    assert_projection_filter_limit(&mut conn, &fixture.static_arm);

    // 7. Last, because it depends on both arms having already proved themselves:
    //    the two arms return the same rows. Ordered by id on both sides — an
    //    unordered comparison would not be an equality check at all. This is
    //    only a comparison of two distinct file sets because group 3 and group 6
    //    established that the arms' key-prefixes are disjoint.
    let vended_rows = projection_rows(&mut conn, &fixture.vended_arm.table());
    let static_rows = projection_rows(&mut conn, &fixture.static_arm.table());
    assert_eq!(
        vended_rows, static_rows,
        "the vended-SAS scan must return exactly the rows the account-key scan returns — both \
         warehouses carry the same deterministic 20-row seed"
    );
    assert_eq!(
        vended_rows.len(),
        SEED_TOTAL_ROWS,
        "the ordered cross-arm projection must cover all {SEED_TOTAL_ROWS} seeded rows"
    );
}

// ---------------------------------------------------------------------------
// Container guard deletes on panic, even nested inside an active runtime.
// ---------------------------------------------------------------------------

/// The container guard deletes its container while unwinding from a panic that
/// crosses an active `rt.block_on` — the case `AzureContainer::drop`'s own
/// teardown thread exists to survive (driving the delete on the ambient runtime
/// from `Drop` would re-enter "Cannot start a runtime from within a runtime").
///
/// One guard covers both credential arms: the container it deletes holds the
/// static and vended warehouses' data alike, so the second arm adds no
/// Azure-side orphan surface. It is also why the arms cannot be split across two
/// tests — the guard has to stay owned by the provisioning test's stack frame.
///
/// Uses `futures::FutureExt::catch_unwind`, not `std::panic::catch_unwind`: the
/// guard's construction is `async`, and a nested `Handle::block_on` inside a
/// synchronous closure would trigger the very panic this test proves is fixed.
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

/// Mirrors the Lakekeeper suite's `lakekeeper_suite_fails_when_stack_unavailable`:
/// a readiness wait against an unreachable stack PANICS rather than returning,
/// so a down stack fails the test instead of silently skipping it.
#[test]
fn azure_suite_fails_when_stack_unavailable() {
    let result = std::panic::catch_unwind(|| {
        // 127.0.0.1:1 refuses immediately, so the poll loop hits the deadline
        // and panics — the fail-not-skip contract.
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

/// A failing, credential-bearing Azure CONNECTION DDL through a redacting
/// `ExaConn` must not leak the SQL text or either sentinel value into the
/// failure output. Mirrors the Lakekeeper suite's
/// `lakekeeper_credentials_never_appear_in_output`.
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

/// `.gitignore` must list `test.env`, and `test.env.example` must name all five
/// Azure variables with only the `placeholder` sentinel — never a real value.
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

/// `make test-e2e-azure`'s shape, asserted against the Makefile text rather than
/// by running `make` (which would trigger the full Docker/build pipeline inside
/// a test).
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
