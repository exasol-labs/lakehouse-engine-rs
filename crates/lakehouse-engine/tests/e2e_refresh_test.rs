//! End-to-end tests for `ALTER VIRTUAL SCHEMA ... REFRESH` and
//! `ALTER VIRTUAL SCHEMA ... SET` (vs-adapter/refresh-and-set-properties, #147).
//!
//! Shares the Exasol + MinIO + Iceberg REST catalog stack with the other E2E
//! test binaries (`iceberg_catalog_url_internal`, BucketFS upload, SLC
//! install), but every test in this file uses its OWN dedicated Iceberg
//! namespace and its OWN Virtual Schema instance. This is deliberate
//! isolation, not incidental duplication: several scenarios here mutate the
//! underlying catalog out of band (add a table, add a column, introduce a
//! flatten-name collision), and a collision or schema change introduced into
//! the SHARED `e2e_lakehouse` namespace used by `e2e_scan_test.rs` and
//! siblings would permanently break every other E2E binary's
//! `createVirtualSchema` call against that namespace for the rest of the
//! Docker session. Dedicated namespaces keep this file's mutations from
//! leaking into any other test binary.
//!
//! All tests FAIL (never skip) when the stack is unavailable, per project
//! rules (`CLAUDE.md` Testing section).
#![cfg(feature = "exasol-e2e")]

mod common;
use common::exasol_ws::ExaConn;
use common::seed::{build_seed_catalog, rest_replace_current_schema};
use common::stack::{
    CatalogConnectionPassword, bucketfs_port, bucketfs_write_password, build_create_connection_sql,
    exasol_host, exasol_sql_port, iceberg_catalog_url, iceberg_catalog_url_internal,
    lakehouse_engine_so_path, local_stack_connection_password, upload_to_bucketfs, wait_for_exasol,
    wait_for_iceberg_catalog, wait_for_minio,
};

use arrow::array::{Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use iceberg::spec::{
    NestedField, PrimitiveType, Schema as IcebergSchema, Type, UnboundPartitionSpec,
};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Constants (mirror e2e_scan_test.rs — same stack, own VS/connection names)
// ---------------------------------------------------------------------------

const SYS_PASSWORD: &str = "exasol";
const SCHEMA_NAME: &str = "LHVS";
const ADAPTER_SCRIPT_NAME: &str = "LAKEHOUSE_ADAPTER";
const SCAN_SCRIPT_NAME: &str = "LAKEHOUSE_SCAN";
const MERGE_SCRIPT_NAME: &str = "LAKEHOUSE_DISTINCT_MERGE_COUNT";
const DISTRIBUTOR_SCRIPT_NAME: &str = "LAKEHOUSE_DISTRIBUTE_FILES";
const SO_BUCKETFS_PUT_PATH: &str = "/default/udf/liblakehouse_engine.so";
const SO_UDF_OBJECT_PATH: &str = "buckets/bfsdefault/default/udf/liblakehouse_engine.so";
const SLC_BUCKETFS_PUT_PATH: &str = "/default/slc/lakehouse-rustslc.tar.gz";
const SLC_VERSION: &str = "0.21.0";
const LANG_ALIAS: &str = "RUST";
/// Dedicated CONNECTION name for this file — distinct from other E2E binaries'
/// `LAKEHOUSE_CATALOG_CREDS` so a `CREATE OR REPLACE CONNECTION` here (used by
/// the unreachable-catalog test) can never race with another binary's use of
/// the same object name.
const CATALOG_CONN_NAME: &str = "REFRESH_CATALOG_CREDS";

// Dedicated namespaces — one per scenario, never the shared `e2e_lakehouse`.
const NS_REENUM: &str = "e2e_refresh_reenum";
const NS_COLCHANGE: &str = "e2e_refresh_colchange";
const NS_SETPROPS_A: &str = "e2e_refresh_setprops_a";
const NS_SETPROPS_B: &str = "e2e_refresh_setprops_b";
const NS_UNREACHABLE: &str = "e2e_refresh_unreachable";
const NS_PARTIAL: &str = "e2e_refresh_partial";
const NS_COLLISION: &str = "e2e_refresh_collision";

// ---------------------------------------------------------------------------
// One-time setup (idempotent; mirrors e2e_scan_test.rs, without the shared seed)
// ponytail: duplicate of e2e_scan_test setup — each E2E binary runs
// independently, so each needs its own OnceLock guard.
// ---------------------------------------------------------------------------

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup_e2e() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_minio();
        wait_for_iceberg_catalog();

        install_slc();

        let so_path = lakehouse_engine_so_path();
        upload_to_bucketfs(&so_path, SO_BUCKETFS_PUT_PATH);

        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
    });
}

fn install_slc() {
    let slc_url = format!(
        "https://github.com/exasol-labs/language-container-rs/releases/download/v{SLC_VERSION}/lc-rust-{SLC_VERSION}.tar.gz"
    );
    let tarball_bytes = reqwest::blocking::get(&slc_url)
        .unwrap_or_else(|e| panic!("download SLC {SLC_VERSION} from {slc_url}: {e}"))
        .bytes()
        .unwrap_or_else(|e| panic!("read SLC tarball bytes: {e}"));
    assert!(
        !tarball_bytes.is_empty(),
        "SLC tarball is empty — download failed"
    );

    let password = bucketfs_write_password();
    let bfs_url = format!(
        "https://{}:{}{}",
        exasol_host(),
        bucketfs_port(),
        SLC_BUCKETFS_PUT_PATH
    );
    let client = reqwest::blocking::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(120))
        .build()
        .expect("BucketFS client");
    let resp = client
        .put(&bfs_url)
        .basic_auth("w", Some(&password))
        .body(tarball_bytes.to_vec())
        .send()
        .unwrap_or_else(|e| panic!("BucketFS PUT SLC to {bfs_url}: {e}"));
    assert!(
        resp.status().is_success(),
        "BucketFS PUT SLC returned {} — expected 2xx",
        resp.status()
    );

    let mut conn = exa_conn();
    let rust_def = format!(
        "{LANG_ALIAS}=localzmq+protobuf:///bfsdefault/default/slc/lakehouse-rustslc?lang=rust#buckets/bfsdefault/default/slc/lakehouse-rustslc/exaudf/exaudfclient"
    );
    let current = conn.query_columns(
        "SELECT SYSTEM_VALUE FROM EXA_PARAMETERS WHERE PARAMETER_NAME='SCRIPT_LANGUAGES'",
    );
    let current_val = current
        .first()
        .and_then(|col| col.first())
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let preserved = current_val
        .split_whitespace()
        .filter(|s| !s.starts_with(&format!("{LANG_ALIAS}=")))
        .collect::<Vec<_>>()
        .join(" ");
    let new_val = format!("{preserved} {rust_def}");
    conn.execute(&format!(
        "ALTER SYSTEM SET SCRIPT_LANGUAGES = '{}'",
        new_val.trim()
    ));
}

fn exa_conn() -> ExaConn {
    ExaConn::connect(&exasol_host(), exasol_sql_port(), "sys", SYS_PASSWORD)
}

fn create_schema_and_scripts(conn: &mut ExaConn) {
    conn.execute(&format!("CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"));
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} ADAPTER SCRIPT {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} SCALAR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME}(common VARCHAR(2000000), files VARCHAR(2000000))
EMITS (...) AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} SCALAR SCRIPT {SCHEMA_NAME}.{MERGE_SCRIPT_NAME}(partials VARCHAR(2000000))
RETURNS DECIMAL(20,0) AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
    conn.execute(&format!(
        r#"CREATE OR REPLACE LUA SET SCRIPT {SCHEMA_NAME}.{DISTRIBUTOR_SCRIPT_NAME}(files VARCHAR(2000000))
EMITS (files VARCHAR(2000000)) AS
function run(ctx)
    repeat
        ctx.emit(ctx.files)
    until not ctx.next()
end
/"#
    ));
}

/// Create (or replace) the shared catalog CONNECTION and issue
/// `CREATE VIRTUAL SCHEMA <vs_name> ... ICEBERG_NAMESPACE = '<namespace>'`.
fn create_virtual_schema(conn: &mut ExaConn, vs_name: &str, namespace: &str) {
    let password = local_stack_connection_password();
    let catalog_uri = iceberg_catalog_url_internal();
    let create_conn_sql = build_create_connection_sql(CATALOG_CONN_NAME, &catalog_uri, &password);
    conn.execute(&create_conn_sql);

    let _ = conn.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {vs_name} CASCADE"));
    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {vs_name}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{CATALOG_CONN_NAME}'
  ICEBERG_NAMESPACE   = '{namespace}'
  ALLOW_HTTP          = 'true'"#
    ));
}

fn vs_table(vs_name: &str, table_name: &str) -> String {
    format!("{vs_name}.{}", table_name.to_uppercase())
}

// ---------------------------------------------------------------------------
// Iceberg fixture helpers — build a REST catalog client and seed/mutate
// dedicated single-column-set tables directly (out-of-band from Exasol),
// mirroring the seed patterns in tests/common/seed.rs but scoped to this
// file's own namespaces.
// ---------------------------------------------------------------------------

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Schema `(id BIGINT NOT NULL, val DOUBLE NOT NULL)`.
fn id_val_schema(schema_id: i32) -> IcebergSchema {
    IcebergSchema::builder()
        .with_schema_id(schema_id)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "val", Type::Primitive(PrimitiveType::Double)).into(),
        ])
        .build()
        .expect("build id+val Iceberg schema")
}

/// Schema `(id BIGINT NOT NULL, val DOUBLE NOT NULL, new_col DOUBLE)` — the
/// result of an `add-schema` evolution over [`id_val_schema`], adding one
/// optional column (field-id 3) while preserving field-ids 1 and 2.
fn id_val_schema_with_new_col(schema_id: i32) -> IcebergSchema {
    IcebergSchema::builder()
        .with_schema_id(schema_id)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "val", Type::Primitive(PrimitiveType::Double)).into(),
            NestedField::optional(3, "new_col", Type::Primitive(PrimitiveType::Double)).into(),
        ])
        .build()
        .expect("build id+val+new_col Iceberg schema")
}

fn id_val_batch(id: i64, val: f64) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Float64Array::from(vec![val])),
        ],
    )
    .expect("id+val RecordBatch construction is infallible")
}

/// Create `namespace.table_name` (single-level namespace) with schema
/// `(id, val)` and one row, if it does not already exist with data.
async fn ensure_id_val_table(
    catalog: &impl Catalog,
    namespace: &str,
    table_name: &str,
    id: i64,
    val: f64,
) {
    common::seed::create_and_append(
        catalog,
        namespace,
        table_name,
        id_val_schema(0),
        std::iter::once(id_val_batch(id, val)),
    )
    .await
    .unwrap_or_else(|e| panic!("seed {namespace}.{table_name}: {e}"));
}

/// Evolve `namespace.table_name`'s current schema to add the optional
/// `new_col DOUBLE` column (field-id 3), via a raw REST `add-schema` +
/// `set-current-schema` commit (mirrors `common::seed::seed_renamed_column`).
/// Existing rows are unaffected on disk; they project `new_col` as NULL
/// (Iceberg column-projection rule 3 — no `initial-default` was set).
async fn add_new_col(catalog: &impl Catalog, namespace: &str, table_name: &str) {
    let ident = TableIdent::new(
        NamespaceIdent::new(namespace.to_string()),
        table_name.to_string(),
    );
    let table = catalog
        .load_table(&ident)
        .await
        .unwrap_or_else(|e| panic!("load {namespace}.{table_name} before column add: {e}"));
    let current_schema_id = table.metadata().current_schema_id();
    rest_replace_current_schema(
        &iceberg_catalog_url(),
        namespace,
        table_name,
        current_schema_id,
        id_val_schema_with_new_col(current_schema_id + 1),
    )
    .await
    .unwrap_or_else(|e| panic!("add new_col to {namespace}.{table_name}: {e}"));
}

/// Create an empty (no data file) table at a possibly multi-level
/// `NamespaceIdent`, for the flatten-collision fixture — collision detection
/// happens in `build_table_map`, before any per-table schema/data resolution,
/// so the colliding tables need no data files.
async fn create_empty_table(catalog: &impl Catalog, ns: &NamespaceIdent, table_name: &str) {
    if !catalog
        .namespace_exists(ns)
        .await
        .unwrap_or_else(|e| panic!("check namespace {}: {e}", ns.join(".")))
    {
        let _ = catalog.create_namespace(ns, HashMap::new()).await;
    }
    let ident = TableIdent::new(ns.clone(), table_name.to_string());
    if catalog
        .table_exists(&ident)
        .await
        .unwrap_or_else(|e| panic!("check table {}.{}: {e}", ns.join("."), table_name))
    {
        return;
    }
    let creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(id_val_schema(0))
        .partition_spec(UnboundPartitionSpec::builder().with_spec_id(0).build())
        .properties(HashMap::new())
        .build();
    // Tolerate a concurrent create (idempotent re-run of this test file).
    let _ = catalog.create_table(ns, creation).await;
}

/// Drop `ns.table_name` if it currently exists, so a table left behind by a
/// PREVIOUS run against this same long-lived Docker warehouse (this stack is
/// not torn down between `make test-e2e` invocations) cannot leak into this
/// run's "must not exist yet" assertions. Mirrors the drop-before-reseed
/// pattern in `common::seed::create_and_append_files`, but unconditional
/// (callers here need a clean slate, not a schema-matches check).
async fn drop_table_if_exists(catalog: &impl Catalog, ns: &NamespaceIdent, table_name: &str) {
    let ident = TableIdent::new(ns.clone(), table_name.to_string());
    if catalog
        .table_exists(&ident)
        .await
        .unwrap_or_else(|e| panic!("check table {}.{table_name}: {e}", ns.join(".")))
    {
        catalog
            .drop_table(&ident)
            .await
            .unwrap_or_else(|e| panic!("drop stale table {}.{table_name}: {e}", ns.join(".")));
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Refresh re-enumerates the namespace: a table added to the catalog after
/// `CREATE VIRTUAL SCHEMA` is unreachable until `REFRESH`, then becomes
/// queryable — proving `refresh` is dispatched to the real enumeration path
/// rather than rejected as `unsupported VS request type` (#147's root cause).
#[test]
fn refresh_reenumerates_namespace() {
    setup_e2e();
    let rt = rt();
    let catalog_url = iceberg_catalog_url();
    let catalog = rt
        .block_on(build_seed_catalog(
            &catalog_url,
            "s3://warehouse/",
            "refresh-reenum",
        ))
        .expect("build seed catalog");

    // Given a clean slate: a PREVIOUS run may have left T_NEW behind (this
    // test's own seeding, further below), which would make it queryable
    // right after CREATE and falsify the "T_NEW must be unknown before
    // REFRESH" assertion below.
    let reenum_ns = NamespaceIdent::new(NS_REENUM.to_string());
    rt.block_on(drop_table_if_exists(&catalog, &reenum_ns, "t_orig"));
    rt.block_on(drop_table_if_exists(&catalog, &reenum_ns, "t_new"));

    rt.block_on(ensure_id_val_table(&catalog, NS_REENUM, "t_orig", 1, 10.0));

    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_REENUM_VS", NS_REENUM);

    let id = conn.query_scalar_i64(&format!(
        "SELECT id FROM {}",
        vs_table("REFRESH_REENUM_VS", "t_orig")
    ));
    assert_eq!(id, 1, "T_ORIG must be queryable right after CREATE");

    let resp = conn.try_execute(&format!(
        "SELECT * FROM {}",
        vs_table("REFRESH_REENUM_VS", "t_new")
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "T_NEW must be unknown before it exists in the catalog and before REFRESH: {resp}"
    );

    // Add the table out-of-band — Exasol/the VS never sees this write directly.
    rt.block_on(ensure_id_val_table(&catalog, NS_REENUM, "t_new", 42, 99.0));

    conn.execute("ALTER VIRTUAL SCHEMA REFRESH_REENUM_VS REFRESH");

    let id = conn.query_scalar_i64(&format!(
        "SELECT id FROM {}",
        vs_table("REFRESH_REENUM_VS", "t_new")
    ));
    assert_eq!(
        id, 42,
        "T_NEW must be queryable after REFRESH re-enumerates the namespace"
    );
}

/// Refresh reflects a column added to the underlying catalog after
/// `CREATE VIRTUAL SCHEMA`: the new column is unknown to Exasol (a SQL-level
/// column-not-found error) until `REFRESH` re-resolves the table's current
/// Iceberg schema, after which the column becomes selectable (NULL for the
/// pre-existing row, per Iceberg's column-projection rule for a column absent
/// from a data file with no `initial-default`).
#[test]
fn refresh_reflects_added_table_and_column_change() {
    setup_e2e();
    let rt = rt();
    let catalog_url = iceberg_catalog_url();
    let catalog = rt
        .block_on(build_seed_catalog(
            &catalog_url,
            "s3://warehouse/",
            "refresh-colchange",
        ))
        .expect("build seed catalog");

    rt.block_on(ensure_id_val_table(&catalog, NS_COLCHANGE, "evt", 1, 10.0));

    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_COLCHANGE_VS", NS_COLCHANGE);

    let val = conn.query_columns(&format!(
        "SELECT val FROM {}",
        vs_table("REFRESH_COLCHANGE_VS", "evt")
    ));
    assert_eq!(val[0].len(), 1, "EVT must be queryable right after CREATE");

    let resp = conn.try_execute(&format!(
        "SELECT new_col FROM {}",
        vs_table("REFRESH_COLCHANGE_VS", "evt")
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "NEW_COL must be unknown to Exasol before REFRESH: {resp}"
    );

    // Add the column out-of-band via a raw REST add-schema commit.
    rt.block_on(add_new_col(&catalog, NS_COLCHANGE, "evt"));

    conn.execute("ALTER VIRTUAL SCHEMA REFRESH_COLCHANGE_VS REFRESH");

    let cols = conn.query_columns(&format!(
        "SELECT id, new_col FROM {}",
        vs_table("REFRESH_COLCHANGE_VS", "evt")
    ));
    assert_eq!(
        cols[0].len(),
        1,
        "expected exactly the one pre-existing row after REFRESH: {cols:?}"
    );
    assert!(
        cols[1][0].is_null(),
        "NEW_COL for the pre-existing row must be NULL (absent from the data \
         file, no initial-default): {cols:?}"
    );
}

/// `setProperties` (`ALTER VIRTUAL SCHEMA ... SET ICEBERG_NAMESPACE=...`)
/// re-targets the virtual schema at a different namespace and rebuilds
/// TABLE_MAP from scratch: the newly targeted namespace's table becomes
/// queryable and the old namespace's table is no longer registered.
#[test]
fn set_properties_retargets_namespace() {
    setup_e2e();
    let rt = rt();
    let catalog_url = iceberg_catalog_url();
    let catalog = rt
        .block_on(build_seed_catalog(
            &catalog_url,
            "s3://warehouse/",
            "refresh-setprops",
        ))
        .expect("build seed catalog");

    rt.block_on(ensure_id_val_table(
        &catalog,
        NS_SETPROPS_A,
        "a_tbl",
        100,
        1.0,
    ));
    rt.block_on(ensure_id_val_table(
        &catalog,
        NS_SETPROPS_B,
        "b_tbl",
        200,
        2.0,
    ));

    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_SETPROPS_VS", NS_SETPROPS_A);

    let id = conn.query_scalar_i64(&format!(
        "SELECT id FROM {}",
        vs_table("REFRESH_SETPROPS_VS", "a_tbl")
    ));
    assert_eq!(
        id, 100,
        "A_TBL must be queryable under the original namespace"
    );

    let resp = conn.try_execute(&format!(
        "SELECT * FROM {}",
        vs_table("REFRESH_SETPROPS_VS", "b_tbl")
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "B_TBL must be unknown while the VS targets the A namespace: {resp}"
    );

    conn.execute(&format!(
        "ALTER VIRTUAL SCHEMA REFRESH_SETPROPS_VS SET ICEBERG_NAMESPACE='{NS_SETPROPS_B}'"
    ));

    let id = conn.query_scalar_i64(&format!(
        "SELECT id FROM {}",
        vs_table("REFRESH_SETPROPS_VS", "b_tbl")
    ));
    assert_eq!(
        id, 200,
        "B_TBL must be queryable after setProperties re-targets the namespace"
    );

    let resp = conn.try_execute(&format!(
        "SELECT * FROM {}",
        vs_table("REFRESH_SETPROPS_VS", "a_tbl")
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "A_TBL must no longer be registered — TABLE_MAP is a full rebuild from \
         the newly targeted namespace, never a merge of both: {resp}"
    );
}

/// Refresh against an unreachable catalog returns a clear error without
/// leaking credentials — mirrors `create_vs_unreachable_catalog_errors_no_secret`
/// (`e2e_scan_test.rs`), but exercised on the `refresh` path: the VS is first
/// created successfully against the real local catalog, then the SAME
/// CONNECTION object is replaced to point at a bogus, unreachable catalog
/// endpoint with bogus secret values before REFRESH is issued.
#[test]
fn refresh_unreachable_catalog_redacts_credentials() {
    setup_e2e();
    let rt = rt();
    let catalog_url = iceberg_catalog_url();
    let catalog = rt
        .block_on(build_seed_catalog(
            &catalog_url,
            "s3://warehouse/",
            "refresh-unreachable",
        ))
        .expect("build seed catalog");
    rt.block_on(ensure_id_val_table(
        &catalog,
        NS_UNREACHABLE,
        "u_tbl",
        1,
        1.0,
    ));

    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_UNREACHABLE_VS", NS_UNREACHABLE);

    // Baseline: the VS works against the real, reachable catalog.
    let id = conn.query_scalar_i64(&format!(
        "SELECT id FROM {}",
        vs_table("REFRESH_UNREACHABLE_VS", "u_tbl")
    ));
    assert_eq!(
        id, 1,
        "U_TBL must be queryable before the connection is broken"
    );

    // Replace the SAME connection object with a bogus, unreachable endpoint
    // carrying secret-shaped credentials.
    let bogus_password = CatalogConnectionPassword {
        warehouse: "s3://warehouse/".to_string(),
        endpoint: "http://does-not-exist.invalid:9000".to_string(),
        region: "us-east-1".to_string(),
        access_key: "SUPER_SECRET_KEY".to_string(),
        secret_key: "SUPER_SECRET_VALUE".to_string(),
        session_token: None,
        path_style: true,
        use_sigv4: false,
        use_vended_credentials: false,
    };
    let bogus_uri = "http://does-not-exist.invalid:8181";
    let replace_conn_sql =
        build_create_connection_sql(CATALOG_CONN_NAME, bogus_uri, &bogus_password);
    conn.execute(&replace_conn_sql);

    let resp = conn.try_execute("ALTER VIRTUAL SCHEMA REFRESH_UNREACHABLE_VS REFRESH");
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "expected an error when REFRESH hits an unreachable catalog: {resp}"
    );
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        !msg.contains("SUPER_SECRET_KEY") && !msg.contains("SUPER_SECRET_VALUE"),
        "refresh error message must not leak credentials: {msg}"
    );

    // Restore a working connection so any later test run against this
    // namespace (or a re-run of this test) is not left in a broken state.
    let create_conn_sql = build_create_connection_sql(
        CATALOG_CONN_NAME,
        &iceberg_catalog_url_internal(),
        &local_stack_connection_password(),
    );
    conn.execute(&create_conn_sql);
}

/// Adversarial-review finding A1 (PR #153): the plan's Non-Goals originally
/// claimed the adapter trusts Exasol to scope a partial `REFRESH TABLES <t>`
/// to only the named table. Running this test against the live stack
/// disproved that claim: after mutating two tables and refreshing only
/// `TABLE_ONE`, `TABLE_TWO`'s new column was ALSO visible. plan.md and
/// spec.md were corrected to state the verified behavior; this test is now
/// the regression test for that behavior instead of the disproved one.
///
/// This test creates two tables, changes BOTH out of band (adds `new_col` to
/// each), then runs `REFRESH TABLES` naming only `TABLE_ONE`, and asserts
/// BOTH tables' new column is visible afterward — Exasol applies the
/// adapter's full-namespace response regardless of `requestedTables`.
#[test]
fn refresh_partial_requested_tables_still_refreshes_whole_namespace() {
    setup_e2e();
    let rt = rt();
    let catalog_url = iceberg_catalog_url();
    let catalog = rt
        .block_on(build_seed_catalog(
            &catalog_url,
            "s3://warehouse/",
            "refresh-partial",
        ))
        .expect("build seed catalog");

    rt.block_on(ensure_id_val_table(
        &catalog,
        NS_PARTIAL,
        "table_one",
        1,
        10.0,
    ));
    rt.block_on(ensure_id_val_table(
        &catalog,
        NS_PARTIAL,
        "table_two",
        2,
        20.0,
    ));

    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_PARTIAL_VS", NS_PARTIAL);

    // Baseline: neither table exposes NEW_COL yet.
    for table in ["table_one", "table_two"] {
        let resp = conn.try_execute(&format!(
            "SELECT new_col FROM {}",
            vs_table("REFRESH_PARTIAL_VS", table)
        ));
        assert_eq!(
            resp["status"].as_str(),
            Some("error"),
            "{table} must not expose NEW_COL before any REFRESH: {resp}"
        );
    }

    // Mutate BOTH tables out of band.
    rt.block_on(add_new_col(&catalog, NS_PARTIAL, "table_one"));
    rt.block_on(add_new_col(&catalog, NS_PARTIAL, "table_two"));

    // Partial refresh naming ONLY table_one.
    conn.execute("ALTER VIRTUAL SCHEMA REFRESH_PARTIAL_VS REFRESH TABLES TABLE_ONE");

    let cols = conn.query_columns(&format!(
        "SELECT id, new_col FROM {}",
        vs_table("REFRESH_PARTIAL_VS", "table_one")
    ));
    assert_eq!(
        cols[0].len(),
        1,
        "TABLE_ONE must still return its one row after a partial refresh: {cols:?}"
    );
    assert!(
        cols[1][0].is_null(),
        "TABLE_ONE's NEW_COL must be visible (NULL, no initial-default) after \
         REFRESH TABLES TABLE_ONE: {cols:?}"
    );

    // Exasol does not scope REFRESH TABLES to requestedTables — verified live.
    let cols_two = conn.query_columns(&format!(
        "SELECT id, new_col FROM {}",
        vs_table("REFRESH_PARTIAL_VS", "table_two")
    ));
    assert_eq!(
        cols_two[0].len(),
        1,
        "TABLE_TWO must still return its one row after a refresh naming only \
         TABLE_ONE: {cols_two:?}"
    );
    assert!(
        cols_two[1][0].is_null(),
        "TABLE_TWO's NEW_COL must ALSO be visible after REFRESH TABLES \
         TABLE_ONE — Exasol applies the adapter's full-namespace response to \
         every table regardless of requestedTables, so a partial refresh has \
         the same real-world effect as a full REFRESH: {cols_two:?}"
    );
}

/// Adversarial-review finding A4 (PR #153): a flatten-name (`__`) collision
/// surfaced by RE-ENUMERATION at refresh time must return the exact same
/// class of error `createVirtualSchema` already returns for this case
/// (`flatten_multilevel_namespace_and_detect_collision`,
/// `crates/lakehouse-engine/src/adapter/mod.rs`) — not a silent overwrite or
/// drop of one of the colliding tables.
///
/// Namespace `e2e_refresh_collision` starts with one baseline table and a
/// working VS. Two tables are then added out of band whose flattened Exasol
/// names collide: a direct table `eu__orders` (namespace
/// `e2e_refresh_collision`) and a table `orders` in the descendant namespace
/// `e2e_refresh_collision.eu` both flatten to `EU__ORDERS`
/// (`adapter::tables::flatten_table_name`). `REFRESH` must then surface the
/// same collision error a fresh `createVirtualSchema` over the same
/// (now-colliding) namespace would.
#[test]
fn refresh_flatten_collision_returns_same_error_as_create() {
    setup_e2e();
    let rt = rt();
    let catalog_url = iceberg_catalog_url();
    let catalog = rt
        .block_on(build_seed_catalog(
            &catalog_url,
            "s3://warehouse/",
            "refresh-collision",
        ))
        .expect("build seed catalog");

    let top_ns = NamespaceIdent::new(NS_COLLISION.to_string());
    let eu_ns = NamespaceIdent::from_vec(vec![NS_COLLISION.to_string(), "eu".to_string()])
        .expect("build descendant NamespaceIdent");

    // Given a clean slate: a PREVIOUS run may have left the colliding tables
    // behind (introduced out-of-band further below), which would make the
    // very first `create_virtual_schema` call hit the collision before the
    // "BASELINE must be queryable before the collision is introduced"
    // assertion ever runs.
    rt.block_on(drop_table_if_exists(&catalog, &top_ns, "baseline"));
    rt.block_on(drop_table_if_exists(&catalog, &top_ns, "eu__orders"));
    rt.block_on(drop_table_if_exists(&catalog, &eu_ns, "orders"));

    rt.block_on(create_empty_table(&catalog, &top_ns, "baseline"));

    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_COLLISION_VS", NS_COLLISION);

    // Baseline VS works before the collision is introduced.
    let resp = conn.try_execute(&format!(
        "SELECT * FROM {}",
        vs_table("REFRESH_COLLISION_VS", "baseline")
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("ok"),
        "BASELINE must be queryable before the collision is introduced: {resp}"
    );

    // Introduce the collision out of band: direct `eu__orders` and descendant
    // `eu.orders` both flatten to `EU__ORDERS`.
    rt.block_on(create_empty_table(&catalog, &top_ns, "eu__orders"));
    rt.block_on(create_empty_table(&catalog, &eu_ns, "orders"));

    let refresh_resp = conn.try_execute("ALTER VIRTUAL SCHEMA REFRESH_COLLISION_VS REFRESH");
    assert_eq!(
        refresh_resp["status"].as_str(),
        Some("error"),
        "REFRESH must error once the flatten-name collision exists: {refresh_resp}"
    );
    let refresh_msg = refresh_resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        refresh_msg.contains("EU__ORDERS"),
        "refresh's collision error must name the colliding Exasol table name: {refresh_msg}"
    );
    assert!(
        refresh_msg.contains("collision"),
        "refresh's collision error must mention 'collision': {refresh_msg}"
    );

    // A fresh createVirtualSchema over the SAME now-colliding namespace must
    // return the same class of error — proving refresh reuses the identical
    // build_table_map path rather than a divergent one.
    let create_resp = conn.try_execute(&format!(
        r#"CREATE VIRTUAL SCHEMA REFRESH_COLLISION_CREATE_VS
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{CATALOG_CONN_NAME}'
  ICEBERG_NAMESPACE   = '{NS_COLLISION}'
  ALLOW_HTTP          = 'true'"#
    ));
    assert_eq!(
        create_resp["status"].as_str(),
        Some("error"),
        "a fresh createVirtualSchema over the colliding namespace must also error: {create_resp}"
    );
    let create_msg = create_resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        create_msg.contains("EU__ORDERS") && create_msg.contains("collision"),
        "createVirtualSchema's collision error must have the same shape as \
         refresh's (same colliding name, same 'collision' wording): {create_msg}"
    );
}
