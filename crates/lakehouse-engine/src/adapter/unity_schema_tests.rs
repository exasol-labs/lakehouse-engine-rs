//! Plain `#[test]`s: `handle_create_virtual_schema` `block_on`s its own current-thread runtime,
//! so a `#[tokio::test]` would panic; the fake serves HTTP from a runtime of its own.

use super::*;

use crate::adapter::pushdown::test_support::{
    RecordingCatalog, UNREACHABLE_CATALOG, named_table, serve_on_own_runtime,
};
use exasol_udf_sdk::test_support::TestContext;
use lakehouse_catalog::TableFormat;

const NAMESPACE: &str = "sales_catalog.public";

const LIST_TABLES_PATH: &str = "/api/2.1/unity-catalog/tables";

fn unity_conn_ctx(address: impl Into<String>, password: impl Into<String>) -> TestContext {
    TestContext::scalar(vec![]).with_connection(
        "uc_conn",
        exasol_udf_sdk::ConnectionObject {
            kind: "PASSWORD".into(),
            address: address.into(),
            user: String::new(),
            password: password.into(),
        },
    )
}

fn create_vs_request() -> Json {
    json!({
        "type": "createVirtualSchema",
        "properties": {
            "CATALOG_CONNECTION": "uc_conn",
            "CATALOG_KIND": "UNITY_CATALOG",
            "NAMESPACE": NAMESPACE,
        },
    })
}

fn long_col(name: &str) -> Json {
    json!({ "name": name, "type_name": "LONG", "type_json": r#"{"name":"c","type":"long","nullable":true,"metadata":{}}"# })
}

fn table_entry(name: &str, columns: Vec<Json>) -> Json {
    json!({
        "name": name,
        "table_type": "MANAGED",
        "storage_location": format!("s3://warehouse/{name}"),
        "data_source_format": "DELTA",
        "columns": columns,
    })
}

fn tables_page(entries: Vec<Json>, next_page_token: Option<&str>) -> String {
    let mut page = json!({ "tables": entries });
    if let Some(token) = next_page_token {
        page["next_page_token"] = json!(token);
    }
    page.to_string()
}

#[test]
fn listing_issues_no_per_table_get_table_call() {
    let (catalog, _runtime) = serve_on_own_runtime(RecordingCatalog::spawn(|target| {
        if target.split('?').next() != Some(LIST_TABLES_PATH) {
            return (
                500,
                json!({ "error": "unexpected per-table get-table call" }).to_string(),
            );
        }
        if target.contains("page_token=") {
            let entries = vec![
                table_entry("shipments", vec![long_col("id")]),
                table_entry("returns", vec![long_col("id")]),
            ];
            (200, tables_page(entries, None))
        } else {
            let entries = vec![
                table_entry("orders", vec![long_col("id")]),
                table_entry("customers", vec![long_col("id")]),
            ];
            (200, tables_page(entries, Some("PAGE2")))
        }
    }));

    let mut ctx = unity_conn_ctx(catalog.uri.clone(), "{}");
    let response =
        dispatch(&mut ctx, &create_vs_request()).expect("Unity createVirtualSchema must succeed");

    assert_eq!(
        response["schemaMetadata"]["tables"]
            .as_array()
            .expect("createVirtualSchema response must carry schemaMetadata.tables")
            .len(),
        4,
        "every table across both pages"
    );
    let targets = catalog.targets();
    assert!(
        targets.len() >= 2,
        "the sweep actually paginated across pages: {targets:?}"
    );
    assert!(
        targets
            .iter()
            .all(|target| target.split('?').next() == Some(LIST_TABLES_PATH)),
        "the list sweep carries columns inline, so only paginated GET /tables requests are \
         issued and no per-table get-table: {targets:?}"
    );
}

#[test]
fn unity_names_differing_only_by_case_collide() {
    let namespace = ["sales_catalog", "public"];
    let listing = CatalogListing {
        tables: vec![
            named_table(&namespace, "sales", TableFormat::Delta, Vec::new()),
            named_table(&namespace, "Sales", TableFormat::Delta, Vec::new()),
        ],
        skipped: Vec::new(),
    };

    let message = build_listing_virtual_tables(
        &namespace.map(str::to_string),
        &listing,
        EngineTimestampSupport::MillisecondOnly,
    )
    .expect_err("a flatten collision must be an error")
    .to_string();

    assert!(
        message.contains("collision") && message.contains("SALES"),
        "the error must name the colliding Exasol table name, got: {message}"
    );
}

#[test]
fn unreachable_unity_catalog_is_credential_safe_error() {
    // Not a dropped ephemeral listener: a parallel test's fake server could take the freed port.
    const SENTINEL: &str = "SENTINEL_SECRET_TOKEN";
    // PAT auth so a real bearer secret rides the request and must never surface.
    let mut ctx = unity_conn_ctx(
        UNREACHABLE_CATALOG,
        json!({ "token": SENTINEL }).to_string(),
    );

    let error = dispatch(&mut ctx, &create_vs_request())
        .expect_err("an unreachable Unity Catalog must fail createVirtualSchema");
    let message = error.to_string();

    assert!(
        message.contains("Unity Catalog") && message.contains("list tables"),
        "error must describe the failed namespace listing, got: {message}"
    );
    assert!(
        !message.contains(SENTINEL),
        "the error must not leak the bearer token"
    );
}
