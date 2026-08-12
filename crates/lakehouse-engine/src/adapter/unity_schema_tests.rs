//! Engine-level createVirtualSchema tests for the native Unity Catalog kind
//! (`CATALOG_KIND=UNITY_CATALOG`).
//!
//! Each test drives the real adapter listing pipeline through `dispatch` against
//! an in-process mock Unity Catalog REST server, so the constructed
//! `UnityCatalogSession` issues genuine HTTP to the mock. That makes assertions
//! about request shape — e.g. "no per-table get-table call" — real observations
//! of the true client, not properties of a stub.
//!
//! Runtime note: `handle_create_virtual_schema` builds its own current-thread
//! runtime and `block_on`s it, so these are plain `#[test]`s (a `#[tokio::test]`
//! would panic on the nested `block_on`). The mock runs on a separate
//! multi-thread runtime whose worker threads serve HTTP while the test thread is
//! parked in dispatch's current-thread runtime.

use super::*;

use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const NAMESPACE: &str = "sales_catalog.public";

/// One request the mock served, reduced to what the assertions need: the HTTP
/// method and the request target (path plus query string).
struct RecordedRequest {
    method: String,
    target: String,
}

impl RecordedRequest {
    /// The path portion of the target, before any query string.
    fn path(&self) -> &str {
        self.target.split('?').next().unwrap_or(&self.target)
    }

    /// A single-table load `GET /tables/{full_name}` — a path segment sits after
    /// `tables`, distinguishing it from the list sweep `GET /tables?...`.
    fn is_get_table_call(&self) -> bool {
        self.path().contains("/tables/")
    }

    /// The paginated list sweep `GET /tables` — the path ends at `tables`.
    fn is_list_tables_call(&self) -> bool {
        self.method == "GET" && self.path().ends_with("/tables")
    }
}

/// An in-process mock Unity Catalog REST server. It records every request and
/// answers each from a caller-supplied responder, closing the connection per
/// request so the pooled `reqwest` client opens a fresh one and a single accept
/// loop serves the whole sequential stream in order.
struct MockUnityCatalog {
    base_url: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    // Kept alive for the server's lifetime: dropping it shuts the mock down.
    _runtime: tokio::runtime::Runtime,
}

impl MockUnityCatalog {
    fn start<F>(responder: F) -> Self
    where
        F: Fn(&RecordedRequest) -> (u16, String) + Send + Sync + 'static,
    {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build mock runtime");
        let listener = runtime
            .block_on(async { TcpListener::bind("127.0.0.1:0").await })
            .expect("bind mock listener");
        let base_url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let requests: Arc<Mutex<Vec<RecordedRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let responder = Arc::new(responder);

        let server_requests = requests.clone();
        runtime.spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 16384];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    continue;
                }
                let raw = String::from_utf8_lossy(&buf[..n]).to_string();
                let recorded = parse_request(&raw);
                let (status, body) = responder(&recorded);
                // Record before writing the response, so every served request is
                // visible under the Mutex once the client (and thus dispatch) returns.
                server_requests.lock().unwrap().push(recorded);
                let reason = if (200..300).contains(&status) {
                    "OK"
                } else {
                    "ERROR"
                };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        Self {
            base_url,
            requests,
            _runtime: runtime,
        }
    }

    fn get_table_call_count(&self) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.is_get_table_call())
            .count()
    }

    fn request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }

    fn all_requests_are_list_sweeps(&self) -> bool {
        let requests = self.requests.lock().unwrap();
        !requests.is_empty() && requests.iter().all(RecordedRequest::is_list_tables_call)
    }
}

fn parse_request(raw: &str) -> RecordedRequest {
    let mut fields = raw.lines().next().unwrap_or("").split_whitespace();
    RecordedRequest {
        method: fields.next().unwrap_or("").to_string(),
        target: fields.next().unwrap_or("").to_string(),
    }
}

/// A `UdfContext` whose `connection()` resolves to a caller-chosen address and
/// credential JSON. The listing tests use an empty-object (no-auth) password;
/// the unreachable test uses a PAT so there is a real secret to prove absent.
struct UnityConnCtx {
    address: String,
    password: String,
}

impl UdfContext for UnityConnCtx {
    fn num_columns(&self) -> usize {
        0
    }
    fn get(&self, _col: usize) -> Result<&exasol_udf_sdk::value::Value, UdfError> {
        Err(UdfError::Type("none".into()))
    }
    fn emit(&mut self, _values: &[exasol_udf_sdk::value::Value]) -> Result<(), UdfError> {
        Ok(())
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        Ok(false)
    }
    fn connection(
        &self,
        _name: &str,
    ) -> Result<exasol_udf_sdk::connect_back::ConnectionObject, UdfError> {
        Ok(exasol_udf_sdk::connect_back::ConnectionObject {
            kind: "PASSWORD".into(),
            address: self.address.clone(),
            user: String::new(),
            password: self.password.clone(),
        })
    }
}

fn create_vs_request() -> Json {
    json!({
        "type": "createVirtualSchema",
        "properties": {
            "CATALOG_CONNECTION": "uc_conn",
            "CATALOG_KIND": "UNITY_CATALOG",
            "ICEBERG_NAMESPACE": NAMESPACE,
        },
    })
}

fn create_vs_over(mock: &MockUnityCatalog) -> Result<Json, UdfError> {
    let mut ctx = UnityConnCtx {
        address: mock.base_url.clone(),
        // Empty-object password: a valid no-auth Unity Catalog CONNECTION.
        password: "{}".into(),
    };
    dispatch(&mut ctx, &create_vs_request())
}

fn long_col(name: &str) -> Json {
    json!({ "name": name, "type_name": "LONG" })
}

fn string_col(name: &str) -> Json {
    json!({ "name": name, "type_name": "STRING" })
}

fn table_entry(name: &str, columns: Vec<Json>) -> Json {
    table_entry_typed(name, "MANAGED", columns)
}

/// A VIEW list entry: columns but no `storage_location` and a null
/// `data_source_format`, exactly as the `GET /tables` sweep returns a view.
fn view_entry(name: &str, columns: Vec<Json>) -> Json {
    json!({
        "name": name,
        "table_type": "VIEW",
        "data_source_format": Json::Null,
        "columns": columns,
    })
}

/// A MANAGED or EXTERNAL Delta base table list entry — the wire shape of both a
/// plain base table and a shallow clone, since Unity Catalog carries no separate
/// clone marker.
fn table_entry_typed(name: &str, table_type: &str, columns: Vec<Json>) -> Json {
    json!({
        "name": name,
        "table_type": table_type,
        "storage_location": format!("s3://warehouse/{name}"),
        "data_source_format": "DELTA",
        "columns": columns,
    })
}

/// A MANAGED base table list entry whose `data_source_format` is not `DELTA`.
fn non_delta_table_entry(name: &str, columns: Vec<Json>) -> Json {
    json!({
        "name": name,
        "table_type": "MANAGED",
        "storage_location": format!("s3://warehouse/{name}"),
        "data_source_format": "ICEBERG",
        "columns": columns,
    })
}

/// A list entry whose `table_type` is neither a base table nor a VIEW.
fn other_type_entry(name: &str, columns: Vec<Json>) -> Json {
    json!({
        "name": name,
        "table_type": "STREAMING_TABLE",
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

/// Fail loudly if the pipeline ever issues a per-table get-table: the listing
/// path must source columns from the list sweep alone.
fn unexpected_get_table() -> (u16, String) {
    (
        500,
        json!({ "error": "unexpected per-table get-table call" }).to_string(),
    )
}

fn response_tables(response: &Json) -> &Vec<Json> {
    response["schemaMetadata"]["tables"]
        .as_array()
        .expect("createVirtualSchema response must carry schemaMetadata.tables")
}

fn table_named<'a>(response: &'a Json, name: &str) -> &'a Json {
    response_tables(response)
        .iter()
        .find(|t| t["name"] == name)
        .unwrap_or_else(|| panic!("expected a virtual table named {name}"))
}

fn long_data_type() -> Json {
    json!({ "type": "decimal", "precision": 20, "scale": 0 })
}

fn string_data_type() -> Json {
    json!({ "type": "varchar", "size": 2000000 })
}

/// The Exasol-name → Unity-identifier map recorded in the response adapterNotes.
fn table_map(response: &Json) -> serde_json::Map<String, Json> {
    let notes_str = response["schemaMetadata"]["adapterNotes"]
        .as_str()
        .expect("adapterNotes is a JSON string");
    let notes: Json = serde_json::from_str(notes_str).expect("adapterNotes must be valid JSON");
    notes[NOTE_TABLE_MAP]
        .as_object()
        .expect("TABLE_MAP must be a JSON object")
        .clone()
}

/// createVirtualSchema under the Unity kind returns one virtual table per table
/// the `GET /tables` sweep reports, with names uppercased through the shared
/// case-fold and columns mapped from the inline sweep — no per-table get-table.
#[test]
fn enumerates_unity_namespace_tables() {
    let mock = MockUnityCatalog::start(|req| {
        if req.is_get_table_call() {
            return unexpected_get_table();
        }
        let entries = vec![
            table_entry(
                "orders",
                vec![long_col("order_id"), string_col("customer_name")],
            ),
            table_entry("customers", vec![long_col("id")]),
            table_entry("shipments", vec![long_col("shipment_id")]),
        ];
        (200, tables_page(entries, None))
    });

    let response = create_vs_over(&mock).expect("Unity createVirtualSchema must succeed");

    let names: Vec<&str> = response_tables(&response)
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names.len(), 3, "one virtual table per listed Unity table");
    assert!(names.contains(&"ORDERS"));
    assert!(names.contains(&"CUSTOMERS"));
    assert!(names.contains(&"SHIPMENTS"));

    let orders = table_named(&response, "ORDERS");
    let columns = orders["columns"].as_array().unwrap();
    assert_eq!(columns[0]["name"], "ORDER_ID");
    assert_eq!(columns[0]["dataType"], long_data_type());
    assert_eq!(columns[1]["name"], "CUSTOMER_NAME");
    assert_eq!(columns[1]["dataType"], string_data_type());

    // Columns came from the list sweep alone — the Delta log was never read.
    assert_eq!(mock.get_table_call_count(), 0);
}

/// The listing path issues only paginated `GET /tables` requests and zero
/// `GET /tables/{full_name}` requests, and the get-table count stays zero across
/// pages regardless of how many tables the sweep returns.
#[test]
fn listing_issues_no_per_table_get_table_call() {
    let mock = MockUnityCatalog::start(|req| {
        if req.is_get_table_call() {
            return unexpected_get_table();
        }
        if req.target.contains("page_token=") {
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
    });

    let response = create_vs_over(&mock).expect("Unity createVirtualSchema must succeed");

    assert_eq!(
        response_tables(&response).len(),
        4,
        "every table across both pages"
    );
    assert_eq!(
        mock.get_table_call_count(),
        0,
        "the list sweep carries columns inline; no per-table get-table is issued"
    );
    assert!(
        mock.all_requests_are_list_sweeps(),
        "only paginated GET /tables requests are issued"
    );
    assert!(
        mock.request_count() >= 2,
        "the sweep actually paginated across pages"
    );
}

#[test]
fn lists_managed_external_and_shallow_clone_delta_tables() {
    let mock = MockUnityCatalog::start(|req| {
        if req.is_get_table_call() {
            return unexpected_get_table();
        }
        let entries = vec![
            table_entry_typed("orders", "MANAGED", vec![long_col("id")]),
            table_entry_typed("customers", "EXTERNAL", vec![long_col("id")]),
            table_entry_typed("orders_clone", "MANAGED", vec![long_col("id")]),
        ];
        (200, tables_page(entries, None))
    });

    let response = create_vs_over(&mock).expect("Unity createVirtualSchema must succeed");

    let names: Vec<&str> = response_tables(&response)
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names.len(),
        3,
        "MANAGED, EXTERNAL, and shallow-clone-shaped Delta tables are all listed"
    );
    assert!(names.contains(&"ORDERS"));
    assert!(names.contains(&"CUSTOMERS"));
    assert!(names.contains(&"ORDERS_CLONE"));

    let map = table_map(&response);
    assert!(map.contains_key("ORDERS"));
    assert!(map.contains_key("CUSTOMERS"));
    assert!(map.contains_key("ORDERS_CLONE"));

    assert_eq!(mock.get_table_call_count(), 0);
}

#[test]
fn excludes_view_non_delta_and_other_type_entries() {
    let mock = MockUnityCatalog::start(|req| {
        if req.is_get_table_call() {
            return unexpected_get_table();
        }
        let entries = vec![
            table_entry("orders", vec![long_col("order_id")]),
            view_entry(
                "orders_summary",
                vec![long_col("order_id"), string_col("region")],
            ),
            non_delta_table_entry("orders_raw", vec![long_col("order_id")]),
            other_type_entry("orders_stream", vec![long_col("order_id")]),
        ];
        (200, tables_page(entries, None))
    });

    let response = create_vs_over(&mock).expect("exclusions must not fail enumeration");

    let names: Vec<&str> = response_tables(&response)
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names.len(), 1, "only the Delta base table is listed");
    assert!(names.contains(&"ORDERS"));

    let map = table_map(&response);
    assert!(map.contains_key("ORDERS"));
    assert!(
        !map.contains_key("ORDERS_SUMMARY"),
        "the view is excluded from TABLE_MAP"
    );
    assert!(
        !map.contains_key("ORDERS_RAW"),
        "the non-Delta-format table is excluded from TABLE_MAP"
    );
    assert!(
        !map.contains_key("ORDERS_STREAM"),
        "the other-table_type entry is excluded from TABLE_MAP"
    );

    assert_eq!(mock.get_table_call_count(), 0);
}

#[test]
fn excluding_every_entry_yields_an_empty_but_successful_schema() {
    let mock = MockUnityCatalog::start(|req| {
        if req.is_get_table_call() {
            return unexpected_get_table();
        }
        let entries = vec![
            view_entry("orders_summary", vec![long_col("order_id")]),
            non_delta_table_entry("orders_raw", vec![long_col("order_id")]),
        ];
        (200, tables_page(entries, None))
    });

    let response =
        create_vs_over(&mock).expect("an all-excluded namespace must not fail enumeration");

    assert!(
        response_tables(&response).is_empty(),
        "no entry survives the Delta-base filter"
    );
    assert!(
        table_map(&response).is_empty(),
        "TABLE_MAP carries no entry for an all-excluded namespace"
    );
    assert_eq!(mock.get_table_call_count(), 0);
}

/// adapterNotes.TABLE_MAP maps each uppercased Exasol name to its original-cased
/// `catalog.schema.table` identifier; two identifiers that flatten to the same
/// Exasol name are rejected with an error naming the colliding name.
#[test]
fn records_table_map_and_rejects_collision() {
    let happy = MockUnityCatalog::start(|req| {
        if req.is_get_table_call() {
            return unexpected_get_table();
        }
        let entries = vec![
            table_entry("orders", vec![long_col("id")]),
            table_entry("customers", vec![long_col("id")]),
        ];
        (200, tables_page(entries, None))
    });

    let response = create_vs_over(&happy).expect("Unity createVirtualSchema must succeed");
    let map = table_map(&response);
    assert_eq!(
        map.get("ORDERS").and_then(|v| v.as_str()),
        Some("sales_catalog.public.orders"),
        "TABLE_MAP maps the uppercased name to the original-cased identifier"
    );
    assert_eq!(
        map.get("CUSTOMERS").and_then(|v| v.as_str()),
        Some("sales_catalog.public.customers")
    );

    // The Unity client stamps every ident with the configured namespace, so two
    // wire names differing only by case both flatten+fold to `SALES`.
    let collision = MockUnityCatalog::start(|req| {
        if req.is_get_table_call() {
            return unexpected_get_table();
        }
        let entries = vec![
            table_entry("sales", vec![long_col("id")]),
            table_entry("Sales", vec![long_col("id")]),
        ];
        (200, tables_page(entries, None))
    });

    let error = create_vs_over(&collision).expect_err("a flatten collision must be an error");
    let message = error.to_string();
    assert!(
        message.contains("collision") && message.contains("SALES"),
        "the error must name the colliding Exasol table name, got: {message}"
    );
}

/// createVirtualSchema against an unreachable Unity Catalog fails with a clear
/// namespace-listing error that leaks no credential value.
#[test]
fn unreachable_unity_catalog_is_credential_safe_error() {
    // Bind then drop a loopback socket to obtain a port nothing listens on, so the
    // connect attempt is refused rather than hanging.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe socket");
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    const SENTINEL: &str = "SENTINEL_SECRET_TOKEN";
    let mut ctx = UnityConnCtx {
        address: format!("http://127.0.0.1:{port}"),
        // PAT auth so a real bearer secret rides the request and must never surface.
        password: json!({ "token": SENTINEL }).to_string(),
    };

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
