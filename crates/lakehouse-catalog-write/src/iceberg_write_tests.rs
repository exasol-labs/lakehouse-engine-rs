use super::*;
use crate::retry::backoff_wait;
use crate::test_support::{
    COMMIT_CONFLICT, LOAD_TABLE_NO_SNAPSHOTS, LOAD_TABLE_TAGGED, PREFIX_CONFIG,
    RECORDED_PLACEHOLDERS, STAGE_TABLE, STAGED_LOCATION, TAGGED_EXPORT_ID, TAGGED_WRITER_ID,
    assert_free_of, catalog, data_file, ident, memory_table, requests_starting, session,
};
use iceberg::spec::{NestedField, PrimitiveType, Type};
use lakehouse_catalog::StorageProps;
use lakehouse_catalog::exported_test_support::{request_body, spawn_server_with_headers};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const CREATE_CONFLICT: &str = include_str!("testdata/create_conflict.json");
const STATIC_SECRETS: [&str; 2] = ["STATIC_ACCESS_KEY", "STATIC_SECRET_KEY"];
const TABLES: &str = "POST /v1/p/namespaces/ns/tables ";
const COMMIT_T: &str = "POST /v1/p/namespaces/ns/tables/t ";
const LOAD_T: &str = "GET /v1/p/namespaces/ns/tables/t?snapshots=all ";
const EXISTS_T: &str = "HEAD /v1/p/namespaces/ns/tables/t ";
const COMMIT_STAGED: &str = "POST /v1/p/namespaces/ns/tables/staged ";
const LOAD_STAGED: &str = "GET /v1/p/namespaces/ns/tables/staged?snapshots=all ";
const EXISTS_STAGED: &str = "HEAD /v1/p/namespaces/ns/tables/staged ";

fn static_storage() -> WriteStorage {
    WriteStorage::Static(StorageBackend::S3(StorageProps {
        endpoint: "https://storage.example".into(),
        region: "us-east-1".into(),
        access_key: STATIC_SECRETS[0].into(),
        secret_key: STATIC_SECRETS[1].into(),
        ..Default::default()
    }))
}

async fn static_writes(uri: &str) -> TableWriteSession {
    TableWriteSession::new(session(uri, false).await, static_storage(), false)
}

async fn vended_writes(uri: &str, allow_http: bool) -> TableWriteSession {
    TableWriteSession::new(
        session(uri, true).await,
        WriteStorage::Vended(StaticStoreAddress::default()),
        allow_http,
    )
}

fn schema() -> Schema {
    Schema::builder()
        .with_fields([
            NestedField::optional(1, "ID", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "V", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap()
}

fn tag() -> CommitTag {
    CommitTag {
        export_id: TAGGED_EXPORT_ID.into(),
        writer_id: TAGGED_WRITER_ID.into(),
    }
}

/// A table of [`LOAD_TABLE_NO_SNAPSHOTS`] over in-memory storage, allowing `retries` commit retries.
fn memory_write_table(retries: u32) -> WriteTable {
    let answer = LOAD_TABLE_NO_SNAPSHOTS.replace(
        "\"commit.retry.num-retries\": \"20\"",
        &format!("\"commit.retry.num-retries\": \"{retries}\""),
    );
    WriteTable {
        table: memory_table(&answer, &ident("ns", "t")),
        credentials: TableCredentials::Static(Vec::new()),
    }
}

fn memory_staged_table() -> StagedTable {
    StagedTable(WriteTable {
        table: memory_table(STAGE_TABLE, &ident("ns", "staged")),
        credentials: TableCredentials::Static(Vec::new()),
    })
}

/// Answers each request line that starts with a key of `answers` with its value, and every
/// other one with HTTP 404.
fn routes(
    answers: Vec<(&'static str, (u16, String))>,
) -> impl Fn(&str) -> (u16, String) + Send + 'static {
    move |line| {
        answers
            .iter()
            .find(|(start, _)| line.starts_with(start))
            .map_or((404, "{}".into()), |(_, answer)| answer.clone())
    }
}

/// Answers the `n`th request (from 0) to which `answer` applies with `answers[n]`, the last one for
/// every later request.
fn in_turn(answers: Vec<(u16, String)>) -> impl Fn() -> (u16, String) + Send + Sync + 'static {
    let sent = AtomicUsize::new(0);
    move || {
        let turn = sent.fetch_add(1, Ordering::SeqCst).min(answers.len() - 1);
        answers[turn].clone()
    }
}

fn json_body(request: &str) -> serde_json::Value {
    serde_json::from_str(request_body(request)).unwrap()
}

#[tokio::test]
async fn ensure_namespace_creates_each_level_top_down_and_counts_an_existing_one_as_created() {
    let answer = in_turn(vec![(409, CREATE_CONFLICT.into()), (200, "{}".into())]);
    let (uri, requests) = catalog(PREFIX_CONFIG, move |_| answer()).await;
    let writes = static_writes(&uri).await;

    writes
        .ensure_namespace(&NamespaceIdent::from_strs(["a", "b"]).unwrap())
        .await
        .unwrap();

    let bodies: Vec<_> = requests_starting(&requests, "POST /v1/p/namespaces ")
        .iter()
        .map(|request| json_body(request))
        .collect();
    assert_eq!(
        bodies,
        [
            serde_json::json!({"namespace": ["a"]}),
            serde_json::json!({"namespace": ["a", "b"]})
        ]
    );
}

#[tokio::test]
async fn a_namespace_level_the_catalog_refuses_fails_at_once_naming_the_namespace() {
    let (uri, requests) = catalog(PREFIX_CONFIG, |_| (403, "{\"error\":\"denied\"}".into())).await;
    let writes = static_writes(&uri).await;

    let refused = writes
        .ensure_namespace(&NamespaceIdent::new("a".into()))
        .await
        .unwrap_err()
        .to_string();

    assert_eq!(
        refused,
        format!(
            "creating namespace a failed: the catalog {uri} answered HTTP 403: {{\"error\":\"denied\"}}"
        )
    );
    assert_eq!(requests_starting(&requests, "POST ").len(), 1);
}

#[tokio::test]
async fn a_namespace_level_without_an_answer_is_sent_again() {
    let answer = in_turn(vec![(504, "{}".into()), (200, "{}".into())]);
    let (uri, requests) = catalog(PREFIX_CONFIG, move |_| answer()).await;
    let writes = static_writes(&uri).await;

    writes
        .ensure_namespace(&NamespaceIdent::new("a".into()))
        .await
        .unwrap();

    assert_eq!(requests_starting(&requests, "POST ").len(), 2);
}

#[tokio::test]
async fn create_table_sends_a_format_2_table_tagged_with_the_export_id() {
    let (uri, requests) = catalog(
        PREFIX_CONFIG,
        routes(vec![(TABLES, (200, LOAD_TABLE_NO_SNAPSHOTS.into()))]),
    )
    .await;
    let writes = static_writes(&uri).await;

    let created = writes
        .create_table(&ident("ns", "t"), schema(), "e1")
        .await
        .unwrap();

    let sent = json_body(&requests_starting(&requests, TABLES)[0]);
    assert_eq!(sent["name"], "t");
    assert_eq!(
        sent["properties"],
        serde_json::json!({
            "format-version": "2",
            "commit.retry.num-retries": "20",
            "lakehouse.export-id": "e1"
        })
    );
    assert_eq!(sent["schema"]["fields"][0]["name"], "ID");
    assert!(sent.get("stage-create").is_none(), "{sent}");
    assert_eq!(
        created.table().metadata().location(),
        "s3://warehouse/lakehouse_vended/01a1bf3b-863b-7a71-a04f-6c41dd312eb2"
    );
    let mut secrets = created.secret_values();
    secrets.sort();
    assert_eq!(secrets, STATIC_SECRETS);
}

#[tokio::test]
async fn a_conflicting_create_returns_the_table_only_when_this_export_created_it() {
    let (uri, _) = catalog(
        PREFIX_CONFIG,
        routes(vec![
            (TABLES, (409, CREATE_CONFLICT.into())),
            (EXISTS_T, (200, String::new())),
            (LOAD_T, (200, LOAD_TABLE_NO_SNAPSHOTS.into())),
        ]),
    )
    .await;
    let writes = static_writes(&uri).await;

    let raced = writes
        .create_table(&ident("ns", "t"), schema(), TAGGED_EXPORT_ID)
        .await;
    let foreign = writes
        .create_table(&ident("ns", "t"), schema(), "another-export")
        .await;

    assert_eq!(
        raced.unwrap().table().metadata().properties()["lakehouse.export-id"],
        TAGGED_EXPORT_ID
    );
    assert_eq!(
        foreign.err().unwrap().to_string(),
        "table ns.t already exists; the export only creates new tables"
    );
}

#[tokio::test]
async fn an_existing_table_is_refused_naming_it_and_an_absent_one_passes() {
    let (uri, _) = catalog(
        PREFIX_CONFIG,
        routes(vec![(EXISTS_T, (204, String::new()))]),
    )
    .await;
    let writes = static_writes(&uri).await;

    let existing = writes.refuse_existing_table(&ident("ns", "t")).await;
    let absent = writes.refuse_existing_table(&ident("ns", "absent")).await;

    assert_eq!(
        existing.unwrap_err().to_string(),
        "table ns.t already exists; the export only creates new tables"
    );
    assert!(absent.is_ok(), "{absent:?}");
}

#[tokio::test]
async fn a_create_without_an_answer_and_without_a_table_is_sent_again() {
    let create = in_turn(vec![
        (504, "{}".into()),
        (200, LOAD_TABLE_NO_SNAPSHOTS.into()),
    ]);
    let (uri, requests) = catalog(PREFIX_CONFIG, move |line| {
        if line.starts_with(TABLES) {
            create()
        } else {
            (404, "{}".into())
        }
    })
    .await;
    let writes = static_writes(&uri).await;

    writes
        .create_table(&ident("ns", "t"), schema(), "e1")
        .await
        .unwrap();

    assert_eq!(requests_starting(&requests, TABLES).len(), 2);
    assert_eq!(requests_starting(&requests, EXISTS_T).len(), 1);
}

#[tokio::test]
async fn a_create_the_catalog_refuses_fails_at_once_naming_the_table() {
    let (uri, requests) = catalog(PREFIX_CONFIG, |_| {
        (400, "{\"error\":\"bad schema\"}".into())
    })
    .await;
    let writes = static_writes(&uri).await;

    let refused = writes
        .create_table(&ident("ns", "t"), schema(), "e1")
        .await
        .err()
        .unwrap()
        .to_string();

    assert_eq!(
        refused,
        format!(
            "creating table ns.t failed: the catalog {uri} answered HTTP 400: \
             {{\"error\":\"bad schema\"}}"
        )
    );
    assert_eq!(requests_starting(&requests, TABLES).len(), 1);
}

#[tokio::test]
async fn a_create_the_catalog_stays_busy_for_fails_after_its_retries() {
    let (uri, requests) = catalog(PREFIX_CONFIG, |_| (503, "{}".into())).await;
    let writes = static_writes(&uri).await;

    let failed = writes
        .create_table(&ident("ns", "t"), schema(), "e1")
        .await
        .err()
        .unwrap()
        .to_string();

    assert_eq!(
        failed,
        format!(
            "creating table ns.t failed after 5 attempts: the catalog {uri} answered HTTP 503: {{}}"
        )
    );
    assert_eq!(requests_starting(&requests, TABLES).len(), 5);
}

/// A catalog that answers its `/v1/config` lookup, then closes that connection and its listener,
/// so every later request meets a refused connection.
async fn catalog_gone_after_config() -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        let mut chunk = [0u8; 1024];
        while !head.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut chunk).await.unwrap();
            head.extend_from_slice(&chunk[..read]);
        }
        let answer = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n{PREFIX_CONFIG}",
            PREFIX_CONFIG.len()
        );
        stream.write_all(answer.as_bytes()).await.unwrap();
    });
    uri
}

#[tokio::test]
async fn a_create_whose_connection_is_refused_is_sent_again_until_its_retries_run_out() {
    let writes = static_writes(&catalog_gone_after_config().await).await;

    let failed = writes
        .create_table(&ident("ns", "t"), schema(), "e1")
        .await
        .err()
        .unwrap()
        .to_string();

    assert!(
        failed
            .starts_with("creating table ns.t failed after 5 attempts: the catalog request POST "),
        "{failed}"
    );
    assert!(failed.contains("Connection refused"), "{failed}");
}

#[tokio::test]
async fn an_unreadable_create_answer_fails_quoting_none_of_it() {
    let answer = format!("{{\"metadata\": \"{}\"}}", RECORDED_PLACEHOLDERS[1]);
    let (uri, _) = catalog(PREFIX_CONFIG, routes(vec![(TABLES, (200, answer))])).await;
    let writes = static_writes(&uri).await;

    let failed = writes
        .create_table(&ident("ns", "t"), schema(), "e1")
        .await
        .err()
        .unwrap()
        .to_string();

    assert!(
        failed.starts_with(&format!(
            "the catalog {uri} answered the creation of table ns.t with no table: invalid JSON at"
        )),
        "{failed}"
    );
    assert_free_of(&failed, &RECORDED_PLACEHOLDERS);
}

#[tokio::test]
async fn a_vended_plaintext_endpoint_needs_allow_http() {
    let (uri, _) = catalog(
        PREFIX_CONFIG,
        routes(vec![(TABLES, (200, LOAD_TABLE_NO_SNAPSHOTS.into()))]),
    )
    .await;

    let refused = vended_writes(&uri, false)
        .await
        .create_table(&ident("ns", "t"), schema(), "e1")
        .await
        .err()
        .unwrap()
        .to_string();
    let allowed = vended_writes(&uri, true)
        .await
        .create_table(&ident("ns", "t"), schema(), "e1")
        .await
        .unwrap();

    assert_eq!(
        refused,
        "the catalog vended the plaintext storage endpoint http://seaweedfs:8333/ for table ns.t, \
         but the export option ALLOW_HTTP is not 'true'"
    );
    assert_free_of(&refused, &RECORDED_PLACEHOLDERS);
    let mut secrets = allowed.secret_values();
    secrets.sort();
    assert_eq!(secrets, RECORDED_PLACEHOLDERS[..3]);
}

#[tokio::test]
async fn stage_table_asks_for_staged_creation_and_opens_the_invisible_table() {
    let (uri, requests) = catalog(
        PREFIX_CONFIG,
        routes(vec![(TABLES, (200, STAGE_TABLE.into()))]),
    )
    .await;
    let writes = vended_writes(&uri, true).await;

    let staged = writes
        .stage_table(&ident("ns", "staged"), schema(), "e1")
        .await
        .unwrap();

    let sent = json_body(&requests_starting(&requests, TABLES)[0]);
    assert_eq!(sent["stage-create"], true);
    assert_eq!(sent["properties"]["lakehouse.export-id"], "e1");
    assert_eq!(staged.table().metadata().location(), STAGED_LOCATION);
    assert_eq!(staged.table().metadata_location(), None);
    assert_eq!(
        requests_starting(&requests, "HEAD /v1/p/namespaces/ns/tables/staged ").len(),
        1
    );
}

#[tokio::test]
async fn an_abfss_stage_answer_is_refused_naming_the_table_and_the_scheme() {
    let abfss = STAGE_TABLE.replace(
        "s3://warehouse/lakehouse_vended/",
        "abfss://warehouse@account.dfs.core.windows.net/",
    );
    let (uri, _) = catalog(PREFIX_CONFIG, routes(vec![(TABLES, (200, abfss))])).await;
    let writes = vended_writes(&uri, true).await;

    let refused = writes
        .stage_table(&ident("ns", "staged"), schema(), "e1")
        .await
        .err()
        .unwrap()
        .to_string();

    assert_eq!(
        refused,
        "table ns.staged has the location abfss://warehouse@account.dfs.core.windows.net/\
         01a1bf3b-af23-7020-ac26-0a0912b927ee, whose scheme abfss is no S3-compatible storage; \
         the export writes only to s3:// or s3a:// locations"
    );
}

#[tokio::test]
async fn a_refused_stage_fails_quoting_the_status_and_naming_the_catalog() {
    let (uri, _) = catalog(PREFIX_CONFIG, |_| {
        (400, "{\"error\":\"stage-create is not supported\"}".into())
    })
    .await;
    let writes = static_writes(&uri).await;

    let refused = writes
        .stage_table(&ident("ns", "staged"), schema(), "e1")
        .await
        .err()
        .unwrap()
        .to_string();

    assert_eq!(
        refused,
        format!(
            "staged creation of table ns.staged failed: the catalog {uri} answered HTTP 400: \
             {{\"error\":\"stage-create is not supported\"}}; SINGLE_COMMIT needs a catalog that \
             accepts staged table creation (stage-create)"
        )
    );
}

#[tokio::test]
async fn an_unreadable_stage_answer_fails_stating_that_single_commit_needs_staged_creation() {
    let (uri, _) = spawn_server_with_headers("application/json", |request| {
        if request.starts_with("GET /v1/config") {
            (200, Vec::new(), PREFIX_CONFIG.into())
        } else {
            (403, vec![("Content-Length", "1000".into())], "{}".into())
        }
    })
    .await;
    let writes = static_writes(&uri).await;

    let refused = writes
        .stage_table(&ident("ns", "staged"), schema(), "e1")
        .await
        .err()
        .unwrap()
        .to_string();

    assert!(
        refused.starts_with("staged creation of table ns.staged failed: ")
            && refused.contains(&uri)
            && refused.ends_with(
                "; SINGLE_COMMIT needs a catalog that accepts staged table creation (stage-create)"
            ),
        "{refused}"
    );
}

#[tokio::test]
async fn a_stage_answer_followed_by_a_visible_table_is_refused_naming_the_catalog() {
    let (uri, _) = catalog(
        PREFIX_CONFIG,
        routes(vec![
            (TABLES, (200, STAGE_TABLE.into())),
            (EXISTS_STAGED, (204, String::new())),
        ]),
    )
    .await;
    let writes = vended_writes(&uri, true).await;

    let refused = writes
        .stage_table(&ident("ns", "staged"), schema(), "e1")
        .await
        .err()
        .unwrap()
        .to_string();

    assert_eq!(
        refused,
        format!(
            "the catalog {uri} ignored staged creation (stage-create): table ns.staged became \
             visible before its commit, so SINGLE_COMMIT cannot make it appear only with its data"
        )
    );
}

#[tokio::test]
async fn load_table_opens_the_table_with_every_snapshot() {
    let (uri, requests) = catalog(
        PREFIX_CONFIG,
        routes(vec![(LOAD_T, (200, LOAD_TABLE_TAGGED.into()))]),
    )
    .await;
    let writes = static_writes(&uri).await;

    let loaded = writes.load_table(&ident("ns", "t")).await.unwrap();

    assert_eq!(loaded.table().metadata().snapshots().len(), 1);
    assert_eq!(requests_starting(&requests, LOAD_T).len(), 1);
}

#[test]
fn a_connection_that_vends_gets_vended_storage_and_one_with_keys_its_own() {
    let mut creds = ConnectionCreds {
        endpoint: "https://storage.example".into(),
        access_key: STATIC_SECRETS[0].into(),
        secret_key: STATIC_SECRETS[1].into(),
        ..Default::default()
    };

    let own = WriteStorage::of(&creds, false);
    creds.use_vended_credentials = true;
    let vended = WriteStorage::of(&creds, false);

    let WriteStorage::Static(backend) = own else {
        panic!("a CONNECTION with keys writes with them");
    };
    assert_eq!(backend.secret_values(), STATIC_SECRETS);
    let WriteStorage::Vended(address) = vended else {
        panic!("a vending CONNECTION writes with vended credentials");
    };
    assert_eq!(address.endpoint(), "https://storage.example");
}

/// Scenario: A commit retried after an unknown outcome adds its rows once
#[tokio::test]
async fn unknown_outcome_with_tagged_reload_commits_once() {
    let (uri, requests) = catalog(
        PREFIX_CONFIG,
        routes(vec![
            (COMMIT_T, (504, "<html>gateway timeout</html>".into())),
            (LOAD_T, (200, LOAD_TABLE_TAGGED.into())),
        ]),
    )
    .await;
    let writes = static_writes(&uri).await;
    let table = memory_write_table(20);
    let files = vec![data_file(table.table(), "w0")];

    writes.append_files(&table, files, &tag()).await.unwrap();

    assert_eq!(requests_starting(&requests, COMMIT_T).len(), 1);
    assert_eq!(requests_starting(&requests, LOAD_T).len(), 1);
}

/// Scenario: A commit retried after an unknown outcome adds its rows once
#[tokio::test]
async fn staged_unknown_outcome_with_created_table_commits_once() {
    let (uri, requests) = catalog(
        PREFIX_CONFIG,
        routes(vec![
            (COMMIT_STAGED, (504, "<html>gateway timeout</html>".into())),
            (EXISTS_STAGED, (204, String::new())),
            (LOAD_STAGED, (200, LOAD_TABLE_TAGGED.into())),
        ]),
    )
    .await;
    let writes = static_writes(&uri).await;
    let staged = memory_staged_table();
    let files = vec![data_file(staged.table(), "w0")];

    writes.commit_staged(&staged, files, &tag()).await.unwrap();

    let commits = requests_starting(&requests, COMMIT_STAGED);
    assert_eq!(commits.len(), 1);
    let sent = json_body(&commits[0]);
    assert_eq!(
        sent["requirements"],
        serde_json::json!([{"type": "assert-create"}])
    );
    assert_eq!(sent["updates"][0]["action"], "assign-uuid");
    assert_eq!(
        sent["updates"][0]["uuid"],
        staged.table().metadata().uuid().to_string()
    );
    let actions: Vec<&str> = sent["updates"]
        .as_array()
        .unwrap()
        .iter()
        .map(|update| update["action"].as_str().unwrap())
        .collect();
    assert_eq!(
        actions[actions.len() - 2..],
        ["add-snapshot", "set-snapshot-ref"]
    );
}

/// Scenario: A commit retried after an unknown outcome adds its rows once
#[tokio::test]
async fn conflict_with_untagged_reload_commits_again() {
    let commit = in_turn(vec![(409, COMMIT_CONFLICT.into()), (200, "{}".into())]);
    let (uri, requests) = catalog(PREFIX_CONFIG, move |line| {
        if line.starts_with(COMMIT_T) {
            commit()
        } else if line.starts_with(LOAD_T) {
            (200, LOAD_TABLE_NO_SNAPSHOTS.into())
        } else {
            (404, "{}".into())
        }
    })
    .await;
    let writes = static_writes(&uri).await;
    let table = memory_write_table(20);
    let files = vec![data_file(table.table(), "w0")];

    writes.append_files(&table, files, &tag()).await.unwrap();

    let commits = requests_starting(&requests, COMMIT_T);
    assert_eq!(commits.len(), 2);
    let resent = json_body(&commits[1]);
    let manifest_list = resent["updates"][0]["snapshot"]["manifest-list"]
        .as_str()
        .unwrap();
    assert!(
        table.table().file_io().exists(manifest_list).await.unwrap(),
        "the rebuilt snapshot's manifests went through the table's own FileIO"
    );
    assert_eq!(
        resent["updates"][0]["snapshot"]["summary"]["lakehouse.export-writer"],
        TAGGED_WRITER_ID
    );
}

#[tokio::test]
async fn a_conflicting_commit_reloads_the_table_after_its_wait() {
    let arrivals = Arc::new(Mutex::new(Vec::new()));
    let commit = in_turn(vec![(409, COMMIT_CONFLICT.into()), (200, "{}".into())]);
    let logged = Arc::clone(&arrivals);
    let (uri, _) = catalog(PREFIX_CONFIG, move |line| {
        logged
            .lock()
            .unwrap()
            .push((line.to_string(), Instant::now()));
        if line.starts_with(COMMIT_T) {
            commit()
        } else {
            (200, LOAD_TABLE_NO_SNAPSHOTS.into())
        }
    })
    .await;
    let writes = static_writes(&uri).await;
    let table = memory_write_table(20);

    writes
        .append_files(&table, vec![data_file(table.table(), "w0")], &tag())
        .await
        .unwrap();

    let arrivals = arrivals.lock().unwrap();
    let arrived = |request: &str| {
        arrivals
            .iter()
            .find(|(line, _)| line.starts_with(request))
            .map(|(_, at)| *at)
            .unwrap()
    };
    assert!(
        arrived(LOAD_T) - arrived(COMMIT_T) >= backoff_wait(1, 0.0),
        "a reload before the wait goes stale when another writer commits during the wait"
    );
}

#[tokio::test]
async fn a_commit_the_catalog_refuses_fails_at_once_without_a_reload() {
    let (uri, requests) = catalog(
        PREFIX_CONFIG,
        routes(vec![(COMMIT_T, (403, "{\"error\":\"forbidden\"}".into()))]),
    )
    .await;
    let writes = static_writes(&uri).await;
    let table = memory_write_table(20);

    let failed = writes
        .append_files(&table, vec![data_file(table.table(), "w0")], &tag())
        .await
        .unwrap_err();

    assert!(!failed.outcome_unknown());
    assert_eq!(
        failed.to_string(),
        format!(
            "the commit of the data files to table ns.t failed: the catalog {uri} answered HTTP \
             403: {{\"error\":\"forbidden\"}}; the catalog did not apply it"
        )
    );
    assert_eq!(requests_starting(&requests, COMMIT_T).len(), 1);
    assert!(requests_starting(&requests, LOAD_T).is_empty());
}

#[tokio::test]
async fn a_commit_answered_with_http_500_fails_at_once_with_an_unknown_outcome() {
    let (uri, requests) =
        catalog(PREFIX_CONFIG, routes(vec![(COMMIT_T, (500, "{}".into()))])).await;
    let writes = static_writes(&uri).await;
    let table = memory_write_table(20);

    let failed = writes
        .append_files(&table, vec![data_file(table.table(), "w0")], &tag())
        .await
        .unwrap_err();

    assert!(failed.outcome_unknown());
    assert!(
        failed.to_string().ends_with(
            "the outcome of its last request is unknown, so the table may hold the data files"
        ),
        "{failed}"
    );
    assert!(requests_starting(&requests, LOAD_T).is_empty());
}

#[tokio::test]
async fn a_commit_that_keeps_failing_stops_after_the_table_s_retries_stating_the_last_outcome() {
    for (status, unknown) in [(409, false), (504, true)] {
        let (uri, requests) = catalog(
            PREFIX_CONFIG,
            routes(vec![
                (COMMIT_T, (status, "{}".into())),
                (LOAD_T, (200, LOAD_TABLE_NO_SNAPSHOTS.into())),
            ]),
        )
        .await;
        let writes = static_writes(&uri).await;
        let table = memory_write_table(1);

        let failed = writes
            .append_files(&table, vec![data_file(table.table(), "w0")], &tag())
            .await
            .unwrap_err();

        assert_eq!(failed.outcome_unknown(), unknown, "HTTP {status}");
        assert!(
            failed.to_string().contains(&format!(
                "2 attempts did not commit, the last because the catalog {uri} answered HTTP {status}"
            )),
            "{failed}"
        );
        assert_eq!(requests_starting(&requests, COMMIT_T).len(), 2);
    }
}

#[tokio::test]
async fn a_failed_reload_after_a_lost_answer_keeps_the_outcome_unknown() {
    let (uri, _) = catalog(
        PREFIX_CONFIG,
        routes(vec![
            (COMMIT_T, (504, "{}".into())),
            (LOAD_T, (403, "{}".into())),
        ]),
    )
    .await;
    let writes = static_writes(&uri).await;
    let table = memory_write_table(20);

    let failed = writes
        .append_files(&table, vec![data_file(table.table(), "w0")], &tag())
        .await
        .unwrap_err();

    assert!(failed.outcome_unknown());
    assert!(failed.to_string().contains("HTTP 403"), "{failed}");
}

#[tokio::test]
async fn a_staged_table_that_another_statement_created_fails_naming_the_table() {
    for (status, answer) in [
        (409, COMMIT_CONFLICT),
        (504, "<html>gateway timeout</html>"),
    ] {
        let (uri, requests) = catalog(
            PREFIX_CONFIG,
            routes(vec![
                (COMMIT_STAGED, (status, answer.into())),
                (EXISTS_STAGED, (204, String::new())),
                (LOAD_STAGED, (200, LOAD_TABLE_NO_SNAPSHOTS.into())),
            ]),
        )
        .await;
        let writes = static_writes(&uri).await;
        let staged = memory_staged_table();

        let failed = writes
            .commit_staged(&staged, vec![data_file(staged.table(), "w0")], &tag())
            .await
            .unwrap_err();

        assert!(!failed.outcome_unknown(), "HTTP {status}");
        assert_eq!(
            failed.to_string(),
            "the commit of the data files to table ns.staged failed: table ns.staged was created \
             by another statement; the catalog did not apply it",
            "HTTP {status}"
        );
        assert_eq!(requests_starting(&requests, COMMIT_STAGED).len(), 1);
    }
}

#[tokio::test]
async fn a_staged_commit_whose_table_is_still_absent_is_sent_again() {
    let commit = in_turn(vec![(503, "{}".into()), (200, "{}".into())]);
    let (uri, requests) = catalog(PREFIX_CONFIG, move |line| {
        if line.starts_with(COMMIT_STAGED) {
            commit()
        } else {
            (404, "{}".into())
        }
    })
    .await;
    let writes = static_writes(&uri).await;
    let staged = memory_staged_table();

    writes
        .commit_staged(&staged, vec![data_file(staged.table(), "w0")], &tag())
        .await
        .unwrap();

    let commits = requests_starting(&requests, COMMIT_STAGED);
    assert_eq!(commits.len(), 2);
    assert_eq!(request_body(&commits[0]), request_body(&commits[1]));
}

#[test]
fn a_tag_is_on_a_table_only_when_one_snapshot_carries_both_ids() {
    let tagged: LoadTableResult = serde_json::from_str(LOAD_TABLE_TAGGED).unwrap();
    let other_writer = CommitTag {
        writer_id: "ffffffffffffffff".into(),
        ..tag()
    };
    let other_export = CommitTag {
        export_id: "ffffffffffffffffffffffffffffffff".into(),
        ..tag()
    };

    assert!(tag().is_on(&tagged.metadata));
    assert!(!other_writer.is_on(&tagged.metadata));
    assert!(!other_export.is_on(&tagged.metadata));
}
