use super::*;
use crate::export::batch::BATCH_ROWS;
use crate::export::batch::tests::measured_column;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use exasol_udf_sdk::connection::ConnectionObject;
use exasol_udf_sdk::test_support::TestContext;
use exasol_udf_sdk::value::Value;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

const WRITER_SPEC: &str = r#"{"connection":"LAKEHOUSE_CATALOG_CREDS","table":"ns.t","export_id":"00ff","columns":["ID"],"single_commit":false,"allow_http":false}"#;
const SENTINELS: [&str; 3] = [
    "https://sentinel-address.example",
    "sentinel-user",
    "sentinel-password",
];

fn sentinel_connection() -> ConnectionObject {
    ConnectionObject {
        kind: "password".to_string(),
        address: SENTINELS[0].to_string(),
        user: SENTINELS[1].to_string(),
        password: serde_json::json!({ "warehouse": "wh", "token": SENTINELS[2] }).to_string(),
    }
}

fn granted(ctx: TestContext) -> TestContext {
    ctx.with_connection(CONNECTION, sentinel_connection())
}

/// The hook over a target that records its calls and makes no request.
fn hook(ctx: &mut TestContext, json: &str) -> Result<String, UdfError> {
    export_hook(ctx, json, fake_target(&Calls::default(), None))
}

fn hook_context() -> TestContext {
    TestContext::set(vec![])
        .with_script_schema("LHVS")
        .with_node_count(2)
        .with_current_user("EXPORTER")
}

fn writer_context(rows: Vec<Vec<Value>>) -> TestContext {
    TestContext::set(rows)
        .with_script_schema("LHVS")
        .with_current_user("EXPORTER")
}

fn writer_row(spec: &str, id: i64) -> Vec<Value> {
    vec![Value::String(spec.to_string()), Value::Int64(id)]
}

fn export_spec_json(connection: serde_json::Value, extra: serde_json::Value) -> String {
    let mut spec = serde_json::json!({
        "has_truncate": false,
        "has_replace": false,
        "source_column_names": [r#""T"."A""#],
        "parameters": [
            {"key": "CATALOG_CONNECTION", "value": CONNECTION},
            {"key": "TABLE_NAME", "value": "ns.t"},
        ],
    });
    let fields = spec.as_object_mut().expect("the spec is an object");
    fields.extend(connection.as_object().cloned().unwrap_or_default());
    fields.extend(extra.as_object().cloned().unwrap_or_default());
    spec.to_string()
}

fn no_at_clause() -> serde_json::Value {
    serde_json::json!({})
}

fn no_extra() -> serde_json::Value {
    serde_json::json!({})
}

fn error_text(result: Result<impl std::fmt::Debug, UdfError>) -> String {
    match result {
        Err(error) => error.to_string(),
        Ok(value) => panic!("expected an error, got {value:?}"),
    }
}

fn assert_no_connection_value(text: &str, what: &str) {
    for sentinel in SENTINELS {
        assert!(
            !text.contains(sentinel),
            "{what} carries the CONNECTION value {sentinel:?}: {text}"
        );
    }
}

fn export_spec_with_columns(columns: &[&str]) -> String {
    let mut spec: serde_json::Value =
        serde_json::from_str(&export_spec_json(no_at_clause(), no_extra())).unwrap();
    spec["source_column_names"] = serde_json::json!(columns);
    spec.to_string()
}

/// The writer spec literal of a generated writer SELECT, unquoted.
fn rendered_writer_spec(sql: &str) -> serde_json::Value {
    let start = sql.find("('").expect("the spec literal opens") + 2;
    let end = sql.rfind("}', ").expect("the spec literal closes") + 1;
    serde_json::from_str(&sql[start..end].replace("''", "'")).expect("the spec is JSON")
}

/// Scenario: No CONNECTION value appears in the generated SQL or an error
#[test]
fn generated_sql_and_errors_carry_no_connection_value() {
    let sql = hook(
        &mut granted(hook_context()),
        &export_spec_json(no_at_clause(), no_extra()),
    )
    .expect("a valid export yields SQL");
    assert!(
        sql.starts_with(r#"SELECT "LHVS"."LAKEHOUSE_EXPORT"('{"#),
        "{sql}"
    );
    assert!(
        sql.ends_with(r#"}', "T"."A") FROM DUAL GROUP BY MOD(ROWNUM, 2)"#),
        "{sql}"
    );
    assert_no_connection_value(&sql, "the generated SQL");

    let inline = serde_json::json!({"connection_information": {
        "kind": "password",
        "address": SENTINELS[0],
        "user": SENTINELS[1],
        "password": SENTINELS[2],
    }});
    let refusals = [
        (
            "inline CONNECTION",
            generate_export_sql(
                &mut granted(hook_context()),
                &export_spec_json(inline, no_extra()),
            ),
        ),
        (
            "missing script grant in the hook",
            generate_export_sql(
                &mut hook_context(),
                &export_spec_json(no_at_clause(), no_extra()),
            ),
        ),
    ];
    for (case, result) in refusals {
        assert_no_connection_value(&error_text(result), case);
    }
    let writer_refusals = [
        (
            "missing script grant in the writer",
            run_writer(&mut writer_context(vec![writer_row(WRITER_SPEC, 1)])),
        ),
        (
            "forged writer spec",
            run_writer(&mut granted(writer_context(vec![writer_row(
                SENTINELS[2],
                1,
            )]))),
        ),
    ];
    for (case, result) in writer_refusals {
        assert_no_connection_value(&error_text(result), case);
    }
}

#[test]
fn hook_without_script_grant_names_connection_script_user_and_grant() {
    let message = error_text(generate_export_sql(
        &mut hook_context(),
        &export_spec_json(no_at_clause(), no_extra()),
    ));

    for fragment in [
        "LAKEHOUSE_EXPORT: user EXPORTER cannot read CONNECTION LAKEHOUSE_CATALOG_CREDS",
        "script LHVS.LAKEHOUSE_EXPORT",
        "GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT LHVS.LAKEHOUSE_EXPORT TO EXPORTER",
    ] {
        assert!(message.contains(fragment), "{message:?} lacks {fragment:?}");
    }
}

#[test]
fn hook_refuses_a_specification_it_cannot_parse() {
    let error = generate_export_sql(&mut granted(hook_context()), "not json")
        .expect_err("an unparseable specification is refused");

    assert!(matches!(error, UdfError::Type(_)), "{error:?}");
}

#[test]
fn the_hook_prepares_its_target_before_it_answers() {
    let calls = Calls::default();

    let sql = export_hook(
        &mut granted(hook_context()),
        &export_spec_json(no_at_clause(), no_extra()),
        fake_target(&calls, None),
    );

    assert!(sql.is_ok(), "{sql:?}");
    assert_eq!(
        calls.recorded(),
        ["target ns.t at https://sentinel-address.example", "prepare"]
    );
}

#[test]
fn a_target_that_refuses_the_table_fails_the_hook_with_its_error() {
    let refused = export_hook(
        &mut granted(hook_context()),
        &export_spec_json(no_at_clause(), no_extra()),
        fake_target(&Calls::default(), Some("prepare")),
    );

    assert_eq!(error_text(refused), "prepare failed");
}

#[test]
fn writer_refuses_a_first_argument_that_is_not_a_writer_spec() {
    let cases = [
        ("integer", Value::Int64(1)),
        ("NULL", Value::Null),
        ("plain string", Value::String("hello".to_string())),
        (
            "JSON of another shape",
            Value::String(r#"{"connection":"LAKEHOUSE_CATALOG_CREDS","extra":1}"#.to_string()),
        ),
    ];

    for (case, first) in cases {
        let message = error_text(run_writer(&mut granted(writer_context(vec![vec![
            first,
            Value::Int64(1),
        ]]))));
        assert!(
            message.contains("runs as the target of EXPORT ... INTO SCRIPT"),
            "{case}: {message}"
        );
    }
}

#[test]
fn writer_refuses_a_spec_whose_table_breaks_the_table_name_rule() {
    let forged = WRITER_SPEC.replace(r#""table":"ns.t""#, r#""table":"t""#);

    let message = error_text(run_writer(&mut granted(writer_context(vec![writer_row(
        &forged, 1,
    )]))));

    assert!(
        message.contains("TABLE_NAME = 't' is not of the form"),
        "{message}"
    );
}

#[test]
fn the_writer_reads_the_spec_the_hook_renders_with_a_fresh_export_id() {
    let render = || {
        let sql = hook(
            &mut granted(hook_context()),
            &export_spec_with_columns(&[r#""T"."ID""#, r#""T"."a""b""#]),
        )
        .expect("a valid export yields SQL");
        rendered_writer_spec(&sql)
    };
    let (first, second) = (render(), render());

    let export_id = first["export_id"].as_str().expect("an export ID");
    assert_eq!(export_id.len(), 32, "{export_id}");
    assert!(
        export_id.bytes().all(|b| b.is_ascii_hexdigit()),
        "{export_id}"
    );
    assert_ne!(first["export_id"], second["export_id"]);
    assert_eq!(
        first,
        serde_json::json!({
            "connection": CONNECTION,
            "table": "ns.t",
            "export_id": export_id,
            "columns": ["ID", "a\"b"],
            "single_commit": false,
            "allow_http": false,
        })
    );
    let mut writer = writer_context(vec![writer_row(&first.to_string(), 1)]);
    writer.next().unwrap();
    let read = WriterSpec::read(&writer).expect("the writer accepts it");
    assert_eq!(read.render(), first.to_string());
}

#[test]
fn column_names_are_the_last_identifier_of_each_reference_unescaped() {
    let names = column_names(&[
        r#""T"."A""#.to_string(),
        r#""T"."a""b""#.to_string(),
        r#""S"."T"."a.b""#.to_string(),
    ])
    .expect("distinct, unreserved names");

    assert_eq!(names, ["A", "a\"b", "a.b"]);
}

#[test]
fn a_repeated_column_name_is_refused_asking_for_a_distinct_alias() {
    let message = error_text(column_names(&[
        r#""T"."A""#.to_string(),
        r#""U"."A""#.to_string(),
    ]));

    assert!(message.contains("column A appears twice"), "{message}");
    assert!(message.contains("distinct alias"), "{message}");
}

/// Scenario: A source column name the new table cannot hold is refused before any data is read
#[test]
fn a_column_named_like_an_iceberg_metadata_column_is_refused() {
    let message = error_text(column_names(&[r#""T"."_file""#.to_string()]));

    assert!(message.contains("column _file"), "{message}");
    assert!(message.contains("reserved"), "{message}");
}

#[test]
fn missing_grant_without_a_known_user_names_a_user_placeholder() {
    let mut ctx = TestContext::set(vec![writer_row(WRITER_SPEC, 1)]).with_script_schema("LHVS");

    let message = error_text(run_writer(&mut ctx));

    assert!(
        message.contains("FOR SCRIPT LHVS.LAKEHOUSE_EXPORT TO <user>"),
        "{message}"
    );
}

mod sql {
    use super::super::*;

    const WRITER_SPEC: &str = r#"{"connection":"C"}"#;

    fn select_for(columns: &[&str], writers: WriterLayout, node_count: usize) -> String {
        let source_columns: Vec<String> = columns.iter().map(ToString::to_string).collect();
        WriterSelect {
            script_schema: "LHVS",
            writer_spec: WRITER_SPEC,
            source_columns: &source_columns,
            writers,
            node_count,
        }
        .render()
    }

    fn group_key(sql: &str) -> &str {
        let at = sql
            .rfind(" GROUP BY ")
            .unwrap_or_else(|| panic!("no GROUP BY in {sql}"));
        &sql[at + 1..]
    }

    #[test]
    fn several_columns_pass_in_source_order() {
        let sql = select_for(
            &[r#""T"."ID""#, r#""T"."NAME""#, r#""T"."G""#],
            WriterLayout::PerNode(1),
            1,
        );

        let expected = format!(
            "SELECT \"LHVS\".\"LAKEHOUSE_EXPORT\"('{WRITER_SPEC}', \"T\".\"ID\", \"T\".\"NAME\", \"T\".\"G\") \
             FROM DUAL GROUP BY MOD(ROWNUM, 1)"
        );
        assert_eq!(sql, expected);
    }

    /// Scenario: The writer count never exceeds 300
    #[test]
    fn group_count_follows_the_writer_layout_and_caps_at_300() {
        let cases = [
            (
                "nodes times writers per node",
                WriterLayout::PerNode(3),
                2,
                6,
            ),
            ("single commit", WriterLayout::SingleCommit, 4, 1),
            ("capped at 300", WriterLayout::PerNode(500), 4, 300),
        ];

        for (case, writers, node_count, expected) in cases {
            let sql = select_for(&[r#""T"."A""#], writers, node_count);
            assert_eq!(
                group_key(&sql),
                format!("GROUP BY MOD(ROWNUM, {expected})"),
                "{case}"
            );
        }
    }

    #[test]
    fn column_names_are_referenced_verbatim() {
        let cases = [
            ("dot inside the identifier", r#""T"."a.b""#),
            ("quote and apostrophe", r#""T"."it's ""q""""#),
        ];

        for (case, reference) in cases {
            let sql = select_for(&[reference], WriterLayout::PerNode(1), 1);
            assert!(
                sql.contains(&format!("{WRITER_SPEC}', {reference}) FROM DUAL")),
                "{case}: {sql}"
            );
        }
    }

    #[test]
    fn writer_spec_and_schema_are_quoted_for_sql() {
        let source_columns = vec![r#""T"."A""#.to_string()];
        let sql = WriterSelect {
            script_schema: r#"my"schema"#,
            writer_spec: r#"{"connection":"A'B\"C"}"#,
            source_columns: &source_columns,
            writers: WriterLayout::PerNode(1),
            node_count: 1,
        }
        .render();

        assert!(
            sql.starts_with(
                r#"SELECT "my""schema"."LAKEHOUSE_EXPORT"('{"connection":"A''B\"C"}', "T"."A") FROM DUAL GROUP BY "#
            ),
            "{sql}"
        );
    }
}

pub(in crate::export) const CONNECTION: &str = "LAKEHOUSE_CATALOG_CREDS";

/// Every call the export makes on its target and sink, in order.
#[derive(Clone, Default)]
pub(in crate::export) struct Calls(Rc<RefCell<Vec<String>>>);

impl Calls {
    fn record(&self, call: String) {
        self.0.borrow_mut().push(call);
    }

    pub(in crate::export) fn recorded(&self) -> Vec<String> {
        self.0.borrow().clone()
    }
}

/// A target that records its calls and fails the one named by `fail_at`, such as `"prepare"`.
pub(in crate::export) fn fake_target(
    calls: &Calls,
    fail_at: Option<&'static str>,
) -> impl FnOnce(ExportConnection, &str) -> Result<Box<dyn ExportTarget>, UdfError> {
    let calls = calls.clone();
    move |connection, table| {
        calls.record(format!("target {table} at {}", connection.address));
        Ok(Box::new(FakeTarget { calls, fail_at }))
    }
}

struct FakeTarget {
    calls: Calls,
    fail_at: Option<&'static str>,
}

struct FakeSink {
    calls: Calls,
    fail_at: Option<&'static str>,
    schema: SchemaRef,
}

fn failure(call: &str, fail_at: Option<&str>) -> Result<(), UdfError> {
    match fail_at {
        Some(failing) if failing == call => Err(UdfError::User(format!("{call} failed"))),
        _ => Ok(()),
    }
}

#[async_trait(?Send)]
impl ExportTarget for FakeTarget {
    async fn prepare(&self) -> Result<(), UdfError> {
        self.calls.record("prepare".into());
        failure("prepare", self.fail_at)
    }

    async fn open_table(
        &self,
        schema: &Schema,
        tag: &WriterTag,
    ) -> Result<Box<dyn ExportSink>, UdfError> {
        Ok(self.sink("open_table", schema, tag))
    }

    async fn stage_table(
        &self,
        schema: &Schema,
        tag: &WriterTag,
    ) -> Result<Box<dyn ExportSink>, UdfError> {
        Ok(self.sink("stage_table", schema, tag))
    }
}

impl FakeTarget {
    fn sink(&self, call: &str, schema: &Schema, tag: &WriterTag) -> Box<dyn ExportSink> {
        self.calls.record(format!(
            "{call} export_id={} writer_id={}",
            tag.export_id,
            tag.writer_id.len()
        ));
        Box::new(FakeSink {
            calls: self.calls.clone(),
            fail_at: self.fail_at,
            schema: Arc::new(schema.clone()),
        })
    }
}

#[async_trait(?Send)]
impl ExportSink for FakeSink {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn write(&mut self, batch: &RecordBatch) -> Result<(), UdfError> {
        self.calls.record(format!("write {}", batch.num_rows()));
        failure("write", self.fail_at)
    }

    async fn finish(&mut self) -> Result<usize, UdfError> {
        self.calls.record("finish".into());
        failure("finish", self.fail_at).map(|()| 1)
    }

    async fn abort(&mut self) {
        self.calls.record("abort".into());
    }
}

/// A CONNECTION the export accepts, named [`CONNECTION`].
pub(in crate::export) fn export_connection(address: &str, token: &str) -> ConnectionObject {
    ConnectionObject {
        kind: "password".to_string(),
        address: address.to_string(),
        user: String::new(),
        password: serde_json::json!({ "warehouse": "wh", "token": token }).to_string(),
    }
}

fn writer_spec(single_commit: bool) -> String {
    serde_json::json!({
        "connection": CONNECTION,
        "table": "ns.t",
        "export_id": "00ff",
        "columns": ["ID"],
        "single_commit": single_commit,
        "allow_http": false,
    })
    .to_string()
}

fn group(spec: &str, rows: usize, id_type: &str) -> TestContext {
    let rows = (0..rows)
        .map(|id| vec![Value::String(spec.to_string()), Value::Int64(id as i64)])
        .collect();
    TestContext::set(rows)
        .with_script_schema("LHVS")
        .with_current_user("EXPORTER")
        .with_connection(
            CONNECTION,
            export_connection("https://catalog.example", "t"),
        )
        .with_input_columns(vec![
            measured_column("VARCHAR(2000000) UTF8"),
            measured_column(id_type),
        ])
}

#[test]
fn a_writer_streams_its_rows_in_batches_into_the_sink_and_commits_once() {
    for (single_commit, opened) in [
        (true, "stage_table export_id=00ff writer_id=16"),
        (false, "open_table export_id=00ff writer_id=16"),
    ] {
        let calls = Calls::default();
        let rows = BATCH_ROWS + 1;

        let written = write_group(
            &mut group(&writer_spec(single_commit), rows, "DECIMAL(18,0)"),
            fake_target(&calls, None),
        );

        assert_eq!(written.expect("the writer succeeds"), rows as i64);
        assert_eq!(
            calls.recorded(),
            [
                "target ns.t at https://catalog.example",
                opened,
                &format!("write {BATCH_ROWS}"),
                "write 1",
                "finish",
            ]
        );
    }
}

#[test]
fn a_failed_write_or_commit_aborts_the_sink_and_fails_the_writer() {
    for failing in ["write", "finish"] {
        let calls = Calls::default();

        let error = write_group(
            &mut group(&writer_spec(false), 3, "DECIMAL(18,0)"),
            fake_target(&calls, Some(failing)),
        )
        .expect_err("the writer fails");

        assert_eq!(error.to_string(), format!("{failing} failed"));
        assert_eq!(calls.recorded().last().map(String::as_str), Some("abort"));
    }
}

#[test]
fn an_empty_group_opens_no_table() {
    let calls = Calls::default();

    let written = write_group(
        &mut group(&writer_spec(false), 0, "TIMESTAMP(9)"),
        fake_target(&calls, None),
    );

    assert_eq!(written.expect("an empty group succeeds"), 0);
    assert!(calls.recorded().is_empty());
}

#[test]
fn an_unwritable_column_fails_the_writer_before_its_target_is_opened() {
    let calls = Calls::default();

    let error = write_group(
        &mut group(&writer_spec(false), 1, "TIMESTAMP(9)"),
        fake_target(&calls, None),
    )
    .expect_err("TIMESTAMP(9) is refused");

    assert!(
        error
            .to_string()
            .contains("column ID has type TIMESTAMP(9)"),
        "{error}"
    );
    assert!(calls.recorded().is_empty());
}

#[test]
fn a_spec_whose_columns_do_not_match_the_input_is_refused() {
    let spec = writer_spec(false).replace(r#"["ID"]"#, r#"["ID","EXTRA"]"#);
    let calls = Calls::default();

    let error = write_group(
        &mut group(&spec, 1, "DECIMAL(18,0)"),
        fake_target(&calls, None),
    )
    .expect_err("the column count differs");

    assert!(
        error.to_string().contains("runs as the target of EXPORT"),
        "{error}"
    );
    assert!(calls.recorded().is_empty());
}
