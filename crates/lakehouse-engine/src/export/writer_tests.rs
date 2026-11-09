use super::*;
use crate::export::batch::BATCH_ROWS;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use exasol_udf_sdk::connection::ConnectionObject;
use exasol_udf_sdk::test_support::TestContext;
use exasol_udf_sdk::value::Value;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use vs_expression::udf_column_test_support::measured_column;

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
