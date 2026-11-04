use super::*;
use exasol_udf_sdk::connection::ConnectionObject;
use exasol_udf_sdk::test_support::TestContext;
use exasol_udf_sdk::value::Value;

const CONNECTION: &str = "LAKEHOUSE_CATALOG_CREDS";
const WRITER_SPEC: &str = r#"{"connection":"LAKEHOUSE_CATALOG_CREDS"}"#;
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
        password: SENTINELS[2].to_string(),
    }
}

fn granted(ctx: TestContext) -> TestContext {
    ctx.with_connection(CONNECTION, sentinel_connection())
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
        "parameters": [{"key": "TABLE_NAME", "value": "ns.t"}],
    });
    let fields = spec.as_object_mut().expect("the spec is an object");
    fields.extend(connection.as_object().cloned().unwrap_or_default());
    fields.extend(extra.as_object().cloned().unwrap_or_default());
    spec.to_string()
}

fn named_connection() -> serde_json::Value {
    serde_json::json!({"connection_name": CONNECTION})
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

/// Scenario: No CONNECTION value appears in the generated SQL or an error
#[test]
fn generated_sql_and_errors_carry_no_connection_value() {
    let sql = generate_export_sql(
        &mut granted(hook_context()),
        &export_spec_json(named_connection(), no_extra()),
    )
    .expect("a valid export yields SQL");
    assert!(
        sql.starts_with(&format!(
            r#"SELECT "LHVS"."LAKEHOUSE_EXPORT"('{WRITER_SPEC}', "T"."A") FROM DUAL WHERE "#
        )),
        "{sql}"
    );
    assert!(sql.ends_with(" GROUP BY MOD(ROWNUM, 2)"), "{sql}");
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
            "TRUNCATE",
            generate_export_sql(
                &mut granted(hook_context()),
                &export_spec_json(
                    named_connection(),
                    serde_json::json!({"has_truncate": true}),
                ),
            ),
        ),
        (
            "missing script grant in the hook",
            generate_export_sql(
                &mut hook_context(),
                &export_spec_json(named_connection(), no_extra()),
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
        &export_spec_json(named_connection(), no_extra()),
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
fn hook_refuses_an_invalid_statement_naming_the_option() {
    let message = error_text(generate_export_sql(
        &mut granted(hook_context()),
        &export_spec_json(
            named_connection(),
            serde_json::json!({"parameters": [{"key": "TABLE_NAME", "value": "t"}]}),
        ),
    ));

    assert!(message.contains("TABLE_NAME = 't'"), "{message}");
}

#[test]
fn hook_refuses_a_specification_it_cannot_parse() {
    let error = generate_export_sql(&mut granted(hook_context()), "not json")
        .expect_err("an unparseable specification is refused");

    assert!(matches!(error, UdfError::Type(_)), "{error:?}");
}

#[test]
fn hook_with_single_commit_renders_one_writer() {
    let sql = generate_export_sql(
        &mut granted(hook_context()),
        &export_spec_json(
            named_connection(),
            serde_json::json!({"parameters": [
                {"key": "TABLE_NAME", "value": "ns.t"},
                {"key": "SINGLE_COMMIT", "value": "true"},
            ]}),
        ),
    )
    .expect("a valid export yields SQL");

    assert!(sql.ends_with(" GROUP BY MOD(ROWNUM, 1)"), "{sql}");
}

#[test]
fn writer_returns_the_exact_row_count_of_its_group() {
    let cases = [("one row", 1), ("several rows", 5), ("many rows", 1_000)];

    for (case, rows) in cases {
        let group = (0..rows).map(|id| writer_row(WRITER_SPEC, id)).collect();
        let counted = run_writer(&mut granted(writer_context(group)))
            .unwrap_or_else(|e| panic!("{case}: {e:?}"));
        assert_eq!(counted, rows, "{case}");
    }
}

#[test]
fn writer_counts_zero_for_an_empty_group() {
    let counted = run_writer(&mut writer_context(vec![])).expect("an empty group counts");

    assert_eq!(counted, 0);
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
fn writer_without_script_grant_names_connection_script_user_and_grant() {
    let message = error_text(run_writer(&mut writer_context(vec![writer_row(
        WRITER_SPEC,
        1,
    )])));

    assert!(
        message.contains(
            "GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT LHVS.LAKEHOUSE_EXPORT TO EXPORTER"
        ),
        "{message}"
    );
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
