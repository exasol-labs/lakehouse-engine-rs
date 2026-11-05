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

/// Scenario: No CONNECTION value appears in the generated SQL or an error
#[test]
fn generated_sql_and_errors_carry_no_connection_value() {
    let sql = generate_export_sql(
        &mut granted(hook_context()),
        &export_spec_json(no_at_clause(), no_extra()),
    )
    .expect("a valid export yields SQL");
    assert!(
        sql.starts_with(&format!(
            r#"SELECT "LHVS"."LAKEHOUSE_EXPORT"('{WRITER_SPEC}', "T"."A") FROM DUAL GROUP BY "#
        )),
        "{sql}"
    );
    assert!(sql.ends_with("GROUP BY MOD(ROWNUM, 2)"), "{sql}");
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
                &export_spec_json(no_at_clause(), serde_json::json!({"has_truncate": true})),
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
fn hook_refuses_an_invalid_statement_naming_the_option() {
    let message = error_text(generate_export_sql(
        &mut granted(hook_context()),
        &export_spec_json(
            no_at_clause(),
            serde_json::json!({"parameters": [
                {"key": "CATALOG_CONNECTION", "value": CONNECTION},
                {"key": "TABLE_NAME", "value": "t"},
            ]}),
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
            no_at_clause(),
            serde_json::json!({"parameters": [
                {"key": "CATALOG_CONNECTION", "value": CONNECTION},
                {"key": "TABLE_NAME", "value": "ns.t"},
                {"key": "SINGLE_COMMIT", "value": "true"},
            ]}),
        ),
    )
    .expect("a valid export yields SQL");

    assert!(sql.ends_with("GROUP BY MOD(ROWNUM, 1)"), "{sql}");
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

mod options {
    use super::super::*;
    use exasol_udf_sdk::connection::ConnectionObject;

    const CONNECTION: &str = "LAKEHOUSE_CATALOG_CREDS";
    const SENTINELS: [&str; 3] = ["sentinel-address", "sentinel-user", "sentinel-password"];

    fn spec_with(parameters: &[(&str, &str)]) -> ExportSpec {
        let mut with = vec![("CATALOG_CONNECTION", CONNECTION)];
        with.extend_from_slice(parameters);
        ExportSpec {
            has_truncate: false,
            has_replace: false,
            created_by: None,
            source_column_names: vec![r#""T"."A""#.to_string()],
            connection_information: None,
            connection_name: None,
            parameters: with
                .iter()
                .map(|(key, value)| Parameter {
                    key: key.to_string(),
                    value: value.to_string(),
                })
                .collect(),
        }
    }

    fn valid_spec() -> ExportSpec {
        spec_with(&[("TABLE_NAME", "ns.t")])
    }

    fn inline_connection() -> ConnectionObject {
        ConnectionObject {
            kind: "password".to_string(),
            address: SENTINELS[0].to_string(),
            user: SENTINELS[1].to_string(),
            password: SENTINELS[2].to_string(),
        }
    }

    fn refusal_message(spec: &ExportSpec) -> String {
        match ExportOptions::from_spec(spec) {
            Err(UdfError::User(message)) => message,
            other => panic!("expected a UdfError::User refusal, got {other:?}"),
        }
    }

    #[test]
    fn refused_statements_name_the_offending_part_and_its_accepted_form() {
        let truncate = ExportSpec {
            has_truncate: true,
            ..valid_spec()
        };
        let replace = ExportSpec {
            has_replace: true,
            ..valid_spec()
        };
        let created_by = ExportSpec {
            created_by: Some("CREATE TABLE ns.t (a INT)".to_string()),
            ..valid_spec()
        };
        let named_at = ExportSpec {
            connection_name: Some(CONNECTION.to_string()),
            ..valid_spec()
        };
        let inline = ExportSpec {
            connection_information: Some(inline_connection()),
            ..valid_spec()
        };
        let appends = "only appends rows";
        let table_form = "'<namespace>.<table>'";
        let integer_form = "a whole number from 1 to";
        let cases: Vec<(&str, ExportSpec, Vec<&str>)> = vec![
            ("TRUNCATE", truncate, vec!["TRUNCATE", appends]),
            ("REPLACE", replace, vec!["REPLACE", appends]),
            ("CREATED BY", created_by, vec!["CREATED BY", appends]),
            (
                "named AT clause",
                named_at,
                vec!["AT clause", "CATALOG_CONNECTION"],
            ),
            (
                "inline AT clause",
                inline,
                vec!["AT clause", "CATALOG_CONNECTION"],
            ),
            (
                "CATALOG_CONNECTION missing",
                ExportSpec {
                    parameters: vec![Parameter {
                        key: "TABLE_NAME".to_string(),
                        value: "ns.t".to_string(),
                    }],
                    ..valid_spec()
                },
                vec!["CATALOG_CONNECTION", "is required"],
            ),
            (
                "CATALOG_CONNECTION empty",
                ExportSpec {
                    parameters: vec![
                        Parameter {
                            key: "CATALOG_CONNECTION".to_string(),
                            value: String::new(),
                        },
                        Parameter {
                            key: "TABLE_NAME".to_string(),
                            value: "ns.t".to_string(),
                        },
                    ],
                    ..valid_spec()
                },
                vec!["CATALOG_CONNECTION", "is required"],
            ),
            (
                "TABLE_NAME missing",
                spec_with(&[("WRITERS_PER_NODE", "2")]),
                vec!["TABLE_NAME", "is required", table_form],
            ),
            (
                "TABLE_NAME empty",
                spec_with(&[("TABLE_NAME", "")]),
                vec!["TABLE_NAME", table_form],
            ),
            (
                "TABLE_NAME without namespace",
                spec_with(&[("TABLE_NAME", "t")]),
                vec!["TABLE_NAME = 't'", table_form],
            ),
            (
                "TABLE_NAME with empty namespace",
                spec_with(&[("TABLE_NAME", ".t")]),
                vec!["TABLE_NAME = '.t'", table_form],
            ),
            (
                "TABLE_NAME with empty table",
                spec_with(&[("TABLE_NAME", "ns.")]),
                vec!["TABLE_NAME = 'ns.'", table_form],
            ),
            (
                "TABLE_NAME with empty namespace level",
                spec_with(&[("TABLE_NAME", "a..t")]),
                vec!["TABLE_NAME = 'a..t'", table_form],
            ),
            (
                "unknown option",
                spec_with(&[("TABLE_NAME", "ns.t"), ("FORMAT", "csv")]),
                vec![
                    "unknown option FORMAT",
                    "CATALOG_CONNECTION, TABLE_NAME, WRITERS_PER_NODE, and SINGLE_COMMIT",
                ],
            ),
            (
                "WRITERS_PER_NODE zero",
                spec_with(&[("TABLE_NAME", "ns.t"), ("WRITERS_PER_NODE", "0")]),
                vec!["WRITERS_PER_NODE = '0'", integer_form],
            ),
            (
                "WRITERS_PER_NODE negative",
                spec_with(&[("TABLE_NAME", "ns.t"), ("WRITERS_PER_NODE", "-1")]),
                vec!["WRITERS_PER_NODE = '-1'", integer_form],
            ),
            (
                "WRITERS_PER_NODE signed",
                spec_with(&[("TABLE_NAME", "ns.t"), ("WRITERS_PER_NODE", "+3")]),
                vec!["WRITERS_PER_NODE = '+3'", integer_form],
            ),
            (
                "WRITERS_PER_NODE fractional",
                spec_with(&[("TABLE_NAME", "ns.t"), ("WRITERS_PER_NODE", "1.5")]),
                vec!["WRITERS_PER_NODE = '1.5'", integer_form],
            ),
            (
                "WRITERS_PER_NODE padded",
                spec_with(&[("TABLE_NAME", "ns.t"), ("WRITERS_PER_NODE", " 3")]),
                vec!["WRITERS_PER_NODE = ' 3'", integer_form],
            ),
            (
                "WRITERS_PER_NODE not a number",
                spec_with(&[("TABLE_NAME", "ns.t"), ("WRITERS_PER_NODE", "four")]),
                vec!["WRITERS_PER_NODE = 'four'", integer_form],
            ),
            (
                "WRITERS_PER_NODE empty",
                spec_with(&[("TABLE_NAME", "ns.t"), ("WRITERS_PER_NODE", "")]),
                vec!["WRITERS_PER_NODE = ''", integer_form],
            ),
            (
                "WRITERS_PER_NODE one past usize::MAX",
                spec_with(&[
                    ("TABLE_NAME", "ns.t"),
                    ("WRITERS_PER_NODE", "18446744073709551616"),
                ]),
                vec!["WRITERS_PER_NODE = '18446744073709551616'", integer_form],
            ),
            (
                "SINGLE_COMMIT not a boolean",
                spec_with(&[("TABLE_NAME", "ns.t"), ("SINGLE_COMMIT", "yes")]),
                vec!["SINGLE_COMMIT = 'yes'", "'true' or 'false'"],
            ),
            (
                "SINGLE_COMMIT empty",
                spec_with(&[("TABLE_NAME", "ns.t"), ("SINGLE_COMMIT", "")]),
                vec!["SINGLE_COMMIT = ''", "'true' or 'false'"],
            ),
            (
                "WRITERS_PER_NODE with SINGLE_COMMIT",
                spec_with(&[
                    ("TABLE_NAME", "ns.t"),
                    ("WRITERS_PER_NODE", "2"),
                    ("SINGLE_COMMIT", "TRUE"),
                ]),
                vec!["WRITERS_PER_NODE", "SINGLE_COMMIT = 'true'"],
            ),
        ];

        for (case, spec, expected) in cases {
            let message = refusal_message(&spec);
            assert!(
                message.starts_with("LAKEHOUSE_EXPORT: "),
                "{case}: message {message:?} lacks the script prefix"
            );
            for fragment in expected {
                assert!(
                    message.contains(fragment),
                    "{case}: message {message:?} lacks {fragment:?}"
                );
            }
            for sentinel in SENTINELS {
                assert!(
                    !message.contains(sentinel),
                    "{case}: message {message:?} echoes the inline CONNECTION value {sentinel:?}"
                );
            }
        }
    }

    type AcceptedCase = (
        &'static str,
        Vec<(&'static str, &'static str)>,
        WriterLayout,
    );

    #[test]
    fn accepted_statements_yield_the_connection_and_writer_layout() {
        let cases: Vec<AcceptedCase> = vec![
            (
                "defaults",
                vec![("TABLE_NAME", "ns.t")],
                WriterLayout::PerNode(1),
            ),
            (
                "multi-level namespace",
                vec![("TABLE_NAME", "cat.db.schema.t")],
                WriterLayout::PerNode(1),
            ),
            (
                "WRITERS_PER_NODE",
                vec![("WRITERS_PER_NODE", "3"), ("TABLE_NAME", "ns.t")],
                WriterLayout::PerNode(3),
            ),
            (
                "WRITERS_PER_NODE with leading zeros",
                vec![("TABLE_NAME", "ns.t"), ("WRITERS_PER_NODE", "007")],
                WriterLayout::PerNode(7),
            ),
            (
                "WRITERS_PER_NODE at usize::MAX",
                vec![
                    ("TABLE_NAME", "ns.t"),
                    ("WRITERS_PER_NODE", "18446744073709551615"),
                ],
                WriterLayout::PerNode(usize::MAX),
            ),
            (
                "SINGLE_COMMIT true",
                vec![("TABLE_NAME", "ns.t"), ("SINGLE_COMMIT", "true")],
                WriterLayout::SingleCommit,
            ),
            (
                "SINGLE_COMMIT in mixed case",
                vec![("TABLE_NAME", "ns.t"), ("SINGLE_COMMIT", "True")],
                WriterLayout::SingleCommit,
            ),
            (
                "SINGLE_COMMIT false",
                vec![("TABLE_NAME", "ns.t"), ("SINGLE_COMMIT", "FALSE")],
                WriterLayout::PerNode(1),
            ),
            (
                "WRITERS_PER_NODE with SINGLE_COMMIT false",
                vec![
                    ("TABLE_NAME", "ns.t"),
                    ("SINGLE_COMMIT", "false"),
                    ("WRITERS_PER_NODE", "2"),
                ],
                WriterLayout::PerNode(2),
            ),
        ];

        for (case, parameters, writers) in cases {
            let options = ExportOptions::from_spec(&spec_with(&parameters))
                .unwrap_or_else(|e| panic!("{case}: expected acceptance, got {e:?}"));
            assert_eq!(
                options,
                ExportOptions {
                    connection_name: CONNECTION.to_string(),
                    writers,
                },
                "{case}"
            );
        }
    }
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
    fn one_column_renders_the_self_call_and_row_number_key() {
        let sql = select_for(&[r#""T"."A""#], WriterLayout::PerNode(1), 1);

        assert_eq!(
            sql,
            "SELECT \"LHVS\".\"LAKEHOUSE_EXPORT\"('{\"connection\":\"C\"}', \"T\".\"A\") FROM DUAL \
             GROUP BY MOD(ROWNUM, 1)"
        );
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
            ("table column", r#""T"."A""#),
            ("query alias", r#""X""#),
            ("schema-qualified column", r#""S"."T"."A""#),
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
