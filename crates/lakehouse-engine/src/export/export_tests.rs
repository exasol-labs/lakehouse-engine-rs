use super::*;
use crate::export::test_support::{CONNECTION, SENTINELS, connection_object, merged};
use exasol_udf_sdk::test_support::TestContext;
use exasol_udf_sdk::value::Value;
use vs_expression::udf_column_test_support::measured_column;

fn sentinel_connection() -> exasol_udf_sdk::connection::ConnectionObject {
    let password = serde_json::json!({ "warehouse": "wh", "token": SENTINELS[2] });
    connection_object(SENTINELS[0], SENTINELS[1], &password.to_string())
}

fn granted(ctx: TestContext) -> TestContext {
    ctx.with_connection(CONNECTION, sentinel_connection())
}

/// The writer SELECT the hook plans before it makes any catalog call, as for a new table.
fn hook(ctx: &mut TestContext, json: &str) -> Result<String, UdfError> {
    let plan = plan_export(ctx, json)?;
    Ok(plan.select.render(&plan.writer_spec(false)?.render()))
}

/// The writer plan of a group, with the context standing on its first row as in a real run.
fn writer_plan(ctx: &mut TestContext) -> Result<WriterPlan, UdfError> {
    ctx.next().expect("the group has a first row");
    plan_writer(ctx)
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
    let spec = serde_json::json!({
        "has_truncate": false,
        "has_replace": false,
        "source_column_names": [r#""T"."A""#],
        "parameters": [
            {"key": "CATALOG_CONNECTION", "value": CONNECTION},
            {"key": "TABLE_NAME", "value": "ns.t"},
        ],
    });
    merged(merged(spec, connection), extra).to_string()
}

fn no_fields() -> serde_json::Value {
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
        serde_json::from_str(&export_spec_json(no_fields(), no_fields())).unwrap();
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
        &export_spec_json(no_fields(), no_fields()),
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
                &export_spec_json(inline, no_fields()),
            ),
        ),
        (
            "missing script grant in the hook",
            generate_export_sql(
                &mut hook_context(),
                &export_spec_json(no_fields(), no_fields()),
            ),
        ),
    ];
    for (case, result) in refusals {
        assert_no_connection_value(&error_text(result), case);
    }
    let writer_refusals = [
        (
            "missing script grant in the writer",
            run_writer(&mut writer_context(vec![writer_row(
                &writer_spec(false),
                1,
            )])),
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
        &export_spec_json(no_fields(), no_fields()),
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
fn writer_refuses_a_first_argument_that_is_not_a_writer_spec() {
    let cases = [
        ("integer", Value::Int64(1)),
        ("NULL", Value::Null),
        ("plain string", Value::String("hello".to_string())),
        (
            "JSON of another shape",
            Value::String(r#"{"connection":"LAKEHOUSE_CATALOG_CREDS","extra":1}"#.to_string()),
        ),
        ("a spec without mode", Value::String(spec_without("mode"))),
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
    let forged = writer_spec(false).replace(r#""table":"ns.t""#, r#""table":"t""#);

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
            "mode": "create",
            "allow_http": false,
            "partition_by": [],
        })
    );
    let mut appending = first.clone();
    appending["mode"] = "append".into();
    for spec in [first, appending] {
        let mut writer = writer_context(vec![writer_row(&spec.to_string(), 1)]);
        writer.next().unwrap();
        let read = WriterSpec::read(&writer).expect("the writer accepts it");
        assert_eq!(read.render(), spec.to_string());
    }
}

fn export_spec_partitioned_by(partition_by: &str) -> String {
    let mut spec: serde_json::Value =
        serde_json::from_str(&export_spec_with_columns(&[r#""T"."ID""#, r#""T"."D""#])).unwrap();
    spec["parameters"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"key": "PARTITION_BY", "value": partition_by}));
    spec.to_string()
}

#[test]
fn the_hook_renders_partition_by_resolved_against_the_source_columns() {
    let sql = hook(
        &mut granted(hook_context()),
        &export_spec_partitioned_by("day(d), bucket(4, id)"),
    )
    .expect("a valid PARTITION_BY");

    assert_eq!(
        rendered_writer_spec(&sql)["partition_by"],
        serde_json::json!([
            {"transform": "day", "column": "D", "name": "D_day"},
            {"transform": "bucket[4]", "column": "ID", "name": "ID_bucket"},
        ])
    );
}

/// The hook refuses an invalid PARTITION_BY while it plans, before its first catalog call.
#[test]
fn the_hook_refuses_an_invalid_partition_by_before_any_catalog_call() {
    let error = error_text(hook(
        &mut granted(hook_context()),
        &export_spec_partitioned_by("bucket(4, X)"),
    ));

    assert!(
        error.contains("PARTITION_BY field 'bucket(4, X)' names column X"),
        "{error}"
    );
}

/// Scenario: PARTITION_BY into an existing table is refused
#[test]
fn partition_by_is_refused_for_a_table_that_exists() {
    let partitioned = export_spec_partitioned_by("id");
    let unpartitioned = export_spec_with_columns(&[r#""T"."ID""#]);
    let cases = [
        (false, &partitioned, Ok(WriterMode::Create)),
        (true, &unpartitioned, Ok(WriterMode::Append)),
        (
            true,
            &partitioned,
            Err("LAKEHOUSE_EXPORT: table ns.t exists, and PARTITION_BY applies only to a table the \
                 export creates; remove PARTITION_BY to append to the table"
                .to_string()),
        ),
    ];

    for (exists, export, expected) in cases {
        let plan = plan_export(&mut granted(hook_context()), export).unwrap();

        let mode = plan.writer_spec(exists).map(|spec| spec.mode);

        assert_eq!(
            mode.map_err(|error| error.to_string()),
            expected,
            "{exists}"
        );
    }
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
    let mut ctx =
        TestContext::set(vec![writer_row(&writer_spec(false), 1)]).with_script_schema("LHVS");

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
        WriterSelect {
            script_schema: "LHVS".to_string(),
            source_columns: columns.iter().map(ToString::to_string).collect(),
            writers,
            node_count,
        }
        .render(WRITER_SPEC)
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

    #[test]
    fn group_count_follows_the_writer_layout() {
        let cases = [
            (
                "nodes times writers per node",
                WriterLayout::PerNode(3),
                2,
                6,
            ),
            ("single commit", WriterLayout::SingleCommit, 4, 1),
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
        let sql = WriterSelect {
            script_schema: r#"my"schema"#.to_string(),
            source_columns: vec![r#""T"."A""#.to_string()],
            writers: WriterLayout::PerNode(1),
            node_count: 1,
        }
        .render(r#"{"connection":"A'B\"C"}"#);

        assert!(
            sql.starts_with(
                r#"SELECT "my""schema"."LAKEHOUSE_EXPORT"('{"connection":"A''B\"C"}', "T"."A") FROM DUAL GROUP BY "#
            ),
            "{sql}"
        );
    }
}

fn writer_spec(single_commit: bool) -> String {
    let mode = if single_commit {
        "create_with_data"
    } else {
        "create"
    };
    serde_json::json!({
        "connection": CONNECTION,
        "table": "ns.t",
        "export_id": "00ff",
        "columns": ["ID"],
        "mode": mode,
        "allow_http": false,
        "partition_by": [],
    })
    .to_string()
}

/// `writer_spec(false)` without the field `name`.
fn spec_without(name: &str) -> String {
    let mut spec: serde_json::Value = serde_json::from_str(&writer_spec(false)).unwrap();
    spec.as_object_mut().unwrap().remove(name);
    spec.to_string()
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
            connection_object(
                "https://catalog.example",
                "",
                &serde_json::json!({ "warehouse": "wh", "token": "t" }).to_string(),
            ),
        )
        .with_input_columns(vec![
            measured_column("VARCHAR(2000000) UTF8"),
            measured_column(id_type),
        ])
}

#[test]
fn a_spec_whose_columns_do_not_match_the_input_is_refused() {
    let spec = writer_spec(false).replace(r#"["ID"]"#, r#"["ID","EXTRA"]"#);

    let error = writer_plan(&mut group(&spec, 1, "DECIMAL(18,0)"))
        .err()
        .expect("the column count differs");

    assert!(
        error.to_string().contains("runs as the target of EXPORT"),
        "{error}"
    );
}

#[test]
fn a_matching_spec_plans_the_writer_with_nullable_fields_in_input_order() {
    let plan = writer_plan(&mut group(&writer_spec(true), 1, "DECIMAL(18,0)"))
        .expect("the spec matches the input");

    assert_eq!(plan.spec.export_id, "00ff");
    assert_eq!(plan.spec.mode, WriterMode::CreateWithData);
    let Some(schema) = plan.schema else {
        panic!("a spec that does not append creates its table");
    };
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| (field.name().as_str(), field.is_nullable()))
        .collect();
    assert_eq!(fields, [("ID", true)]);
}

/// Scenario: A transform that the column's type does not take fails the export before any data is written
#[test]
fn a_new_table_writer_checks_each_partition_field_against_its_column_type() {
    let partitioned = |transform: &str| {
        writer_spec(false).replace(
            r#""partition_by":[]"#,
            &format!(r#""partition_by":[{{"transform":"{transform}","column":"ID","name":"P"}}]"#),
        )
    };

    let plan = writer_plan(&mut group(&partitioned("truncate[10]"), 1, "DECIMAL(18,0)"))
        .expect("truncate takes a DECIMAL");
    let error = writer_plan(&mut group(&partitioned("hour"), 1, "DECIMAL(18,0)"))
        .err()
        .expect("hour does not take a DECIMAL")
        .to_string();

    assert!(
        plan.schema.is_some(),
        "a spec that does not append creates its table"
    );
    assert_eq!(plan.spec.partition_by[0].name, "P");
    assert!(
        error.contains("applies hour to column ID of type DECIMAL(18,0)"),
        "{error}"
    );
}

#[test]
fn an_appending_writer_derives_no_table_schema_from_its_source() {
    let appending = writer_spec(false).replace(r#""mode":"create""#, r#""mode":"append""#);

    let plan = writer_plan(&mut group(&appending, 1, "TIMESTAMP(9)"))
        .expect("the table's columns, not the source's types, decide an append");

    assert!(plan.schema.is_none());
}

#[test]
fn export_and_writer_ids_are_fresh_hex_of_their_length() {
    let (export_id, writer_id) = (new_export_id(), new_writer_id());

    assert_eq!((export_id.len(), writer_id.len()), (32, 16));
    assert!(export_id.chars().all(|c| c.is_ascii_hexdigit()));
    assert!(writer_id.chars().all(|c| c.is_ascii_hexdigit()));
    assert_ne!(new_export_id(), export_id);
}

#[test]
fn the_export_runtime_drives_a_future() {
    let runtime = export_runtime().expect("a runtime");

    assert_eq!(runtime.block_on(async { 7 }), 7);
}
