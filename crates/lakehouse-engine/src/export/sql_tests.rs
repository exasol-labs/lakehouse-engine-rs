use super::*;

const WRITER_SPEC: &str = r#"{"connection":"C"}"#;

fn select_for(columns: &[&str], writers: WriterLayout, node_count: usize) -> String {
    let source_columns: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
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

fn guard(reference: &str, name: &str) -> String {
    format!(
        "(CASE WHEN TYPEOF({reference}) LIKE 'GEOMETRY%' OR TYPEOF({reference}) LIKE 'HASHTYPE%' \
         OR TYPEOF({reference}) LIKE 'INTERVAL%' OR TYPEOF({reference}) LIKE '%WITH LOCAL TIME ZONE' \
         THEN CAST('LAKEHOUSE_EXPORT: column \"{name}\" has type ' || TYPEOF({reference}) || \
         ', which the export does not support; cast it to a supported type' AS DECIMAL(1,0)) \
         ELSE 0 END) = 0"
    )
}

#[test]
fn one_column_renders_the_self_call_guard_and_row_number_key() {
    let sql = select_for(&[r#""T"."A""#], WriterLayout::PerNode(1), 1);

    assert_eq!(
        sql,
        "SELECT \"LHVS\".\"LAKEHOUSE_EXPORT\"('{\"connection\":\"C\"}', \"T\".\"A\") FROM DUAL \
         WHERE (CASE WHEN TYPEOF(\"T\".\"A\") LIKE 'GEOMETRY%' OR TYPEOF(\"T\".\"A\") LIKE 'HASHTYPE%' \
         OR TYPEOF(\"T\".\"A\") LIKE 'INTERVAL%' OR TYPEOF(\"T\".\"A\") LIKE '%WITH LOCAL TIME ZONE' \
         THEN CAST('LAKEHOUSE_EXPORT: column \"A\" has type ' || TYPEOF(\"T\".\"A\") || \
         ', which the export does not support; cast it to a supported type' AS DECIMAL(1,0)) \
         ELSE 0 END) = 0 GROUP BY MOD(ROWNUM, 1)"
    );
}

#[test]
fn several_columns_pass_in_source_order_with_one_guard_each() {
    let sql = select_for(
        &[r#""T"."ID""#, r#""T"."NAME""#, r#""T"."G""#],
        WriterLayout::PerNode(1),
        1,
    );

    let expected = format!(
        "SELECT \"LHVS\".\"LAKEHOUSE_EXPORT\"('{WRITER_SPEC}', \"T\".\"ID\", \"T\".\"NAME\", \"T\".\"G\") \
         FROM DUAL WHERE {} AND {} AND {} GROUP BY MOD(ROWNUM, 1)",
        guard(r#""T"."ID""#, "ID"),
        guard(r#""T"."NAME""#, "NAME"),
        guard(r#""T"."G""#, "G"),
    );
    assert_eq!(sql, expected);
}

#[test]
fn group_count_follows_the_writer_layout_and_caps_at_300() {
    let cases = [
        (
            "single commit on one node",
            WriterLayout::SingleCommit,
            1,
            1,
        ),
        (
            "single commit on four nodes",
            WriterLayout::SingleCommit,
            4,
            1,
        ),
        ("default on one node", WriterLayout::PerNode(1), 1, 1),
        ("default on four nodes", WriterLayout::PerNode(1), 4, 4),
        (
            "three per node on two nodes",
            WriterLayout::PerNode(3),
            2,
            6,
        ),
        ("unknown node count", WriterLayout::PerNode(2), 0, 1),
        ("exactly at the cap", WriterLayout::PerNode(300), 1, 300),
        ("one past the cap", WriterLayout::PerNode(301), 1, 300),
        ("far past the cap", WriterLayout::PerNode(500), 4, 300),
        (
            "overflowing product",
            WriterLayout::PerNode(usize::MAX),
            4,
            300,
        ),
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
fn column_names_are_referenced_verbatim_and_named_by_their_last_identifier() {
    let cases = [
        ("table column", r#""T"."A""#, "A"),
        ("query alias", r#""X""#, "X"),
        ("schema-qualified column", r#""S"."T"."A""#, "A"),
        ("dot inside the identifier", r#""T"."a.b""#, "a.b"),
        (
            "quote and apostrophe",
            r#""T"."it's ""q""""#,
            r#"it''s "q""#,
        ),
        ("unquoted reference", "T.A", "T.A"),
    ];

    for (case, reference, message_name) in cases {
        let sql = select_for(&[reference], WriterLayout::PerNode(1), 1);
        assert!(
            sql.contains(&format!("{WRITER_SPEC}', {reference}) FROM DUAL")),
            "{case}: {sql}"
        );
        assert!(
            sql.contains(&guard(reference, message_name)),
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
            r#"SELECT "my""schema"."LAKEHOUSE_EXPORT"('{"connection":"A''B\"C"}', "T"."A") FROM DUAL WHERE "#
        ),
        "{sql}"
    );
}
