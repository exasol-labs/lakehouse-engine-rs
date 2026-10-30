use super::*;
use crate::adapter::pushdown::test_support::{assert_mentions, nullable_field, user_message};

fn declared_schema(columns: Vec<(&str, Option<String>)>) -> Result<DeclaredSchema, UdfError> {
    TableNotes {
        table: "SALES",
        columns: columns
            .iter()
            .map(|(name, note)| (*name, note.as_deref()))
            .collect(),
    }
    .declared_schema()
}

/// The encoded note of the `utf8` column `name`, at `partition` and refused for `refused`.
fn note(name: &str, partition: Option<usize>, refused: Option<&str>) -> Option<String> {
    let declaration = ColumnDeclaration {
        source_type: nullable_field(name, "utf8"),
        partition,
        refused: refused.map(str::to_string),
    };
    Some(declaration.encode())
}

fn id_note() -> Option<String> {
    let declaration = ColumnDeclaration {
        source_type: nullable_field("id", "int64"),
        partition: None,
        refused: None,
    };
    Some(declaration.encode())
}

#[test]
fn partition_columns_are_ordered_by_their_recorded_position_not_by_column_order() {
    let declared = declared_schema(vec![
        ("REGION", note("region", Some(1), None)),
        ("ID", note("id", None, None)),
        ("YEAR", note("year", Some(0), None)),
    ])
    .unwrap();

    assert_eq!(declared.partition_columns, vec!["year", "region"]);
    let logical: Vec<&str> = declared
        .logical_schema
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    assert_eq!(logical, vec!["region", "id", "year"]);
}

/// Scenario: A column without a readable note fails the pushdown naming the fix
#[test]
fn missing_or_unreadable_note_names_table_column_and_refresh() {
    let id = id_note();
    for (unreadable, cause) in [
        (None, "the column carries no note"),
        (Some("not json".to_string()), "expected ident at line 1"),
        (Some("[]".to_string()), "expected struct ColumnDeclaration"),
        (
            Some(r#"{"partition":0}"#.to_string()),
            "missing field `sourceType`",
        ),
        (
            Some(r#"{"sourceType":{"name":"x"}}"#.to_string()),
            "missing field",
        ),
    ] {
        let message = user_message(
            declared_schema(vec![("ID", id.clone()), ("AMOUNT", unreadable.clone())])
                .expect_err("a column without a readable note must fail"),
        );

        assert_mentions(
            &message,
            &[
                "SALES",
                "AMOUNT",
                "missing or unreadable",
                "ALTER VIRTUAL SCHEMA ... REFRESH",
                cause,
            ],
        );
    }
}

#[test]
fn encoder_refuses_a_note_over_the_character_limit_naming_table_and_column() {
    let at_limit = "x".repeat(MAX_COLUMN_NOTE_CHARS);
    assert_eq!(
        checked_note("SALES", "WIDE", at_limit.clone()).unwrap(),
        at_limit
    );
    let two_byte_chars = "é".repeat(MAX_COLUMN_NOTE_CHARS);
    assert!(
        checked_note("SALES", "WIDE", two_byte_chars).is_ok(),
        "the limit counts characters, not bytes"
    );

    let message = user_message(
        checked_note("SALES", "WIDE", "x".repeat(MAX_COLUMN_NOTE_CHARS + 1))
            .expect_err("a note over the limit must fail"),
    );

    assert_mentions(&message, &["SALES", "WIDE", "2000001", "2000000"]);
}

#[test]
fn an_unread_type_refusal_names_the_label_column_member_and_declared_type() {
    let whole = column_refusal("Parquet", "shape", None, &unread_type_cause("geometry"));
    assert_eq!(
        whole,
        "Parquet column 'shape' has type 'geometry', which this engine does not read"
    );

    let member = column_refusal(
        "Iceberg",
        "doc",
        Some("doc.area"),
        &unread_type_cause("geography"),
    );
    assert!(
        member.starts_with("Iceberg column 'doc', whose member 'doc.area' has type 'geography'")
    );
}

/// Scenario: A Delta table with no mappable column is refused as a whole
#[test]
fn a_table_is_refused_as_a_whole_only_when_every_column_is_refused() {
    let id = || ("ID", id_note());
    let shape = || {
        let reason = "geometry is not read";
        ("SHAPE_COL", note("shape_col", None, Some(reason)))
    };
    let variant = (
        "VARIANT_COL",
        note("variant_col", None, Some("variant renders nothing")),
    );

    declared_schema(vec![id(), shape()])
        .expect("a table with a mappable column must not be refused as a whole");
    declared_schema(Vec::new())
        .expect("an empty schema with nothing refused must not be refused as a whole");
    let message = user_message(
        declared_schema(vec![shape(), variant])
            .expect_err("a table with zero mappable columns must be refused as a whole"),
    );

    assert_eq!(
        message,
        "virtual table 'SALES' has no mappable column; every column is refused: 'shape_col': \
         geometry is not read; 'variant_col': variant renders nothing"
    );
}

/// Scenario: The reader never infers partition columns from paths or partition metadata logging, and fails a plan whose partition column is refused
#[test]
fn a_refused_partition_column_fails_the_parse_naming_the_column_and_its_reason() {
    let id = || ("ID", id_note());

    declared_schema(vec![id(), ("BLOB", note("blob", None, Some("binary")))])
        .expect("a refused column outside the partition columns stays a column refusal");
    let message = user_message(
        declared_schema(vec![
            id(),
            ("SHARD", note("shard", Some(0), Some("binary"))),
        ])
        .expect_err("the scan cannot materialize a refused partition column"),
    );

    assert_eq!(
        message,
        "virtual table 'SALES' declares 'shard' as a partition column, but that column is refused \
         (binary); the scan cannot materialize a partition column absent from the logical schema"
    );
}
