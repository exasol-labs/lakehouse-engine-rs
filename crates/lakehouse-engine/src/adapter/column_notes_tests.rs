use super::*;

fn user_message(error: UdfError) -> String {
    match error {
        UdfError::User(message) => message,
        other => panic!("expected a user error, got {other:?}"),
    }
}

fn field(name: &str, arrow_type: &str) -> LogicalField {
    LogicalField {
        field_id: None,
        name: name.to_string(),
        arrow_type: arrow_type.to_string(),
        nullable: true,
        initial_default: None,
        nested: None,
        physical_name: None,
    }
}

fn declaration(field: LogicalField) -> ColumnDeclaration {
    ColumnDeclaration {
        source_type: field,
        partition: None,
        refused: None,
    }
}

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

fn encoded(declaration: &ColumnDeclaration) -> Option<String> {
    Some(declaration.encode())
}

#[test]
fn partition_columns_are_ordered_by_their_recorded_position_not_by_column_order() {
    let at = |name: &str, position: Option<usize>| {
        encoded(&ColumnDeclaration {
            partition: position,
            ..declaration(field(name, "utf8"))
        })
    };
    let declared = declared_schema(vec![
        ("REGION", at("region", Some(1))),
        ("ID", at("id", None)),
        ("YEAR", at("year", Some(0))),
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
    let id = encoded(&declaration(field("id", "int64")));
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

        assert!(message.contains("SALES"), "{unreadable:?}: {message}");
        assert!(message.contains("AMOUNT"), "{unreadable:?}: {message}");
        assert!(
            message.contains("missing or unreadable"),
            "{unreadable:?}: {message}"
        );
        assert!(
            message.contains("ALTER VIRTUAL SCHEMA ... REFRESH"),
            "{unreadable:?}: {message}"
        );
        assert!(message.contains(cause), "{unreadable:?}: {message}");
    }
}

#[test]
fn encoder_refuses_a_note_over_the_limit_naming_table_and_column() {
    let at_limit = "x".repeat(MAX_COLUMN_NOTE_CHARS);
    assert_eq!(
        checked_note("SALES", "WIDE", at_limit.clone()).unwrap(),
        at_limit
    );

    let message = user_message(
        checked_note("SALES", "WIDE", "x".repeat(MAX_COLUMN_NOTE_CHARS + 1))
            .expect_err("a note over the limit must fail"),
    );

    assert!(message.contains("SALES"), "{message}");
    assert!(message.contains("WIDE"), "{message}");
    assert!(message.contains("2000001"), "{message}");
    assert!(message.contains("2000000"), "{message}");
}

#[test]
fn the_limit_counts_characters_not_bytes() {
    let two_byte_chars = "é".repeat(MAX_COLUMN_NOTE_CHARS);
    assert!(checked_note("SALES", "WIDE", two_byte_chars).is_ok());
}

#[test]
fn binary_refusal_names_the_label_column_member_and_declared_type() {
    let whole = binary_refusal("Parquet", "blob", None, "binary");
    assert!(whole.starts_with("Parquet column 'blob' has type 'binary'"));
    assert!(whole.ends_with("tracked as issue #351"));

    let member = binary_refusal("Iceberg", "doc", Some("doc.raw"), "binary");
    assert!(member.starts_with("Iceberg column 'doc', whose member 'doc.raw' has type 'binary'"));
}

fn refused(name: &str, reason: &str, partition: Option<usize>) -> Option<String> {
    encoded(&ColumnDeclaration {
        source_type: field(name, "utf8"),
        partition,
        refused: Some(reason.to_string()),
    })
}

/// Scenario: A Delta table with no mappable column is refused as a whole
#[test]
fn a_table_is_refused_as_a_whole_only_when_every_column_is_refused() {
    let id = || ("ID", encoded(&declaration(field("id", "int64"))));
    let binary = || {
        (
            "BINARY_COL",
            refused("binary_col", "binary is refused, see #351", None),
        )
    };
    let variant = (
        "VARIANT_COL",
        refused("variant_col", "variant renders nothing", None),
    );

    declared_schema(vec![id(), binary()])
        .expect("a table with a mappable column must not be refused as a whole");
    declared_schema(Vec::new())
        .expect("an empty schema with nothing refused must not be refused as a whole");
    let message = user_message(
        declared_schema(vec![binary(), variant])
            .expect_err("a table with zero mappable columns must be refused as a whole"),
    );

    assert_eq!(
        message,
        "virtual table 'SALES' has no mappable column; every column is refused: 'binary_col': \
         binary is refused, see #351; 'variant_col': variant renders nothing"
    );
}

/// Scenario: The reader never infers partition columns from paths or partition metadata logging, and fails a plan whose partition column is refused
#[test]
fn a_refused_partition_column_fails_the_parse_naming_the_column_and_its_reason() {
    let id = || ("ID", encoded(&declaration(field("id", "int64"))));

    declared_schema(vec![id(), ("BLOB", refused("blob", "binary", None))])
        .expect("a refused column outside the partition columns stays a column refusal");
    let message = user_message(
        declared_schema(vec![id(), ("SHARD", refused("shard", "binary", Some(0)))])
            .expect_err("the scan cannot materialize a refused partition column"),
    );

    assert_eq!(
        message,
        "virtual table 'SALES' declares 'shard' as a partition column, but that column is refused \
         (binary); the scan cannot materialize a partition column absent from the logical schema"
    );
}
