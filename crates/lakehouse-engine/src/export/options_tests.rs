use super::*;
use crate::export::test_support::{CONNECTION, SENTINELS, connection_object};
use exasol_udf_sdk::connection::ConnectionObject;

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
    connection_object(SENTINELS[0], SENTINELS[1], SENTINELS[2])
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
    let named_at = ExportSpec {
        connection_name: Some(CONNECTION.to_string()),
        ..valid_spec()
    };
    let inline = ExportSpec {
        connection_information: Some(inline_connection()),
        ..valid_spec()
    };
    let appends = "the export never truncates or replaces a table";
    let table_form = "'<namespace>.<table>'";
    let integer_form = "a whole number from 1 to";
    let cases: Vec<(&str, ExportSpec, Vec<&str>)> = vec![
        ("TRUNCATE", truncate, vec!["TRUNCATE", appends]),
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
            "TABLE_NAME missing",
            spec_with(&[("WRITERS_PER_NODE", "2")]),
            vec!["TABLE_NAME", "is required", table_form],
        ),
        (
            "TABLE_NAME without namespace",
            spec_with(&[("TABLE_NAME", "t")]),
            vec!["TABLE_NAME = 't'", table_form],
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
                "CATALOG_CONNECTION, TABLE_NAME, WRITERS_PER_NODE, SINGLE_COMMIT, and ALLOW_HTTP",
            ],
        ),
        (
            "WRITERS_PER_NODE zero",
            spec_with(&[("TABLE_NAME", "ns.t"), ("WRITERS_PER_NODE", "0")]),
            vec!["WRITERS_PER_NODE = '0'", integer_form],
        ),
        (
            "SINGLE_COMMIT not a boolean",
            spec_with(&[("TABLE_NAME", "ns.t"), ("SINGLE_COMMIT", "yes")]),
            vec!["SINGLE_COMMIT = 'yes'", "'true' or 'false'"],
        ),
        (
            "ALLOW_HTTP not a boolean",
            spec_with(&[("TABLE_NAME", "ns.t"), ("ALLOW_HTTP", "1")]),
            vec!["ALLOW_HTTP = '1'", "'true' or 'false'"],
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
    bool,
);

#[test]
fn accepted_statements_yield_the_connection_table_writer_layout_and_http_consent() {
    let cases: Vec<AcceptedCase> = vec![
        (
            "defaults",
            vec![("TABLE_NAME", "ns.t")],
            WriterLayout::PerNode(1),
            false,
        ),
        (
            "WRITERS_PER_NODE",
            vec![("WRITERS_PER_NODE", "3"), ("TABLE_NAME", "ns.t")],
            WriterLayout::PerNode(3),
            false,
        ),
        (
            "SINGLE_COMMIT false",
            vec![("TABLE_NAME", "ns.t"), ("SINGLE_COMMIT", "FALSE")],
            WriterLayout::PerNode(1),
            false,
        ),
        (
            "WRITERS_PER_NODE with SINGLE_COMMIT false",
            vec![
                ("TABLE_NAME", "ns.t"),
                ("SINGLE_COMMIT", "false"),
                ("WRITERS_PER_NODE", "2"),
            ],
            WriterLayout::PerNode(2),
            false,
        ),
        (
            "ALLOW_HTTP true in any case",
            vec![("TABLE_NAME", "ns.t"), ("ALLOW_HTTP", "True")],
            WriterLayout::PerNode(1),
            true,
        ),
        (
            "ALLOW_HTTP false",
            vec![("TABLE_NAME", "ns.t"), ("ALLOW_HTTP", "false")],
            WriterLayout::PerNode(1),
            false,
        ),
    ];

    for (case, parameters, writers, allow_http) in cases {
        let options = ExportOptions::from_spec(&spec_with(&parameters))
            .unwrap_or_else(|e| panic!("{case}: expected acceptance, got {e:?}"));
        assert_eq!(
            options,
            ExportOptions {
                connection_name: CONNECTION.to_string(),
                table_name: "ns.t".to_string(),
                writers,
                allow_http,
            },
            "{case}"
        );
    }
}
