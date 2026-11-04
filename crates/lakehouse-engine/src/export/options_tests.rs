use super::*;
use exasol_udf_sdk::connection::ConnectionObject;

const CONNECTION: &str = "LAKEHOUSE_CATALOG_CREDS";
const SENTINELS: [&str; 3] = ["sentinel-address", "sentinel-user", "sentinel-password"];

fn spec_with(parameters: &[(&str, &str)]) -> ExportSpec {
    ExportSpec {
        has_truncate: false,
        has_replace: false,
        created_by: None,
        source_column_names: vec![r#""T"."A""#.to_string()],
        connection_information: None,
        connection_name: Some(CONNECTION.to_string()),
        parameters: parameters
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
    let no_connection = ExportSpec {
        connection_name: None,
        ..valid_spec()
    };
    let inline = ExportSpec {
        connection_name: None,
        connection_information: Some(inline_connection()),
        ..valid_spec()
    };
    let appends = "only appends rows";
    let table_form = "'<namespace>.<table>'";
    let integer_form = "an integer of at least 1";
    let cases: Vec<(&str, ExportSpec, Vec<&str>)> = vec![
        ("TRUNCATE", truncate, vec!["TRUNCATE", appends]),
        ("REPLACE", replace, vec!["REPLACE", appends]),
        ("CREATED BY", created_by, vec!["CREATED BY", appends]),
        ("no AT clause", no_connection, vec!["AT <connection>"]),
        (
            "inline AT clause",
            inline,
            vec!["inline", "AT <connection>"],
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
                "TABLE_NAME, WRITERS_PER_NODE, and SINGLE_COMMIT",
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
