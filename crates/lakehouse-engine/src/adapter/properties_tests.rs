use super::*;
use serde_json::json;

const ALL_NAMES: [&str; 20] = [
    "CATALOG_KIND",
    "CATALOG_CONNECTION",
    "NAMESPACE",
    "ALLOW_HTTP",
    "PARALLELISM_FACTOR",
    "DATAFUSION_THREADING_MODE",
    "DATAFUSION_TARGET_PARTITIONS",
    "DATAFUSION_THREADS_PER_UDF",
    "DATAFUSION_BATCH_SIZE",
    "MEMORY_POOL_FRACTION",
    "INSTANCE_OVERHEAD_MB",
    "JOIN_BROADCAST_MAX_BYTES",
    "S3_MAX_CONNECTIONS",
    "MERGE_SCHEMA",
    "HIVE_PARTITIONING",
    "BINARY_AS_STRING",
    "PERMISSION_CHECK",
    "USER_MAPPING",
    "TABLE_INCLUDE",
    "TABLE_EXCLUDE",
];

/// Name, minimum, and maximum of each whole-number tuning property.
const WHOLE_NUMBER_RANGES: [(&str, &str, &str); 7] = [
    ("PARALLELISM_FACTOR", "1", "255"),
    ("DATAFUSION_THREADS_PER_UDF", "1", "65535"),
    ("DATAFUSION_TARGET_PARTITIONS", "1", "65535"),
    ("S3_MAX_CONNECTIONS", "1", "65535"),
    ("INSTANCE_OVERHEAD_MB", "0", "65535"),
    ("DATAFUSION_BATCH_SIZE", "1", "4294967295"),
    ("JOIN_BROADCAST_MAX_BYTES", "1", "18446744073709551615"),
];

fn props(pairs: &[(&str, &str)]) -> Json {
    Json::Object(
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), json!(value)))
            .collect(),
    )
}

fn message(pairs: &[(&str, &str)]) -> String {
    match check_properties(&props(pairs)) {
        Err(UdfError::User(message)) => message,
        other => panic!("{pairs:?} must fail with a user error, got {other:?}"),
    }
}

fn above(maximum: &str) -> String {
    (maximum.parse::<u128>().expect("maximum") + 1).to_string()
}

fn invalid_entry(name: &str, echoed: &str, expected: &str) -> String {
    format!("invalid value '{echoed}' for '{name}': expected {expected}")
}

#[test]
fn every_known_name_passes() {
    let valid_values = [
        ("CATALOG_KIND", "GLUE"),
        ("CATALOG_CONNECTION", "CONN"),
        ("NAMESPACE", "sales"),
        ("ALLOW_HTTP", "true"),
        ("PARALLELISM_FACTOR", "2"),
        ("DATAFUSION_THREADING_MODE", "FIXED"),
        ("DATAFUSION_TARGET_PARTITIONS", "8"),
        ("DATAFUSION_THREADS_PER_UDF", "4"),
        ("DATAFUSION_BATCH_SIZE", "4096"),
        ("MEMORY_POOL_FRACTION", "0.5"),
        ("INSTANCE_OVERHEAD_MB", "100"),
        ("JOIN_BROADCAST_MAX_BYTES", "1024"),
        ("S3_MAX_CONNECTIONS", "16"),
        ("MERGE_SCHEMA", "not-checked-here"),
        ("HIVE_PARTITIONING", "not-checked-here"),
        ("BINARY_AS_STRING", "true"),
        ("PERMISSION_CHECK", "not-checked-here"),
        ("USER_MAPPING", "not-checked-here"),
        ("TABLE_INCLUDE", "ORDERS*, ?_LOG"),
        ("TABLE_EXCLUDE", "TMP_*"),
    ];
    let names: Vec<&str> = valid_values.iter().map(|(name, _)| *name).collect();
    assert_eq!(names, ALL_NAMES, "the fixture must cover the 20 names");
    assert_eq!(
        PROPERTY_NAMES.into_iter().collect::<BTreeSet<&str>>(),
        ALL_NAMES.into_iter().collect::<BTreeSet<&str>>(),
        "the adapter must accept exactly the 20 names of vs-properties/property-name-validation"
    );

    check_properties(&props(&valid_values)).expect("every known name with a valid value passes");
}

#[test]
fn a_name_differing_only_in_case_is_unknown() {
    let msg = message(&[("Namespace", "sales")]);

    assert_eq!(
        msg,
        "invalid virtual schema properties: unknown property 'Namespace' (did you mean 'NAMESPACE'?)"
    );
}

#[test]
fn unknown_names_get_the_nearest_known_name_within_two_edits() {
    for (typo, suggestion) in [
        ("NAMESPCE", "NAMESPACE"),
        ("PARALLELISM_FACTR", "PARALLELISM_FACTOR"),
        ("PERMISSION_CHEK", "PERMISSION_CHECK"),
        ("CATALOG_CONECTION", "CATALOG_CONNECTION"),
        ("NAMSPCE", "NAMESPACE"),
    ] {
        let msg = message(&[(typo, "x")]);

        assert!(
            msg.contains(&format!(
                "unknown property '{typo}' (did you mean '{suggestion}'?)"
            )),
            "{typo}: {msg}"
        );
    }
}

#[test]
fn an_unknown_name_three_edits_from_every_known_name_gets_no_suggestion() {
    let msg = message(&[("NAMSPCEX", "x"), ("MADE_UP_PROPERTY", "x")]);

    assert!(!msg.contains("did you mean"), "{msg}");
    assert!(msg.contains("unknown property 'MADE_UP_PROPERTY'"), "{msg}");
}

#[test]
fn a_tie_between_two_names_suggests_the_alphabetically_first() {
    assert_eq!(nearest_name("CBB", &["BBB", "ABB"]), Some("ABB"));
    assert_eq!(nearest_name("CBB", &["ABB", "BBB"]), Some("ABB"));
}

#[test]
fn a_closer_name_wins_over_an_alphabetically_earlier_one() {
    assert_eq!(nearest_name("BBBB", &["BBBA", "ABBX"]), Some("BBBA"));
}

#[test]
fn the_suggestion_compares_without_regard_to_case() {
    assert_eq!(nearest_name("namespace", &["NAMESPACE"]), Some("NAMESPACE"));
}

#[test]
fn whole_number_tuning_values_accept_the_minimum_and_the_maximum() {
    for (name, min, max) in WHOLE_NUMBER_RANGES {
        for value in [min, max] {
            check_properties(&props(&[(name, value)]))
                .unwrap_or_else(|e| panic!("{name} = {value} must pass: {e:?}"));
        }
    }
}

#[test]
fn whole_number_tuning_values_return_their_typed_value() {
    let tuning = check_properties(&props(&[
        ("PARALLELISM_FACTOR", "255"),
        ("DATAFUSION_THREADS_PER_UDF", "65535"),
        ("DATAFUSION_TARGET_PARTITIONS", "12"),
        ("S3_MAX_CONNECTIONS", "9"),
        ("INSTANCE_OVERHEAD_MB", "0"),
        ("DATAFUSION_BATCH_SIZE", "4294967295"),
        ("JOIN_BROADCAST_MAX_BYTES", "18446744073709551615"),
        ("DATAFUSION_THREADING_MODE", "fixed"),
        ("MEMORY_POOL_FRACTION", "1"),
    ]))
    .expect("valid values pass")
    .tuning;

    assert_eq!(tuning.parallelism_factor.map(|n| n.get()), Some(255));
    assert_eq!(tuning.df_threads_per_udf.map(|n| n.get()), Some(65535));
    assert_eq!(tuning.df_target_partitions.map(|n| n.get()), Some(12));
    assert_eq!(tuning.s3_max_connections.map(|n| n.get()), Some(9));
    assert_eq!(tuning.instance_overhead_mb, Some(0));
    assert_eq!(tuning.df_batch_size.map(|n| n.get()), Some(u32::MAX));
    assert_eq!(
        tuning.join_broadcast_max_bytes.map(|n| n.get()),
        Some(u64::MAX)
    );
    assert_eq!(tuning.threading_mode, Some(ThreadingMode::Fixed));
    assert_eq!(tuning.memory_pool_fraction, Some(1.0));
}

#[test]
fn absent_tuning_properties_return_none() {
    let checked = check_properties(&props(&[("NAMESPACE", "sales")])).expect("passes");

    assert_eq!(checked, CheckedProperties::default());
}

#[test]
fn an_empty_or_null_value_counts_as_absent() {
    let props = json!({
        "MADE_UP_PROPERTY": "",
        "NAMESPCE": null,
        "MEMORY_POOL_FRACTION": "",
        "DATAFUSION_BATCH_SIZE": null,
    });

    let checked = check_properties(&props).expect("an empty or null value is never checked");

    assert_eq!(checked, CheckedProperties::default());
}

#[test]
fn whole_number_tuning_values_outside_the_range_name_the_range() {
    for (name, min, max) in WHOLE_NUMBER_RANGES {
        let expected = format!("a whole number from {min} to {max}");
        let mut rejected = vec![
            above(max),
            "123456789012345678901".to_string(),
            "-1".to_string(),
            "abc".to_string(),
            "1.5".to_string(),
        ];
        if min == "1" {
            rejected.push("0".to_string());
        }
        for value in rejected {
            let msg = message(&[(name, &value)]);

            assert_eq!(
                msg,
                format!(
                    "invalid virtual schema properties: {}",
                    invalid_entry(name, &value, &expected)
                ),
                "{name} = {value}"
            );
        }
    }
}

#[test]
fn memory_pool_fraction_accepts_only_a_number_above_zero_and_at_most_one() {
    let expected = "a number greater than 0 and at most 1";
    for value in ["0.0001", "0.6", "1", "1.0"] {
        check_properties(&props(&[("MEMORY_POOL_FRACTION", value)]))
            .unwrap_or_else(|e| panic!("{value} must pass: {e:?}"));
    }
    for value in ["0", "1.5", "60", "-0.5", "NaN", "inf", "abc"] {
        let msg = message(&[("MEMORY_POOL_FRACTION", value)]);

        assert!(
            msg.contains(&invalid_entry("MEMORY_POOL_FRACTION", value, expected)),
            "{value}: {msg}"
        );
    }
}

#[test]
fn threading_mode_accepts_auto_or_fixed_in_any_case() {
    for value in ["AUTO", "FIXED", "auto", "fixed", "Fixed"] {
        check_properties(&props(&[("DATAFUSION_THREADING_MODE", value)]))
            .unwrap_or_else(|e| panic!("{value} must pass: {e:?}"));
    }
    for value in ["garbage", "FIXED ", "1"] {
        let msg = message(&[("DATAFUSION_THREADING_MODE", value)]);

        assert!(
            msg.contains(&invalid_entry(
                "DATAFUSION_THREADING_MODE",
                value,
                "AUTO or FIXED"
            )),
            "{value}: {msg}"
        );
    }
}

#[test]
fn boolean_properties_accept_true_or_false_in_any_case() {
    for name in ["ALLOW_HTTP", "BINARY_AS_STRING"] {
        for value in ["true", "FALSE", "True", "false"] {
            check_properties(&props(&[(name, value)]))
                .unwrap_or_else(|e| panic!("{name}={value} must pass: {e:?}"));
        }
        for value in ["yes", "1", "t"] {
            let msg = message(&[(name, value)]);

            assert!(
                msg.contains(&invalid_entry(name, value, "TRUE or FALSE")),
                "{name}={value}: {msg}"
            );
        }
    }
}

#[test]
fn resolve_allow_http_accepts_true_or_false_and_treats_a_rejected_value_as_false() {
    assert!(resolve_allow_http(&props(&[("ALLOW_HTTP", "True")])));
    assert!(!resolve_allow_http(&props(&[("ALLOW_HTTP", "false")])));
    assert!(!resolve_allow_http(&props(&[("ALLOW_HTTP", "yes")])));
    assert!(!resolve_allow_http(&props(&[])));
}

#[test]
fn a_value_of_100_characters_is_echoed_whole() {
    let value = "x".repeat(100);

    let msg = message(&[("DATAFUSION_BATCH_SIZE", &value)]);

    assert!(
        msg.contains(&format!("invalid value '{value}' for")),
        "{msg}"
    );
    assert!(!msg.contains('…'), "{msg}");
}

#[test]
fn a_value_of_101_characters_is_cut_to_100_and_an_ellipsis() {
    let value = "x".repeat(101);

    let msg = message(&[("DATAFUSION_BATCH_SIZE", &value)]);

    assert!(
        msg.contains(&format!("invalid value '{}…' for", "x".repeat(100))),
        "{msg}"
    );
}

#[test]
fn a_multi_byte_character_counts_as_one_character_of_the_echo() {
    let value = "é".repeat(101);

    let msg = message(&[("DATAFUSION_BATCH_SIZE", &value)]);

    assert!(
        msg.contains(&format!("invalid value '{}…' for", "é".repeat(100))),
        "{msg}"
    );
}

#[test]
fn a_quote_in_the_echo_is_doubled() {
    let msg = message(&[("DATAFUSION_BATCH_SIZE", "it's")]);

    assert!(msg.contains("invalid value 'it''s' for"), "{msg}");
}

#[test]
fn the_cut_counts_characters_before_a_quote_is_doubled() {
    let value = format!("'{}", "b".repeat(149));

    let msg = message(&[("DATAFUSION_BATCH_SIZE", &value)]);

    assert!(
        msg.contains(&format!("invalid value '''{}…' for", "b".repeat(99))),
        "{msg}"
    );
}

#[test]
fn the_issue_example_reports_every_problem_unknown_names_first() {
    let msg = message(&[
        ("CATALOG_CONNECTION", "conn"),
        ("NAMESPCE", "sales"),
        ("PARALLELISM_FACTR", "64"),
        ("DATAFUSION_BATCH_SIZE", "abc"),
        ("MADE_UP_PROPERTY", "x"),
    ]);

    assert_eq!(
        msg,
        "invalid virtual schema properties: unknown property 'MADE_UP_PROPERTY'; \
         unknown property 'NAMESPCE' (did you mean 'NAMESPACE'?); \
         unknown property 'PARALLELISM_FACTR' (did you mean 'PARALLELISM_FACTOR'?); \
         invalid value 'abc' for 'DATAFUSION_BATCH_SIZE': expected a whole number from 1 to 4294967295"
    );
}

#[test]
fn invalid_values_are_listed_in_alphabetical_order_of_the_name() {
    let msg = message(&[
        ("S3_MAX_CONNECTIONS", "AUTO"),
        ("DATAFUSION_BATCH_SIZE", "0"),
        ("MEMORY_POOL_FRACTION", "60"),
    ]);

    let positions: Vec<usize> = [
        "'DATAFUSION_BATCH_SIZE'",
        "'MEMORY_POOL_FRACTION'",
        "'S3_MAX_CONNECTIONS'",
    ]
    .iter()
    .map(|name| msg.find(name).unwrap_or_else(|| panic!("{name}: {msg}")))
    .collect();
    assert!(positions.windows(2).all(|w| w[0] < w[1]), "{msg}");
}

#[test]
fn an_unknown_property_value_never_appears_in_the_message() {
    let msg = message(&[("MADE_UP_PROPERTY", "secret-looking-text")]);

    assert!(!msg.contains("secret-looking-text"), "{msg}");
}

#[test]
fn a_tuning_value_is_checked_whatever_the_threading_mode() {
    let msg = message(&[("DATAFUSION_TARGET_PARTITIONS", "0")]);

    assert!(
        msg.contains(&invalid_entry(
            "DATAFUSION_TARGET_PARTITIONS",
            "0",
            "a whole number from 1 to 65535"
        )),
        "{msg}"
    );
}

#[test]
fn s3_max_connections_auto_and_zero_are_rejected() {
    for value in ["AUTO", "0"] {
        let msg = message(&[("S3_MAX_CONNECTIONS", value)]);

        assert!(
            msg.contains(&invalid_entry(
                "S3_MAX_CONNECTIONS",
                value,
                "a whole number from 1 to 65535"
            )),
            "{value}: {msg}"
        );
    }
}

#[test]
fn values_the_owning_modules_check_are_not_checked_here() {
    check_properties(&props(&[
        ("CATALOG_KIND", "NOPE"),
        ("PERMISSION_CHECK", "NOPE"),
        ("USER_MAPPING", "{{"),
        ("MERGE_SCHEMA", "maybe"),
        ("HIVE_PARTITIONING", "maybe"),
        ("NAMESPACE", ""),
    ]))
    .expect("the catalog checks only the names of these properties");
}
