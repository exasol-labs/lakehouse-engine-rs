use super::*;

fn has_disallowed_join_capability(cap_strs: &[&str]) -> bool {
    cap_strs.iter().any(|c| {
        c.contains("CARTESIAN")
            || *c == "JOIN_TYPE_LEFT_OUTER"
            || *c == "JOIN_TYPE_RIGHT_OUTER"
            || *c == "JOIN_TYPE_FULL_OUTER"
            || *c == "JOIN_CONDITION_ALL"
    })
}

/// Scenario: capability advertisement cannot consult the catalog kind.
#[test]
fn capabilities_are_assembled_without_the_catalog_kind() {
    let _capabilities: &[&str] = CAPABILITIES;
    let _response_builder: fn() -> Json = get_capabilities_response;
}

/// Scenario: adapter advertises GROUP BY column/expression/tuple, backed by multi-key detection.
#[test]
fn reports_group_by_capabilities() {
    let resp = get_capabilities_response();
    let caps = resp["capabilities"].as_array().unwrap();
    let cap_strs: Vec<&str> = caps.iter().map(|c| c.as_str().unwrap()).collect();

    assert!(
        cap_strs.contains(&"AGGREGATE_GROUP_BY_COLUMN"),
        "AGGREGATE_GROUP_BY_COLUMN must be advertised: {cap_strs:?}"
    );
    assert!(
        cap_strs.contains(&"AGGREGATE_GROUP_BY_EXPRESSION"),
        "AGGREGATE_GROUP_BY_EXPRESSION must be advertised: {cap_strs:?}"
    );
    assert!(
        cap_strs.contains(&"AGGREGATE_GROUP_BY_TUPLE"),
        "AGGREGATE_GROUP_BY_TUPLE must be advertised: {cap_strs:?}"
    );

    let multi_key_group_by = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [
            {"type": "column", "name": "REGION"},
            {"type": "column", "name": "YEAR"},
        ],
        "selectList": [
            {"type": "column", "name": "REGION"},
            {"type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false},
            {"type": "column", "name": "YEAR"},
        ],
    });
    let detection = crate::adapter::pushdown::detect_group_by_aggregates(&multi_key_group_by)
        .expect("multi-key GROUP BY must be detected by the backing pushdown path");
    assert_eq!(
        detection.group_keys.len(),
        2,
        "backing path must detect both tuple group keys: {:?}",
        detection.group_keys
    );
    assert_eq!(
        detection.plans.len(),
        1,
        "backing path must detect the aggregate over the tuple keys: {:?}",
        detection.plans
    );

    assert!(
        cap_strs.contains(&"FN_AGG_COUNT_DISTINCT"),
        "FN_AGG_COUNT_DISTINCT must be advertised: {cap_strs:?}"
    );

    assert!(
        !has_disallowed_join_capability(&cap_strs),
        "outer/all-condition/Cartesian join capabilities must not be advertised: {cap_strs:?}"
    );
}

/// Scenario: adapter reports the full audited capability set.
#[test]
fn reports_audited_capability_set() {
    let resp = get_capabilities_response();
    let caps = resp["capabilities"].as_array().unwrap();
    let cap_strs: Vec<&str> = caps.iter().map(|c| c.as_str().unwrap()).collect();

    for name in &[
        "FN_PRED_LIKE_ESCAPE",
        "FN_PRED_REGEXP_LIKE",
        "LITERAL_TIMESTAMP_UTC",
        "SELECTLIST_EXPRESSIONS",
        "AGGREGATE_HAVING",
        "AGGREGATE_GROUP_BY_TUPLE",
        "FN_AGG_COUNT_DISTINCT",
    ] {
        assert!(
            cap_strs.contains(name),
            "{name} must be advertised: {cap_strs:?}"
        );
    }

    for name in &["FN_CAST", "FN_NEG", "FN_WEEK"] {
        assert!(
            cap_strs.contains(name),
            "{name} must be advertised: {cap_strs:?}"
        );
    }

    for name in &[
        "FN_DAYS_BETWEEN",
        "FN_HOURS_BETWEEN",
        "FN_MINUTES_BETWEEN",
        "FN_SECONDS_BETWEEN",
    ] {
        assert!(
            cap_strs.contains(name),
            "{name} must be advertised: {cap_strs:?}"
        );
    }

    for name in &[
        "FN_DIV",
        "FN_TO_CHAR",
        "FN_TO_NUMBER",
        // Issue #106: PCRE vs regex-crate dialect gap.
        "FN_REGEXP_REPLACE",
        "FN_REGEXP_SUBSTR",
        "FN_REGEXP_INSTR",
        "FN_REGEXP_COUNT",
        "FN_ADD_HOURS",
        "FN_ADD_MINUTES",
        "FN_ADD_DAYS",
        "FN_ADD_SECONDS",
        "FN_ADD_WEEKS",
        "FN_ADD_MONTHS",
        "FN_ADD_YEARS",
        "FN_MONTHS_BETWEEN",
        "FN_YEARS_BETWEEN",
        "FN_DAYOFWEEK",
        "FN_LAST_DAY",
        "FN_CONVERT_TZ",
        "FN_CURRENT_DATE",
        "FN_CURRENT_TIMESTAMP",
        "FN_SYSDATE",
        "FN_SYSTIMESTAMP",
    ] {
        assert!(
            !cap_strs.contains(name),
            "{name} must NOT be advertised: {cap_strs:?}"
        );
    }

    // Issue #108: Exasol bit functions use the unsigned 64-bit domain; DataFusion's
    // bit operators are signed and its `>>` sign-extends.
    for name in &[
        "FN_BIT_AND",
        "FN_BIT_OR",
        "FN_BIT_XOR",
        "FN_BIT_LSHIFT",
        "FN_BIT_RSHIFT",
    ] {
        assert!(
            !cap_strs.contains(name),
            "{name} must NOT be advertised (issue #108, unsigned-domain divergence): {cap_strs:?}"
        );
    }
    for name in &[
        "FN_BIT_NOT",
        "FN_BIT_LROTATE",
        "FN_BIT_RROTATE",
        "FN_BIT_CHECK",
        "FN_BIT_SET",
        "FN_BIT_TO_NUM",
    ] {
        assert!(
            !cap_strs.contains(name),
            "{name} must NOT be advertised (issue #108, no DataFusion builtin): {cap_strs:?}"
        );
    }

    for name in &["FN_ADD", "FN_SUB", "FN_MULT", "FN_FLOAT_DIV"] {
        assert!(
            cap_strs.contains(name),
            "{name} must be advertised: {cap_strs:?}"
        );
    }

    for name in &[
        "FN_ABS",
        "FN_ACOS",
        "FN_ASIN",
        "FN_ATAN",
        "FN_ATAN2",
        "FN_CEIL",
        "FN_COS",
        "FN_COSH",
        "FN_COT",
        "FN_DEGREES",
        "FN_EXP",
        "FN_FLOOR",
        "FN_LN",
        "FN_LOG",
        "FN_MOD",
        "FN_POWER",
        "FN_RADIANS",
        "FN_ROUND",
        "FN_SIGN",
        "FN_SIN",
        "FN_SINH",
        "FN_SQRT",
        "FN_TAN",
        "FN_TANH",
        "FN_TRUNC",
    ] {
        assert!(
            cap_strs.contains(name),
            "{name} must be advertised: {cap_strs:?}"
        );
    }

    for name in &[
        "FN_ASCII",
        "FN_CHR",
        "FN_CONCAT",
        "FN_INITCAP",
        "FN_INSTR",
        "FN_LEFT",
        "FN_LENGTH",
        "FN_LOCATE",
        "FN_LOWER",
        "FN_LPAD",
        "FN_LTRIM",
        "FN_OCTET_LENGTH",
        "FN_REPEAT",
        "FN_REPLACE",
        "FN_REVERSE",
        "FN_RIGHT",
        "FN_RPAD",
        "FN_RTRIM",
        "FN_SUBSTR",
        "FN_TRANSLATE",
        "FN_TRIM",
        "FN_UNICODE",
        "FN_UNICODECHR",
        "FN_UPPER",
    ] {
        assert!(
            cap_strs.contains(name),
            "{name} must be advertised: {cap_strs:?}"
        );
    }

    for name in &[
        "FN_DATE_TRUNC",
        "FN_DAY",
        "FN_EXTRACT",
        "FN_HOUR",
        "FN_MINUTE",
        "FN_MONTH",
        "FN_SECOND",
        "FN_TO_DATE",
        "FN_TO_TIMESTAMP",
        "FN_YEAR",
    ] {
        assert!(
            cap_strs.contains(name),
            "{name} must be advertised: {cap_strs:?}"
        );
    }

    for name in &[
        "FN_CASE",
        "FN_GREATEST",
        "FN_LEAST",
        "FN_NULLIFZERO",
        "FN_ZEROIFNULL",
    ] {
        assert!(
            cap_strs.contains(name),
            "{name} must be advertised: {cap_strs:?}"
        );
    }

    for name in &[
        "FN_AGG_STDDEV",
        "FN_AGG_STDDEV_POP",
        "FN_AGG_STDDEV_SAMP",
        "FN_AGG_VARIANCE",
        "FN_AGG_VAR_POP",
        "FN_AGG_VAR_SAMP",
    ] {
        assert!(
            cap_strs.contains(name),
            "{name} must be advertised: {cap_strs:?}"
        );
    }

    for name in &["FN_PRED_GREATER", "FN_PRED_GREATEREQUAL"] {
        assert!(
            !cap_strs.contains(name),
            "{name} must NOT be advertised: {cap_strs:?}"
        );
    }

    for name in &["FN_AGG_MEDIAN", "FN_AGG_APPROXIMATE_COUNT_DISTINCT"] {
        assert!(
            !cap_strs.contains(name),
            "{name} must NOT be advertised: {cap_strs:?}"
        );
    }
    let has_listagg = cap_strs
        .iter()
        .any(|c| c.contains("LISTAGG") || c.contains("GROUP_CONCAT"));
    assert!(
        !has_listagg,
        "LISTAGG/GROUP_CONCAT must not be advertised: {cap_strs:?}"
    );
    assert!(
        cap_strs.contains(&"ORDER_BY_COLUMN"),
        "ORDER_BY_COLUMN must be advertised: {cap_strs:?}"
    );
    assert!(
        cap_strs.contains(&"LIMIT_WITH_OFFSET"),
        "LIMIT_WITH_OFFSET must be advertised: {cap_strs:?}"
    );
    assert!(
        cap_strs.contains(&"JOIN")
            && cap_strs.contains(&"JOIN_TYPE_INNER")
            && cap_strs.contains(&"JOIN_CONDITION_EQUI"),
        "inner equi-join capabilities must be advertised: {cap_strs:?}"
    );
    assert!(
        !has_disallowed_join_capability(&cap_strs),
        "outer/all-condition/Cartesian join capabilities must not be advertised: {cap_strs:?}"
    );

    if cap_strs.contains(&"AGGREGATE_GROUP_BY_TUPLE") {
        assert!(
            cap_strs.contains(&"AGGREGATE_GROUP_BY_COLUMN")
                && cap_strs.contains(&"AGGREGATE_GROUP_BY_EXPRESSION"),
            "TUPLE group-by must not be advertised without its single-key backing capabilities: {cap_strs:?}"
        );
    }
}

/// Scenario: adapter advertises both bare-column and expression ORDER BY sort keys.
#[test]
fn advertises_order_by_column_and_expression() {
    let resp = get_capabilities_response();
    let caps = resp["capabilities"].as_array().unwrap();
    let cap_strs: Vec<&str> = caps.iter().map(|c| c.as_str().unwrap()).collect();

    assert!(
        cap_strs.contains(&"ORDER_BY_COLUMN"),
        "ORDER_BY_COLUMN must be advertised: {cap_strs:?}"
    );
    assert!(
        cap_strs.contains(&"ORDER_BY_EXPRESSION"),
        "ORDER_BY_EXPRESSION must be advertised: {cap_strs:?}"
    );
}

#[test]
fn reports_projection_filter_and_limit_capabilities() {
    let resp = get_capabilities_response();
    let caps = resp["capabilities"].as_array().unwrap();
    let cap_strs: Vec<&str> = caps.iter().map(|c| c.as_str().unwrap()).collect();

    assert!(cap_strs.contains(&"SELECTLIST_PROJECTION"));
    assert!(cap_strs.contains(&"FILTER_EXPRESSIONS"));
    assert!(cap_strs.contains(&"LIMIT"));

    assert_eq!(resp["type"].as_str().unwrap(), "getCapabilities");
}

/// Scenario: adapter advertises aggregate pushdown for supported functions.
#[test]
fn reports_supported_aggregate_capabilities() {
    let resp = get_capabilities_response();
    let caps = resp["capabilities"].as_array().unwrap();
    let cap_strs: Vec<&str> = caps.iter().map(|c| c.as_str().unwrap()).collect();

    for name in &["FN_ADD", "FN_SUB", "FN_MULT", "FN_FLOAT_DIV"] {
        assert!(
            cap_strs.contains(name),
            "{name} must be advertised: {cap_strs:?}"
        );
    }

    for name in &[
        "AGGREGATE_SINGLE_GROUP",
        "FN_AGG_COUNT",
        "FN_AGG_COUNT_STAR",
        "FN_AGG_SUM",
        "FN_AGG_MIN",
        "FN_AGG_MAX",
        "FN_AGG_AVG",
        "FN_AGG_COUNT_DISTINCT",
    ] {
        assert!(
            cap_strs.contains(name),
            "{name} must be advertised: {cap_strs:?}"
        );
    }

    for name in &[
        "AGGREGATE_GROUP_BY_COLUMN",
        "AGGREGATE_GROUP_BY_EXPRESSION",
        "AGGREGATE_GROUP_BY_TUPLE",
        "AGGREGATE_HAVING",
    ] {
        assert!(
            cap_strs.contains(name),
            "{name} must be advertised: {cap_strs:?}"
        );
    }

    for name in &[
        "FN_AGG_STDDEV",
        "FN_AGG_STDDEV_POP",
        "FN_AGG_STDDEV_SAMP",
        "FN_AGG_VARIANCE",
        "FN_AGG_VAR_POP",
        "FN_AGG_VAR_SAMP",
    ] {
        assert!(
            cap_strs.contains(name),
            "{name} must be advertised: {cap_strs:?}"
        );
    }

    assert!(
        cap_strs.contains(&"AGGREGATE_GROUP_BY_COLUMN")
            && cap_strs.contains(&"AGGREGATE_GROUP_BY_EXPRESSION"),
        "TUPLE group-by must be backed by single-key GROUP BY capabilities: {cap_strs:?}"
    );

    assert!(
        !has_disallowed_join_capability(&cap_strs),
        "outer/all-condition/Cartesian join capabilities must not be advertised: {cap_strs:?}"
    );

    assert!(cap_strs.contains(&"SELECTLIST_PROJECTION"));
    assert!(cap_strs.contains(&"FILTER_EXPRESSIONS"));
    assert!(cap_strs.contains(&"LIMIT"));
}

/// Scenario: adapter advertises `FN_AGG_COUNT_DISTINCT` for single-group COUNT(DISTINCT).
#[test]
fn capabilities_advertise_count_distinct() {
    let resp = get_capabilities_response();
    let caps = resp["capabilities"].as_array().unwrap();
    let cap_strs: Vec<&str> = caps.iter().map(|c| c.as_str().unwrap()).collect();

    assert!(
        cap_strs.contains(&"FN_AGG_COUNT_DISTINCT"),
        "FN_AGG_COUNT_DISTINCT must be advertised: {cap_strs:?}"
    );
    assert!(
        cap_strs.contains(&"AGGREGATE_SINGLE_GROUP"),
        "single-group COUNT(DISTINCT) requires AGGREGATE_SINGLE_GROUP: {cap_strs:?}"
    );
}

/// Scenario: advertising `FN_CAST`, `FN_NEG`, and `FN_WEEK` adds no join capability.
#[test]
fn cast_neg_week_introduce_no_join_capability() {
    let resp = get_capabilities_response();
    let caps = resp["capabilities"].as_array().unwrap();
    let cap_strs: Vec<&str> = caps.iter().map(|c| c.as_str().unwrap()).collect();

    let join_caps: Vec<&&str> = cap_strs.iter().filter(|c| c.starts_with("JOIN")).collect();
    assert_eq!(
        join_caps,
        vec![&"JOIN", &"JOIN_TYPE_INNER", &"JOIN_CONDITION_EQUI"],
        "join capability set must remain exactly the inner equi-join contract: {cap_strs:?}"
    );
    assert!(
        !has_disallowed_join_capability(&cap_strs),
        "outer/all-condition/Cartesian join capabilities must not be advertised: {cap_strs:?}"
    );
}

/// Scenario: adapter advertises inner equi-join capabilities and no other join shape.
#[test]
fn advertises_inner_equi_join_capabilities() {
    let resp = get_capabilities_response();
    let caps = resp["capabilities"].as_array().unwrap();
    let cap_strs: Vec<&str> = caps.iter().map(|c| c.as_str().unwrap()).collect();

    for name in &["JOIN", "JOIN_TYPE_INNER", "JOIN_CONDITION_EQUI"] {
        assert!(
            cap_strs.contains(name),
            "{name} must be advertised: {cap_strs:?}"
        );
    }

    for name in &[
        "JOIN_TYPE_LEFT_OUTER",
        "JOIN_TYPE_RIGHT_OUTER",
        "JOIN_TYPE_FULL_OUTER",
        "JOIN_CONDITION_ALL",
    ] {
        assert!(
            !cap_strs.contains(name),
            "{name} must NOT be advertised: {cap_strs:?}"
        );
    }

    let has_cartesian = cap_strs.iter().any(|c| c.contains("CARTESIAN"));
    assert!(
        !has_cartesian,
        "Cartesian-product capabilities must not be advertised: {cap_strs:?}"
    );
}

/// Scenario: the capability set includes inner equi-join alongside existing pushdowns.
#[test]
fn reports_capabilities_includes_inner_join() {
    let resp = get_capabilities_response();
    let caps = resp["capabilities"].as_array().unwrap();
    let cap_strs: Vec<&str> = caps.iter().map(|c| c.as_str().unwrap()).collect();

    assert!(
        cap_strs.contains(&"JOIN")
            && cap_strs.contains(&"JOIN_TYPE_INNER")
            && cap_strs.contains(&"JOIN_CONDITION_EQUI"),
        "inner equi-join capabilities must be advertised: {cap_strs:?}"
    );

    assert!(cap_strs.contains(&"SELECTLIST_PROJECTION"));
    assert!(cap_strs.contains(&"FILTER_EXPRESSIONS"));
    assert!(cap_strs.contains(&"LIMIT"));
    assert!(cap_strs.contains(&"AGGREGATE_SINGLE_GROUP"));
    assert!(cap_strs.contains(&"AGGREGATE_GROUP_BY_COLUMN"));
}
