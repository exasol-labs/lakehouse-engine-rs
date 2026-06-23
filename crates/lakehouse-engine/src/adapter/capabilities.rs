/// Virtual Schema capabilities for the Lakehouse VS adapter.
///
/// Reports projection, filter predicates, LIMIT, and single-group aggregate pushdown.
use serde_json::{Value as Json, json};

/// The set of capabilities this VS adapter advertises to Exasol.
///
/// See Exasol VS adapter documentation for the full capability name list.
pub const CAPABILITIES: &[&str] = &[
    // Column projection and scalar select-list expressions
    "SELECTLIST_PROJECTION",
    "SELECTLIST_EXPRESSIONS",
    // Filter pushdown: literal types
    "FILTER_EXPRESSIONS",
    "LITERAL_BOOL",
    "LITERAL_DATE",
    "LITERAL_DOUBLE",
    "LITERAL_EXACTNUMERIC",
    "LITERAL_NULL",
    "LITERAL_STRING",
    "LITERAL_TIMESTAMP",
    "LITERAL_TIMESTAMP_UTC",
    // Filter pushdown: logical operators
    "FN_PRED_AND",
    "FN_PRED_OR",
    "FN_PRED_NOT",
    // Filter pushdown: comparison operators
    // NOTE: FN_PRED_GREATER and FN_PRED_GREATEREQUAL are NOT Exasol capability names;
    // Exasol normalises a > b to b < a before reaching the adapter.
    "FN_PRED_EQUAL",
    "FN_PRED_NOTEQUAL",
    "FN_PRED_LESS",
    "FN_PRED_LESSEQUAL",
    "FN_PRED_BETWEEN",
    "FN_PRED_IN_CONSTLIST",
    "FN_PRED_IS_NULL",
    "FN_PRED_IS_NOT_NULL",
    "FN_PRED_LIKE",
    "FN_PRED_LIKE_ESCAPE",
    "FN_PRED_REGEXP_LIKE",
    // LIMIT pushdown
    "LIMIT",
    // Math scalar functions
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
    // String scalar functions
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
    // Date/time scalar functions
    "FN_CURRENT_DATE",
    "FN_CURRENT_TIMESTAMP",
    "FN_DATE_TRUNC",
    "FN_DAY",
    "FN_EXTRACT",
    "FN_HOUR",
    "FN_MINUTE",
    "FN_MONTH",
    "FN_SECOND",
    "FN_SYSDATE",
    "FN_SYSTIMESTAMP",
    "FN_TO_DATE",
    "FN_TO_TIMESTAMP",
    "FN_YEAR",
    // Conditional scalar functions
    "FN_CASE",
    "FN_GREATEST",
    "FN_LEAST",
    "FN_NULLIFZERO",
    "FN_ZEROIFNULL",
    // Single-group aggregate pushdown
    "AGGREGATE_SINGLE_GROUP",
    "FN_AGG_COUNT",
    "FN_AGG_COUNT_STAR",
    "FN_AGG_SUM",
    "FN_AGG_MIN",
    "FN_AGG_MAX",
    "FN_AGG_AVG",
    // Statistical aggregates (decomposed via sufficient statistics)
    "FN_AGG_STDDEV",
    "FN_AGG_STDDEV_POP",
    "FN_AGG_STDDEV_SAMP",
    "FN_AGG_VARIANCE",
    "FN_AGG_VAR_POP",
    "FN_AGG_VAR_SAMP",
    // GROUP BY aggregate pushdown: column references and scalar expressions.
    // HAVING is advertised; COUNT(DISTINCT) and join pushdown are NOT.
    "AGGREGATE_GROUP_BY_COLUMN",
    "AGGREGATE_GROUP_BY_EXPRESSION",
    "AGGREGATE_HAVING",
];

/// Build the `getCapabilities` JSON response.
pub fn get_capabilities_response() -> Json {
    json!({
        "type": "getCapabilities",
        "capabilities": CAPABILITIES,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Adapter advertises GROUP BY column and expression capabilities.
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

        // Excluded capabilities must NOT appear.
        assert!(
            !cap_strs.contains(&"AGGREGATE_GROUP_BY_TUPLE"),
            "AGGREGATE_GROUP_BY_TUPLE must not be advertised (not supported)"
        );
        assert!(
            !cap_strs.contains(&"FN_AGG_COUNT_DISTINCT"),
            "FN_AGG_COUNT_DISTINCT must not be advertised"
        );
        let has_join = cap_strs
            .iter()
            .any(|c| c.contains("JOIN") || c.contains("CARTESIAN"));
        assert!(!has_join, "join capabilities must not be advertised");
    }

    /// Adapter reports the full audited capability set (tasks 1.1-1.4).
    ///
    /// Asserts: new names present; removed/excluded names absent.
    #[test]
    fn reports_audited_capability_set() {
        let resp = get_capabilities_response();
        let caps = resp["capabilities"].as_array().unwrap();
        let cap_strs: Vec<&str> = caps.iter().map(|c| c.as_str().unwrap()).collect();

        // --- task 1.2: additions ---
        for name in &[
            "FN_PRED_LIKE_ESCAPE",
            "FN_PRED_REGEXP_LIKE",
            "LITERAL_TIMESTAMP_UTC",
            "SELECTLIST_EXPRESSIONS",
            "AGGREGATE_HAVING",
        ] {
            assert!(
                cap_strs.contains(name),
                "{name} must be advertised: {cap_strs:?}"
            );
        }

        // --- task 1.3: math scalar functions ---
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

        // --- task 1.3: string scalar functions ---
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

        // --- task 1.3: date/time scalar functions ---
        for name in &[
            "FN_CURRENT_DATE",
            "FN_CURRENT_TIMESTAMP",
            "FN_DATE_TRUNC",
            "FN_DAY",
            "FN_EXTRACT",
            "FN_HOUR",
            "FN_MINUTE",
            "FN_MONTH",
            "FN_SECOND",
            "FN_SYSDATE",
            "FN_SYSTIMESTAMP",
            "FN_TO_DATE",
            "FN_TO_TIMESTAMP",
            "FN_YEAR",
        ] {
            assert!(
                cap_strs.contains(name),
                "{name} must be advertised: {cap_strs:?}"
            );
        }

        // --- task 1.3: conditional scalar functions ---
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

        // --- task 1.3: statistical aggregates ---
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

        // --- task 1.1 + 1.4: removed/excluded names MUST NOT appear ---
        for name in &["FN_PRED_GREATER", "FN_PRED_GREATEREQUAL"] {
            assert!(
                !cap_strs.contains(name),
                "{name} must NOT be advertised: {cap_strs:?}"
            );
        }

        // Non-decomposable / non-supported aggregates must not appear.
        for name in &[
            "FN_AGG_MEDIAN",
            "FN_AGG_APPROXIMATE_COUNT_DISTINCT",
            "FN_AGG_COUNT_DISTINCT",
        ] {
            assert!(
                !cap_strs.contains(name),
                "{name} must NOT be advertised: {cap_strs:?}"
            );
        }
        let has_distinct_agg = cap_strs.iter().any(|c| c.ends_with("_DISTINCT"));
        assert!(
            !has_distinct_agg,
            "*_DISTINCT aggregates must not be advertised: {cap_strs:?}"
        );
        let has_listagg = cap_strs
            .iter()
            .any(|c| c.contains("LISTAGG") || c.contains("GROUP_CONCAT"));
        assert!(
            !has_listagg,
            "LISTAGG/GROUP_CONCAT must not be advertised: {cap_strs:?}"
        );
        let has_order_by = cap_strs.iter().any(|c| c.starts_with("ORDER_BY"));
        assert!(
            !has_order_by,
            "ORDER_BY* must not be advertised: {cap_strs:?}"
        );
        let has_join = cap_strs
            .iter()
            .any(|c| c.contains("JOIN") || c.contains("CARTESIAN"));
        assert!(
            !has_join,
            "join capabilities must not be advertised: {cap_strs:?}"
        );
        assert!(
            !cap_strs.contains(&"AGGREGATE_GROUP_BY_TUPLE"),
            "AGGREGATE_GROUP_BY_TUPLE must not be advertised: {cap_strs:?}"
        );
    }

    /// Scenario: Adapter advertises projection, filter, and LIMIT capabilities.
    #[test]
    fn reports_projection_filter_and_limit_capabilities() {
        let resp = get_capabilities_response();
        let caps = resp["capabilities"].as_array().unwrap();
        let cap_strs: Vec<&str> = caps.iter().map(|c| c.as_str().unwrap()).collect();

        // Must include projection, filter, and LIMIT.
        assert!(cap_strs.contains(&"SELECTLIST_PROJECTION"));
        assert!(cap_strs.contains(&"FILTER_EXPRESSIONS"));
        assert!(cap_strs.contains(&"LIMIT"));

        assert_eq!(resp["type"].as_str().unwrap(), "getCapabilities");
    }

    /// Scenario: Adapter advertises aggregate pushdown for supported functions.
    ///
    /// Single-group aggregates, GROUP BY, HAVING, and statistical aggregates must be
    /// present. COUNT_DISTINCT, MEDIAN, APPROX_COUNT_DISTINCT, join, and
    /// GROUP_BY_TUPLE must be absent.
    #[test]
    fn reports_supported_aggregate_capabilities() {
        let resp = get_capabilities_response();
        let caps = resp["capabilities"].as_array().unwrap();
        let cap_strs: Vec<&str> = caps.iter().map(|c| c.as_str().unwrap()).collect();

        // Supported single-group aggregate capabilities must be advertised.
        for name in &[
            "AGGREGATE_SINGLE_GROUP",
            "FN_AGG_COUNT",
            "FN_AGG_COUNT_STAR",
            "FN_AGG_SUM",
            "FN_AGG_MIN",
            "FN_AGG_MAX",
            "FN_AGG_AVG",
        ] {
            assert!(
                cap_strs.contains(name),
                "{name} must be advertised: {cap_strs:?}"
            );
        }

        // GROUP BY and HAVING must be advertised.
        for name in &[
            "AGGREGATE_GROUP_BY_COLUMN",
            "AGGREGATE_GROUP_BY_EXPRESSION",
            "AGGREGATE_HAVING",
        ] {
            assert!(
                cap_strs.contains(name),
                "{name} must be advertised: {cap_strs:?}"
            );
        }

        // Statistical aggregates must be advertised.
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

        // Unsupported capabilities must NOT be advertised.
        assert!(
            !cap_strs.contains(&"AGGREGATE_GROUP_BY_TUPLE"),
            "AGGREGATE_GROUP_BY_TUPLE must not be advertised"
        );
        assert!(
            !cap_strs.contains(&"FN_AGG_COUNT_DISTINCT"),
            "FN_AGG_COUNT_DISTINCT must not be advertised"
        );
        let has_join = cap_strs
            .iter()
            .any(|c| c.contains("JOIN") || c.contains("CARTESIAN"));
        assert!(!has_join, "join capabilities must not be advertised");

        // Projection, filter, and LIMIT must still be present.
        assert!(cap_strs.contains(&"SELECTLIST_PROJECTION"));
        assert!(cap_strs.contains(&"FILTER_EXPRESSIONS"));
        assert!(cap_strs.contains(&"LIMIT"));
    }
}
