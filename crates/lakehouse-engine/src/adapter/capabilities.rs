/// Virtual Schema capabilities for the Lakehouse VS adapter.
///
/// Reports projection, filter predicates, LIMIT, and single-group aggregate pushdown.
use serde_json::{Value as Json, json};

/// The set of capabilities this VS adapter advertises to Exasol.
///
/// See Exasol VS adapter documentation for the full capability name list.
pub const CAPABILITIES: &[&str] = &[
    // Column projection
    "SELECTLIST_PROJECTION",
    // Filter pushdown: literal types
    "FILTER_EXPRESSIONS",
    "LITERAL_BOOL",
    "LITERAL_DATE",
    "LITERAL_DOUBLE",
    "LITERAL_EXACTNUMERIC",
    "LITERAL_NULL",
    "LITERAL_STRING",
    "LITERAL_TIMESTAMP",
    // Filter pushdown: logical operators
    "FN_PRED_AND",
    "FN_PRED_OR",
    "FN_PRED_NOT",
    // Filter pushdown: comparison operators
    "FN_PRED_EQUAL",
    "FN_PRED_NOTEQUAL",
    "FN_PRED_LESS",
    "FN_PRED_LESSEQUAL",
    "FN_PRED_GREATER",
    "FN_PRED_GREATEREQUAL",
    "FN_PRED_BETWEEN",
    "FN_PRED_IN_CONSTLIST",
    "FN_PRED_IS_NULL",
    "FN_PRED_IS_NOT_NULL",
    "FN_PRED_LIKE",
    // LIMIT pushdown
    "LIMIT",
    // Single-group aggregate pushdown (no GROUP BY, no HAVING, no DISTINCT)
    "AGGREGATE_SINGLE_GROUP",
    "FN_AGG_COUNT",
    "FN_AGG_COUNT_STAR",
    "FN_AGG_SUM",
    "FN_AGG_MIN",
    "FN_AGG_MAX",
    "FN_AGG_AVG",
    // GROUP BY aggregate pushdown: column references and scalar expressions.
    // HAVING, COUNT(DISTINCT), and join pushdown are NOT advertised.
    "AGGREGATE_GROUP_BY_COLUMN",
    "AGGREGATE_GROUP_BY_EXPRESSION",
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

    /// Task 2.6: Adapter advertises GROUP BY column and expression capabilities.
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
            !cap_strs.contains(&"AGGREGATE_HAVING"),
            "AGGREGATE_HAVING must not be advertised"
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
    /// The 6 supported single-group aggregate function capabilities plus
    /// AGGREGATE_SINGLE_GROUP must be present.  GROUP BY / HAVING / COUNT_DISTINCT /
    /// join capabilities must be absent.
    #[test]
    fn reports_supported_aggregate_capabilities() {
        let resp = get_capabilities_response();
        let caps = resp["capabilities"].as_array().unwrap();
        let cap_strs: Vec<&str> = caps.iter().map(|c| c.as_str().unwrap()).collect();

        // Supported single-group aggregate capabilities must be advertised.
        assert!(
            cap_strs.contains(&"AGGREGATE_SINGLE_GROUP"),
            "AGGREGATE_SINGLE_GROUP must be advertised: {cap_strs:?}"
        );
        assert!(
            cap_strs.contains(&"FN_AGG_COUNT"),
            "FN_AGG_COUNT must be advertised: {cap_strs:?}"
        );
        assert!(
            cap_strs.contains(&"FN_AGG_COUNT_STAR"),
            "FN_AGG_COUNT_STAR must be advertised: {cap_strs:?}"
        );
        assert!(
            cap_strs.contains(&"FN_AGG_SUM"),
            "FN_AGG_SUM must be advertised: {cap_strs:?}"
        );
        assert!(
            cap_strs.contains(&"FN_AGG_MIN"),
            "FN_AGG_MIN must be advertised: {cap_strs:?}"
        );
        assert!(
            cap_strs.contains(&"FN_AGG_MAX"),
            "FN_AGG_MAX must be advertised: {cap_strs:?}"
        );
        assert!(
            cap_strs.contains(&"FN_AGG_AVG"),
            "FN_AGG_AVG must be advertised: {cap_strs:?}"
        );

        // Unsupported capabilities must NOT be advertised.
        assert!(
            !cap_strs.contains(&"AGGREGATE_GROUP_BY"),
            "AGGREGATE_GROUP_BY must not be advertised"
        );
        assert!(
            !cap_strs.contains(&"AGGREGATE_HAVING"),
            "AGGREGATE_HAVING must not be advertised"
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
