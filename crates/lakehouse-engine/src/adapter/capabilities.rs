/// Virtual Schema capabilities for the Lakehouse VS adapter.
///
/// Reports projection, filter predicates, and LIMIT only.
/// Does NOT include aggregation or join pushdown (deferred to later plans).
use serde_json::{Value as Json, json};

/// The set of capabilities this VS adapter advertises to Exasol.
///
/// Mirror of strata-rs's CAPABILITIES, minus any aggregate/join entries.
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

    #[test]
    fn reports_projection_filter_limit_only() {
        let resp = get_capabilities_response();
        let caps = resp["capabilities"].as_array().unwrap();
        let cap_strs: Vec<&str> = caps.iter().map(|c| c.as_str().unwrap()).collect();

        // Must include projection, filter, and LIMIT.
        assert!(cap_strs.contains(&"SELECTLIST_PROJECTION"));
        assert!(cap_strs.contains(&"FILTER_EXPRESSIONS"));
        assert!(cap_strs.contains(&"LIMIT"));

        // Must NOT include aggregation or join pushdown.
        let has_agg = cap_strs
            .iter()
            .any(|c| c.starts_with("FN_AGG_") || *c == "AGGREGATE_HAVING");
        assert!(!has_agg, "aggregation capabilities must not be advertised");

        let has_join = cap_strs
            .iter()
            .any(|c| c.contains("JOIN") || c.contains("CARTESIAN"));
        assert!(!has_join, "join capabilities must not be advertised");

        assert_eq!(resp["type"].as_str().unwrap(), "getCapabilities");
    }
}
