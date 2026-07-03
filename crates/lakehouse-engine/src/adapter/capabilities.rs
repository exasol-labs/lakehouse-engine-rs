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
    // GROUP BY aggregate pushdown: column references, scalar expressions, and
    // multi-column (tuple) group keys. HAVING is advertised; COUNT(DISTINCT)
    // and join pushdown are NOT.
    "AGGREGATE_GROUP_BY_COLUMN",
    "AGGREGATE_GROUP_BY_EXPRESSION",
    "AGGREGATE_GROUP_BY_TUPLE",
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

    /// Adapter advertises GROUP BY column, expression, and multi-column (tuple)
    /// capabilities — and `AGGREGATE_GROUP_BY_TUPLE` is advertised ONLY because the
    /// N-key detection path that serves it works (issue #53). This test therefore
    /// both asserts the flag's presence and exercises the backing multi-key path
    /// (`pushdown::detect_group_by_aggregates` on a two-key GROUP BY), so it fails if
    /// the flag is dropped OR if the multi-key path regresses to a single key or no
    /// detection. The full behavioral coverage this flag is contingent on lives in
    /// the `pushdown.rs` detection tests
    /// (`detect_group_by_aggregates_interleaved_multi_key_preserves_order`,
    /// `grouped_wrapper_interleaved_multi_key_ordering`, and the Group B multi-key
    /// tests).
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

        // The TUPLE capability is advertised ONLY because the multi-key detection
        // path exists and works. Exercise that path directly: a two-key GROUP BY
        // (SELECT k1, COUNT(*), k2 ... GROUP BY k1, k2) must be detected with both
        // group keys and the aggregate. If this regresses, the capability is no
        // longer backed and must not be advertised.
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

        // COUNT(DISTINCT) and join pushdown remain genuinely unsupported.
        assert!(
            !cap_strs.contains(&"FN_AGG_COUNT_DISTINCT"),
            "FN_AGG_COUNT_DISTINCT must not be advertised"
        );
        let has_join = cap_strs
            .iter()
            .any(|c| c.contains("JOIN") || c.contains("CARTESIAN"));
        assert!(!has_join, "join capabilities must not be advertised");
    }

    /// Adapter reports the full audited capability set.
    ///
    /// Asserts new names present (including `AGGREGATE_GROUP_BY_TUPLE`, added for
    /// issue #53 and backed by the N-key GROUP BY path) and removed/excluded names
    /// absent. TUPLE is checked for coherence — it must never be advertised without
    /// the single-key GROUP BY capabilities the same detection path serves.
    #[test]
    fn reports_audited_capability_set() {
        let resp = get_capabilities_response();
        let caps = resp["capabilities"].as_array().unwrap();
        let cap_strs: Vec<&str> = caps.iter().map(|c| c.as_str().unwrap()).collect();

        // --- additions (incl. AGGREGATE_GROUP_BY_TUPLE, issue #53) ---
        for name in &[
            "FN_PRED_LIKE_ESCAPE",
            "FN_PRED_REGEXP_LIKE",
            "LITERAL_TIMESTAMP_UTC",
            "SELECTLIST_EXPRESSIONS",
            "AGGREGATE_HAVING",
            "AGGREGATE_GROUP_BY_TUPLE",
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

        // AGGREGATE_GROUP_BY_TUPLE is now advertised (issue #53), backed by the
        // N-key detection/SQL path. Multi-key grouping is only coherent alongside
        // the single-key GROUP BY capabilities the same path serves, so it must
        // never appear without them.
        if cap_strs.contains(&"AGGREGATE_GROUP_BY_TUPLE") {
            assert!(
                cap_strs.contains(&"AGGREGATE_GROUP_BY_COLUMN")
                    && cap_strs.contains(&"AGGREGATE_GROUP_BY_EXPRESSION"),
                "TUPLE group-by must not be advertised without its single-key backing capabilities: {cap_strs:?}"
            );
        }
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
    /// Single-group aggregates, GROUP BY (column, expression, and multi-column
    /// tuple), HAVING, and statistical aggregates must be present. COUNT_DISTINCT,
    /// MEDIAN, APPROX_COUNT_DISTINCT, and join must be absent.
    ///
    /// `AGGREGATE_GROUP_BY_TUPLE` is advertised as of issue #53 — reversing the
    /// 2026-06-22 decision that excluded it — because the N-key detection, per-key
    /// type resolution, and grouped-scan SQL builder handle multi-key tuples. The
    /// behavioral guarantee lives in the `pushdown.rs` detection tests
    /// (`detect_group_by_aggregates_interleaved_multi_key_preserves_order` and the
    /// Group B multi-key tests); this test guards the advertisement plus the
    /// coherence invariant that TUPLE is never advertised without its single-key
    /// GROUP BY backing capabilities.
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

        // GROUP BY (column, expression, and multi-column tuple) and HAVING must be
        // advertised.
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

        // Multi-column tuple GROUP BY is only coherent alongside the single-key
        // GROUP BY capabilities the same detection path serves.
        assert!(
            cap_strs.contains(&"AGGREGATE_GROUP_BY_COLUMN")
                && cap_strs.contains(&"AGGREGATE_GROUP_BY_EXPRESSION"),
            "TUPLE group-by must be backed by single-key GROUP BY capabilities: {cap_strs:?}"
        );

        // Unsupported capabilities must NOT be advertised.
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
