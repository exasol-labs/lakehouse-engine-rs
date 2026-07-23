/// Virtual Schema capabilities for the Lakehouse VS adapter.
///
/// Reports projection, filter predicates, LIMIT, ORDER BY (bare column keys), and
/// single-group aggregate pushdown.
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
    // ORDER BY pushdown: bare-column sort keys only (add-topn-pushdown).
    // ORDER_BY_EXPRESSION and LIMIT_WITH_OFFSET stay unadvertised — no backing path.
    "ORDER_BY_COLUMN",
    // Arithmetic binary-operator functions (issue #59, task 1.2)
    "FN_ADD",
    "FN_SUB",
    "FN_MULT",
    "FN_FLOAT_DIV",
    // Unary negation (issue #105): composes inside aggregates (e.g. SUM(-col)).
    "FN_NEG",
    // Type conversion (issue #104): CAST over its faithful target-type set
    // (VARCHAR/CHAR/DECIMAL(p,s)/DOUBLE/BOOLEAN/DATE/TIMESTAMP); declines (falls
    // back) for INTERVAL/GEOMETRY/HASHTYPE/TIMESTAMP WITH LOCAL TIME ZONE.
    "FN_CAST",
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
    // ISO-8601 week number (issue #107): renders date_part('week', <arg>), verified
    // to match Exasol WEEK including year-boundary dates.
    "FN_WEEK",
    // Date-difference pushdown (issue #107): *_BETWEEN via date_part('epoch', ..)
    // deltas (DAYS_BETWEEN via DATE-DATE). ADD_HOURS/ADD_MINUTES are NOT advertised:
    // E2E parity (task 3.1) proved the microsecond round-trip diverges on a DATE
    // argument — Exasol infers TIMESTAMP(0) for ADD_HOURS(DATE, n) while the rendering
    // yields TIMESTAMP(3), and Exasol rejects the pushdown ("Data type mismatch ...
    // Expected TIMESTAMP(0), but got TIMESTAMP(3)"). The type-blind string translator
    // cannot vary the result precision by argument type, so they were withdrawn (same
    // input-type class as the deferred ADD_DAYS/ADD_WEEKS). The other eleven issue #107
    // functions stay deferred — see plan `add-date-arithmetic-pushdown`.
    "FN_DAYS_BETWEEN",
    "FN_HOURS_BETWEEN",
    "FN_MINUTES_BETWEEN",
    "FN_SECONDS_BETWEEN",
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
    // Single-group COUNT(DISTINCT col): decomposed into per-shard local distinct
    // sets, merged by a scalar merge UDF. A COUNT(DISTINCT ...) inside a GROUP BY
    // request still falls back to row scanning (see pushdown.rs group-by detection).
    "FN_AGG_COUNT_DISTINCT",
    // Statistical aggregates (decomposed via sufficient statistics)
    "FN_AGG_STDDEV",
    "FN_AGG_STDDEV_POP",
    "FN_AGG_STDDEV_SAMP",
    "FN_AGG_VARIANCE",
    "FN_AGG_VAR_POP",
    "FN_AGG_VAR_SAMP",
    // GROUP BY aggregate pushdown: column references, scalar expressions, and
    // multi-column (tuple) group keys. HAVING is advertised; COUNT(DISTINCT)
    // inside a GROUP BY is NOT.
    "AGGREGATE_GROUP_BY_COLUMN",
    "AGGREGATE_GROUP_BY_EXPRESSION",
    "AGGREGATE_GROUP_BY_TUPLE",
    "AGGREGATE_HAVING",
    // Join pushdown: two-table inner equi-join only (broadcast, add-join-pushdown-broadcast).
    // Outer joins, non-equi ("all condition") joins, and Cartesian products are NOT advertised.
    "JOIN",
    "JOIN_TYPE_INNER",
    "JOIN_CONDITION_EQUI",
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

    /// True if `cap_strs` advertises any join shape outside the broadcast inner
    /// equi-join contract: outer joins, non-equi ("all condition") joins, or any
    /// Cartesian-product capability. `JOIN`, `JOIN_TYPE_INNER`, and
    /// `JOIN_CONDITION_EQUI` are the only sanctioned join capabilities.
    fn has_disallowed_join_capability(cap_strs: &[&str]) -> bool {
        cap_strs.iter().any(|c| {
            c.contains("CARTESIAN")
                || *c == "JOIN_TYPE_LEFT_OUTER"
                || *c == "JOIN_TYPE_RIGHT_OUTER"
                || *c == "JOIN_TYPE_FULL_OUTER"
                || *c == "JOIN_CONDITION_ALL"
        })
    }

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

        // Single-group COUNT(DISTINCT) is now advertised (issue #56); grouped
        // COUNT(DISTINCT) still falls back to row scanning via
        // `pushdown::detect_group_by_aggregates` rejecting `distinct:true`.
        assert!(
            cap_strs.contains(&"FN_AGG_COUNT_DISTINCT"),
            "FN_AGG_COUNT_DISTINCT must be advertised: {cap_strs:?}"
        );

        // Inner equi-join pushdown (add-join-pushdown-broadcast) is now advertised,
        // but outer joins, non-equi ("all condition") joins, and any Cartesian
        // product remain out of scope and unadvertised.
        assert!(
            !has_disallowed_join_capability(&cap_strs),
            "outer/all-condition/Cartesian join capabilities must not be advertised: {cap_strs:?}"
        );
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

        // --- additions (incl. AGGREGATE_GROUP_BY_TUPLE, issue #53; FN_AGG_COUNT_DISTINCT, issue #56) ---
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

        // --- conversion, unary-negation, and ISO-week capabilities (issues #104, #105, #107) ---
        for name in &["FN_CAST", "FN_NEG", "FN_WEEK"] {
            assert!(
                cap_strs.contains(name),
                "{name} must be advertised: {cap_strs:?}"
            );
        }

        // --- date-difference pushdown: supported subset (issue #107) ---
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

        // --- declined translations must stay unadvertised ---
        // FN_ADD_HOURS/FN_ADD_MINUTES were withdrawn after E2E parity (task 3.1):
        // the microsecond round-trip diverges on a DATE argument (Exasol infers
        // TIMESTAMP(0), the rendering yields TIMESTAMP(3), pushdown rejected).
        for name in &[
            "FN_DIV",
            "FN_TO_CHAR",
            "FN_TO_NUMBER",
            // FN_REGEXP_* decline: see issue #106 (PCRE/regex-crate dialect gap).
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
        ] {
            assert!(
                !cap_strs.contains(name),
                "{name} must NOT be advertised: {cap_strs:?}"
            );
        }

        // --- bitwise operator functions declined: see issue #108 ---
        // Exasol's bit functions operate on the unsigned 64-bit domain
        // (0..=18446744073709551615, result DECIMAL(20,0)); Iceberg has no unsigned
        // integer primitive. None of the eleven has a faithful DataFusion 54.0.0
        // translation, so all stay unadvertised and fall back to row scanning.
        //
        // Class 1 — operator-backed but signed/unsigned-divergent (same limitation as
        // FN_DIV): DataFusion's &/|/#/<</>> act on signed operands, so a bit-63-set
        // result is negative under Int64 and carries that negative value into
        // DECIMAL(20,0); >> is arithmetic (sign-extend) vs Exasol's logical (zero-fill).
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
        // Class 2 — no DataFusion 54.0.0 builtin exists (unary ~ is not_impl_err; no
        // rotate / bit-test / bit-set / bits-to-number scalar function is registered).
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

        // --- task 1.2: arithmetic binary-operator functions ---
        for name in &["FN_ADD", "FN_SUB", "FN_MULT", "FN_FLOAT_DIV"] {
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
        // FN_AGG_COUNT_DISTINCT is decomposable (single-group only, issue #56) and
        // is asserted present above; it is intentionally excluded from this
        // must-not-appear list.
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
        // ORDER_BY_COLUMN is now advertised (add-topn-pushdown): a bare-column sort
        // key backs the per-shard bounded top-N + Exasol-side merge. Expression sort
        // keys and OFFSET remain unadvertised — no backing path exists for either.
        assert!(
            cap_strs.contains(&"ORDER_BY_COLUMN"),
            "ORDER_BY_COLUMN must be advertised: {cap_strs:?}"
        );
        for name in &["ORDER_BY_EXPRESSION", "LIMIT_WITH_OFFSET"] {
            assert!(
                !cap_strs.contains(name),
                "{name} must NOT be advertised: {cap_strs:?}"
            );
        }
        // Inner equi-join pushdown is advertised (add-join-pushdown-broadcast);
        // outer joins, non-equi ("all condition") joins, and any Cartesian product
        // remain out of scope and must not be advertised.
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

        // Arithmetic binary-operator functions must be advertised.
        for name in &["FN_ADD", "FN_SUB", "FN_MULT", "FN_FLOAT_DIV"] {
            assert!(
                cap_strs.contains(name),
                "{name} must be advertised: {cap_strs:?}"
            );
        }

        // Supported single-group aggregate capabilities must be advertised.
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

        // Unsupported join shapes must NOT be advertised: outer joins, non-equi
        // ("all condition") joins, and any Cartesian product. Inner equi-join
        // pushdown itself IS advertised (add-join-pushdown-broadcast) and is
        // asserted separately in `advertises_inner_equi_join_capabilities`.
        assert!(
            !has_disallowed_join_capability(&cap_strs),
            "outer/all-condition/Cartesian join capabilities must not be advertised: {cap_strs:?}"
        );

        // Projection, filter, and LIMIT must still be present.
        assert!(cap_strs.contains(&"SELECTLIST_PROJECTION"));
        assert!(cap_strs.contains(&"FILTER_EXPRESSIONS"));
        assert!(cap_strs.contains(&"LIMIT"));
    }

    /// Scenario: Adapter advertises `FN_AGG_COUNT_DISTINCT` for single-group
    /// `COUNT(DISTINCT col)` pushdown (issue #56).
    ///
    /// A `COUNT(DISTINCT ...)` inside a GROUP BY request still falls back to row
    /// scanning via `pushdown::detect_group_by_aggregates` rejecting
    /// `distinct:true`; that behavior is covered separately in
    /// `adapter::pushdown`'s `grouped_count_distinct_falls_back_to_row_scan`. This
    /// test only guards the capability advertisement.
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

    /// Scenarios (vs-adapter/pushdown-planning-capability-extensions): advertising
    /// `FN_CAST`, `FN_NEG`, and `FN_WEEK` (issues #104, #105, #107) introduces no
    /// additional join or cross-join capability — the join capability set stays
    /// exactly `JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, unchanged by this
    /// diff.
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

    /// Scenario (vs-adapter/pushdown-planning-join): Adapter advertises inner
    /// equi-join capabilities.
    ///
    /// `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI` must be advertised so
    /// Exasol pushes single two-table inner equi-joins to the adapter
    /// (`add-join-pushdown-broadcast`). Outer joins, non-equi ("all condition")
    /// joins, and any Cartesian product are explicit non-goals and must never be
    /// advertised.
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

    /// Scenario (vs-adapter/pushdown-planning, CHANGED): the full advertised
    /// capability set includes inner equi-join pushdown alongside the existing
    /// projection/filter/LIMIT/aggregate pushdown capabilities.
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

        // Existing pushdown capabilities remain advertised alongside join.
        assert!(cap_strs.contains(&"SELECTLIST_PROJECTION"));
        assert!(cap_strs.contains(&"FILTER_EXPRESSIONS"));
        assert!(cap_strs.contains(&"LIMIT"));
        assert!(cap_strs.contains(&"AGGREGATE_SINGLE_GROUP"));
        assert!(cap_strs.contains(&"AGGREGATE_GROUP_BY_COLUMN"));
    }
}
