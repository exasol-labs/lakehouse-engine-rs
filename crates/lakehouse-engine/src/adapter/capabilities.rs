/// Virtual Schema capabilities for the Lakehouse VS adapter.
///
/// Reports projection, filter predicates, LIMIT, ORDER BY (bare-column and
/// expression sort keys), and single-group aggregate pushdown.
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
    // ORDER BY pushdown: bare-column sort keys (add-topn-pushdown) and expression
    // sort keys (issue #198), backed by the declined row-scan wrapper, the grouped
    // merge, and the qualified wrapper — see docs/capabilities.md for the full
    // explanation. LIMIT_WITH_OFFSET is now backed by the same three wrappers
    // (issue #191); the per-shard bounded top-N never carries an offset — a
    // non-zero offset always declines that path to the row-scan wrapper instead.
    "ORDER_BY_COLUMN",
    "ORDER_BY_EXPRESSION",
    "LIMIT_WITH_OFFSET",
    // Arithmetic binary-operator functions (issue #59, task 1.2)
    "FN_ADD",
    "FN_SUB",
    "FN_MULT",
    "FN_FLOAT_DIV",
    // Unary negation (issue #105): composes inside aggregates (e.g. SUM(-col)).
    "FN_NEG",
    // Type conversion (issue #104): CAST is advertised over its faithful
    // target-type set (VARCHAR/CHAR/DECIMAL(p,s)/DOUBLE/BOOLEAN/DATE/TIMESTAMP).
    // A CAST target renderable under neither dialect (INTERVAL/GEOMETRY/HASHTYPE/
    // TIMESTAMP WITH LOCAL TIME ZONE) fails the query — it can be applied nowhere
    // (see `pushdown`'s module header).
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
    // Date/time scalar functions. FN_CURRENT_DATE/FN_CURRENT_TIMESTAMP/FN_SYSDATE/
    // FN_SYSTIMESTAMP (the now-family) are NOT advertised: rendering Exasol's three
    // distinct now-family semantics (session-zone CURRENT_TIMESTAMP, database-zone
    // SYSTIMESTAMP, and their TO_DATE forms) needs SESSIONTIMEZONE/DBTIMEZONE, but
    // neither reaches the scan UDF — the pushdown request carries no zone,
    // CommonScanSpec carries no temporal field, the scan opens no connect-back
    // session, and the SDK's UdfContext exposes no clock or zone. The scan can only
    // read its own container clock in UTC, once per shard (a fresh SessionContext
    // per invocation), so a pushed clock call would be evaluated G times with no
    // statement anchor while Exasol's now-family is statement-constant. Measured
    // live against Exasol 2025.2.1: a pushed SYSTIMESTAMP returned a value ~2 hours
    // off native (UTC container clock vs EUROPE/BERLIN DBTIMEZONE/SESSIONTIMEZONE),
    // and GROUP BY SYSTIMESTAMP over a two-file table returned two distinct
    // timestamps against one statement-constant native value. Withdrawn so Exasol
    // evaluates its own clock instead — see
    // vs-adapter/pushdown-planning-capability-extensions.
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
#[path = "capabilities_tests.rs"]
mod tests;
