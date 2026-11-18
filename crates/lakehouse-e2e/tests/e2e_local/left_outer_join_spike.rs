//! SPIKE (not for merge): LEFT OUTER JOIN pushdown, broadcast and N-scan, against native Exasol.

use crate::common::e2e_harness::*;
use crate::common::seed::E2E_NAMESPACE;
use crate::fixtures::{JOIN_ORACLE, copy_join_tables_natively, echoed_pushdown_requests, events, explain_virtual_pushdown_sql, stack};

const VS: &str = "SPIKE_LOJ";
const VS_LOW: &str = "SPIKE_LOJ_LOW";

#[derive(Clone, Copy, Debug, PartialEq)]
enum Want {
    Broadcast,
    NScan,
    Any,
}

/// Every `{T}` query must match native on both virtual schemas; `want` is the path on `VS`.
const CASES: &[(&str, Want)] = &[
    // 5 matched + 5 NULL-padded fact rows.
    (
        "SELECT o.O_ORDERKEY, c.C_NAME FROM {T}.FACT_ORDERS o \
         LEFT JOIN {T}.DIM_CUSTOMER c ON o.O_ORDERKEY = c.C_CUSTKEY",
        Want::Broadcast,
    ),
    // Anti-join: the WHERE must not prune or filter the null-supplying leg.
    (
        "SELECT o.O_ORDERKEY FROM {T}.FACT_ORDERS o \
         LEFT JOIN {T}.DIM_CUSTOMER c ON o.O_ORDERKEY = c.C_CUSTKEY WHERE c.C_CUSTKEY IS NULL",
        Want::Broadcast,
    ),
    (
        "SELECT o.O_ORDERKEY, c.C_NAME FROM {T}.FACT_ORDERS o \
         LEFT JOIN {T}.DIM_CUSTOMER c ON o.O_ORDERKEY = c.C_CUSTKEY \
         WHERE COALESCE(c.C_NAME, 'none') = 'none'",
        Want::Any,
    ),
    // Null-rejecting dimension filter.
    (
        "SELECT o.O_ORDERKEY, c.C_NAME FROM {T}.FACT_ORDERS o \
         LEFT JOIN {T}.DIM_CUSTOMER c ON o.O_ORDERKEY = c.C_CUSTKEY WHERE c.C_NAME = 'customer-02'",
        Want::Broadcast,
    ),
    // Preserved-side filter.
    (
        "SELECT o.O_ORDERKEY, c.C_NAME FROM {T}.FACT_ORDERS o \
         LEFT JOIN {T}.DIM_CUSTOMER c ON o.O_ORDERKEY = c.C_CUSTKEY \
         WHERE o.O_ORDERDATE >= DATE '2024-01-04'",
        Want::Broadcast,
    ),
    // Aggregates over NULL-padded rows.
    (
        "SELECT COUNT(*), COUNT(c.C_CUSTKEY), SUM(c.C_CUSTKEY) FROM {T}.FACT_ORDERS o \
         LEFT JOIN {T}.DIM_CUSTOMER c ON o.O_ORDERKEY = c.C_CUSTKEY",
        Want::Broadcast,
    ),
    (
        "SELECT c.C_CUSTKEY, COUNT(*) FROM {T}.FACT_ORDERS o \
         LEFT JOIN {T}.DIM_CUSTOMER c ON o.O_ORDERKEY = c.C_CUSTKEY GROUP BY c.C_CUSTKEY",
        Want::Broadcast,
    ),
    // Ordered windows over NULL-padded sort keys.
    (
        "SELECT o.O_ORDERKEY, c.C_CUSTKEY FROM {T}.FACT_ORDERS o \
         LEFT JOIN {T}.DIM_CUSTOMER c ON o.O_ORDERKEY = c.C_CUSTKEY \
         ORDER BY c.C_CUSTKEY NULLS FIRST, o.O_ORDERKEY LIMIT 7",
        Want::Broadcast,
    ),
    (
        "SELECT o.O_ORDERKEY, c.C_CUSTKEY FROM {T}.FACT_ORDERS o \
         LEFT JOIN {T}.DIM_CUSTOMER c ON o.O_ORDERKEY = c.C_CUSTKEY \
         ORDER BY c.C_CUSTKEY DESC NULLS LAST, o.O_ORDERKEY LIMIT 4 OFFSET 2",
        Want::Any,
    ),
    // Small preserved side: the null-supplying fact is the only leg that may be broadcast.
    (
        "SELECT c.C_CUSTKEY, o.O_ORDERKEY FROM {T}.DIM_CUSTOMER c \
         LEFT JOIN {T}.FACT_ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY + 3",
        Want::Any,
    ),
    // Self-join.
    (
        "SELECT a.O_ORDERKEY, b.O_ORDERKEY FROM {T}.FACT_ORDERS a \
         LEFT JOIN {T}.FACT_ORDERS b ON a.O_ORDERKEY = b.O_CUSTKEY",
        Want::Broadcast,
    ),
    // Mixed inner/left three-leg tree.
    (
        "SELECT o.O_ORDERKEY, c.C_NAME, o2.O_ORDERKEY FROM {T}.FACT_ORDERS o \
         JOIN {T}.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY \
         LEFT JOIN {T}.FACT_ORDERS o2 ON c.C_CUSTKEY = o2.O_ORDERKEY - 7",
        Want::NScan,
    ),
    (
        "SELECT o.O_ORDERKEY, c.C_NAME, o2.O_ORDERKEY FROM {T}.FACT_ORDERS o \
         LEFT JOIN {T}.DIM_CUSTOMER c ON o.O_ORDERKEY = c.C_CUSTKEY \
         LEFT JOIN {T}.FACT_ORDERS o2 ON c.C_CUSTKEY = o2.O_CUSTKEY WHERE o2.O_ORDERKEY IS NULL",
        Want::NScan,
    ),
    // Extra ON conjunct on the null-supplying side.
    (
        "SELECT o.O_ORDERKEY, c.C_NAME FROM {T}.FACT_ORDERS o \
         LEFT JOIN {T}.DIM_CUSTOMER c ON o.O_ORDERKEY = c.C_CUSTKEY AND c.C_NAME <> 'customer-01'",
        Want::Any,
    ),
    // Extra ON conjunct on the preserved side: it must not filter fact rows.
    (
        "SELECT o.O_ORDERKEY, c.C_NAME FROM {T}.FACT_ORDERS o \
         LEFT JOIN {T}.DIM_CUSTOMER c ON o.O_ORDERKEY = c.C_CUSTKEY AND o.O_ORDERKEY > 2",
        Want::Any,
    ),
    // An empty null-supplying leg keeps every preserved row; an empty preserved leg empties it.
    (
        "SELECT o.O_ORDERKEY, e.C_NAME FROM {T}.FACT_ORDERS o \
         LEFT JOIN {T}.SPIKE_EMPTY_DIM e ON o.O_CUSTKEY = e.C_CUSTKEY",
        Want::Broadcast,
    ),
    (
        "SELECT e.C_NAME, o.O_ORDERKEY FROM {T}.SPIKE_EMPTY_DIM e \
         LEFT JOIN {T}.FACT_ORDERS o ON o.O_CUSTKEY = e.C_CUSTKEY",
        Want::Any,
    ),
    // RIGHT JOIN is not advertised: Exasol either rewrites it or keeps it.
    (
        "SELECT o.O_ORDERKEY, c.C_NAME FROM {T}.DIM_CUSTOMER c \
         RIGHT JOIN {T}.FACT_ORDERS o ON o.O_ORDERKEY = c.C_CUSTKEY",
        Want::Any,
    ),
];

#[test]
fn spike_left_outer_join_matches_native() {
    stack();
    events();
    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, &VsProps::new(VS, E2E_NAMESPACE));
    create_virtual_schema(
        &mut conn,
        &VsProps::new(VS_LOW, E2E_NAMESPACE).with_join_broadcast_max_bytes("1"),
    );
    copy_join_tables_natively(&mut conn, VS);
    conn.execute(&format!(
        "CREATE OR REPLACE TABLE {JOIN_ORACLE}.SPIKE_EMPTY_DIM AS SELECT * FROM {VS}.SPIKE_EMPTY_DIM"
    ));

    let first = CASES[0].0.replace("{T}", VS);
    println!(
        "=== pushdown request of the first case ===\n{}",
        serde_json::to_string_pretty(&echoed_pushdown_requests(&mut conn, &first)).unwrap()
    );

    let mut failures = Vec::new();
    for (query, want) in CASES {
        for vs in [VS, VS_LOW] {
            if let Some(f) =
                native_copy_mismatch(&mut conn, vs, JOIN_ORACLE, query, PathCheck::Unchecked, Expect::Rows)
            {
                failures.push(format!("[{vs}] {f}"));
                continue;
            }
            let plan = explain_virtual_pushdown_sql(&mut conn, &query.replace("{T}", vs));
            let path = if has_broadcast_join_block(&plan) {
                Want::Broadcast
            } else if plan.contains(r#"AS "LHS_T0""#) {
                Want::NScan
            } else {
                Want::Any
            };
            println!("[{vs}] {path:?} <- {query}");
            if vs == VS && *want != Want::Any && path != *want {
                failures.push(format!("[{vs}] `{query}` took {path:?}, want {want:?}: {plan}"));
            }
            if vs == VS_LOW && path == Want::Broadcast && !query.contains("SPIKE_EMPTY_DIM") {
                failures.push(format!("[{vs}] `{query}` must not broadcast: {plan}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}
