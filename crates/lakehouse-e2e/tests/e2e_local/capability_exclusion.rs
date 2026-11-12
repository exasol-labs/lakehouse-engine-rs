//! `EXCLUDED_CAPABILITIES` through Exasol (#622): an excluded capability leaves the
//! `getCapabilities` answer, Exasol evaluates its construct over the returned rows, and every
//! query returns its native rows. Every test names its own virtual schemas. Fail, never skip,
//! without the stack.

use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::common::seed::{E2E_NAMESPACE, E2E_TABLE, SEED_TOTAL_ROWS};
use crate::fixtures::{
    JOIN_ORACLE, LAKEHOUSE_VS, STUB_ADAPTER_SCHEMA, copy_join_tables_natively,
    echoed_pushdown_requests, events_table, explain_virtual_messages, explain_virtual_pushdown_sql,
    lakehouse_vs, persist_properties, vs_table,
};

use serde_json::Value;

const PROPERTY: &str = "EXCLUDED_CAPABILITIES";
/// Names the events table `{T}`; matches `event-10` to `event-19`, a strict subset of the rows.
const LIKE_QUERY: &str = "SELECT id, name FROM {T} WHERE name LIKE 'event-1%'";

/// Seeds the namespace, creates the virtual schema without the property, and copies the queried
/// tables natively.
fn setup() -> ExaConn {
    lakehouse_vs();
    let mut conn = exa_conn();
    create_full_native_copy(&mut conn, &events_table());
    copy_join_tables_natively(&mut conn, LAKEHOUSE_VS);
    conn
}

fn create_excluding(conn: &mut ExaConn, vs: &str, excluded: &str) {
    create_virtual_schema(
        conn,
        &VsProps::new(vs, E2E_NAMESPACE).with_property(PROPERTY, excluded),
    );
}

fn like_query(vs: &str) -> String {
    LIKE_QUERY.replace("{T}", &vs_table(vs, E2E_TABLE))
}

fn pushes_like(conn: &mut ExaConn, query: &str) -> bool {
    explain_virtual_pushdown_sql(conn, query).contains("LIKE")
}

/// Each pushdown request Exasol sent for `query`, for a failure message.
fn echoed_requests(conn: &mut ExaConn, query: &str) -> String {
    echoed_pushdown_requests(conn, query)
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

/// The `getCapabilities` answer that `EXPLAIN VIRTUAL` echoes for `query`'s first pushdown. The
/// answer is parsed, because the echoed request also carries the property value.
fn echoed_capabilities(conn: &mut ExaConn, query: &str) -> Vec<String> {
    let messages = explain_virtual_messages(conn, query)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("EXPLAIN VIRTUAL echoed no pushdown for {query}"));
    messages
        .iter()
        .filter(|m| m["type"] == "getCapabilities")
        .find_map(|m| m["capabilities"].as_array())
        .unwrap_or_else(|| panic!("{query} echoed no getCapabilities answer: {messages:?}"))
        .iter()
        .map(value_to_string)
        .collect()
}

/// The capabilities a schema without the property receives, in order.
fn advertised_capabilities(conn: &mut ExaConn) -> Vec<String> {
    let advertised = echoed_capabilities(conn, &like_query(LAKEHOUSE_VS));
    assert!(
        !advertised.is_empty(),
        "{LAKEHOUSE_VS} must receive capabilities"
    );
    advertised
}

/// `query` echoes `advertised` less exactly `excluded`, in order.
#[track_caller]
fn assert_withholds_exactly(
    conn: &mut ExaConn,
    advertised: &[String],
    query: &str,
    excluded: &[&str],
) {
    for name in excluded {
        assert!(
            advertised.iter().any(|c| c == name),
            "{LAKEHOUSE_VS} must receive {name}: {advertised:?}"
        );
    }
    let expected: Vec<&String> = advertised
        .iter()
        .filter(|c| !excluded.contains(&c.as_str()))
        .collect();
    assert_eq!(
        echoed_capabilities(conn, query).iter().collect::<Vec<_>>(),
        expected,
        "{query} must receive the advertised capabilities less exactly {excluded:?}"
    );
}

/// The `LIKE` query over `vs` returns its native rows, and its pushdown carries no `LIKE`.
#[track_caller]
fn assert_like_evaluated_by_exasol(conn: &mut ExaConn, vs: &str) {
    let native_rows = conn.query_row_count(&LIKE_QUERY.replace("{T}", NATIVE_COPY));
    assert!(
        native_rows > 0 && native_rows < SEED_TOTAL_ROWS as i64,
        "{LIKE_QUERY} must narrow the events table, got {native_rows} rows natively"
    );
    let events = vs_table(vs, E2E_TABLE);
    let path = PathCheck::Lacks("LIKE");
    if let Some(failure) =
        native_copy_mismatch(conn, &events, NATIVE_COPY, LIKE_QUERY, path, Expect::Rows)
    {
        panic!("{failure}");
    }
}

/// Scenario: An excluded capability is evaluated by Exasol
#[test]
fn excluded_like_is_evaluated_by_exasol() {
    const VS: &str = "CE_LIKE";
    let mut conn = setup();
    let advertised = advertised_capabilities(&mut conn);
    create_excluding(&mut conn, VS, "FN_PRED_LIKE");

    assert_withholds_exactly(&mut conn, &advertised, &like_query(VS), &["FN_PRED_LIKE"]);
    assert_like_evaluated_by_exasol(&mut conn, VS);
    let unexcluded = like_query(LAKEHOUSE_VS);
    assert!(
        pushes_like(&mut conn, &unexcluded),
        "without the exclusion, {unexcluded} must push its LIKE"
    );
}

/// Scenario: A changed exclusion applies from the next query
#[test]
fn set_exclusion_applies_from_the_next_query_without_refresh() {
    const VS: &str = "CE_SET";
    let mut conn = setup();
    let advertised = advertised_capabilities(&mut conn);
    create_virtual_schema(&mut conn, &VsProps::new(VS, E2E_NAMESPACE));
    let query = like_query(VS);
    assert!(pushes_like(&mut conn, &query), "{query} must push its LIKE");

    conn.execute(&format!(
        "ALTER VIRTUAL SCHEMA {VS} SET {PROPERTY} = 'FN_PRED_LIKE'"
    ));
    assert_withholds_exactly(&mut conn, &advertised, &query, &["FN_PRED_LIKE"]);
    assert!(
        !pushes_like(&mut conn, &query),
        "{query} must not push its LIKE"
    );
    let notes = Value::Object(schema_notes(&mut conn, VS));
    assert!(
        notes.get(PROPERTY).is_none() && !notes.to_string().contains("FN_PRED_LIKE"),
        "the adapterNotes must not record the exclusion: {notes}"
    );

    conn.execute(&format!("ALTER VIRTUAL SCHEMA {VS} SET {PROPERTY} = null"));
    assert_withholds_exactly(&mut conn, &advertised, &query, &[]);
    assert!(
        pushes_like(&mut conn, &query),
        "{query} must push its LIKE again"
    );
}

/// Scenario: An excluded capability is withheld from getCapabilities
#[test]
fn excluded_capabilities_are_withheld_from_get_capabilities() {
    const VS: &str = "CE_WHITESPACE";
    let mut conn = setup();
    let advertised = advertised_capabilities(&mut conn);

    create_excluding(&mut conn, VS, " FN_PRED_LIKE ,, FN_AGG_COUNT_DISTINCT,");
    assert_withholds_exactly(
        &mut conn,
        &advertised,
        &like_query(VS),
        &["FN_PRED_LIKE", "FN_AGG_COUNT_DISTINCT"],
    );
}

/// What a query names `{T}`.
#[derive(Clone, Copy, Debug)]
enum Source {
    /// The events table, natively [`NATIVE_COPY`].
    Events,
    /// The schema of the fact and dimension tables, natively [`JOIN_ORACLE`].
    Join,
}

impl Source {
    /// `{T}` over the virtual schema `vs`, and over the native copy.
    fn targets(self, vs: &str) -> (String, &'static str) {
        match self {
            Source::Events => (vs_table(vs, E2E_TABLE), NATIVE_COPY),
            Source::Join => (vs.to_string(), JOIN_ORACLE),
        }
    }
}

/// What an exclusion leaves of a query's pushdown.
#[derive(Clone, Copy, Debug)]
enum Pushdown {
    /// One pushdown statement, which passes every check.
    One(&'static [PathCheck<'static>]),
    /// One pushdown per table, none of which joins: Exasol joins them.
    PerTable(usize),
}

impl Pushdown {
    fn mismatch(self, conn: &mut ExaConn, sql: &str, query: &str) -> Option<String> {
        match self {
            Pushdown::One(checks) => {
                let plan = isolated_pushdown_statement(conn, query);
                checks
                    .iter()
                    .find(|check| !check.holds(sql, &plan))
                    .map(|check| format!("`{query}` must take {check:?}: {plan}"))
            }
            Pushdown::PerTable(tables) => {
                let plans = pushdown_statements(conn, query);
                let joins =
                    |plan: &String| has_broadcast_join_block(plan) || plan.contains(" JOIN ");
                (plans.len() != tables || plans.iter().any(joins))
                    .then(|| format!("`{query}` must push {tables} unjoined scans: {plans:?}"))
            }
        }
    }
}

struct Query {
    source: Source,
    /// Names its table or schema `{T}`, as `source` says.
    sql: &'static str,
    pushdown: Pushdown,
}

impl Query {
    /// What differs from the native rows or from the pushdown the exclusion forces, with the
    /// requests Exasol sent.
    fn mismatch(&self, conn: &mut ExaConn, vs: &str) -> Option<String> {
        let (vs_target, native) = self.source.targets(vs);
        let query = self.sql.replace("{T}", &vs_target);
        let failure = native_copy_mismatch(
            conn,
            &vs_target,
            native,
            self.sql,
            PathCheck::Unchecked,
            Expect::Rows,
        )
        .or_else(|| self.pushdown.mismatch(conn, self.sql, &query))?;
        Some(format!(
            "{failure}\npushdown requests:\n{}",
            echoed_requests(conn, &query)
        ))
    }
}

/// An `EXCLUDED_CAPABILITIES` set and queries that use the constructs it withholds.
struct Combination {
    excluded: &'static [&'static str],
    queries: &'static [Query],
}

/// The events column that no query selects, in an `EMITS` clause: only a full-row scan declares it.
const EVENTS_FULL_ROW: &str = r#""EVENT_TS" TIMESTAMP"#;
/// The fact column that no query selects, under a broadcast join's prefixed name or a leg's own.
const FACT_FULL_ROW: &str = r#"O_TOTALPRICE" DECIMAL"#;
const BROADCAST_FULL_ROW: &[PathCheck] = &[
    PathCheck::Has(BROADCAST_JOIN),
    PathCheck::Has(FACT_FULL_ROW),
];
const NO_PARTIAL: &[PathCheck] = &[PathCheck::Lacks(PARTIAL)];
const NO_DISTINCT: &[PathCheck] = &[PathCheck::Lacks("DISTINCT")];

const COMBINATIONS: &[Combination] = &[
    Combination {
        excluded: &["SELECTLIST_PROJECTION", "SELECTLIST_EXPRESSIONS"],
        queries: &[
            Query {
                source: Source::Events,
                sql: "SELECT id, name FROM {T} WHERE id < 5",
                pushdown: Pushdown::One(&[PathCheck::Has(EVENTS_FULL_ROW)]),
            },
            Query {
                source: Source::Events,
                sql: "SELECT name, COUNT(*), SUM(score) FROM {T} GROUP BY name",
                pushdown: Pushdown::One(&[
                    PathCheck::Has(EVENTS_FULL_ROW),
                    PathCheck::Lacks(PARTIAL),
                ]),
            },
            Query {
                source: Source::Join,
                sql: "SELECT c.C_NAME, o.O_ORDERDATE FROM {T}.FACT_ORDERS o \
                      JOIN {T}.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY",
                pushdown: Pushdown::One(BROADCAST_FULL_ROW),
            },
            Query {
                source: Source::Join,
                sql: "SELECT o.O_ORDERDATE, c.C_NAME FROM {T}.DIM_CUSTOMER c \
                      JOIN {T}.FACT_ORDERS o ON o.O_CUSTKEY = c.C_CUSTKEY \
                      WHERE o.O_ORDERDATE >= DATE '2024-01-05'",
                pushdown: Pushdown::One(BROADCAST_FULL_ROW),
            },
            Query {
                source: Source::Join,
                sql: "SELECT c.C_NAME, o.O_ORDERDATE FROM {T}.FACT_ORDERS o \
                      JOIN {T}.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY \
                      ORDER BY o.O_ORDERDATE DESC LIMIT 3",
                pushdown: Pushdown::One(BROADCAST_FULL_ROW),
            },
            Query {
                source: Source::Join,
                sql: "SELECT c.C_NAME, o.O_ORDERDATE FROM {T}.FACT_ORDERS o \
                      JOIN {T}.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY \
                      ORDER BY o.O_ORDERDATE LIMIT 3 OFFSET 2",
                pushdown: Pushdown::One(BROADCAST_FULL_ROW),
            },
            // A condition beyond the one equality is never broadcast.
            Query {
                source: Source::Join,
                sql: "SELECT c.C_NAME, o.O_ORDERDATE FROM {T}.FACT_ORDERS o \
                      JOIN {T}.DIM_CUSTOMER c \
                      ON o.O_CUSTKEY = c.C_CUSTKEY AND o.O_ORDERKEY > c.C_CUSTKEY",
                pushdown: Pushdown::One(&[PathCheck::NScan(2), PathCheck::Has(FACT_FULL_ROW)]),
            },
            Query {
                source: Source::Join,
                sql: "SELECT a.O_ORDERKEY, b.O_ORDERDATE FROM {T}.FACT_ORDERS a \
                      JOIN {T}.FACT_ORDERS b ON a.O_CUSTKEY = b.O_CUSTKEY",
                pushdown: Pushdown::One(BROADCAST_FULL_ROW),
            },
            // No file holds an order key above 100, so the plan is the empty full row.
            Query {
                source: Source::Join,
                sql: "SELECT c.C_NAME, o.O_ORDERDATE FROM {T}.FACT_ORDERS o \
                      JOIN {T}.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY \
                      WHERE o.O_ORDERKEY > 100",
                pushdown: Pushdown::One(&[
                    PathCheck::Has("WHERE 1=0"),
                    PathCheck::Has(r#"AS "O_TOTALPRICE""#),
                ]),
            },
        ],
    },
    Combination {
        excluded: &["AGGREGATE_SINGLE_GROUP"],
        queries: &[
            Query {
                source: Source::Events,
                sql: "SELECT COUNT(*), SUM(score) FROM {T}",
                pushdown: Pushdown::One(NO_PARTIAL),
            },
            Query {
                source: Source::Events,
                sql: "SELECT COUNT(*), SUM(score) FROM {T} WHERE id > 100",
                pushdown: Pushdown::One(NO_PARTIAL),
            },
        ],
    },
    Combination {
        excluded: &["FN_AGG_COUNT"],
        queries: &[Query {
            source: Source::Events,
            sql: "SELECT name, AVG(score), COUNT(score) FROM {T} GROUP BY name",
            pushdown: Pushdown::One(NO_PARTIAL),
        }],
    },
    Combination {
        excluded: &["FN_AGG_COUNT_DISTINCT"],
        queries: &[
            Query {
                source: Source::Events,
                sql: "SELECT COUNT(DISTINCT name) FROM {T}",
                pushdown: Pushdown::One(NO_DISTINCT),
            },
            Query {
                source: Source::Events,
                sql: "SELECT event_date, COUNT(DISTINCT name) FROM {T} GROUP BY event_date",
                pushdown: Pushdown::One(NO_DISTINCT),
            },
        ],
    },
    Combination {
        excluded: &["JOIN"],
        queries: &[Query {
            source: Source::Join,
            sql: "SELECT c.C_NAME, o.O_ORDERDATE FROM {T}.FACT_ORDERS o \
                  JOIN {T}.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY",
            pushdown: Pushdown::PerTable(2),
        }],
    },
];

/// Scenario: Request shapes that an exclusion makes reachable return the same rows
#[test]
fn exclusion_combinations_return_native_rows() {
    const VS: &str = "CE_COMBINATION";
    let mut conn = setup();

    let advertised = advertised_capabilities(&mut conn);

    let mut failures = Vec::new();
    for combination in COMBINATIONS {
        create_excluding(&mut conn, VS, &combination.excluded.join(", "));
        assert_withholds_exactly(
            &mut conn,
            &advertised,
            &like_query(VS),
            combination.excluded,
        );
        for query in combination.queries {
            if let Some(failure) = query.mismatch(&mut conn, VS) {
                failures.push(format!("[{:?}] {failure}", combination.excluded));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}

/// Scenario: getCapabilities ignores an entry the adapter does not advertise
/// Scenario: Create, refresh, and set reject an entry the adapter does not advertise
#[test]
fn get_capabilities_ignores_a_persisted_unadvertised_entry() {
    const VS: &str = "CE_PERSISTED";
    let mut conn = setup();
    create_schema_and_scripts_in(&mut conn, STUB_ADAPTER_SCHEMA);
    create_virtual_schema(
        &mut conn,
        &VsProps::new(VS, E2E_NAMESPACE).with_adapter_schema(STUB_ADAPTER_SCHEMA),
    );
    persist_properties(
        &mut conn,
        VS,
        &format!("{PROPERTY} = 'NO_SUCH_CAPABILITY, FN_PRED_LIKE'"),
    );

    let advertised = advertised_capabilities(&mut conn);
    assert_withholds_exactly(&mut conn, &advertised, &like_query(VS), &["FN_PRED_LIKE"]);
    assert_like_evaluated_by_exasol(&mut conn, VS);

    let expected = format!(
        "invalid virtual schema properties: invalid value 'NO_SUCH_CAPABILITY' for \
         '{PROPERTY}': expected an advertised capability name"
    );
    let refresh = format!("ALTER VIRTUAL SCHEMA {VS} REFRESH");
    for statement in [
        refresh.clone(),
        format!("ALTER VIRTUAL SCHEMA {VS} SET PARALLELISM_FACTOR = '3'"),
    ] {
        let msg = query_error(&mut conn, &statement);
        assert!(
            msg.contains(&expected) && !msg.contains("'FN_PRED_LIKE'"),
            "{statement} must name only the unadvertised entry: {msg}"
        );
    }
    assert_like_evaluated_by_exasol(&mut conn, VS);

    conn.execute(&format!(
        "ALTER VIRTUAL SCHEMA {VS} SET {PROPERTY} = 'FN_PRED_LIKE'"
    ));
    conn.execute(&refresh);
    assert_withholds_exactly(&mut conn, &advertised, &like_query(VS), &["FN_PRED_LIKE"]);
}
