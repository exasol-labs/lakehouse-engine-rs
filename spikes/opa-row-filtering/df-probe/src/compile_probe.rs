//! Does the translation layer's output actually plan and return the right rows?
//!
//! Reads the REAL OPA Compile API responses captured in ../compiled/*.json,
//! compiles each one, and runs the result against the same two-shard DataFusion
//! fixture the other probe uses.

mod opa_compile;
use opa_compile::{Decision, compile};

use std::sync::Arc;

use arrow::array::{Date32Array, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;

const COLS: &[&str] = &[
    "region", "o_totalprice", "classification", "o_orderdate", "o_comment", "owner", "dept",
    "c_nationkey",
];

fn batch(shard: usize) -> RecordBatch {
    let (region, price, classification, date, comment, owner, dept, nat): (
        Vec<&str>, Vec<f64>, Vec<Option<&str>>, Vec<i32>, Vec<&str>, Vec<&str>, Vec<&str>, Vec<i32>,
    ) = if shard == 0 {
        (
            vec!["EU", "US", "EU", "UK"],
            vec![10.0, 20.0, 30.0, 40.0],
            vec![Some("PUBLIC"), None, Some("SECRET"), Some("PUBLIC")],
            vec![19723, 19800, 19800, 19600],
            vec!["eu-north", "us-west", "EU-south", "uk-1"],
            vec!["alice", "bob", "alice", "carol"],
            vec!["FINANCE", "SALES", "FINANCE", "SALES"],
            vec![3, 1, 3, 2],
        )
    } else {
        (
            vec!["EU", "CH", "US", "EU"],
            vec![50.0, 60.0, 70.0, 80.0],
            vec![None, Some("PUBLIC"), Some("SECRET"), Some("PUBLIC")],
            vec![19850, 19900, 19500, 19723],
            vec!["eu-east", "ch-1", "us-east", "eu-west"],
            vec!["alice", "dave", "bob", "alice"],
            vec!["FINANCE", "SALES", "FINANCE", "FINANCE"],
            vec![3, 4, 1, 3],
        )
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("classification", DataType::Utf8, true),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_comment", DataType::Utf8, false),
        Field::new("owner", DataType::Utf8, false),
        Field::new("dept", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(region)),
            Arc::new(Float64Array::from(price)),
            Arc::new(StringArray::from(classification)),
            Arc::new(Date32Array::from(date)),
            Arc::new(StringArray::from(comment)),
            Arc::new(StringArray::from(owner)),
            Arc::new(StringArray::from(dept)),
            Arc::new(Int32Array::from(nat)),
        ],
    )
    .unwrap()
}

fn aliased(table: &str) -> String {
    let items: Vec<String> = COLS
        .iter()
        .map(|n| format!("\"{}\" AS \"{}\"", n, n.to_uppercase()))
        .collect();
    format!("SELECT {} FROM {table}", items.join(", "))
}

#[tokio::main]
async fn main() {
    let ctx = SessionContext::new();
    let b0 = batch(0);
    let b1 = batch(1);
    let t = MemTable::try_new(b0.schema(), vec![vec![b0], vec![b1]]).unwrap();
    ctx.register_table("scan_target", Arc::new(t)).unwrap();
    for shard in 0..2 {
        let b = batch(shard);
        let t = MemTable::try_new(b.schema(), vec![vec![b]]).unwrap();
        ctx.register_table(format!("scan_target_s{shard}"), Arc::new(t))
            .unwrap();
    }
    let inner = aliased("scan_target");

    println!("==============================================================================");
    println!("Compiling REAL OPA /v1/compile responses into DataFusion predicates");
    println!("==============================================================================");
    println!("policy: policies/filtering2.rego   unknown: input.row\n");

    // The last three come from the IdP-driven policy: the user's GROUPS are
    // fetched from Keycloak inside Rego, then resolved away by partial
    // evaluation, so only row conditions remain. See evidence/10.
    for name in [
        "alice", "bob", "root", "carol", "unsupported",
        "idp-opa-alice", "idp-opa-bob", "idp-opa-nobody",
        "data-alice", "data-bob", "data-root", "data-nobody",
        // The table gate and the row filter from ONE policy and ONE call:
        // see evidence/14. DENY here means "no table access at all".
        "tbl-alice-orders_public", "tbl-alice-customers", "tbl-alice-orders_secret",
        "tbl-bob-orders_public", "tbl-carol-orders_public", "tbl-nobody-orders_public",
    ] {
        let path = format!("../compiled/{name}.json");
        let body: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        println!("------------------------------------------------------------------");
        print!("user/case `{name}` -> ");
        match compile(&body, "input.row") {
            Err(e) => println!("REFUSED: {}\n   (a refusal is a failed query, never a missing filter)", e.0),
            Ok(Decision::AllowAll) => println!("ALLOW ALL (no filter; scan spec `filter` stays None)"),
            Ok(Decision::DenyAll) => println!("DENY ALL (refuse, or return an empty result)"),
            Ok(Decision::Filter(f)) => {
                println!("FILTER");
                println!("   {f}");
                let rows = run(&ctx, &format!("SELECT \"REGION\", \"CLASSIFICATION\", \"O_TOTALPRICE\" FROM ({inner}) WHERE {f}")).await;
                println!("   plans and returns: {rows}");
                // Same filter must survive every pushdown shape.
                let s0 = aliased("scan_target_s0");
                let s1 = aliased("scan_target_s1");
                let p0 = format!("SELECT count(1) AS \"C\" FROM ({s0}) WHERE {f}");
                let p1 = format!("SELECT count(1) AS \"C\" FROM ({s1}) WHERE {f}");
                let sharded = scalar(&ctx, &format!("SELECT sum(\"C\") FROM (({p0}) UNION ALL ({p1}))")).await;
                let single = scalar(&ctx, &format!("SELECT count(1) FROM ({inner}) WHERE {f}")).await;
                println!("   sharded partial/merge count = {sharded}, single-node = {single}");
            }
        }
    }

    println!("\n==============================================================================");
    println!("NEGATIVE CONTROLS on the translation layer itself");
    println!("==============================================================================");
    let cases: &[(&str, &str)] = &[
        ("unknown operator (startswith)", r#"{"result":{"queries":[[{"index":0,"terms":[{"type":"ref","value":[{"type":"var","value":"startswith"}]},{"type":"ref","value":[{"type":"var","value":"input"},{"type":"string","value":"row"},{"type":"string","value":"x"}]},{"type":"string","value":"eu"}]}]]}}"#),
        ("clock builtin (time.now_ns)", r#"{"result":{"queries":[[{"index":0,"terms":[{"type":"ref","value":[{"type":"var","value":"time"},{"type":"string","value":"now_ns"}]},{"type":"ref","value":[{"type":"var","value":"input"},{"type":"string","value":"row"},{"type":"string","value":"t"}]},{"type":"number","value":1}]}]]}}"#),
        ("column from a DIFFERENT table (join attempt)", r#"{"result":{"queries":[[{"index":0,"terms":[{"type":"ref","value":[{"type":"var","value":"eq"}]},{"type":"ref","value":[{"type":"var","value":"data"},{"type":"string","value":"acl"},{"type":"string","value":"r"}]},{"type":"string","value":"EU"}]}]]}}"#),
        ("quote-injection in a string literal", r#"{"result":{"queries":[[{"index":0,"terms":[{"type":"ref","value":[{"type":"var","value":"eq"}]},{"type":"ref","value":[{"type":"var","value":"input"},{"type":"string","value":"row"},{"type":"string","value":"region"}]},{"type":"string","value":"EU' OR 1=1 -- "}]}]]}}"#),
        ("support module present (default rule)", r#"{"result":{"support":[{"package":{"path":[]}}]}}"#),
        ("empty set in an `in`", r#"{"result":{"queries":[[{"index":0,"terms":[{"type":"ref","value":[{"type":"var","value":"internal"},{"type":"string","value":"member_2"}]},{"type":"ref","value":[{"type":"var","value":"input"},{"type":"string","value":"row"},{"type":"string","value":"region"}]},{"type":"set","value":[]}]}]]}}"#),
    ];
    for (label, raw) in cases {
        let body: serde_json::Value = serde_json::from_str(raw).unwrap();
        print!("  {label:<46} -> ");
        match compile(&body, "input.row") {
            Err(e) => println!("REFUSED: {}", e.0),
            Ok(Decision::AllowAll) => println!("ALLOW ALL"),
            Ok(Decision::DenyAll) => println!("DENY ALL"),
            Ok(Decision::Filter(f)) => {
                let r = run(&ctx, &format!("SELECT 1 FROM ({inner}) WHERE {f}")).await;
                println!("FILTER {f}   -> {r}");
            }
        }
    }
}

async fn run(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Err(e) => format!("PLAN ERROR: {}", e.to_string().lines().next().unwrap_or("")),
        Ok(df) => match df.collect().await {
            Err(e) => format!("EXEC ERROR: {}", e.to_string().lines().next().unwrap_or("")),
            Ok(b) => format!("{} rows", b.iter().map(|x| x.num_rows()).sum::<usize>()),
        },
    }
}

async fn scalar(ctx: &SessionContext, sql: &str) -> String {
    use arrow::array::{Array, Int64Array};
    match ctx.sql(sql).await {
        Err(e) => format!("ERR {}", e.to_string().lines().next().unwrap_or("")),
        Ok(df) => match df.collect().await {
            Err(e) => format!("ERR {}", e.to_string().lines().next().unwrap_or("")),
            Ok(b) => {
                let c = b[0].column(0);
                match c.as_any().downcast_ref::<Int64Array>() {
                    Some(a) if !a.is_null(0) => a.value(0).to_string(),
                    _ => "0".into(),
                }
            }
        },
    }
}
