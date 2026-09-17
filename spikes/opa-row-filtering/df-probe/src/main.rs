//! Feasibility probe: can a row-filter expression STRING, in the shape OPA
//! returns it to Trino, be pushed into `CommonScanSpec.filter` and planned by
//! the DataFusion the scan UDF runs?
//!
//! The engine treats `CommonScanSpec.filter` as raw SQL text and splices it into
//! a WHERE clause. The three splice sites, quoted from production source:
//!
//!   crates/lakehouse-engine/src/scan/raw_scan.rs:563-571
//!       let mut sql = format!("SELECT {select_clause} FROM ({inner})");
//!       if let Some(filter) = &spec.common.filter && !filter.is_empty() {
//!           sql.push_str(" WHERE "); sql.push_str(filter); }
//!
//!   crates/lakehouse-engine/src/scan/partial_agg.rs:365-377  (single-group)
//!       let mut sql = format!("SELECT {} FROM ({})", select_items.join(", "), aliased_table);
//!       if let Some(f) = filter && !f.is_empty() { sql.push_str(" WHERE "); sql.push_str(f); }
//!
//!   crates/lakehouse-engine/src/scan/partial_agg.rs:238-250  (grouped)
//!       ... same WHERE splice, then " GROUP BY <keys>"
//!
//! `aliased_table` / `inner` is built by `build_alias_items`
//! (crates/lakehouse-engine/src/scan/sql_support.rs:15-28), which emits
//! `"parquet_name" AS "UPPERCASE_NAME"` for every field. That uppercase,
//! double-quoted alias is what any injected filter must resolve against.

use std::sync::Arc;

use arrow::array::{Date32Array, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;

/// Mirror of `build_alias_items` (sql_support.rs:15-28), reproduced here rather
/// than imported so this probe stays outside the workspace. The engine's own
/// golden tests pin the same shape.
fn build_alias_items(names: &[&str]) -> Vec<String> {
    names
        .iter()
        .map(|n| format!("\"{}\" AS \"{}\"", n, n.to_uppercase()))
        .collect()
}

fn aliased_inner(names: &[&str], table: &str) -> String {
    format!("SELECT {} FROM {table}", build_alias_items(names).join(", "))
}

const COLS: &[&str] = &[
    "region",
    "o_totalprice",
    "classification",
    "o_orderdate",
    "o_comment",
    "owner",
    "dept",
    "c_nationkey",
];

/// Two shards' worth of rows, so the partial/merge composition in probe 4 is a
/// real two-step and not a single-node query wearing a costume.
fn shard_rows(shard: usize) -> (Vec<&'static str>, Vec<f64>, Vec<Option<&'static str>>, Vec<i32>, Vec<&'static str>, Vec<&'static str>, Vec<&'static str>, Vec<i32>) {
    // day 19723 = 2024-01-01, 19800 ~ 2024-03-18
    if shard == 0 {
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
    }
}

fn batch(shard: usize) -> RecordBatch {
    let (region, price, classification, date, comment, owner, dept, nat) = shard_rows(shard);
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

async fn ctx_with_shards() -> SessionContext {
    let ctx = SessionContext::new();
    for shard in 0..2 {
        let b = batch(shard);
        let t = MemTable::try_new(b.schema(), vec![vec![b]]).unwrap();
        // The scan UDF registers its assigned files under exactly this name
        // (partial_agg.rs:42 `let table_name = "scan_target";`).
        ctx.register_table(format!("scan_target_s{shard}"), Arc::new(t)).unwrap();
    }
    // Whole table = both shards, for the single-node reference answer.
    let b0 = batch(0);
    let b1 = batch(1);
    let t = MemTable::try_new(b0.schema(), vec![vec![b0], vec![b1]]).unwrap();
    ctx.register_table("scan_target", Arc::new(t)).unwrap();
    ctx
}

/// Run `sql`, return Ok(row count) or Err(the DataFusion error's first line).
async fn try_sql(ctx: &SessionContext, sql: &str) -> Result<usize, String> {
    match ctx.sql(sql).await {
        Err(e) => Err(format!("PLAN: {}", first_line(&e.to_string()))),
        Ok(df) => match df.collect().await {
            Err(e) => Err(format!("EXEC: {}", first_line(&e.to_string()))),
            Ok(batches) => Ok(batches.iter().map(|b| b.num_rows()).sum()),
        },
    }
}

async fn scalar_i64(ctx: &SessionContext, sql: &str) -> Result<i64, String> {
    use arrow::array::{Array, Int64Array};
    match ctx.sql(sql).await {
        Err(e) => return Err(format!("PLAN: {}", first_line(&e.to_string()))),
        Ok(df) => {
            let b = df.collect().await.map_err(|e| format!("EXEC: {}", first_line(&e.to_string())))?;
            let col = b[0].column(0);
            let arr = col.as_any().downcast_ref::<Int64Array>().ok_or("not i64")?;
            if arr.is_null(0) { Ok(0) } else { Ok(arr.value(0)) }
        }
    }
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").trim().to_string()
}

fn h(title: &str) {
    println!("\n{}", "=".repeat(78));
    println!("{title}");
    println!("{}", "=".repeat(78));
}

/// The row filters exactly as OPA returned them (evidence/01-row-filters.txt).
/// (label, expression as returned by OPA)
const OPA_FILTERS: &[(&str, &str)] = &[
    ("A simple equality", "region = 'EU'"),
    ("B multi-filter (ANDed by the consumer)", "(o_totalprice < 100000) AND (region = 'EU')"),
    ("C IN list", "region IN ('EU', 'UK', 'CH')"),
    ("D NULL / three-valued logic", "(classification IS NULL OR classification <> 'SECRET')"),
    ("E typed DATE literal + INTERVAL", "o_orderdate >= DATE '2024-01-01' AND o_orderdate < current_date - INTERVAL '7' DAY"),
    ("F subquery against a mapping table", "region IN (SELECT region FROM security.acl.user_region WHERE user_name = CURRENT_USER)"),
    ("G engine-specific function", "regexp_like(o_comment, '^(?i)eu-')"),
    ("H session function in the expression", "owner = CURRENT_USER"),
    ("I filter with an `identity` override", "dept = 'FINANCE'"),
];

/// Same nine, hand-rewritten the way an adapter-side rewriter would have to:
/// every bare identifier uppercased and double-quoted.
const REWRITTEN: &[(&str, &str)] = &[
    ("A simple equality", "\"REGION\" = 'EU'"),
    ("B multi-filter (ANDed by the consumer)", "(\"O_TOTALPRICE\" < 100000) AND (\"REGION\" = 'EU')"),
    ("C IN list", "\"REGION\" IN ('EU', 'UK', 'CH')"),
    ("D NULL / three-valued logic", "(\"CLASSIFICATION\" IS NULL OR \"CLASSIFICATION\" <> 'SECRET')"),
    ("E typed DATE literal + INTERVAL", "\"O_ORDERDATE\" >= DATE '2024-01-01' AND \"O_ORDERDATE\" < current_date - INTERVAL '7' DAY"),
    ("F subquery against a mapping table", "\"REGION\" IN (SELECT region FROM security.acl.user_region WHERE user_name = CURRENT_USER)"),
    ("G engine-specific function", "regexp_like(\"O_COMMENT\", '^(?i)eu-')"),
    ("H session function in the expression", "\"OWNER\" = CURRENT_USER"),
    ("I filter with an `identity` override", "\"DEPT\" = 'FINANCE'"),
];

#[tokio::main]
async fn main() {
    let ctx = ctx_with_shards().await;
    let inner = aliased_inner(COLS, "scan_target");

    h("PROBE 0 -- the aliased inner SELECT the filter must resolve against");
    println!("{inner}");
    let total = try_sql(&ctx, &format!("SELECT * FROM ({inner})")).await;
    println!("rows in fixture: {total:?}");

    h("PROBE 1 -- OPA's filter string spliced VERBATIM into the row-scan WHERE");
    println!("shape: SELECT \"REGION\", ... FROM (<aliased inner>) WHERE <opa expression>\n");
    for (label, f) in OPA_FILTERS {
        let sql = format!("SELECT \"REGION\" FROM ({inner}) WHERE {f}");
        match try_sql(&ctx, &sql).await {
            Ok(n) => println!("  PLANNED  {label:<42} rows={n}"),
            Err(e) => println!("  REJECTED {label:<42} {e}"),
        }
    }

    h("PROBE 2 -- same filters after an adapter-side identifier rewrite");
    println!("(every bare column reference uppercased and double-quoted)\n");
    for (label, f) in REWRITTEN {
        let sql = format!("SELECT \"REGION\" FROM ({inner}) WHERE {f}");
        match try_sql(&ctx, &sql).await {
            Ok(n) => println!("  PLANNED  {label:<42} rows={n}"),
            Err(e) => println!("  REJECTED {label:<42} {e}"),
        }
    }

    h("PROBE 3 -- NEGATIVE CONTROLS (each MUST fail / MUST differ)");
    let controls: &[(&str, &str)] = &[
        ("unknown column must be rejected", "\"NO_SUCH_COL\" = 'EU'"),
        ("lowercase bare ident vs uppercase alias", "region = 'EU'"),
        ("mixed case bare ident", "Region = 'EU'"),
        ("quoted lowercase ident", "\"region\" = 'EU'"),
        ("non-boolean expression as a filter", "\"O_TOTALPRICE\""),
        ("trailing semicolon (statement injection)", "\"REGION\" = 'EU'; DROP TABLE scan_target"),
        ("comment-terminated filter", "\"REGION\" = 'EU' -- rest"),
        ("always-true filter must NOT reduce rows", "1 = 1"),
    ];
    for (label, f) in controls {
        let sql = format!("SELECT \"REGION\" FROM ({inner}) WHERE {f}");
        match try_sql(&ctx, &sql).await {
            Ok(n) => println!("  PLANNED  {label:<42} rows={n}"),
            Err(e) => println!("  REJECTED {label:<42} {e}"),
        }
    }

    h("PROBE 4 -- composition with pushdown shapes (item 4)");
    // The engine's aggregate builders splice the SAME filter into the WHERE of
    // the SAME SELECT that aggregates, so the filter is PRE-aggregation.
    let f = "\"REGION\" = 'EU'";
    let s0 = aliased_inner(COLS, "scan_target_s0");
    let s1 = aliased_inner(COLS, "scan_target_s1");

    println!("-- 4a single-group partial/merge, filter INSIDE each shard (engine's placement)");
    let p0 = format!("SELECT count(1) AS \"PARTIAL_0_COUNT\", sum(\"O_TOTALPRICE\") AS \"PARTIAL_0_SUM\" FROM ({s0}) WHERE {f}");
    let p1 = format!("SELECT count(1) AS \"PARTIAL_0_COUNT\", sum(\"O_TOTALPRICE\") AS \"PARTIAL_0_SUM\" FROM ({s1}) WHERE {f}");
    let merged = format!("SELECT sum(\"PARTIAL_0_COUNT\") FROM (({p0}) UNION ALL ({p1}))");
    let reference = format!("SELECT count(1) FROM ({inner}) WHERE {f}");
    println!("   merged partial count    = {:?}", scalar_i64(&ctx, &merged).await);
    println!("   single-node reference   = {:?}", scalar_i64(&ctx, &reference).await);

    println!("-- 4b THE BUG: filter applied to the MERGE instead of the shard scan");
    let p0_nf = format!("SELECT count(1) AS \"PARTIAL_0_COUNT\", max(\"REGION\") AS \"GK_0\" FROM ({s0})");
    let p1_nf = format!("SELECT count(1) AS \"PARTIAL_0_COUNT\", max(\"REGION\") AS \"GK_0\" FROM ({s1})");
    let wrong = format!("SELECT sum(\"PARTIAL_0_COUNT\") FROM (({p0_nf}) UNION ALL ({p1_nf})) WHERE \"GK_0\" = 'EU'");
    println!("   post-aggregate filter   = {:?}   <-- wrong number, and it is not even the same question",
        scalar_i64(&ctx, &wrong).await);

    println!("-- 4c GROUP BY partial/merge, filter INSIDE each shard (multi-group)");
    // A filter that leaves rows in MORE THAN ONE group, so the merge step is
    // actually exercised rather than trivially collapsing to one row.
    let fg = "\"REGION\" IN ('EU', 'UK', 'CH')";
    let g0 = format!("SELECT \"DEPT\" AS \"GK_0\", count(1) AS \"PARTIAL_0_COUNT\" FROM ({s0}) WHERE {fg} GROUP BY \"DEPT\"");
    let g1 = format!("SELECT \"DEPT\" AS \"GK_0\", count(1) AS \"PARTIAL_0_COUNT\" FROM ({s1}) WHERE {fg} GROUP BY \"DEPT\"");
    let gmerge = format!("SELECT \"GK_0\", sum(\"PARTIAL_0_COUNT\") AS C FROM (({g0}) UNION ALL ({g1})) GROUP BY \"GK_0\" ORDER BY \"GK_0\"");
    print_rows(&ctx, &gmerge, "   merged grouped").await;
    let gref = format!("SELECT \"DEPT\", count(1) AS C FROM ({inner}) WHERE {fg} GROUP BY \"DEPT\" ORDER BY \"DEPT\"");
    print_rows(&ctx, &gref, "   single-node ref").await;

    println!("-- 4d COUNT(DISTINCT): per-shard DISTINCT row scan, filter INSIDE, outer COUNT(DISTINCT)");
    // support.rs:391-396 composes the NULL guard as `({f}) AND ({col} IS NOT NULL)`.
    let composed = format!("({f}) AND (\"OWNER\" IS NOT NULL)");
    let d0 = format!("SELECT DISTINCT \"OWNER\" AS \"V\" FROM ({s0}) WHERE {composed}");
    let d1 = format!("SELECT DISTINCT \"OWNER\" AS \"V\" FROM ({s1}) WHERE {composed}");
    let dmerge = format!("SELECT count(DISTINCT \"V\") FROM (({d0}) UNION ALL ({d1}))");
    println!("   sharded distinct count  = {:?}", scalar_i64(&ctx, &dmerge).await);
    let dref = format!("SELECT count(DISTINCT \"OWNER\") FROM ({inner}) WHERE {composed}");
    println!("   single-node reference   = {:?}", scalar_i64(&ctx, &dref).await);

    println!("-- 4e broadcast join: engine splices the filter AFTER the join (join_scan.rs:206-212)");
    let dim = "SELECT * FROM (VALUES ('FINANCE','f'),('SALES','s')) AS d(\"DEPT_D\",\"TAG\")";
    let post = format!(
        "SELECT count(1) FROM ({dim}) INNER JOIN ({inner}) ON \"DEPT_D\" = \"DEPT\" WHERE {f}"
    );
    let pre = format!(
        "SELECT count(1) FROM ({dim}) INNER JOIN (SELECT * FROM ({inner}) WHERE {f}) ON \"DEPT_D\" = \"DEPT\""
    );
    println!("   filter AFTER join       = {:?}", scalar_i64(&ctx, &post).await);
    println!("   filter BEFORE join      = {:?}", scalar_i64(&ctx, &pre).await);

    println!("-- 4f TopN: ORDER BY + LIMIT per shard, filter INSIDE (raw_scan.rs:563-592 order)");
    let t0 = format!("SELECT \"O_TOTALPRICE\" FROM ({s0}) WHERE {f} ORDER BY \"O_TOTALPRICE\" DESC LIMIT 2");
    let t1 = format!("SELECT \"O_TOTALPRICE\" FROM ({s1}) WHERE {f} ORDER BY \"O_TOTALPRICE\" DESC LIMIT 2");
    let tm = format!("SELECT \"O_TOTALPRICE\" FROM (({t0}) UNION ALL ({t1})) ORDER BY \"O_TOTALPRICE\" DESC LIMIT 2");
    print_rows(&ctx, &tm, "   sharded topn   ").await;
    let tr = format!("SELECT \"O_TOTALPRICE\" FROM ({inner}) WHERE {f} ORDER BY \"O_TOTALPRICE\" DESC LIMIT 2");
    print_rows(&ctx, &tr, "   single-node ref").await;

    h("PROBE 6 -- SILENT DIALECT DIVERGENCE (same SQL, different answer)");
    // Nothing here fails to plan. Each one returns a DIFFERENT answer than the
    // Trino dialect the policy was written against -- which is the dangerous case.
    println!("-- 6a Trino substring(str, -3) = last 3 chars. DataFusion?");
    print_rows(&ctx,
        &format!("SELECT DISTINCT \"OWNER\", '****' || substring(\"OWNER\", -3) AS \"MASKED\" FROM ({inner}) ORDER BY \"OWNER\""),
        "   ").await;
    println!("   Trino would render alice -> '****ice'. Anything else is an UNMASKED leak.");

    println!("-- 6b regexp_like: SAME NAME, different regex engine");
    println!("   Trino evaluates it with java.util.regex; DataFusion with the Rust");
    println!("   `regex` crate. Trino was NOT run here, so these are one-sided results:");
    println!("   an accepted pattern is only proof that DataFusion answers, not that it");
    println!("   answers the same as Trino.");
    for pat in [
        r"^(?i)eu-",      // both engines support this
        r"\p{Alpha}+",    // both engines support this
        r"(?<g>eu)-",     // java named group; rust `regex` >= 1.9 also accepts
        r"(?=eu)",        // java lookahead -- NOT in the rust `regex` crate
        r"(\w)\1",        // java backreference -- NOT in the rust `regex` crate
    ] {
        let sql = format!("SELECT count(1) FROM ({inner}) WHERE regexp_like(\"O_COMMENT\", '{pat}')");
        match scalar_i64(&ctx, &sql).await {
            Ok(n) => println!("      {pat:<12} -> ACCEPTED, matched {n} rows"),
            Err(e) => println!("      {pat:<12} -> {e}"),
        }
    }
    println!("      Lookahead/backreference fail loudly, which is the SAFE outcome.");
    println!("      The unsafe class is a pattern both engines accept and read differently.");

    println!("-- 6c clock functions are evaluated INSIDE the shard, once per shard");
    print_rows(&ctx, &format!("SELECT current_date AS \"D\" FROM ({s0}) LIMIT 1"), "   shard 0").await;
    println!("   capabilities.rs:117-132 withdrew the now-family precisely because of this:");
    println!("   \"GROUP BY SYSTIMESTAMP over a two-file table returned two distinct timestamps\"");

    h("PROBE 7 -- SQL-COMMENT TRUNCATION of the clauses the engine appends AFTER the filter");
    // raw_scan.rs appends ORDER BY and LIMIT *after* the spliced filter, on the
    // same line. A filter containing `--` therefore comments them out.
    let honest = format!("SELECT \"O_TOTALPRICE\" FROM ({inner}) WHERE \"REGION\" = 'EU' ORDER BY \"O_TOTALPRICE\" DESC LIMIT 2");
    let hostile = format!("SELECT \"O_TOTALPRICE\" FROM ({inner}) WHERE \"REGION\" = 'EU' -- x ORDER BY \"O_TOTALPRICE\" DESC LIMIT 2");
    println!("   filter without a comment: {:?} rows", try_sql(&ctx, &honest).await);
    println!("   filter ending in `-- x` : {:?} rows  <-- ORDER BY and LIMIT swallowed", try_sql(&ctx, &hostile).await);

    h("PROBE 5 -- column masks as a projection item");
    // ProjectionItem::Expr fragments are spliced into the SELECT list verbatim
    // (raw_scan.rs:520-560). A mask is exactly that shape.
    let masks: &[(&str, &str)] = &[
        ("NULL", "NULL"),
        ("'****' || substring(c_name, -3)", "'****' || substring(\"OWNER\", -3)"),
        ("CASE WHEN ... THEN ... ELSE NULL END", "CASE WHEN \"C_NATIONKEY\" = 3 THEN \"O_TOTALPRICE\" ELSE NULL END"),
        ("to_hex(sha256(to_utf8(x)))", "to_hex(sha256(to_utf8(\"OWNER\")))"),
        ("sha256(x) alone", "sha256(\"OWNER\")"),
        ("to_hex(x) alone", "to_hex(\"C_NATIONKEY\")"),
        ("md5(x) alone", "md5(\"OWNER\")"),
        ("Trino to_utf8(x) alone", "to_utf8(\"OWNER\")"),
        ("Trino try_cast", "try_cast(\"OWNER\" AS integer)"),
        ("Trino cardinality/array", "cardinality(array['a','b'])"),
    ];
    for (label, m) in masks {
        let sql = format!("SELECT {m} AS \"M\" FROM ({inner})");
        match try_sql(&ctx, &sql).await {
            Ok(n) => println!("  PLANNED  {label:<42} rows={n}"),
            Err(e) => println!("  REJECTED {label:<42} {e}"),
        }
    }
}

async fn print_rows(ctx: &SessionContext, sql: &str, label: &str) {
    match ctx.sql(sql).await {
        Err(e) => println!("{label} PLAN ERROR: {}", first_line(&e.to_string())),
        Ok(df) => match df.collect().await {
            Err(e) => println!("{label} EXEC ERROR: {}", first_line(&e.to_string())),
            Ok(b) => {
                let s = arrow::util::pretty::pretty_format_batches(&b)
                    .map(|d| d.to_string())
                    .unwrap_or_default();
                let flat: Vec<&str> = s.lines().filter(|l| l.starts_with('|') && !l.contains("---")).collect();
                println!("{label} {}", flat.join("  "));
            }
        },
    }
}
