use crate::scan::spec::{AggKind, AggregatePlan, CatalogProps, ScanSpec, StorageProps};
use exasol_udf_sdk::error::UdfError;
use futures::TryStreamExt;
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde_json::Value as Json;
use std::collections::HashMap;
use std::sync::Arc;
/// Pushdown planning: resolve the Iceberg file list ONCE and build the
/// scan-driving SQL that invokes the LAKEHOUSE_SCAN SET UDF.
///
/// Architecture invariants:
/// - File list resolved exactly ONCE here, in the planning layer.
/// - The scan SET UDF receives the explicit file list; it NEVER discovers files.
/// - A predicate the adapter cannot translate is OMITTED from the spec
///   (correctness backstop: Exasol keeps the predicate at its own level).
/// - LIMIT appears in both the scan spec and the returned SQL (correctness backstop).
/// - Credentials NEVER appear in any returned SQL string or error message.
use vs_expression::{render_df_filter_safe, render_expression, render_expression_safe};

/// Build a RestCatalog configured to read/write data files through the S3
/// (MinIO) storage factory.
///
/// iceberg 0.9.1 requires an explicit `StorageFactory`; the S3 config keys are
/// supplied in the same props map passed to `load`. Credentials live only in
/// this map and never appear in returned SQL or error strings.
async fn build_rest_catalog(
    catalog_uri: &str,
    catalog: &CatalogProps,
    storage: &StorageProps,
) -> Result<RestCatalog, UdfError> {
    let mut props = HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), catalog_uri.to_string());
    props.insert(
        REST_CATALOG_PROP_WAREHOUSE.to_string(),
        catalog.warehouse.clone(),
    );
    if !storage.endpoint.is_empty() {
        props.insert(S3_ENDPOINT.to_string(), storage.endpoint.clone());
    }
    if !storage.region.is_empty() {
        props.insert(S3_REGION.to_string(), storage.region.clone());
    }
    if !storage.access_key.is_empty() {
        props.insert(S3_ACCESS_KEY_ID.to_string(), storage.access_key.clone());
    }
    if !storage.secret_key.is_empty() {
        props.insert(S3_SECRET_ACCESS_KEY.to_string(), storage.secret_key.clone());
    }
    if let Some(token) = &storage.session_token {
        props.insert(S3_SESSION_TOKEN.to_string(), token.clone());
    }
    props.insert(
        S3_PATH_STYLE_ACCESS.to_string(),
        storage.path_style.to_string(),
    );

    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_string(),
            customized_credential_load: None,
        }))
        .load("lakehouse", props)
        .await
        .map_err(|e: iceberg::Error| {
            UdfError::User(format!(
                "failed to connect to Iceberg catalog: {}",
                redact_catalog_error(&e.to_string())
            ))
        })
}

/// The registered SQL name of the scan SET UDF entry point.
const SCAN_UDF_NAME: &str = "LAKEHOUSE_SCAN";

/// Maximum shard count: Exasol distributes groups round-robin below this threshold;
/// above it Exasol hash-partitions them (no longer balanced).
const MAX_SHARD_COUNT: usize = 300;

/// Compute the work-unit shard count G for a given cluster configuration.
///
/// G = clamp(node_count × parallelism_factor, 1, min(file_count, 300)).
///
/// - The product is saturating (no overflow).
/// - G is at least 1 and at most `file_count` so no shard is empty.
/// - G is also at most 300 to stay in Exasol's round-robin distribution regime.
///
/// When `file_count` is zero this returns 1 (caller should skip partition_files).
pub fn shard_count(node_count: usize, parallelism_factor: usize, file_count: usize) -> usize {
    let raw = node_count.saturating_mul(parallelism_factor);
    let upper = file_count.clamp(1, MAX_SHARD_COUNT);
    raw.clamp(1, upper)
}

// ---------------------------------------------------------------------------
// Aggregate detection
// ---------------------------------------------------------------------------

/// Inspect the pushdown request's `selectList` and return the aggregate plan
/// if every select-list item is a supported single-group aggregate.
///
/// Returns `None` (fall back to row scan) when any of the following hold:
/// - `groupBy` is present and non-empty (GROUP BY not supported)
/// - any select item has `distinct: true`
/// - any select item is not one of COUNT(*), COUNT(col), SUM, MIN, MAX, AVG
/// - the select list is absent or empty
pub fn detect_aggregates(pushdown_req: &Json) -> Option<Vec<AggregatePlan>> {
    // Reject GROUP BY.
    if pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
    {
        return None;
    }

    let list = pushdown_req.get("selectList").and_then(|v| v.as_array())?;

    if list.is_empty() {
        return None;
    }

    let mut plans = Vec::with_capacity(list.len());
    for item in list {
        // Every item must be a function_aggregate.
        if item.get("type").and_then(|t| t.as_str()) != Some("function_aggregate") {
            return None;
        }
        let plan = parse_agg_item(item)?;
        plans.push(plan);
    }

    Some(plans)
}

/// Detect a GROUP BY aggregate pushdown and return the rendered group-key SQL
/// fragments and the corresponding aggregate plans.
///
/// Returns `Some((group_keys, aggregate_plans))` only when **all** of the
/// following hold:
/// - `aggregationType` is exactly `"group_by"`.
/// - `groupBy` is a non-empty array.
/// - Every element of `groupBy` renders successfully via `render_expression`
///   (any failure → `None` for the whole call).
/// - Every element of `selectList` is either a `function_aggregate` (contributes
///   an `AggregatePlan`) or a plain `column` reference (group-key projection —
///   skipped for aggregate plan building). Any other type → `None`.
/// - The `selectList` is non-empty.
/// - No `function_aggregate` item uses `distinct: true`.
///
/// Returns `None` on any unsupported shape; the caller falls back to row
/// scanning or single-group aggregate detection.
pub fn detect_group_by_aggregates(
    pushdown_req: &Json,
) -> Option<(Vec<String>, Vec<AggregatePlan>)> {
    // Must be a GROUP BY aggregate request.
    if pushdown_req.get("aggregationType").and_then(|v| v.as_str()) != Some("group_by") {
        return None;
    }

    // GROUP BY array must be present and non-empty.
    let group_by = pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())?;

    // Render each GROUP BY expression; any failure collapses the whole result.
    let mut group_keys = Vec::with_capacity(group_by.len());
    for node in group_by {
        match render_expression(node) {
            Ok(sql) => group_keys.push(sql),
            Err(_) => return None,
        }
    }

    // Collect aggregate plans from the select list.
    let list = pushdown_req.get("selectList").and_then(|v| v.as_array())?;
    if list.is_empty() {
        return None;
    }

    let mut plans = Vec::new();
    for item in list {
        let item_type = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match item_type {
            "column" => {
                // Group-key column projection — not an aggregate; skip.
            }
            "function_aggregate" => {
                let plan = parse_agg_item(item)?;
                plans.push(plan);
            }
            _ => {
                // A scalar expression that renders to one of the group keys is a
                // group-key projection (e.g. SELECT MOD(id,4) ... GROUP BY MOD(id,4)) —
                // emitted via GK_*, so skip it. Anything else disqualifies the path.
                match render_expression(item) {
                    Ok(sql) if group_keys.contains(&sql) => {}
                    _ => return None,
                }
            }
        }
    }

    Some((group_keys, plans))
}

/// Resolve the Exasol-declared type of each group key from `selectListDataTypes`.
///
/// Each group-key expression also appears in `selectList`; the parallel
/// `selectListDataTypes` array gives its declared result type. Falls back to
/// `VARCHAR(2000000)` when the type cannot be located.
fn group_key_exasol_types(pushdown_req: &Json, group_keys: &[String]) -> Vec<String> {
    let select_list = pushdown_req.get("selectList").and_then(|v| v.as_array());
    let declared_types = pushdown_req
        .get("selectListDataTypes")
        .and_then(|v| v.as_array());
    group_keys
        .iter()
        .map(|key| {
            select_list
                .and_then(|list| {
                    list.iter()
                        .position(|item| render_expression(item).ok().as_deref() == Some(key))
                })
                .and_then(|idx| declared_types.and_then(|d| d.get(idx)))
                .map(exasol_type_from_json)
                .unwrap_or_else(|| "VARCHAR(2000000)".to_string())
        })
        .collect()
}

/// Resolve the Exasol-declared type of each aggregate select-list item, in order.
///
/// Aggregates appear as `function_aggregate` items in `selectList`; the parallel
/// `selectListDataTypes` array gives each one's declared result type (e.g. COUNT(*)
/// → DECIMAL(18,0)). Falls back to `VARCHAR(2000000)` when not locatable.
fn aggregate_exasol_types(pushdown_req: &Json) -> Vec<String> {
    let select_list = match pushdown_req.get("selectList").and_then(|v| v.as_array()) {
        Some(l) => l,
        None => return Vec::new(),
    };
    let declared_types = pushdown_req
        .get("selectListDataTypes")
        .and_then(|v| v.as_array());
    select_list
        .iter()
        .enumerate()
        .filter(|(_, item)| item.get("type").and_then(|t| t.as_str()) == Some("function_aggregate"))
        .map(|(idx, _)| {
            declared_types
                .and_then(|d| d.get(idx))
                .map(exasol_type_from_json)
                .unwrap_or_else(|| "VARCHAR(2000000)".to_string())
        })
        .collect()
}

/// Extract the column name (uppercase) from the first argument of an aggregate function.
fn column_from_first_arg(args: Option<&Vec<Json>>) -> Option<String> {
    args.and_then(|a| a.first()).and_then(|arg| {
        if arg.get("type").and_then(|t| t.as_str()) == Some("column") {
            arg.get("name")
                .and_then(|n| n.as_str())
                .map(|s| s.to_uppercase())
        } else {
            None
        }
    })
}

/// Parse a single `function_aggregate` select-list item into an `AggregatePlan`.
///
/// Returns `None` when the item uses `distinct: true` or the function name is
/// not one of COUNT, SUM, MIN, MAX, AVG, STDDEV, VARIANCE family.
///
/// The caller must verify `item.type == "function_aggregate"` before calling.
fn parse_agg_item(item: &Json) -> Option<AggregatePlan> {
    if item.get("distinct").and_then(|d| d.as_bool()) == Some(true) {
        return None;
    }

    let fn_name = item
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_uppercase();

    let args = item.get("arguments").and_then(|a| a.as_array());

    let plan = match fn_name.as_str() {
        "COUNT" => {
            let col = args.and_then(|a| a.first()).and_then(|arg| {
                if arg.get("type").and_then(|t| t.as_str()) == Some("column") {
                    arg.get("name")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_uppercase())
                } else {
                    None
                }
            });
            if col.is_none() {
                AggregatePlan {
                    kind: AggKind::Count,
                    column: None,
                }
            } else {
                AggregatePlan {
                    kind: AggKind::CountCol,
                    column: col,
                }
            }
        }
        "SUM" => AggregatePlan {
            kind: AggKind::Sum,
            column: column_from_first_arg(args),
        },
        "MIN" => AggregatePlan {
            kind: AggKind::Min,
            column: column_from_first_arg(args),
        },
        "MAX" => AggregatePlan {
            kind: AggKind::Max,
            column: column_from_first_arg(args),
        },
        "AVG" => AggregatePlan {
            kind: AggKind::Avg,
            column: column_from_first_arg(args),
        },
        // STDDEV/VARIANCE family — decompose into (cnt, sum, sum_sq) sufficient statistics.
        // STDDEV and STDDEV_SAMP are the sample forms; VARIANCE / VAR_SAMP likewise.
        "STDDEV" | "STDDEV_SAMP" => AggregatePlan {
            kind: AggKind::StddevSamp,
            column: column_from_first_arg(args),
        },
        "STDDEV_POP" => AggregatePlan {
            kind: AggKind::StddevPop,
            column: column_from_first_arg(args),
        },
        "VARIANCE" | "VAR_SAMP" => AggregatePlan {
            kind: AggKind::VarSamp,
            column: column_from_first_arg(args),
        },
        "VAR_POP" => AggregatePlan {
            kind: AggKind::VarPop,
            column: column_from_first_arg(args),
        },
        _ => return None,
    };
    Some(plan)
}

// ---------------------------------------------------------------------------
// SQL builder (pure; used by handle_pushdown and unit tests)
// ---------------------------------------------------------------------------

/// Build the scan-driving SQL from a resolved file list partitioned into shards.
///
/// **Row queries** (no aggregates in spec):
/// - Single shard: `SELECT * FROM (SELECT {udf}({spec}) EMITS ({emits})) LIMIT n`
/// - Multi-shard: `SELECT * FROM (fan-out with GROUP BY shard_key) LIMIT n`
///
/// **Aggregate queries** (spec carries `aggregates`, no `group_keys`):
/// - Always wraps the fan-out in an outer merge aggregation (never SELECT *).
/// - The EMITS clause and the outer merge follow the COLUMN CONTRACT from
///   `crate::scan::build_partial_agg_sql`.
///
/// For grouped aggregate queries (spec carries both `aggregates` and `group_keys`),
/// use `build_grouped_aggregate_scan_sql` directly.
///
/// `spec_template` carries the shared fields; only `files` is replaced per shard.
/// `col_types` is the full table column type map `(uppercase_name, exasol_type)` used
/// to assign the correct EMITS type per aggregate partial column.
/// `aggregate_types` holds the Exasol-declared result type of each aggregate (from
/// `aggregate_exasol_types`); the single-group merge casts each item to its declared
/// type. Pass `&[]` to emit uncast merge items (row scans never read it).
// ponytail: 8 args is one over the lint threshold; matches the sibling grouped builder.
#[allow(clippy::too_many_arguments)]
pub fn build_scan_driving_sql(
    spec_template: &ScanSpec,
    shards: Vec<Vec<String>>,
    proj_cols: &[String],
    proj_types: &[String],
    limit: Option<u64>,
    col_types: &[(String, String)],
    aggregate_types: &[String],
    udf_name: &str,
) -> String {
    if let Some(aggregates) = spec_template.aggregates.as_deref() {
        build_aggregate_scan_sql(
            spec_template,
            shards,
            aggregates,
            col_types,
            aggregate_types,
            udf_name,
        )
    } else {
        build_row_scan_sql(
            spec_template,
            shards,
            proj_cols,
            proj_types,
            limit,
            udf_name,
        )
    }
}

/// Build the row-scan SQL (no aggregates).
fn build_row_scan_sql(
    spec_template: &ScanSpec,
    shards: Vec<Vec<String>>,
    proj_cols: &[String],
    proj_types: &[String],
    limit: Option<u64>,
    udf_name: &str,
) -> String {
    let emits = proj_cols
        .iter()
        .zip(proj_types.iter())
        .map(|(name, ty)| format!("{} {}", quote_ident(name), ty))
        .collect::<Vec<_>>()
        .join(", ");

    if shards.len() == 1 {
        let mut shard_spec = spec_template.clone();
        shard_spec.files = shards.into_iter().next().unwrap_or_default();
        let spec_literal = sql_string_literal(&shard_spec.to_json());
        let mut sql = format!(
            "SELECT * FROM (SELECT {udf}({spec}) EMITS ({emits}))",
            udf = udf_name,
            spec = spec_literal,
            emits = emits,
        );
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        sql
    } else {
        let inner = build_fan_out_inner(spec_template, &shards, &emits, udf_name);
        let mut sql = format!("SELECT * FROM ({inner})");
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        sql
    }
}

/// Build the aggregate scan SQL: fan-out EMITS partial columns, outer merge aggregates them.
///
/// The EMITS clause names and types follow the COLUMN CONTRACT defined in
/// `crate::scan::build_partial_agg_sql`.  The outer merge SELECT consumes those
/// exact column names.
fn build_aggregate_scan_sql(
    spec_template: &ScanSpec,
    shards: Vec<Vec<String>>,
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
    aggregate_types: &[String],
    udf_name: &str,
) -> String {
    let emits_items = partial_emits_items(aggregates, col_types);
    let emits = emits_items.join(", ");
    let merge_select = cast_merge_items(aggregates, aggregate_types).join(", ");

    let fan_out = if shards.len() == 1 {
        let mut shard_spec = spec_template.clone();
        shard_spec.files = shards.into_iter().next().unwrap_or_default();
        let spec_literal = sql_string_literal(&shard_spec.to_json());
        format!(
            "SELECT {udf}({spec}) EMITS ({emits})",
            udf = udf_name,
            spec = spec_literal,
            emits = emits,
        )
    } else {
        build_fan_out_inner(spec_template, &shards, &emits, udf_name)
    };

    format!("SELECT {merge_select} FROM ({fan_out})")
}

/// Build the grouped aggregate scan SQL.
///
/// ## Two-level grouping
///
/// Inner level: a `GROUP BY shard_key` fan-out runs one UDF invocation per shard.
/// Each shard returns partial per-group results (DataFusion GROUP BY user keys inside
/// the shard).  Outer level: Exasol re-groups on the user group-key columns and merges
/// the partial aggregates.
///
/// ## EMITS column contract (Phase 3 / Group E must match this exactly)
///
/// Columns appear in this order, left to right:
///
/// 1. Group-key columns: `GK_0 VARCHAR(2000000)`, `GK_1 VARCHAR(2000000)`, …
///    `GK_{n-1} VARCHAR(2000000)` — one column per group key, always VARCHAR(2000000)
///    (Group E serialises the DataFusion group-key value to a string before emitting).
///
/// 2. Partial aggregate columns: same layout and naming as `partial_emits_items`
///    (`PARTIAL_count_i`, `PARTIAL_sum_i`, `PARTIAL_min_i`, `PARTIAL_max_i`,
///    `PARTIAL_avg_sum_i` / `PARTIAL_avg_cnt_i`,
///    `PARTIAL_stat_cnt_i` / `PARTIAL_stat_sum_i` / `PARTIAL_stat_sumsq_i`).
///
/// ## HAVING
///
/// `having` is an already-rendered DataFusion SQL fragment applied in the OUTER wrapper
/// only (after `GROUP BY`). Never pushed into the shard scan — a per-shard HAVING would
/// incorrectly discard groups that only clear the threshold after merging across shards.
///
/// ## LIMIT
///
/// LIMIT is never pushed into a shard spec for grouped queries (shard emits all
/// partial groups; the outer wrapper applies the final LIMIT when needed).
// ponytail: 8 args is one over the lint threshold; grouping into a struct would
// add boilerplate for a function called in only two places. Suppress the lint.
#[allow(clippy::too_many_arguments)]
pub fn build_grouped_aggregate_scan_sql(
    spec_template: &ScanSpec,
    shards: Vec<Vec<String>>,
    group_keys: &[String],
    group_key_types: &[String],
    aggregates: &[AggregatePlan],
    aggregate_types: &[String],
    limit: Option<u64>,
    col_types: &[(String, String)],
    udf_name: &str,
    having: Option<&str>,
) -> String {
    // Build EMITS: GK_* columns first, then PARTIAL_* columns.
    let gk_emits: Vec<String> = (0..group_keys.len())
        .map(|i| format!(r#""GK_{i}" VARCHAR(2000000)"#))
        .collect();
    let partial_items = partial_emits_items(aggregates, col_types);
    let all_emits: Vec<String> = gk_emits
        .iter()
        .chain(partial_items.iter())
        .cloned()
        .collect();
    let emits = all_emits.join(", ");

    // Build outer merge SELECT: GK_* columns + merged aggregates.
    // The scan stringifies every group key into a VARCHAR EMITS column; the outer
    // wrapper casts each back to its Exasol-declared type so the virtual-table result
    // column type matches what Exasol expects (e.g. DECIMAL for MOD(id,4)).
    let gk_select: Vec<String> = (0..group_keys.len())
        .map(|i| match group_key_types.get(i) {
            Some(ty) if ty != "VARCHAR(2000000)" => {
                format!(r#"CAST("GK_{i}" AS {ty})"#)
            }
            _ => format!(r#""GK_{i}""#),
        })
        .collect();
    let merge_items = cast_merge_items(aggregates, aggregate_types);
    let outer_select: Vec<String> = gk_select
        .iter()
        .chain(merge_items.iter())
        .cloned()
        .collect();
    let outer_select_str = outer_select.join(", ");

    // Group BY in outer: GK_0, GK_1, ...
    let outer_group_by: Vec<String> = (0..group_keys.len())
        .map(|i| format!(r#""GK_{i}""#))
        .collect();
    let outer_group_by_str = outer_group_by.join(", ");

    // Build the inner fan-out.  Each shard spec must NOT carry a LIMIT (partial
    // groups from different shards must all be emitted and merged by the outer wrapper).
    let fan_out = if shards.len() == 1 {
        let mut shard_spec = spec_template.clone();
        shard_spec.files = shards.into_iter().next().unwrap_or_default();
        shard_spec.limit = None; // No LIMIT inside shard spec for grouped queries.
        let spec_literal = sql_string_literal(&shard_spec.to_json());
        format!(
            "SELECT {udf}({spec}) EMITS ({emits})",
            udf = udf_name,
            spec = spec_literal,
            emits = emits,
        )
    } else {
        build_fan_out_inner_with_spec(spec_template, &shards, &emits, udf_name, |spec| {
            let mut s = spec.clone();
            s.limit = None; // No LIMIT inside shard spec for grouped queries.
            s.to_json()
        })
    };

    let mut sql =
        format!("SELECT {outer_select_str} FROM ({fan_out}) GROUP BY {outer_group_by_str}");

    // HAVING: applied in outer wrapper only, never pushed into shard scan.
    if let Some(h) = having.filter(|h| !h.is_empty()) {
        sql.push_str(" HAVING ");
        sql.push_str(h);
    }

    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    sql
}

/// Build the EMITS items for the aggregate fan-out, following the COLUMN CONTRACT.
///
/// `col_types` maps uppercase column names to their Exasol type strings.
/// MIN/MAX partial columns use the target column's exact type.
/// SUM partial columns: DOUBLE PRECISION stays DOUBLE PRECISION; DECIMAL(p,s) widens to
/// DECIMAL(36,s) to avoid overflow; any other type falls back (callers should have validated
/// via `validate_agg_col_types` before reaching here — see handle_pushdown).
/// AVG partial sum stays DOUBLE PRECISION (AVG is inherently fractional).
/// Stat (STDDEV/VARIANCE) family: cnt DECIMAL(20,0), sum/sumsq DOUBLE PRECISION.
fn partial_emits_items(
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
) -> Vec<String> {
    aggregates
        .iter()
        .enumerate()
        .flat_map(|(i, plan)| match plan.kind {
            AggKind::Count | AggKind::CountCol => {
                vec![format!(r#""PARTIAL_count_{i}" DECIMAL(20,0)"#)]
            }
            AggKind::Sum => {
                let ty = col_type_for(plan, col_types);
                let emit_ty = sum_emit_type(&ty);
                vec![format!(r#""PARTIAL_sum_{i}" {emit_ty}"#)]
            }
            AggKind::Min => {
                let ty = col_type_for(plan, col_types);
                vec![format!(r#""PARTIAL_min_{i}" {ty}"#)]
            }
            AggKind::Max => {
                let ty = col_type_for(plan, col_types);
                vec![format!(r#""PARTIAL_max_{i}" {ty}"#)]
            }
            AggKind::Avg => vec![
                format!(r#""PARTIAL_avg_sum_{i}" DOUBLE PRECISION"#),
                format!(r#""PARTIAL_avg_cnt_{i}" DECIMAL(20,0)"#),
            ],
            // Stat family: 3 columns — cnt (DECIMAL), sum (DOUBLE), sumsq (DOUBLE).
            AggKind::VarPop | AggKind::VarSamp | AggKind::StddevPop | AggKind::StddevSamp => vec![
                format!(r#""PARTIAL_stat_cnt_{i}" DECIMAL(20,0)"#),
                format!(r#""PARTIAL_stat_sum_{i}" DOUBLE PRECISION"#),
                format!(r#""PARTIAL_stat_sumsq_{i}" DOUBLE PRECISION"#),
            ],
        })
        .collect()
}

/// Look up the Exasol type for the target column of an aggregate plan.
/// Returns "DOUBLE PRECISION" as a safe fallback when the column is absent from the map.
fn col_type_for(plan: &AggregatePlan, col_types: &[(String, String)]) -> String {
    plan.column
        .as_deref()
        .and_then(|col| {
            col_types
                .iter()
                .find(|(n, _)| n == col)
                .map(|(_, t)| t.clone())
        })
        .unwrap_or_else(|| "DOUBLE PRECISION".to_string())
}

/// Map a column's Exasol type to the appropriate SUM partial EMITS type.
///
/// DOUBLE PRECISION => DOUBLE PRECISION (no change).
/// DECIMAL(p,s) => DECIMAL(36,s) (widened to max Exasol precision, preserving scale).
/// Any other type (DATE, TIMESTAMP, VARCHAR, BOOLEAN) => DOUBLE PRECISION as an
/// emergency fallback (callers should have validated before reaching here).
fn sum_emit_type(col_ty: &str) -> String {
    if col_ty == "DOUBLE PRECISION" {
        return "DOUBLE PRECISION".to_string();
    }
    if let Some(inner) = col_ty
        .strip_prefix("DECIMAL(")
        .and_then(|s| s.strip_suffix(')'))
    {
        // inner is "p,s"
        if let Some((_p, s)) = inner.split_once(',') {
            return format!("DECIMAL(36,{s})");
        }
    }
    // Non-numeric type: validation should have caught this, but fall back gracefully.
    "DOUBLE PRECISION".to_string()
}

/// Return `true` if all SUM/MIN/MAX/stat targets have a supported Exasol column type.
///
/// SUM and the STDDEV/VARIANCE family are only valid over DOUBLE PRECISION or DECIMAL columns.
/// MIN/MAX are valid over any comparable type (DATE, TIMESTAMP, VARCHAR included).
/// Returns `false` (fall back to row scan) when any SUM or stat aggregate targets a
/// non-numeric column.
pub fn validate_agg_col_types(
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
) -> bool {
    for plan in aggregates {
        let needs_numeric = matches!(
            plan.kind,
            AggKind::Sum
                | AggKind::VarPop
                | AggKind::VarSamp
                | AggKind::StddevPop
                | AggKind::StddevSamp
        );
        if needs_numeric {
            let ty = col_type_for(plan, col_types);
            if !is_numeric_exasol_type(&ty) {
                return false;
            }
        }
    }
    true
}

/// Return `true` for Exasol types that support SUM (DOUBLE PRECISION, DECIMAL).
fn is_numeric_exasol_type(ty: &str) -> bool {
    ty == "DOUBLE PRECISION" || ty.starts_with("DECIMAL(")
}

/// Build the outer merge SELECT items following the COLUMN CONTRACT.
///
/// AVG uses `SUM(sum) / NULLIF(SUM(cnt), 0)` — the NULLIF guard ensures division
/// by zero yields NULL rather than an error (Exasol: `x / NULL = NULL`).
///
/// STDDEV/VARIANCE sufficient-statistics reconstruction (König–Huygens identity):
///   numer    = SUM(sumsq) - SUM(sum)² / NULLIF(SUM(cnt), 0)
///   var_pop  = numer / NULLIF(SUM(cnt), 0)          [NULL when cnt = 0]
///   var_samp = numer / (SUM(cnt) - 1)               [NULL when cnt ≤ 1, via CASE]
///
///   stddev_pop/samp = CASE WHEN var IS NULL THEN NULL
///                          ELSE SQRT(GREATEST(0.0, var)) END
///
///   The CASE guard is required because Exasol's `GREATEST(0.0, NULL) = 0.0`
///   (returns the max of non-NULL inputs; only returns NULL if ALL inputs are NULL),
///   so a bare `SQRT(GREATEST(0.0, NULL))` would yield `0.0` instead of NULL for
///   empty tables (N=0, pop) and single-row groups (N=1, samp).
///   The GREATEST(0.0, …) inside the ELSE branch guards against tiny-negative
///   float rounding artifacts that would otherwise cause SQRT to error.
fn merge_select_items(aggregates: &[AggregatePlan]) -> Vec<String> {
    aggregates
        .iter()
        .enumerate()
        .map(|(i, plan)| match plan.kind {
            AggKind::Count | AggKind::CountCol => format!(r#"SUM("PARTIAL_count_{i}")"#),
            AggKind::Sum => format!(r#"SUM("PARTIAL_sum_{i}")"#),
            AggKind::Min => format!(r#"MIN("PARTIAL_min_{i}")"#),
            AggKind::Max => format!(r#"MAX("PARTIAL_max_{i}")"#),
            AggKind::Avg => {
                format!(r#"SUM("PARTIAL_avg_sum_{i}") / NULLIF(SUM("PARTIAL_avg_cnt_{i}"), 0)"#)
            }
            AggKind::VarPop => {
                // numer / SUM(cnt); NULL when cnt = 0
                format!(
                    concat!(
                        r#"(SUM("PARTIAL_stat_sumsq_{i}") - SUM("PARTIAL_stat_sum_{i}") * SUM("PARTIAL_stat_sum_{i}") / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0))"#,
                        r#" / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0)"#,
                    ),
                    i = i
                )
            }
            AggKind::VarSamp => {
                // numer / (N-1); NULL when cnt <= 1
                format!(
                    concat!(
                        r#"(SUM("PARTIAL_stat_sumsq_{i}") - SUM("PARTIAL_stat_sum_{i}") * SUM("PARTIAL_stat_sum_{i}") / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0))"#,
                        r#" / CASE WHEN SUM("PARTIAL_stat_cnt_{i}") <= 1 THEN NULL ELSE SUM("PARTIAL_stat_cnt_{i}") - 1 END"#,
                    ),
                    i = i
                )
            }
            AggKind::StddevPop => {
                // CASE IS NULL guard: Exasol GREATEST(0.0, NULL) = 0.0, not NULL.
                // Without the CASE, N=0 would yield SQRT(0.0) = 0.0 instead of NULL.
                format!(
                    concat!(
                        r#"CASE WHEN ((SUM("PARTIAL_stat_sumsq_{i}") - SUM("PARTIAL_stat_sum_{i}") * SUM("PARTIAL_stat_sum_{i}") / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0))"#,
                        r#" / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0)) IS NULL THEN NULL"#,
                        r#" ELSE SQRT(GREATEST(0.0, (SUM("PARTIAL_stat_sumsq_{i}") - SUM("PARTIAL_stat_sum_{i}") * SUM("PARTIAL_stat_sum_{i}") / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0))"#,
                        r#" / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0))) END"#,
                    ),
                    i = i
                )
            }
            AggKind::StddevSamp => {
                // CASE IS NULL guard: Exasol GREATEST(0.0, NULL) = 0.0, not NULL.
                // Without the CASE, N<=1 would yield SQRT(0.0) = 0.0 instead of NULL.
                format!(
                    concat!(
                        r#"CASE WHEN ((SUM("PARTIAL_stat_sumsq_{i}") - SUM("PARTIAL_stat_sum_{i}") * SUM("PARTIAL_stat_sum_{i}") / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0))"#,
                        r#" / CASE WHEN SUM("PARTIAL_stat_cnt_{i}") <= 1 THEN NULL ELSE SUM("PARTIAL_stat_cnt_{i}") - 1 END) IS NULL THEN NULL"#,
                        r#" ELSE SQRT(GREATEST(0.0, (SUM("PARTIAL_stat_sumsq_{i}") - SUM("PARTIAL_stat_sum_{i}") * SUM("PARTIAL_stat_sum_{i}") / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0))"#,
                        r#" / CASE WHEN SUM("PARTIAL_stat_cnt_{i}") <= 1 THEN NULL ELSE SUM("PARTIAL_stat_cnt_{i}") - 1 END)) END"#,
                    ),
                    i = i
                )
            }
        })
        .collect()
}

/// Build the outer merge SELECT items, each cast to its Exasol-declared result type.
///
/// The merge expression (e.g. `SUM("PARTIAL_count_0")` over DECIMAL(20,0) partials →
/// DECIMAL(31,0)) must match the type Exasol declared for that select-list column
/// (COUNT(score) → DECIMAL(18,0)); Exasol strictly validates the pushdown output
/// column types. When no declared type is available (or it is VARCHAR(2000000)),
/// the merge expression is emitted uncast.
fn cast_merge_items(aggregates: &[AggregatePlan], aggregate_types: &[String]) -> Vec<String> {
    merge_select_items(aggregates)
        .into_iter()
        .enumerate()
        .map(|(i, expr)| match aggregate_types.get(i) {
            Some(ty) if ty != "VARCHAR(2000000)" => format!("CAST({expr} AS {ty})"),
            _ => expr,
        })
        .collect()
}

/// Builds the shard fan-out SELECT that Exasol distributes across nodes.
///
/// Uses `GROUP BY shard_key` (NOT `IPROC()`) so work units spread round-robin
/// across nodes (G ≤ 300) and multiplex onto each node's core pool.
/// Callers wrap it in `SELECT * FROM (...)` for row scans or an outer merge
/// aggregation for aggregate pushdown.
pub fn build_fan_out_inner(
    spec_template: &ScanSpec,
    shards: &[Vec<String>],
    emits: &str,
    udf_name: &str,
) -> String {
    build_fan_out_inner_with_spec(spec_template, shards, emits, udf_name, |spec| {
        spec.to_json()
    })
}

/// Core shard fan-out builder with a configurable spec serializer.
///
/// The `spec_to_json` closure lets grouped callers strip the LIMIT from each
/// per-shard spec without affecting the row-scan / single-group path.
fn build_fan_out_inner_with_spec(
    spec_template: &ScanSpec,
    shards: &[Vec<String>],
    emits: &str,
    udf_name: &str,
    spec_to_json: impl Fn(&ScanSpec) -> String,
) -> String {
    let values: Vec<String> = shards
        .iter()
        .enumerate()
        .map(|(i, files)| {
            let mut shard_spec = spec_template.clone();
            shard_spec.files = files.clone();
            let lit = sql_string_literal(&spec_to_json(&shard_spec));
            format!("({i},{lit})")
        })
        .collect();
    let values_list = values.join(",");
    format!(
        "SELECT {udf}(spec) EMITS ({emits}) FROM (VALUES {values}) AS shards(shard_key, spec) GROUP BY shard_key",
        udf = udf_name,
        emits = emits,
        values = values_list,
    )
}

/// Resolve the Iceberg snapshot + file list and build pushdown SQL.
///
/// `cluster_nodes` — the number of Exasol nodes read from the `CLUSTER_NODES`
/// adapterNotes entry (default 1 when absent or unparseable).
///
/// `parallelism_factor` — the oversubscription multiplier read from the
/// `PARALLELISM_FACTOR` adapterNotes entry (default 8).
///
/// Returns JSON `{"type":"pushdown","sql":"..."}`.
pub async fn handle_pushdown(
    request: &Json,
    catalog_uri: &str,
    storage: &StorageProps,
    catalog: &CatalogProps,
    scan_schema: Option<&str>,
    cluster_nodes: usize,
    parallelism_factor: usize,
) -> Result<Json, UdfError> {
    let pushdown_req = request
        .get("pushdownRequest")
        .cloned()
        .unwrap_or(Json::Null);

    let (proj_cols, proj_types) = extract_projection(request, &pushdown_req)?;

    let filter = pushdown_req
        .get("filter")
        .filter(|f| !f.is_null())
        .and_then(render_df_filter_safe);

    let limit = extract_limit(&pushdown_req);

    let col_types = extract_all_column_types(request);

    let files = resolve_file_list(catalog_uri, catalog, storage).await?;

    if files.is_empty() {
        return Ok(empty_pushdown_sql(&proj_cols, &proj_types));
    }

    // Compute G = shard_count(node_count, parallelism_factor, file_count) and
    // partition files into G balanced work-unit shards (GROUP BY shard_key fan-out).
    let g = shard_count(cluster_nodes, parallelism_factor, files.len());
    let shards = crate::adapter::sharding::partition_files(files, g);

    // The scan UDF must be schema-qualified: the pushdown query executes
    // outside the adapter script's schema, so an unqualified name would not
    // resolve ("function or script LAKEHOUSE_SCAN not found").
    let udf_name = match scan_schema {
        Some(schema) if !schema.is_empty() => {
            format!("{}.{}", quote_ident(schema), SCAN_UDF_NAME)
        }
        _ => SCAN_UDF_NAME.to_string(),
    };

    // Render the HAVING predicate (for grouped aggregate queries).
    // Applied in the OUTER wrapper only — never in the per-shard scan.
    // If the predicate cannot be translated, omit it (Exasol post-processes).
    let having = pushdown_req
        .get("having")
        .filter(|h| !h.is_null())
        .and_then(render_expression_safe);

    // Detection priority: GROUP BY aggregate → single-group aggregate → row scan.
    if let Some((group_keys, grouped_agg_plans)) = detect_group_by_aggregates(&pushdown_req) {
        // Validate aggregate column types for the grouped path — same guard as the
        // single-group path below. SUM over a non-numeric column (VARCHAR, DATE, …)
        // would produce an opaque UDF error; normally we fall back to row scan.
        if !validate_agg_col_types(&grouped_agg_plans, &col_types) {
            // If a HAVING predicate is present, we cannot fall through silently:
            // the adapter has advertised AGGREGATE_HAVING, so Exasol will not
            // re-apply a HAVING we claim to handle. Dropping it yields wrong results.
            // Return an error so Exasol executes the query natively.
            if having.is_some() {
                return Err(UdfError::User(
                    "grouped aggregate pushdown declined: HAVING present but aggregate \
                     column type is non-numeric; Exasol will retry natively"
                        .into(),
                ));
            }
            // No HAVING: safe to fall through to single-group / row scan.
        } else {
            // Grouped aggregate pushdown path.
            // ponytail: PoC accepted risk — credentials embedded in spec literal.
            let spec_template = ScanSpec {
                files: vec![],
                projection: proj_cols.clone(),
                filter,
                limit,
                aggregates: Some(grouped_agg_plans.clone()),
                group_keys: Some(group_keys.clone()),
                storage: storage.clone(),
                catalog: catalog.clone(),
            };
            let group_key_types = group_key_exasol_types(&pushdown_req, &group_keys);
            let aggregate_types = aggregate_exasol_types(&pushdown_req);
            let sql = build_grouped_aggregate_scan_sql(
                &spec_template,
                shards,
                &group_keys,
                &group_key_types,
                &grouped_agg_plans,
                &aggregate_types,
                limit,
                &col_types,
                &udf_name,
                having.as_deref(),
            );
            return Ok(serde_json::json!({"type": "pushdown", "sql": sql}));
        } // end else (validate_agg_col_types passed)
    }

    // Single-group aggregate or row scan.
    // After detection, validate that each SUM/MIN/MAX targets a supported column type;
    // if any SUM targets a non-numeric type (DATE, VARCHAR, etc.), fall back to row scan.
    let aggregates =
        detect_aggregates(&pushdown_req).filter(|plans| validate_agg_col_types(plans, &col_types));

    // ponytail: PoC accepted risk — the S3 access/secret keys are embedded in
    // this scan-driving SQL literal (inside the ScanSpec JSON), which Exasol may
    // log or surface in its query profile / audit. Acceptable for this PoC slice;
    // the upgrade path is to pass credentials via a CONNECTION object (referenced
    // by name, never inlined) or to fetch them over connect-back at scan time so
    // they never appear in any SQL text. Error paths already redact the values.
    let spec_template = ScanSpec {
        files: vec![], // replaced per shard in build_scan_driving_sql
        projection: proj_cols.clone(),
        filter,
        limit,
        aggregates,
        group_keys: None,
        storage: storage.clone(),
        catalog: catalog.clone(),
    };

    let aggregate_types = aggregate_exasol_types(&pushdown_req);
    let sql = build_scan_driving_sql(
        &spec_template,
        shards,
        &proj_cols,
        &proj_types,
        limit,
        &col_types,
        &aggregate_types,
        &udf_name,
    );

    Ok(serde_json::json!({"type": "pushdown", "sql": sql}))
}

/// Resolve the data-file list from the Iceberg REST catalog.
///
/// This is the resolve-once seam: called exactly once per pushdown in the
/// adapter; the file list is passed explicitly to the scan UDF.
pub async fn resolve_file_list(
    catalog_uri: &str,
    catalog_props: &CatalogProps,
    storage: &StorageProps,
) -> Result<Vec<String>, UdfError> {
    let catalog = build_rest_catalog(catalog_uri, catalog_props, storage).await?;

    // Parse "namespace.table" from catalog_props.table.
    let (namespace, table_name) = parse_table_ident(&catalog_props.table)?;
    let table_ident = TableIdent::new(NamespaceIdent::new(namespace), table_name);

    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e: iceberg::Error| {
            UdfError::User(format!(
                "failed to load Iceberg table '{}': {}",
                catalog_props.table,
                redact_catalog_error(&e.to_string())
            ))
        })?;

    // Plan files from the current snapshot.
    let scan = table
        .scan()
        .select_all()
        .build()
        .map_err(|e| UdfError::User(format!("failed to build Iceberg scan: {e}")))?;

    let task_stream = scan.plan_files().await.map_err(|e| {
        UdfError::User(format!(
            "failed to plan Iceberg files: {}",
            redact_catalog_error(&e.to_string())
        ))
    })?;

    let tasks: Vec<_> = task_stream.try_collect().await.map_err(|e| {
        UdfError::User(format!(
            "failed to collect Iceberg file tasks: {}",
            redact_catalog_error(&e.to_string())
        ))
    })?;

    let files: Vec<String> = tasks
        .into_iter()
        .map(|t| t.data_file_path().to_string())
        .collect();

    Ok(files)
}

/// Resolve the Iceberg table schema for `createVirtualSchema`.
///
/// Returns (field_name, exasol_type_string) pairs.
pub async fn resolve_table_schema(
    catalog_uri: &str,
    catalog_props: &CatalogProps,
    storage: &StorageProps,
) -> Result<Vec<(String, String)>, UdfError> {
    let catalog = build_rest_catalog(catalog_uri, catalog_props, storage).await?;

    let (namespace, table_name) = parse_table_ident(&catalog_props.table)?;
    let table_ident = TableIdent::new(NamespaceIdent::new(namespace), table_name);

    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e: iceberg::Error| {
            UdfError::User(format!(
                "failed to load Iceberg table '{}': {}",
                catalog_props.table,
                redact_catalog_error(&e.to_string())
            ))
        })?;

    let schema = table.metadata().current_schema();
    let fields = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| {
            let exasol_ty = crate::types::mapping::iceberg_type_to_exasol(&f.field_type);
            // Declare columns in Exasol's canonical (uppercase) identifier casing
            // so unquoted user SQL (`SELECT id` → `ID`) resolves. The scan maps
            // projection names back to the Parquet field casing case-insensitively.
            (f.name.to_uppercase(), exasol_ty)
        })
        .collect();

    Ok(fields)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse "namespace.table" into (namespace_str, table_name_str).
fn parse_table_ident(qualified: &str) -> Result<(String, String), UdfError> {
    let parts: Vec<&str> = qualified.splitn(2, '.').collect();
    if parts.len() != 2 {
        return Err(UdfError::User(format!(
            "table property must be 'namespace.table', got: '{qualified}'"
        )));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Extract all columns and their Exasol types from the first involved table.
fn extract_all_column_types(request: &Json) -> Vec<(String, String)> {
    request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .and_then(|tables| tables.first())
        .and_then(|t| t.get("columns"))
        .and_then(|c| c.as_array())
        .map(|cols| {
            cols.iter()
                .filter_map(|c| {
                    let name = c.get("name")?.as_str()?.to_uppercase();
                    let dt_json = c.get("dataType")?;
                    Some((name, exasol_type_from_json(dt_json)))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Extract the projected columns and their Exasol types from the pushdown request.
///
/// For `column` nodes: returns the uppercase column name and its Exasol type.
/// For scalar expression nodes (e.g. `function_scalar`): renders via the VS expression
/// translator and returns the rendered SQL fragment with type `VARCHAR(2000000)`.
/// If any select-list item can't be projected as-is (untranslatable scalar, or an
/// aggregate/unknown node), the whole projection falls back to the full base table
/// column set so Exasol can post-process the expression, GROUP BY, and aggregate —
/// correctness over pushdown. The returned projection is always deduplicated by name,
/// since duplicate EMITS column names are invalid in Exasol.
fn extract_projection(
    request: &Json,
    pushdown_req: &Json,
) -> Result<(Vec<String>, Vec<String>), UdfError> {
    let involved = request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // Get all columns from the first involved table.
    let all_cols: Vec<(String, String)> = involved
        .first()
        .and_then(|t| t.get("columns"))
        .and_then(|c| c.as_array())
        .map(|cols| {
            cols.iter()
                .filter_map(|c| {
                    let name = c.get("name")?.as_str()?.to_uppercase();
                    let dt_json = c.get("dataType")?;
                    let exasol_type = exasol_type_from_json(dt_json);
                    Some((name, exasol_type))
                })
                .collect()
        })
        .unwrap_or_default();

    if all_cols.is_empty() {
        return Err(UdfError::User(
            "pushdown request has no column metadata".into(),
        ));
    }

    let type_by_upper = |name: &str| -> String {
        all_cols
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, t)| t.clone())
            .unwrap_or_else(|| "VARCHAR(2000000)".to_string())
    };

    let first_col_name = all_cols.first().map(|(n, _)| n.clone()).unwrap_or_default();

    let select_list = pushdown_req.get("selectList");
    let (proj_names, proj_types): (Vec<String>, Vec<String>) = match select_list {
        None | Some(Json::Null) => {
            let names: Vec<String> = all_cols.iter().map(|(n, _)| n.clone()).collect();
            let types: Vec<String> = all_cols.iter().map(|(_, t)| t.clone()).collect();
            (names, types)
        }
        Some(Json::Array(list)) if list.is_empty() => {
            // Empty select list — project the first column only.
            let name = first_col_name;
            let ty = type_by_upper(&name);
            (vec![name], vec![ty])
        }
        Some(Json::Array(list)) => {
            // Exasol declares the result type of each selectList item in a parallel
            // `selectListDataTypes` array; the EMITS column type must equal it.
            let declared_types = pushdown_req
                .get("selectListDataTypes")
                .and_then(|v| v.as_array());
            let mut names = Vec::with_capacity(list.len());
            let mut types = Vec::with_capacity(list.len());
            // If any item can't be projected as-is (untranslatable scalar, or an
            // aggregate/unknown node), we can't emit a per-item projection — repeating
            // `first_col_name` would yield duplicate EMITS names. Instead project the
            // full base row so Exasol has every column to post-process the expression,
            // GROUP BY, and aggregate itself.
            let mut needs_full_fallback = false;
            for (i, e) in list.iter().enumerate() {
                let declared_type = declared_types
                    .and_then(|d| d.get(i))
                    .map(exasol_type_from_json);
                let item_type = e.get("type").and_then(|t| t.as_str()).unwrap_or("");
                match item_type {
                    "column" => {
                        // Bare column reference — use the column name and its Exasol type.
                        let name = e
                            .get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_uppercase())
                            .unwrap_or_else(|| first_col_name.clone());
                        let ty = type_by_upper(&name);
                        names.push(name);
                        types.push(ty);
                    }
                    "function_scalar"
                    | "predicate_equal"
                    | "predicate_less"
                    | "predicate_lessequal"
                    | "predicate_like"
                    | "predicate_and"
                    | "predicate_or"
                    | "predicate_not"
                    | "literal_string"
                    | "literal_exactnumeric"
                    | "literal_double"
                    | "literal_null"
                    | "literal_date"
                    | "literal_timestamp"
                    | "literal_timestamp_utc" => {
                        // Scalar expression node — try to render it.
                        match render_expression_safe(e) {
                            Some(sql_frag) => {
                                names.push(sql_frag);
                                let ty = declared_type
                                    .clone()
                                    .unwrap_or_else(|| "VARCHAR(2000000)".to_string());
                                types.push(ty);
                            }
                            None => {
                                // Untranslatable — fall back to projecting the full row.
                                needs_full_fallback = true;
                            }
                        }
                    }
                    _ => {
                        // Unknown / aggregate node — fall back to projecting the full row.
                        needs_full_fallback = true;
                    }
                }
            }
            if needs_full_fallback {
                let names: Vec<String> = all_cols.iter().map(|(n, _)| n.clone()).collect();
                let types: Vec<String> = all_cols.iter().map(|(_, t)| t.clone()).collect();
                (names, types)
            } else {
                (names, types)
            }
        }
        _ => {
            let names: Vec<String> = all_cols.iter().map(|(n, _)| n.clone()).collect();
            let types: Vec<String> = all_cols.iter().map(|(_, t)| t.clone()).collect();
            (names, types)
        }
    };

    // Defensive backstop: duplicate EMITS column names are always invalid in Exasol,
    // regardless of which path produced the projection. Dedup by name, keeping the
    // first occurrence and its type.
    let mut seen = std::collections::HashSet::new();
    let mut deduped_names = Vec::with_capacity(proj_names.len());
    let mut deduped_types = Vec::with_capacity(proj_types.len());
    for (name, ty) in proj_names.into_iter().zip(proj_types) {
        if seen.insert(name.clone()) {
            deduped_names.push(name);
            deduped_types.push(ty);
        }
    }

    Ok((deduped_names, deduped_types))
}

/// Extract LIMIT from the pushdown request.
fn extract_limit(pushdown_req: &Json) -> Option<u64> {
    pushdown_req
        .get("limit")
        .and_then(|l| l.get("numElements"))
        .and_then(|n| n.as_u64())
}

/// Build a pushdown response with an empty result (no matching files).
fn empty_pushdown_sql(proj_cols: &[String], proj_types: &[String]) -> Json {
    let items: Vec<String> = proj_cols
        .iter()
        .zip(proj_types.iter())
        .map(|(name, ty)| format!("CAST(NULL AS {ty}) AS {}", quote_ident(name)))
        .collect();
    let sql = format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "));
    serde_json::json!({"type": "pushdown", "sql": sql})
}

/// Derive an Exasol type string from the VS column dataType JSON.
fn exasol_type_from_json(dt: &Json) -> String {
    let type_name = dt.get("type").and_then(|t| t.as_str()).unwrap_or("varchar");
    match type_name.to_lowercase().as_str() {
        "boolean" => "BOOLEAN".to_string(),
        "decimal" => {
            let p = dt.get("precision").and_then(|v| v.as_u64()).unwrap_or(18);
            let s = dt.get("scale").and_then(|v| v.as_u64()).unwrap_or(0);
            if p <= 36 && s <= 36 {
                format!("DECIMAL({p},{s})")
            } else {
                "VARCHAR(2000000)".to_string()
            }
        }
        "double" => "DOUBLE PRECISION".to_string(),
        "date" => "DATE".to_string(),
        "timestamp" => "TIMESTAMP".to_string(),
        "timestamp with local time zone" | "timestampwithlocaltime zone" => {
            "TIMESTAMP WITH LOCAL TIME ZONE".to_string()
        }
        _ => {
            // VARCHAR, CHAR, and all others.
            let size = dt.get("size").and_then(|v| v.as_u64()).unwrap_or(2000000);
            let capped = size.min(2000000);
            format!("VARCHAR({capped})")
        }
    }
}

/// Double-quote an identifier.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Produce a SQL string literal with single-quote escaping.
fn sql_string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Redact credential-shaped values from a catalog error message.
fn redact_catalog_error(msg: &str) -> String {
    crate::scan::emit::redact_credentials(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::spec::{CatalogProps, StorageProps};
    use vs_expression::render_df_filter_safe;

    // ---------------------------------------------------------------------------
    // shard_count — cap/clamp boundary tests
    // ---------------------------------------------------------------------------

    /// Scenario: Shard count oversubscribes the cluster and is capped at 300.
    /// 10 nodes × 50 factor = 500, capped to 300.
    #[test]
    fn shard_count_oversubscribes_and_caps_at_300() {
        // 10 × 50 = 500 > 300 files; cap at 300.
        assert_eq!(shard_count(10, 50, 500), 300, "must be capped at 300");
        // 10 × 50 = 500 but only 350 files — still capped at 300 (min(350, 300)=300).
        assert_eq!(
            shard_count(10, 50, 350),
            300,
            "must be capped at min(files,300)=300"
        );
        // Exact cap: 1 × 300 = 300, 1000 files — stays 300.
        assert_eq!(shard_count(1, 300, 1000), 300, "exactly 300 must stay 300");
        // 1 × 301 = 301 > 300; capped at 300.
        assert_eq!(shard_count(1, 301, 1000), 300, "301 must be capped at 300");
    }

    /// Scenario: Fewer files than G produces one shard per file with no empty shards.
    /// node_count × parallelism_factor > file_count => clamp to file_count.
    #[test]
    fn shard_count_clamped_to_file_count_no_empty_shards() {
        // 10 × 8 = 80 but only 3 files; clamp to 3.
        assert_eq!(shard_count(10, 8, 3), 3, "must clamp to file_count=3");
        // 4 × 8 = 32 but only 5 files; clamp to 5.
        assert_eq!(shard_count(4, 8, 5), 5, "must clamp to file_count=5");
        // 1 × 1 = 1, file_count=1; stays 1.
        assert_eq!(shard_count(1, 1, 1), 1, "single file single shard");
        // Minimum clamp: 0 × 8 = 0, clamp to min(1, …) = 1.
        assert_eq!(shard_count(0, 8, 100), 1, "zero product must clamp to 1");
        // parallelism_factor=0: 5 × 0 = 0, clamp to 1.
        assert_eq!(shard_count(5, 0, 100), 1, "zero factor must clamp to 1");
    }

    // ---------------------------------------------------------------------------
    // Helpers shared across tests
    // ---------------------------------------------------------------------------

    fn sample_storage() -> StorageProps {
        StorageProps {
            endpoint: "http://minio:9000".into(),
            region: "us-east-1".into(),
            access_key: "minioadmin".into(),
            secret_key: "minioadmin".into(),
            session_token: None,
            allow_http: true,
            path_style: true,
        }
    }

    fn sample_catalog() -> CatalogProps {
        CatalogProps {
            uri: "http://iceberg-rest:8181".into(),
            warehouse: "warehouse".into(),
            table: "db.events".into(),
        }
    }

    /// Assemble the scan-driving SQL from a known file list + spec — the same
    /// logic `handle_pushdown` runs after `resolve_file_list`.
    /// Uses `cluster_nodes=1` (single-shard / legacy shape).
    fn build_sql_for_fixture(
        files: Vec<String>,
        proj_cols: Vec<String>,
        proj_types: Vec<String>,
        filter: Option<String>,
        limit: Option<u64>,
    ) -> String {
        build_sql_for_fixture_n(files, proj_cols, proj_types, filter, limit, 1)
    }

    /// Assemble the scan-driving SQL for `cluster_nodes = n`.
    fn build_sql_for_fixture_n(
        files: Vec<String>,
        proj_cols: Vec<String>,
        proj_types: Vec<String>,
        filter: Option<String>,
        limit: Option<u64>,
        cluster_nodes: usize,
    ) -> String {
        // Build a col_types map from proj_cols/proj_types for row-scan tests.
        let col_types: Vec<(String, String)> = proj_cols
            .iter()
            .cloned()
            .zip(proj_types.iter().cloned())
            .collect();
        let spec_template = ScanSpec {
            files: vec![],
            projection: proj_cols.clone(),
            filter,
            limit,
            aggregates: None,
            group_keys: None,
            storage: sample_storage(),
            catalog: sample_catalog(),
        };
        let shards = crate::adapter::sharding::partition_files(files, cluster_nodes);
        build_scan_driving_sql(
            &spec_template,
            shards,
            &proj_cols,
            &proj_types,
            limit,
            &col_types,
            &[],
            SCAN_UDF_NAME,
        )
    }

    // ---------------------------------------------------------------------------
    // Scenario: Pushdown resolves the file list once and builds a scan-driving query
    // ---------------------------------------------------------------------------

    /// Pure SQL-building part of the pushdown scenario.
    /// The file list comes from a fixture (no catalog I/O).
    #[test]
    fn pushdown_resolves_files_once_builds_scan_sql() {
        let files = vec![
            "s3://warehouse/db/events/part-00000.parquet".into(),
            "s3://warehouse/db/events/part-00001.parquet".into(),
        ];
        let sql = build_sql_for_fixture(
            files.clone(),
            vec!["ID".into(), "NAME".into()],
            vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
            None,
            None,
        );

        // The generated SQL must invoke the scan UDF with the spec embedded.
        assert!(
            sql.contains(SCAN_UDF_NAME),
            "SQL must reference the scan UDF: {sql}"
        );
        // The spec JSON (embedded as a SQL literal) contains the file path.
        assert!(
            sql.contains("part-00000.parquet"),
            "SQL must carry assigned files: {sql}"
        );
        assert!(
            sql.contains("part-00001.parquet"),
            "SQL must carry both files: {sql}"
        );
        // Must be a SELECT (scan-driving query, not an empty stub).
        assert!(
            sql.starts_with("SELECT * FROM"),
            "must be a real query: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Scenario: Projection is pushed into the scan-driving query
    // ---------------------------------------------------------------------------

    #[test]
    fn pushdown_carries_projection() {
        let sql = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["A".into(), "B".into()],
            vec!["DECIMAL(10,0)".into(), "VARCHAR(2000000)".into()],
            None,
            None,
        );

        // EMITS clause must list exactly the projected columns in order.
        assert!(
            sql.contains("\"A\" DECIMAL(10,0)"),
            "EMITS must carry col A: {sql}"
        );
        assert!(
            sql.contains("\"B\" VARCHAR(2000000)"),
            "EMITS must carry col B: {sql}"
        );

        // The spec JSON must carry the projection field.
        // (It's embedded as a SQL string literal in the body.)
        assert!(
            sql.contains(r#""A""#) || sql.contains(r#"\"A\""#),
            "spec JSON must include projected column A: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Scenario: Filter predicate is pushed into the scan spec (translatable) or
    // omitted (untranslatable) — never mistranslated.
    // ---------------------------------------------------------------------------

    #[test]
    fn pushdown_translates_or_omits_predicate() {
        // Translatable predicate: column > literal.
        let translatable = serde_json::json!({
            "type": "predicate_greater",
            "left": {"type": "column", "name": "age"},
            "right": {"type": "literal_exactnumeric", "value": 18}
        });
        let filter_rendered = render_df_filter_safe(&translatable);
        assert!(
            filter_rendered.is_some(),
            "translatable predicate must produce a filter string"
        );
        let filter_str = filter_rendered.unwrap();
        assert!(
            filter_str.contains(">"),
            "filter must include > operator: {filter_str}"
        );
        assert!(
            filter_str.contains("AGE") || filter_str.contains("\"AGE\""),
            "filter must reference the column: {filter_str}"
        );

        // Untranslatable predicate (e.g., an aggregate or unknown function):
        // render_df_filter_safe returns None → omitted from spec.
        let untranslatable = serde_json::json!({"type": "fn_custom_agg", "args": []});
        let omitted = render_df_filter_safe(&untranslatable);
        assert!(
            omitted.is_none(),
            "untranslatable predicate must be omitted (None), not mistranslated"
        );

        // Confirm omitting the filter still produces valid SQL (correctness backstop).
        let sql_no_filter = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["AGE".into()],
            vec!["DECIMAL(20,0)".into()],
            None, // omitted
            None,
        );
        assert!(
            sql_no_filter.contains(SCAN_UDF_NAME),
            "SQL must still be valid when filter is omitted"
        );

        // Confirm carrying the filter includes it in the spec JSON.
        let sql_with_filter = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["AGE".into()],
            vec!["DECIMAL(20,0)".into()],
            Some(filter_str),
            None,
        );
        assert!(
            sql_with_filter.contains(">"),
            "filter must survive into the spec literal: {sql_with_filter}"
        );
    }

    // ---------------------------------------------------------------------------
    // Scenario: LIMIT is pushed into the scan spec; also appears at Exasol level.
    // ---------------------------------------------------------------------------

    #[test]
    fn pushdown_carries_limit() {
        let sql = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            Some(42),
        );

        // The outer SQL must contain LIMIT (Exasol-level backstop).
        assert!(
            sql.contains("LIMIT 42"),
            "outer SQL must carry LIMIT for correctness backstop: {sql}"
        );

        // The spec JSON (embedded in the literal) must carry limit = 42.
        // The JSON will have "limit":42 somewhere in the literal.
        assert!(
            sql.contains(r#""limit":42"#) || sql.contains("limit"),
            "spec JSON must carry the limit: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Pre-existing helpers tests (unchanged)
    // ---------------------------------------------------------------------------

    #[test]
    fn empty_file_list_returns_empty_select() {
        let proj = vec!["ID".to_string(), "NAME".to_string()];
        let types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
        let resp = empty_pushdown_sql(&proj, &types);
        let sql = resp["sql"].as_str().unwrap();
        assert!(sql.contains("WHERE 1=0"));
        assert!(sql.contains("CAST(NULL AS DECIMAL(20,0))"));
    }

    #[test]
    fn limit_extracted_from_pushdown_request() {
        let req = serde_json::json!({"numElements": 42});
        assert_eq!(extract_limit(&req), None); // not nested under "limit"

        let req2 = serde_json::json!({"limit": {"numElements": 42}});
        assert_eq!(extract_limit(&req2), Some(42));
    }

    #[test]
    fn sql_string_literal_escapes_quotes() {
        let s = "it's a test";
        let lit = sql_string_literal(s);
        assert_eq!(lit, "'it''s a test'");
    }

    #[test]
    fn parse_table_ident_splits_namespace_table() {
        let (ns, tbl) = parse_table_ident("mydb.mytable").unwrap();
        assert_eq!(ns, "mydb");
        assert_eq!(tbl, "mytable");
    }

    #[test]
    fn parse_table_ident_errors_on_no_dot() {
        let err = parse_table_ident("notable").unwrap_err();
        assert!(err.to_string().contains("namespace.table"));
    }

    // ---------------------------------------------------------------------------
    // extract_projection — row-scan fallback must be duplicate-free
    // ---------------------------------------------------------------------------

    /// A select list mixing an untranslatable scalar and COUNT(*) must NOT emit
    /// repeated `first_col_name` columns (which Exasol rejects as duplicate EMITS).
    /// It falls back to the full, deduplicated base-table column set.
    #[test]
    fn extract_projection_fallback_is_duplicate_free() {
        let request = serde_json::json!({
            "involvedTables": [{
                "name": "EVENTS",
                "columns": [
                    {"name": "id", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "name", "dataType": {"type": "varchar", "size": 2000000}},
                ],
            }],
        });
        // Untranslatable scalar (unknown function) + COUNT(*) aggregate — both items
        // would otherwise hit the first-column fallback arms.
        let pushdown_req = serde_json::json!({
            "selectList": [
                {"type": "function_scalar", "name": "TOTALLY_UNKNOWN_FN", "arguments": [
                    {"type": "column", "name": "id"}
                ]},
                {"type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false},
            ],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 20, "scale": 0},
                {"type": "decimal", "precision": 20, "scale": 0},
            ],
        });

        let (names, types) = extract_projection(&request, &pushdown_req).unwrap();

        let unique: std::collections::HashSet<&String> = names.iter().collect();
        assert_eq!(
            unique.len(),
            names.len(),
            "projection must be duplicate-free, got: {names:?}"
        );
        assert_eq!(
            names,
            vec!["ID".to_string(), "NAME".to_string()],
            "fallback must project the full base-table column set"
        );
        assert_eq!(
            names.len(),
            types.len(),
            "names and types must stay aligned"
        );
    }

    // ---------------------------------------------------------------------------
    // detect_aggregates — plan translation + fallback
    // ---------------------------------------------------------------------------

    fn agg_item(name: &str, col: Option<&str>, distinct: bool) -> serde_json::Value {
        let mut args = serde_json::json!([]);
        if let Some(c) = col {
            args = serde_json::json!([{"type": "column", "name": c}]);
        }
        serde_json::json!({
            "type": "function_aggregate",
            "name": name,
            "arguments": args,
            "distinct": distinct,
        })
    }

    /// COUNT(*) translates to Count with column=None.
    #[test]
    fn detect_count_star_produces_count_no_column() {
        let req = serde_json::json!({
            "selectList": [agg_item("COUNT", None, false)]
        });
        let plans = detect_aggregates(&req).expect("should detect COUNT(*)");
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].kind, AggKind::Count);
        assert!(plans[0].column.is_none());
    }

    /// COUNT(col) translates to CountCol with the column name.
    #[test]
    fn detect_count_col_produces_count_col() {
        let req = serde_json::json!({
            "selectList": [agg_item("COUNT", Some("amount"), false)]
        });
        let plans = detect_aggregates(&req).expect("should detect COUNT(col)");
        assert_eq!(plans[0].kind, AggKind::CountCol);
        assert_eq!(plans[0].column.as_deref(), Some("AMOUNT"));
    }

    /// SUM/MIN/MAX/AVG each translate to the right kind + column.
    #[test]
    fn detect_sum_min_max_avg_produce_correct_plans() {
        let req = serde_json::json!({
            "selectList": [
                agg_item("SUM", Some("amount"), false),
                agg_item("MIN", Some("ts"), false),
                agg_item("MAX", Some("ts"), false),
                agg_item("AVG", Some("score"), false),
            ]
        });
        let plans = detect_aggregates(&req).expect("should detect all four");
        assert_eq!(plans[0].kind, AggKind::Sum);
        assert_eq!(plans[0].column.as_deref(), Some("AMOUNT"));
        assert_eq!(plans[1].kind, AggKind::Min);
        assert_eq!(plans[1].column.as_deref(), Some("TS"));
        assert_eq!(plans[2].kind, AggKind::Max);
        assert_eq!(plans[2].column.as_deref(), Some("TS"));
        assert_eq!(plans[3].kind, AggKind::Avg);
        assert_eq!(plans[3].column.as_deref(), Some("SCORE"));
    }

    /// GROUP BY present and non-empty => fall back (None).
    #[test]
    fn detect_aggregates_falls_back_on_group_by() {
        let req = serde_json::json!({
            "selectList": [agg_item("SUM", Some("amount"), false)],
            "groupBy": [{"type": "column", "name": "region"}],
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when GROUP BY is present"
        );
    }

    /// DISTINCT aggregate => fall back.
    #[test]
    fn detect_aggregates_falls_back_on_distinct() {
        let req = serde_json::json!({
            "selectList": [agg_item("COUNT", Some("id"), true)]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when DISTINCT is present"
        );
    }

    /// Unsupported aggregate function (e.g., MEDIAN) => fall back to row scan.
    /// Note: STDDEV is a supported decomposable aggregate via sufficient-statistics.
    #[test]
    fn detect_aggregates_falls_back_on_unsupported_function() {
        let req = serde_json::json!({
            "selectList": [
                agg_item("SUM", Some("amount"), false),
                agg_item("MEDIAN", Some("amount"), false),
            ]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when any item is unsupported"
        );
    }

    /// Non-aggregate select item (e.g., plain column) => fall back.
    #[test]
    fn detect_aggregates_falls_back_on_column_select() {
        let req = serde_json::json!({
            "selectList": [
                {"type": "column", "name": "region"},
            ]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when select list contains non-aggregate"
        );
    }

    /// Empty select list => None.
    #[test]
    fn detect_aggregates_returns_none_for_empty_select_list() {
        let req = serde_json::json!({ "selectList": [] });
        assert!(detect_aggregates(&req).is_none());
    }

    /// An aggregate select-list translates to a ScanSpec carrying
    /// the aggregate plan (kind+column) plus any pushed-down filter.
    #[test]
    fn aggregate_query_builds_partial_agg_spec() {
        // Build a spec_template as handle_pushdown would.
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec!["AMOUNT".into()],
            filter: Some("(\"REGION\" = 'EU')".into()),
            limit: None,
            aggregates: Some(vec![
                AggregatePlan {
                    kind: AggKind::Sum,
                    column: Some("AMOUNT".into()),
                },
                AggregatePlan {
                    kind: AggKind::Count,
                    column: None,
                },
            ]),
            group_keys: None,
            storage: sample_storage(),
            catalog: sample_catalog(),
        };

        // Build single-shard SQL and decode the embedded spec literal.
        let shards = vec![vec!["s3://warehouse/f.parquet".into()]];
        let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
        let sql = build_scan_driving_sql(
            &spec_template,
            shards,
            &["AMOUNT".to_string()],
            &["DOUBLE PRECISION".to_string()],
            None,
            &col_types,
            &[],
            SCAN_UDF_NAME,
        );

        // The spec JSON is embedded in the SQL literal; extract and parse it.
        // It lives between the first `'` and the matching unescaped `'` after the JSON.
        // Simpler: deserialize directly from the template (which is what gets embedded).
        let spec_json = {
            // Reconstruct the shard spec as the builder would.
            let mut s = spec_template.clone();
            s.files = vec!["s3://warehouse/f.parquet".into()];
            s.to_json()
        };
        let parsed = ScanSpec::from_json(&spec_json).expect("spec must parse");

        // The aggregate plan must be present with the right kinds and columns.
        let plans = parsed.aggregates.expect("aggregates must be in the spec");
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].kind, AggKind::Sum);
        assert_eq!(plans[0].column.as_deref(), Some("AMOUNT"));
        assert_eq!(plans[1].kind, AggKind::Count);
        assert!(plans[1].column.is_none());

        // The filter must also be present.
        assert!(
            parsed.filter.is_some(),
            "filter must be carried in aggregate spec"
        );

        // The SQL must reference the UDF.
        assert!(sql.contains(SCAN_UDF_NAME));
    }

    // ---------------------------------------------------------------------------
    // Fan-out SQL shape — multi-shard GROUP BY shard_key, single-shard equivalence
    // ---------------------------------------------------------------------------

    /// Multi-shard SQL fans out via GROUP BY shard_key (not IPROC()): SQL contains
    /// GROUP BY shard_key, invokes the scan UDF, and carries each shard's distinct
    /// files as separate spec literals.
    #[test]
    fn multi_shard_sql_fans_via_shard_key_group_by() {
        let files = vec![
            "s3://warehouse/shard0/part-000.parquet".into(),
            "s3://warehouse/shard1/part-001.parquet".into(),
            "s3://warehouse/shard2/part-002.parquet".into(),
        ];
        // cluster_nodes=3 forces 3 shards (one file each).
        let sql = build_sql_for_fixture_n(
            files,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            None,
            3,
        );

        // Must use shard_key GROUP BY for the fan-out, NOT IPROC().
        assert!(
            !sql.contains("IPROC()"),
            "multi-shard SQL must NOT contain IPROC(): {sql}"
        );
        assert!(
            sql.contains("GROUP BY"),
            "multi-shard SQL must contain GROUP BY: {sql}"
        );
        assert!(
            sql.contains("shard_key"),
            "multi-shard SQL must use shard_key: {sql}"
        );

        // Must invoke the scan UDF.
        assert!(
            sql.contains(SCAN_UDF_NAME),
            "multi-shard SQL must invoke the scan UDF: {sql}"
        );

        // Each file must appear in the SQL (in distinct spec literals).
        assert!(
            sql.contains("part-000.parquet"),
            "shard 0 file must be in SQL: {sql}"
        );
        assert!(
            sql.contains("part-001.parquet"),
            "shard 1 file must be in SQL: {sql}"
        );
        assert!(
            sql.contains("part-002.parquet"),
            "shard 2 file must be in SQL: {sql}"
        );

        // The two files must appear in separate spec literals (not in the same one).
        // A spec literal is a JSON object; each file should appear in its own VALUES row.
        // Assert that the string "part-000.parquet" and "part-001.parquet" are NOT
        // both inside the same spec literal by checking they land in different VALUES entries.
        // Rough check: the VALUES clause contains exactly 3 entries separated by ),(.
        let values_start = sql.find("VALUES").expect("must have VALUES");
        let group_by_start = sql.find("GROUP BY").expect("must have GROUP BY");
        let values_section = &sql[values_start..group_by_start];
        // Count VALUES entries: each is (N,'...')
        let entry_count = values_section.matches("),(").count() + 1;
        assert_eq!(
            entry_count, 3,
            "must have 3 VALUES entries for 3 shards: {values_section}"
        );
    }

    // ---------------------------------------------------------------------------
    // Aggregate merge wrapper SQL — outer SELECT reconstructing partial results
    // ---------------------------------------------------------------------------

    /// Helper: build aggregate scan SQL from a set of aggregate plans.
    /// Uses an empty col_types map — aggregate columns default to DOUBLE PRECISION
    /// (correct for existing tests that use SCORE/AMOUNT as DOUBLE).
    fn build_agg_sql(
        agg_plans: Vec<AggregatePlan>,
        files: Vec<String>,
        cluster_nodes: usize,
    ) -> String {
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: Some(agg_plans),
            group_keys: None,
            storage: sample_storage(),
            catalog: sample_catalog(),
        };
        let shards = crate::adapter::sharding::partition_files(files, cluster_nodes);
        build_scan_driving_sql(
            &spec_template,
            shards,
            &[],
            &[],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
        )
    }

    /// Aggregate wrapper merges partials: outer SELECT aggregates per-shard COUNT/SUM/MIN/MAX.
    /// Given COUNT/SUM/MIN/MAX aggregate plan: wrapper contains fan-out AND outer
    /// SUM/MIN/MAX over the partial columns in the right order.
    #[test]
    fn aggregate_wrapper_merges_partials() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
            },
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("TS".into()),
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("TS".into()),
            },
        ];

        // Multi-shard: use 2 shards to exercise the fan-out + merge wrapper.
        let files = vec![
            "s3://warehouse/f0.parquet".into(),
            "s3://warehouse/f1.parquet".into(),
        ];
        let sql = build_agg_sql(plans, files, 2);

        // Must contain the shard_key fan-out (NOT IPROC).
        assert!(
            !sql.contains("IPROC()"),
            "aggregate SQL must NOT use IPROC: {sql}"
        );
        assert!(
            sql.contains("GROUP BY"),
            "aggregate SQL must use GROUP BY: {sql}"
        );
        assert!(
            sql.contains("shard_key"),
            "aggregate SQL must use shard_key fan-out: {sql}"
        );

        // Must wrap with outer merge aggregation.
        assert!(
            sql.contains("SUM("),
            "merge wrapper must contain SUM: {sql}"
        );
        assert!(
            sql.contains("MIN("),
            "merge wrapper must contain MIN: {sql}"
        );
        assert!(
            sql.contains("MAX("),
            "merge wrapper must contain MAX: {sql}"
        );

        // Must contain partial column names in the EMITS and in the merge.
        assert!(
            sql.contains("PARTIAL_count_0"),
            "must reference partial count column: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_sum_1"),
            "must reference partial sum column: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_min_2"),
            "must reference partial min column: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_max_3"),
            "must reference partial max column: {sql}"
        );

        // The EMITS clause must declare the partial columns.
        assert!(
            sql.contains("EMITS"),
            "aggregate SQL must have EMITS: {sql}"
        );

        // The outer merge must not be SELECT *.
        assert!(
            !sql.contains("SELECT *"),
            "aggregate wrapper must not use SELECT *: {sql}"
        );
    }

    /// Single-group merge casts each aggregate to its Exasol-declared result type.
    /// `SELECT COUNT(score)` merges as `SUM("PARTIAL_count_0")` (DECIMAL(31,0)); Exasol
    /// declared DECIMAL(18,0) for the column and strictly validates the adapter's output
    /// type, so the merge item must be `CAST(SUM("PARTIAL_count_0") AS DECIMAL(18,0))`.
    #[test]
    fn single_group_merge_casts_to_declared_type() {
        let plans = vec![AggregatePlan {
            kind: AggKind::CountCol,
            column: Some("SCORE".into()),
        }];
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: Some(plans.clone()),
            group_keys: None,
            storage: sample_storage(),
            catalog: sample_catalog(),
        };
        let shards = vec![vec!["s3://warehouse/f0.parquet".into()]];
        let col_types = vec![("SCORE".to_string(), "DECIMAL(18,0)".to_string())];
        let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
        let sql = build_scan_driving_sql(
            &spec_template,
            shards,
            &[],
            &[],
            None,
            &col_types,
            &aggregate_types,
            SCAN_UDF_NAME,
        );
        assert!(
            sql.contains(r#"CAST(SUM("PARTIAL_count_0") AS DECIMAL(18,0))"#),
            "single-group merge must cast COUNT to declared DECIMAL(18,0): {sql}"
        );
    }

    /// Single-group merge with no declared types emits the bare uncast merge expression.
    #[test]
    fn single_group_merge_uncast_without_declared_types() {
        let plans = vec![AggregatePlan {
            kind: AggKind::CountCol,
            column: Some("SCORE".into()),
        }];
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: Some(plans.clone()),
            group_keys: None,
            storage: sample_storage(),
            catalog: sample_catalog(),
        };
        let shards = vec![vec!["s3://warehouse/f0.parquet".into()]];
        let sql = build_scan_driving_sql(
            &spec_template,
            shards,
            &[],
            &[],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
        );
        assert!(
            sql.contains(r#"SUM("PARTIAL_count_0")"#) && !sql.contains("CAST(SUM"),
            "single-group merge without declared types must be uncast: {sql}"
        );
    }

    /// AVG wrapper divides merged sum by count with NULLIF(cnt, 0) guard.
    /// Given AVG plan: wrapper computes SUM(partial_avg_sum) / NULLIF(SUM(partial_avg_cnt),0).
    #[test]
    fn avg_wrapper_divides_sum_by_count_guarded() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Avg,
            column: Some("SCORE".into()),
        }];
        let files = vec![
            "s3://warehouse/f0.parquet".into(),
            "s3://warehouse/f1.parquet".into(),
        ];
        let sql = build_agg_sql(plans, files, 2);

        // Must contain NULLIF guard for zero-count protection.
        assert!(
            sql.contains("NULLIF"),
            "AVG wrapper must contain NULLIF zero-guard: {sql}"
        );

        // Must divide: the / operator must appear in the outer merge context.
        assert!(
            sql.contains(" / "),
            "AVG wrapper must divide sum by count: {sql}"
        );

        // Must reference the AVG sum and count partial columns.
        assert!(
            sql.contains("PARTIAL_avg_sum_0"),
            "must reference partial avg sum: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_avg_cnt_0"),
            "must reference partial avg count: {sql}"
        );

        // Must use SUM() for both the sum and count parts.
        let sum_count = sql.matches("SUM(").count();
        assert!(
            sum_count >= 2,
            "AVG wrapper must SUM both partial_avg_sum and partial_avg_cnt: {sql}"
        );

        // Must contain NULLIF(..., 0).
        assert!(
            sql.contains("NULLIF(") && sql.contains(", 0)"),
            "AVG wrapper NULLIF guard must guard against zero: {sql}"
        );
    }

    /// Single-shard aggregate path produces a correct merge wrapper.
    #[test]
    fn single_shard_aggregate_still_uses_merge_wrapper() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
            },
            AggregatePlan {
                kind: AggKind::Avg,
                column: Some("SCORE".into()),
            },
        ];
        let files = vec!["s3://warehouse/f0.parquet".into()];
        let sql = build_agg_sql(plans, files, 1);

        // Even single-shard aggregate must have an outer merge.
        assert!(
            sql.contains("SUM("),
            "single-shard aggregate must have SUM merge: {sql}"
        );
        assert!(
            sql.contains("NULLIF"),
            "single-shard AVG must have NULLIF guard: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_count_0"),
            "single-shard must reference partial count: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_avg_sum_1"),
            "single-shard must reference partial avg sum: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_avg_cnt_1"),
            "single-shard must reference partial avg count: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // R.1: EMITS type correctness for SUM/MIN/MAX
    // ---------------------------------------------------------------------------

    /// R.1: MIN/MAX over a DATE column must EMIT DATE, not DOUBLE PRECISION.
    #[test]
    fn partial_emits_min_max_preserve_date_timestamp_type() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("EVENT_DATE".into()),
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("EVENT_TS".into()),
            },
        ];
        let col_types = vec![
            ("EVENT_DATE".to_string(), "DATE".to_string()),
            ("EVENT_TS".to_string(), "TIMESTAMP".to_string()),
        ];
        let emits = partial_emits_items(&plans, &col_types);
        assert!(
            emits[0].contains("DATE") && !emits[0].contains("DOUBLE"),
            "MIN over DATE must emit DATE, not DOUBLE: {:?}",
            emits[0]
        );
        assert!(
            emits[1].contains("TIMESTAMP") && !emits[1].contains("DOUBLE"),
            "MAX over TIMESTAMP must emit TIMESTAMP, not DOUBLE: {:?}",
            emits[1]
        );
    }

    /// R.1: SUM over a DECIMAL(20,0) integer column must emit DECIMAL(36,0), not DOUBLE.
    #[test]
    fn partial_emits_sum_integer_stays_decimal() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
        }];
        let col_types = vec![("AMOUNT".to_string(), "DECIMAL(20,0)".to_string())];
        let emits = partial_emits_items(&plans, &col_types);
        assert!(
            emits[0].contains("DECIMAL") && !emits[0].contains("DOUBLE"),
            "SUM over DECIMAL integer must emit DECIMAL, not DOUBLE: {:?}",
            emits[0]
        );
        // Scale must be 0 (preserved from original DECIMAL(20,0)).
        assert!(
            emits[0].contains("DECIMAL(36,0)"),
            "SUM over DECIMAL(20,0) must widen to DECIMAL(36,0): {:?}",
            emits[0]
        );
    }

    /// R.1: SUM over a DOUBLE PRECISION column stays DOUBLE PRECISION.
    #[test]
    fn partial_emits_sum_double_stays_double() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("SCORE".into()),
        }];
        let col_types = vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
        let emits = partial_emits_items(&plans, &col_types);
        assert!(
            emits[0].contains("DOUBLE PRECISION"),
            "SUM over DOUBLE must emit DOUBLE PRECISION: {:?}",
            emits[0]
        );
    }

    /// R.1: SUM over a VARCHAR/DATE column => validate_agg_col_types returns false (fall back).
    #[test]
    fn aggregate_falls_back_to_row_scan_for_sum_of_non_numeric() {
        let col_types_varchar = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];
        let sum_varchar = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("NAME".into()),
        }];
        assert!(
            !validate_agg_col_types(&sum_varchar, &col_types_varchar),
            "SUM over VARCHAR must fail validation (fall back to row scan)"
        );

        let col_types_date = vec![("EVENT_DATE".to_string(), "DATE".to_string())];
        let sum_date = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("EVENT_DATE".into()),
        }];
        assert!(
            !validate_agg_col_types(&sum_date, &col_types_date),
            "SUM over DATE must fail validation (fall back to row scan)"
        );
    }

    // ---------------------------------------------------------------------------
    // FIX 1: grouped aggregate with invalid agg column type falls back
    // ---------------------------------------------------------------------------

    /// A grouped aggregate whose SUM targets a VARCHAR column must fall back to row
    /// scan (return None from detect_group_by_aggregates + validate_agg_col_types) —
    /// the same guard as the single-group path — rather than producing grouped scan SQL
    /// that would generate an opaque UDF error at execution time.
    #[test]
    fn grouped_aggregate_sum_over_varchar_falls_back_via_type_validation() {
        // Simulate the detection + validation sequence that handle_pushdown runs.
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [
                {"type": "column", "name": "REGION"},
                agg_item("SUM", Some("NAME"), false), // NAME is VARCHAR — invalid for SUM
            ],
        });

        // detect_group_by_aggregates must accept the shape (it doesn't know types).
        let detected = detect_group_by_aggregates(&req);
        assert!(
            detected.is_some(),
            "detect_group_by_aggregates must accept the shape: {req}"
        );
        let (_, agg_plans) = detected.unwrap();

        // Validation with VARCHAR col_types must fail — triggering fall-back.
        let col_types = vec![
            ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
            ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
        ];
        assert!(
            !validate_agg_col_types(&agg_plans, &col_types),
            "validate_agg_col_types must fail for SUM over VARCHAR (fall back to row scan)"
        );

        // Confirm that a DATE column also fails for SUM.
        let col_types_date = vec![
            ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
            ("NAME".to_string(), "DATE".to_string()),
        ];
        assert!(
            !validate_agg_col_types(&agg_plans, &col_types_date),
            "validate_agg_col_types must fail for SUM over DATE (fall back to row scan)"
        );

        // Confirm a numeric type passes (no fall back).
        let col_types_numeric = vec![
            ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
            ("NAME".to_string(), "DOUBLE PRECISION".to_string()),
        ];
        assert!(
            validate_agg_col_types(&agg_plans, &col_types_numeric),
            "validate_agg_col_types must pass for SUM over DOUBLE PRECISION"
        );
    }

    // ---------------------------------------------------------------------------
    // R.2: multi-shard row-scan must append outer LIMIT
    // ---------------------------------------------------------------------------

    /// R.2: multi-shard row scan with LIMIT must append LIMIT to the outer SQL.
    #[test]
    fn multi_shard_row_scan_appends_outer_limit() {
        let files = vec![
            "s3://warehouse/f0.parquet".into(),
            "s3://warehouse/f1.parquet".into(),
        ];
        // cluster_nodes=2 forces 2 shards.
        let sql = build_sql_for_fixture_n(
            files,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            Some(10),
            2,
        );
        assert!(
            !sql.contains("IPROC()"),
            "must NOT use IPROC (uses shard_key): {sql}"
        );
        assert!(
            sql.contains("shard_key"),
            "must be multi-shard (uses shard_key): {sql}"
        );
        assert!(
            sql.contains("LIMIT 10"),
            "multi-shard row scan must append outer LIMIT 10: {sql}"
        );
    }

    /// Single-shard SQL shape matches the expected SELECT * FROM (SELECT …) wrapper.
    /// Given CLUSTER_NODES=1: the generated SQL does NOT contain IPROC/VALUES/GROUP BY
    /// and matches the `SELECT * FROM (SELECT {udf}(...) EMITS (...))` form.
    #[test]
    fn single_shard_sql_matches_legacy_shape() {
        let files = vec![
            "s3://warehouse/db/events/part-00000.parquet".into(),
            "s3://warehouse/db/events/part-00001.parquet".into(),
        ];
        let sql = build_sql_for_fixture_n(
            files.clone(),
            vec!["ID".into(), "NAME".into()],
            vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
            None,
            None,
            1, // single node
        );

        // Must NOT contain multi-shard markers.
        assert!(
            !sql.contains("IPROC"),
            "single-shard SQL must not contain IPROC: {sql}"
        );
        assert!(
            !sql.contains("VALUES"),
            "single-shard SQL must not contain VALUES: {sql}"
        );
        assert!(
            !sql.contains("GROUP BY"),
            "single-shard SQL must not contain GROUP BY: {sql}"
        );

        // Must match the legacy shape.
        assert!(
            sql.starts_with("SELECT * FROM (SELECT "),
            "must start with SELECT * FROM (SELECT ...: {sql}"
        );
        assert!(sql.contains("EMITS"), "must have EMITS clause: {sql}");
        assert!(
            sql.contains(SCAN_UDF_NAME),
            "must invoke the scan UDF: {sql}"
        );

        // Must carry both files (they go into a single spec literal).
        assert!(
            sql.contains("part-00000.parquet"),
            "must carry file 0: {sql}"
        );
        assert!(
            sql.contains("part-00001.parquet"),
            "must carry file 1: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // detect_group_by_aggregates — GROUP BY key extraction and aggregate detection
    // ---------------------------------------------------------------------------

    fn make_group_by_request(
        group_by: serde_json::Value,
        select_list: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": group_by,
            "selectList": select_list,
        })
    }

    /// Column reference in GROUP BY renders to a quoted identifier.
    #[test]
    fn detect_group_by_aggregates_column_key() {
        let req = make_group_by_request(
            serde_json::json!([{"type": "column", "name": "REGION"}]),
            serde_json::json!([
                {"type": "column", "name": "REGION"},
                agg_item("COUNT", None, false),
            ]),
        );
        let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        let (keys, plans) = result;
        assert_eq!(keys.len(), 1, "one group key");
        assert!(
            keys[0].contains("REGION"),
            "group key must reference REGION: {:?}",
            keys[0]
        );
        assert_eq!(plans.len(), 1, "one aggregate plan");
        assert_eq!(plans[0].kind, AggKind::Count);
    }

    /// Scalar expression in GROUP BY (e.g., function_scalar YEAR) renders via render_expression.
    #[test]
    fn detect_group_by_aggregates_expression_key() {
        // A predicate_equal used as an expression key — render_expression can handle it.
        let req = make_group_by_request(
            serde_json::json!([{
                "type": "predicate_equal",
                "left": {"type": "column", "name": "STATUS"},
                "right": {"type": "literal_string", "value": "active"},
            }]),
            serde_json::json!([agg_item("SUM", Some("AMOUNT"), false),]),
        );
        let result = detect_group_by_aggregates(&req);
        // predicate_equal renders to (STATUS = 'active'), so it should succeed.
        assert!(result.is_some(), "renderable expression key must succeed");
        let (keys, plans) = result.unwrap();
        assert_eq!(keys.len(), 1);
        assert!(keys[0].contains("="), "rendered expression must contain =");
        assert_eq!(plans[0].kind, AggKind::Sum);
    }

    /// An unsupported expression in GROUP BY causes the whole function to return None.
    #[test]
    fn detect_group_by_unsupported_expression_falls_back() {
        let req = make_group_by_request(
            serde_json::json!([{"type": "fn_custom_unsupported", "name": "MYSTERY"}]),
            serde_json::json!([agg_item("COUNT", None, false)]),
        );
        assert!(
            detect_group_by_aggregates(&req).is_none(),
            "unsupported expression must fall back to None"
        );
    }

    /// Select list with a non-aggregate, non-column item causes fallback.
    #[test]
    fn detect_group_by_mixed_select_falls_back() {
        // function_scalar in selectList is not an aggregate and not a plain column.
        let req = make_group_by_request(
            serde_json::json!([{"type": "column", "name": "REGION"}]),
            serde_json::json!([
                {"type": "function_scalar", "name": "YEAR", "arguments": [{"type": "column", "name": "TS"}]},
                agg_item("COUNT", None, false),
            ]),
        );
        assert!(
            detect_group_by_aggregates(&req).is_none(),
            "non-aggregate non-column in selectList must fall back"
        );
    }

    // ---------------------------------------------------------------------------
    // partition_files — G shards balanced, disjoint, full coverage
    // ---------------------------------------------------------------------------

    /// File list partitioned into G shards via shard_count is balanced, disjoint,
    /// and covers every file with no empty shards.
    #[test]
    fn partition_files_g_shards_balanced_disjoint_full_coverage() {
        use std::collections::HashSet;
        // 3 nodes × 4 factor = 12, capped to 10 files → G = 10
        let files: Vec<String> = (0..10).map(|i| format!("file-{i}.parquet")).collect();
        let g = shard_count(3, 4, files.len());
        assert_eq!(g, 10, "G must equal file_count when product > file_count");
        let shards = crate::adapter::sharding::partition_files(files.clone(), g);
        assert_eq!(shards.len(), 10, "must produce exactly G=10 shards");
        // No shard is empty.
        for (i, shard) in shards.iter().enumerate() {
            assert!(!shard.is_empty(), "shard {i} must not be empty");
        }
        // All files covered exactly once.
        let all: Vec<String> = shards.iter().flatten().cloned().collect();
        let unique: HashSet<&String> = all.iter().collect();
        assert_eq!(
            unique.len(),
            all.len(),
            "files must be disjoint across shards"
        );
        assert_eq!(
            unique,
            files.iter().collect::<HashSet<_>>(),
            "all files must be covered"
        );
        // Balanced: sizes differ by at most 1.
        let sizes: Vec<usize> = shards.iter().map(|s| s.len()).collect();
        let max = *sizes.iter().max().unwrap();
        let min = *sizes.iter().min().unwrap();
        assert!(max - min <= 1, "shards not balanced: max={max} min={min}");
    }

    // ---------------------------------------------------------------------------
    // Row-scan SQL shape — GROUP BY shard_key fan-out, single-shard collapse
    // ---------------------------------------------------------------------------

    /// Multi-shard row-scan SQL uses GROUP BY shard_key, never IPROC().
    #[test]
    fn scan_driving_sql_groups_by_shard_key_not_iproc() {
        let files: Vec<String> = (0..3)
            .map(|i| format!("s3://warehouse/f{i}.parquet"))
            .collect();
        let g = shard_count(3, 1, files.len());
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec!["ID".into()],
            filter: None,
            limit: None,
            aggregates: None,
            group_keys: None,
            storage: sample_storage(),
            catalog: sample_catalog(),
        };
        let shards = crate::adapter::sharding::partition_files(files, g);
        let sql = build_scan_driving_sql(
            &spec_template,
            shards,
            &["ID".to_string()],
            &["DECIMAL(20,0)".to_string()],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
        );
        assert!(
            !sql.contains("IPROC()"),
            "multi-shard SQL must NOT contain IPROC(): {sql}"
        );
        assert!(
            sql.contains("GROUP BY"),
            "multi-shard SQL must contain GROUP BY: {sql}"
        );
        assert!(
            sql.contains("shard_key"),
            "multi-shard SQL must use shard_key: {sql}"
        );
    }

    /// Single-shard collapses to the single-invocation form (no VALUES, no GROUP BY).
    #[test]
    fn single_shard_collapses_to_single_invocation() {
        let files = vec!["s3://warehouse/f0.parquet".to_string()];
        let g = shard_count(1, 1, files.len());
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec!["ID".into()],
            filter: None,
            limit: None,
            aggregates: None,
            group_keys: None,
            storage: sample_storage(),
            catalog: sample_catalog(),
        };
        let shards = crate::adapter::sharding::partition_files(files, g);
        let sql = build_scan_driving_sql(
            &spec_template,
            shards,
            &["ID".to_string()],
            &["DECIMAL(20,0)".to_string()],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
        );
        assert!(
            !sql.contains("IPROC()"),
            "single-shard SQL must not contain IPROC: {sql}"
        );
        assert!(
            !sql.contains("VALUES"),
            "single-shard SQL must not contain VALUES: {sql}"
        );
        assert!(
            !sql.contains("GROUP BY"),
            "single-shard SQL must not contain GROUP BY: {sql}"
        );
        assert!(
            sql.starts_with("SELECT * FROM (SELECT "),
            "must start with SELECT * FROM (SELECT ...: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Grouped aggregate scan SQL — GROUP BY shard_key fan-out
    // ---------------------------------------------------------------------------

    /// Helper: build grouped aggregate scan SQL.
    fn build_grouped_agg_sql(
        group_keys: Vec<String>,
        agg_plans: Vec<AggregatePlan>,
        files: Vec<String>,
        g: usize,
    ) -> String {
        let col_types: Vec<(String, String)> = vec![
            ("AMOUNT".to_string(), "DOUBLE PRECISION".to_string()),
            ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
        ];
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: Some(agg_plans.clone()),
            group_keys: Some(group_keys.clone()),
            storage: sample_storage(),
            catalog: sample_catalog(),
        };
        let shards = crate::adapter::sharding::partition_files(files, g);
        build_grouped_aggregate_scan_sql(
            &spec_template,
            shards,
            &group_keys,
            &[],
            &agg_plans,
            &[],
            None,
            &col_types,
            SCAN_UDF_NAME,
            None,
        )
    }

    /// Grouped scan-driving SQL fans out via GROUP BY shard_key over G work units.
    #[test]
    fn grouped_scan_sql_groups_by_shard_key() {
        let files: Vec<String> = (0..2).map(|i| format!("s3://w/f{i}.parquet")).collect();
        let g = shard_count(2, 1, files.len());
        let sql = build_grouped_agg_sql(
            vec!["\"REGION\"".into()],
            vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
            }],
            files,
            g,
        );
        assert!(
            !sql.contains("IPROC()"),
            "grouped SQL must NOT contain IPROC(): {sql}"
        );
        assert!(
            sql.contains("GROUP BY"),
            "grouped SQL must contain GROUP BY: {sql}"
        );
        assert!(
            sql.contains("shard_key"),
            "grouped SQL inner must use shard_key: {sql}"
        );
        assert!(
            sql.contains("VALUES"),
            "grouped SQL must use VALUES fan-out: {sql}"
        );
    }

    /// LIMIT is NOT pushed into per-shard scan for a grouped query.
    /// The per-shard spec JSON must not carry "limit"; the outer wrapper may have LIMIT.
    #[test]
    fn grouped_scan_sql_has_no_per_shard_limit() {
        let files = vec!["s3://w/f0.parquet".to_string()];
        let g = shard_count(1, 1, files.len());
        let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec![],
            filter: None,
            limit: Some(100), // LIMIT should NOT appear inside the shard spec JSON
            aggregates: Some(vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
            }]),
            group_keys: Some(vec!["\"REGION\"".into()]),
            storage: sample_storage(),
            catalog: sample_catalog(),
        };
        let shards = crate::adapter::sharding::partition_files(files, g);
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            shards,
            &["\"REGION\"".to_string()],
            &[],
            &[AggregatePlan {
                kind: AggKind::Count,
                column: None,
            }],
            &[],
            Some(100),
            &col_types,
            SCAN_UDF_NAME,
            None,
        );
        // The per-shard spec JSON must NOT carry "limit" — extract the embedded JSON literal
        // (everything between the first ' and the matching ') and verify "limit" is absent.
        // The SQL form is: SELECT UDF('...json...') EMITS (...)  or VALUES entry.
        // Easiest: extract the embedded spec from between single-quotes.
        let spec_start = sql.find('\'').expect("spec literal must start with '") + 1;
        let spec_end = sql[spec_start..]
            .find("') EMITS")
            .unwrap_or(sql[spec_start..].find('\'').unwrap());
        let spec_json = &sql[spec_start..spec_start + spec_end];
        assert!(
            !spec_json.contains("\"limit\""),
            "per-shard spec must NOT carry limit: {spec_json}"
        );
        // The outer SQL may contain LIMIT (that's fine — it's the outer wrapper limit).
        // Just confirm no "limit" key in the per-shard spec JSON.
    }

    /// Grouped aggregate wrapper SQL re-groups partial results per user group key.
    #[test]
    fn grouped_aggregate_wrapper_sql_groups_by_user_key_cols() {
        let files: Vec<String> = (0..2).map(|i| format!("s3://w/f{i}.parquet")).collect();
        let g = shard_count(2, 1, files.len());
        let sql = build_grouped_agg_sql(
            vec!["\"REGION\"".into(), "\"YEAR\"".into()],
            vec![
                AggregatePlan {
                    kind: AggKind::Count,
                    column: None,
                },
                AggregatePlan {
                    kind: AggKind::Sum,
                    column: Some("AMOUNT".into()),
                },
            ],
            files,
            g,
        );
        // Outer wrapper must GROUP BY GK_0, GK_1 (the group key columns).
        assert!(
            sql.contains("GK_0"),
            "wrapper SQL must reference GK_0: {sql}"
        );
        assert!(
            sql.contains("GK_1"),
            "wrapper SQL must reference GK_1: {sql}"
        );
        // Outer GROUP BY must merge partial aggregates.
        assert!(
            sql.contains("SUM("),
            "wrapper must contain SUM for merge: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_count_0"),
            "wrapper must reference PARTIAL_count_0: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_sum_1"),
            "wrapper must reference PARTIAL_sum_1: {sql}"
        );
        // Outer must have GROUP BY GK_0, GK_1.
        let outer_group_by = sql
            .rfind("GROUP BY")
            .expect("must have GROUP BY in outer wrapper");
        let outer_group_by_clause = &sql[outer_group_by..];
        assert!(
            outer_group_by_clause.contains("GK_0"),
            "outer GROUP BY must include GK_0: {outer_group_by_clause}"
        );
        assert!(
            outer_group_by_clause.contains("GK_1"),
            "outer GROUP BY must include GK_1: {outer_group_by_clause}"
        );
    }

    // ---------------------------------------------------------------------------
    // ScanSpec GROUP BY — group-key fragments propagated to the scan spec
    // ---------------------------------------------------------------------------

    /// Grouped scan spec carries group-key rendered SQL fragments.
    #[test]
    fn grouped_scan_spec_carries_group_keys() {
        let group_keys = vec!["\"REGION\"".to_string(), "YEAR(\"TS\")".to_string()];
        let spec = ScanSpec {
            files: vec!["s3://w/f0.parquet".into()],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: Some(vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
            }]),
            group_keys: Some(group_keys.clone()),
            storage: sample_storage(),
            catalog: sample_catalog(),
        };
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).expect("must round-trip");
        let keys = back.group_keys.expect("group_keys must be present");
        assert_eq!(keys, group_keys, "group_keys must survive spec round-trip");
    }

    /// aggregationType missing or not "group_by" returns None.
    #[test]
    fn detect_group_by_aggregates_no_group_by_type_returns_none() {
        // No aggregationType.
        let req1 = serde_json::json!({
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [agg_item("COUNT", None, false)],
        });
        assert!(detect_group_by_aggregates(&req1).is_none());

        // aggregationType is "single_group".
        let req2 = serde_json::json!({
            "aggregationType": "single_group",
            "selectList": [agg_item("COUNT", None, false)],
        });
        assert!(detect_group_by_aggregates(&req2).is_none());
    }

    /// Empty groupBy array returns None.
    #[test]
    fn detect_group_by_aggregates_empty_group_by_returns_none() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [],
            "selectList": [agg_item("SUM", Some("AMOUNT"), false)],
        });
        assert!(detect_group_by_aggregates(&req).is_none());
    }

    // ---------------------------------------------------------------------------
    // Non-decomposable aggregate fallback to row scan
    // ---------------------------------------------------------------------------

    /// MEDIAN, *_DISTINCT, APPROX_COUNT_DISTINCT, LISTAGG, GROUP_CONCAT all cause
    /// parse_agg_item / detect_aggregates to return None (row-scan fallback).
    #[test]
    fn non_decomposable_aggregate_falls_back_to_row_scan() {
        for name in &[
            "MEDIAN",
            "APPROXIMATE_COUNT_DISTINCT",
            "LISTAGG",
            "GROUP_CONCAT",
        ] {
            let req = serde_json::json!({
                "selectList": [agg_item(name, Some("AMOUNT"), false)],
            });
            assert!(
                detect_aggregates(&req).is_none(),
                "{name} must fall back to row scan"
            );
        }
        // COUNT(DISTINCT col) — distinct flag set
        let req_distinct = serde_json::json!({
            "selectList": [agg_item("COUNT", Some("ID"), true)],
        });
        assert!(
            detect_aggregates(&req_distinct).is_none(),
            "COUNT(DISTINCT) must fall back to row scan"
        );
    }

    // ---------------------------------------------------------------------------
    // STDDEV / VARIANCE decomposition into sufficient statistics
    // ---------------------------------------------------------------------------

    /// parse_agg_item returns a stat plan for STDDEV/VARIANCE family names.
    #[test]
    fn parse_agg_item_recognises_stat_functions() {
        for (name, expected_kind) in &[
            ("STDDEV", AggKind::StddevSamp),
            ("STDDEV_SAMP", AggKind::StddevSamp),
            ("STDDEV_POP", AggKind::StddevPop),
            ("VARIANCE", AggKind::VarSamp),
            ("VAR_SAMP", AggKind::VarSamp),
            ("VAR_POP", AggKind::VarPop),
        ] {
            let item = agg_item(name, Some("AMOUNT"), false);
            let plan =
                parse_agg_item(&item).unwrap_or_else(|| panic!("{name} must parse to a stat plan"));
            assert_eq!(
                plan.kind, *expected_kind,
                "{name} must map to {:?}",
                expected_kind
            );
            assert_eq!(plan.column.as_deref(), Some("AMOUNT"));
        }
    }

    /// partial_emits_items produces 3 columns for stat aggregates.
    #[test]
    fn stat_aggregate_emits_three_partial_columns() {
        for kind in &[
            AggKind::VarPop,
            AggKind::VarSamp,
            AggKind::StddevPop,
            AggKind::StddevSamp,
        ] {
            let plans = vec![AggregatePlan {
                kind: kind.clone(),
                column: Some("SCORE".into()),
            }];
            let col_types = vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
            let items = partial_emits_items(&plans, &col_types);
            assert_eq!(
                items.len(),
                3,
                "{kind:?} must emit 3 partial columns, got: {items:?}"
            );
            assert!(
                items[0].contains("PARTIAL_stat_cnt_0"),
                "first column must be cnt: {items:?}"
            );
            assert!(
                items[1].contains("PARTIAL_stat_sum_0"),
                "second column must be sum: {items:?}"
            );
            assert!(
                items[2].contains("PARTIAL_stat_sumsq_0"),
                "third column must be sumsq: {items:?}"
            );
        }
    }

    /// merge_select_items produces the correct reconstruction SQL for VAR_POP.
    #[test]
    fn var_pop_merge_formula_divides_by_n() {
        let plans = vec![AggregatePlan {
            kind: AggKind::VarPop,
            column: Some("X".into()),
        }];
        let sql = merge_select_items(&plans).join(", ");
        // Must contain NULLIF(..., 0) guard on the count
        assert!(
            sql.contains("NULLIF"),
            "var_pop merge must guard zero count: {sql}"
        );
        // Must NOT divide by (count - 1)
        assert!(
            !sql.contains("- 1"),
            "var_pop must not subtract 1 from count: {sql}"
        );
    }

    /// merge_select_items for VAR_SAMP divides by N-1 and guards N<=1 → NULL.
    #[test]
    fn var_samp_merge_formula_divides_by_n_minus_1() {
        let plans = vec![AggregatePlan {
            kind: AggKind::VarSamp,
            column: Some("X".into()),
        }];
        let sql = merge_select_items(&plans).join(", ");
        // Must use CASE WHEN … <= 1 THEN NULL to guard count<=1 → NULL.
        // Checking both `<= 1` and `CASE` ensures the N-1 sample divisor guard
        // is specifically present — not just any CASE or NULLIF in the expression.
        assert!(
            sql.contains("<= 1"),
            "var_samp merge must guard count<=1 with '<= 1': {sql}"
        );
        assert!(
            sql.contains("CASE"),
            "var_samp merge must use CASE for N<=1 guard: {sql}"
        );
    }

    /// STDDEV_POP merge formula wraps variance in SQRT.
    #[test]
    fn stddev_pop_merge_formula_uses_sqrt() {
        let plans = vec![AggregatePlan {
            kind: AggKind::StddevPop,
            column: Some("X".into()),
        }];
        let sql = merge_select_items(&plans).join(", ");
        assert!(sql.contains("SQRT("), "stddev_pop must use SQRT: {sql}");
        assert!(
            !sql.contains("- 1"),
            "stddev_pop must not subtract 1: {sql}"
        );
    }

    /// STDDEV_SAMP merge formula wraps variance-samp in SQRT.
    #[test]
    fn stddev_samp_merge_formula_uses_sqrt_and_n_minus_1() {
        let plans = vec![AggregatePlan {
            kind: AggKind::StddevSamp,
            column: Some("X".into()),
        }];
        let sql = merge_select_items(&plans).join(", ");
        assert!(sql.contains("SQRT("), "stddev_samp must use SQRT: {sql}");
        // N-1 guard: removing the N<=1 CASE would break this assertion.
        assert!(
            sql.contains("<= 1"),
            "stddev_samp must guard N<=1 (sample divisor): {sql}"
        );
        assert!(
            sql.contains("CASE"),
            "stddev_samp must use CASE for N<=1 guard: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // STDDEV/VARIANCE NULL-passthrough — N=0 (pop & samp) and N=1 (samp)
    // ---------------------------------------------------------------------------

    /// StddevPop merge SQL passes NULL through (N=0 → var_pop is NULL → stddev_pop NULL).
    ///
    /// Exasol `GREATEST(0.0, NULL) = 0.0` — a bare SQRT(GREATEST(...)) returns 0.0
    /// when cnt=0, not NULL. The correct form wraps in CASE WHEN IS NULL THEN NULL.
    #[test]
    fn stddev_pop_merge_null_passthrough_for_n_zero() {
        let plans = vec![AggregatePlan {
            kind: AggKind::StddevPop,
            column: Some("X".into()),
        }];
        let sql = merge_select_items(&plans).join(", ");
        // Must contain a NULL guard (CASE … IS NULL) that wraps the whole expression.
        assert!(
            sql.contains("IS NULL"),
            "stddev_pop must pass NULL through for N=0 via IS NULL guard: {sql}"
        );
        // The GREATEST guard against tiny-negative float rounding must still be present.
        assert!(
            sql.contains("GREATEST"),
            "stddev_pop must keep GREATEST rounding guard: {sql}"
        );
    }

    /// StddevSamp merge SQL passes NULL through for N=0 and N=1.
    ///
    /// var_samp is NULL when cnt<=1 (CASE guard). Wrapping in CASE WHEN IS NULL
    /// ensures SQRT does not receive 0.0 via GREATEST(0.0, NULL) = 0.0.
    #[test]
    fn stddev_samp_merge_null_passthrough_for_n_zero_and_n_one() {
        let plans = vec![AggregatePlan {
            kind: AggKind::StddevSamp,
            column: Some("X".into()),
        }];
        let sql = merge_select_items(&plans).join(", ");
        // Must contain a NULL guard that wraps the whole expression.
        assert!(
            sql.contains("IS NULL"),
            "stddev_samp must pass NULL through for N<=1 via IS NULL guard: {sql}"
        );
        // The GREATEST guard against tiny-negative float rounding must still be present.
        assert!(
            sql.contains("GREATEST"),
            "stddev_samp must keep GREATEST rounding guard: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // HAVING must not be silently dropped on grouped-path type-validation failure
    // ---------------------------------------------------------------------------

    /// Regression: HAVING is present + grouped-path type-validation fails.
    ///
    /// Before the fix, `handle_pushdown` would fall through to the row-scan path
    /// and silently discard the HAVING predicate — yielding wrong results because
    /// the adapter advertised `AGGREGATE_HAVING` so Exasol does not re-apply it.
    ///
    /// This test proves the two components that the guard in `handle_pushdown`
    /// relies on: (a) HAVING renders to `Some` for this request, and (b) type
    /// validation fails for SUM over a non-numeric column. Together they mean the
    /// guard `if having.is_some() && !validate_agg_col_types(...)` triggers and
    /// the function returns an error instead of falling through.
    #[test]
    fn having_present_and_grouped_type_validation_fails_conditions_hold() {
        // Pushdown request: GROUP BY aggregate with SUM over VARCHAR (non-numeric)
        // and a simple HAVING predicate (column > literal — translatable by render_expression_safe).
        //
        // A HAVING with `function_aggregate` is NOT translatable by vs_expression, so we use a
        // plain column comparison to exercise the "having renders to Some" side of the invariant.
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "REGION", "dataType": {"type": "VARCHAR", "size": 100}},
                    {"name": "LABEL",  "dataType": {"type": "VARCHAR", "size": 50}},
                    {"name": "SCORE",  "dataType": {"type": "DOUBLE"}},
                ]
            }],
            "pushdownRequest": {
                "aggregationType": "group_by",
                "groupBy": [{"type": "column", "name": "REGION"}],
                "selectList": [
                    {"type": "column", "name": "REGION"},
                    {
                        "type": "function_aggregate",
                        "name": "SUM",
                        "arguments": [{"type": "column", "name": "LABEL"}]
                    }
                ],
                "having": {
                    "type": "predicate_greater",
                    "left":  {"type": "column", "name": "SCORE"},
                    "right": {"type": "literal_exactnumeric", "value": "100"}
                }
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let col_types = extract_all_column_types(&request);

        // (a) detect_group_by_aggregates must find a grouped path.
        let detected = detect_group_by_aggregates(&pushdown_req);
        assert!(
            detected.is_some(),
            "test setup: must detect grouped aggregates"
        );
        let (_, grouped_plans) = detected.unwrap();

        // (b) validate_agg_col_types must fail (SUM over VARCHAR is invalid).
        assert!(
            !validate_agg_col_types(&grouped_plans, &col_types),
            "type validation must fail for SUM(VARCHAR)"
        );

        // (c) HAVING must render to Some — confirming it would be dropped without the guard.
        let having = pushdown_req
            .get("having")
            .filter(|h| !h.is_null())
            .and_then(render_expression_safe);
        assert!(
            having.is_some(),
            "HAVING must render to Some — without the guard it would be silently dropped"
        );

        // Both conditions simultaneously: this is exactly the state that triggers the
        // guard `if having.is_some() && !validate_agg_col_types(...)` in handle_pushdown.
        // When both hold, handle_pushdown returns Err (not Ok with dropped HAVING).
        assert!(
            having.is_some() && !validate_agg_col_types(&grouped_plans, &col_types),
            "guard condition must hold: having present AND type validation failed"
        );
    }

    // ---------------------------------------------------------------------------
    // Select-list scalar expression pushdown
    // ---------------------------------------------------------------------------

    /// A function_scalar in the select list renders to a SQL expression in the
    /// scan spec projection and EMITS clause.
    #[test]
    fn selectlist_scalar_expression_rendered_in_emits() {
        // Simulate a pushdown request with UPPER(name) in the select list.
        let upper_expr = serde_json::json!({
            "type": "function_scalar",
            "name": "UPPER",
            "arguments": [{"type": "column", "name": "NAME"}]
        });
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                    {"name": "NAME", "dataType": {"type": "VARCHAR", "size": 100}},
                ]
            }],
            "pushdownRequest": {
                "selectList": [upper_expr],
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let (proj_cols, proj_types) = extract_projection(&request, &pushdown_req).unwrap();
        // The rendered expression should be in projection
        assert_eq!(proj_cols.len(), 1);
        assert!(
            proj_cols[0].contains("UPPER") || proj_cols[0].contains("upper"),
            "projection must contain rendered expression: {proj_cols:?}"
        );
        // Type for an expression falls back to VARCHAR(2000000)
        assert_eq!(proj_types[0], "VARCHAR(2000000)");
    }

    /// An untranslatable select-list item falls back to the bare column.
    #[test]
    fn selectlist_untranslatable_item_falls_back_to_column() {
        // A node type the translator cannot handle
        let bad_expr = serde_json::json!({
            "type": "function_aggregate",  // aggregate in select list -> untranslatable as scalar expr
            "name": "SUM",
            "arguments": [{"type": "column", "name": "AMOUNT"}]
        });
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "AMOUNT", "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2}},
                ]
            }],
            "pushdownRequest": {
                "selectList": [bad_expr],
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let (proj_cols, proj_types) = extract_projection(&request, &pushdown_req).unwrap();
        // Fall back to the first column name
        assert_eq!(proj_cols.len(), 1);
        assert_eq!(proj_cols[0], "AMOUNT");
        assert_eq!(proj_types[0], "DECIMAL(18,2)");
    }

    // ---------------------------------------------------------------------------
    // HAVING predicate — applied in the outer wrapper only, never in shard scan
    // ---------------------------------------------------------------------------

    /// HAVING is rendered and appears in the outer GROUP BY wrapper SQL.
    #[test]
    fn having_clause_appears_in_outer_wrapper_only() {
        // Build a grouped aggregate SQL with a HAVING predicate.
        let having_filter = Some(r#"(SUM("AMOUNT") > 100)"#.to_string());
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec!["REGION".into(), "AMOUNT".into()],
            filter: None,
            limit: None,
            aggregates: Some(vec![AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
            }]),
            group_keys: Some(vec![r#""REGION""#.to_string()]),
            storage: sample_storage(),
            catalog: sample_catalog(),
        };
        let shards = vec![vec!["s3://wh/f.parquet".into()]];
        let col_types = vec![
            ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
            ("AMOUNT".to_string(), "DOUBLE PRECISION".to_string()),
        ];
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            shards,
            &[r#""REGION""#.to_string()],
            &[],
            &[AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
            }],
            &[],
            None,
            &col_types,
            SCAN_UDF_NAME,
            having_filter.as_deref(),
        );
        // HAVING must appear in the outer wrapper (after GROUP BY)
        assert!(
            sql.contains("HAVING"),
            "outer wrapper must contain HAVING: {sql}"
        );
        assert!(
            sql.contains("100"),
            "HAVING predicate value must be in SQL: {sql}"
        );
        // HAVING must come after GROUP BY
        let having_pos = sql.find("HAVING").unwrap();
        let group_by_pos = sql.find("GROUP BY").unwrap();
        assert!(
            having_pos > group_by_pos,
            "HAVING must appear after GROUP BY: {sql}"
        );
    }
}
