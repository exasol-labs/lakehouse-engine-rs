use super::super::tests::{
    equi_condition, join_request, nq3_join_request, resolved_side, three_table_join_request,
};
use super::*;
use crate::adapter::pushdown::test_support::*;

// ---------------------------------------------------------------------------
// Join detection: `detect_join` shape classification.
// ---------------------------------------------------------------------------

/// A genuine two-table inner equi-join is detected as the unified `Join` shape,
/// with both leaves' original-cased Iceberg identifiers recovered from `TABLE_MAP`
/// (the two-table case is simply N = 2).
#[test]
fn genuine_inner_equi_join_is_detected_with_both_idents() {
    let request = join_request(Json::Null, equi_condition());
    let pushdown_req = pd(&request);

    let shape = detect_join(&request, &pushdown_req).expect("TABLE_MAP has both tables");
    match shape {
        JoinShape::Join(join) => {
            assert_eq!(join.tables.len(), 2);
            assert_eq!(join.tables[0].table_name, "CUSTOMER");
            assert_eq!(join.tables[1].table_name, "ORDERS");
            assert_eq!(join.tables[0].table_identifier, "lh.customer");
            assert_eq!(join.tables[1].table_identifier, "lh.orders");
            assert_eq!(join.conditions, vec![equi_condition()]);
        }
        other => panic!("expected Join, got {other:?}"),
    }
}

/// A plain single-table pushdown request (today's normal case, no `from` field
/// at all) is `NotAJoin` and completely unaffected by the detector.
#[test]
fn plain_single_table_request_is_not_a_join() {
    let request = nq4_request();
    let shape = detect_join(&request, &pd(&request)).expect("not a join, no TABLE_MAP lookup");
    assert_eq!(shape, JoinShape::NotAJoin);
}

/// A `from` clause that is a plain table reference (`type: "table"`) is also
/// `NotAJoin` — the single-table shape some requests carry explicitly.
#[test]
fn from_table_node_is_not_a_join() {
    let mut request = nq4_request();
    request["pushdownRequest"]["from"] = serde_json::json!({"name": "LINEITEM", "type": "table"});
    let shape = detect_join(&request, &pd(&request)).expect("not a join");
    assert_eq!(shape, JoinShape::NotAJoin);
}

/// Left/right/full outer joins are declined as `Ineligible(NotInnerJoinType)`,
/// never `Eligible` — the broadcast contract advertises only `JOIN_TYPE_INNER`.
#[test]
fn outer_join_is_ineligible() {
    for outer in ["left_outer", "right_outer", "full_outer"] {
        let request = join_request(serde_json::json!({"join_type": outer}), equi_condition());
        let shape = detect_join(&request, &pd(&request)).expect("shape decline, no Err");
        assert_eq!(
            shape,
            JoinShape::Ineligible(IneligibleJoinReason::NotInnerJoinType),
            "join_type '{outer}' must be ineligible, not broadcast-eligible"
        );
    }
}

/// A non-equi two-table inner join (e.g. `<`) is NOT declined — it is served by
/// the unified fallback, so it yields the `Join` shape carrying both tables and
/// the (non-equi) condition. Only broadcast (an inner optimization) is gated on
/// equi; the N-scan fallback renders any inner-join condition into its WHERE.
#[test]
fn non_equi_two_table_join_is_served_by_unified_fallback() {
    let condition = serde_json::json!({
        "type": "predicate_less",
        "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
        "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"},
    });
    let request = join_request(Json::Null, condition.clone());
    match detect_join(&request, &pd(&request)).expect("served, not declined") {
        JoinShape::Join(join) => {
            assert_eq!(join.tables.len(), 2);
            assert_eq!(join.conditions, vec![condition]);
        }
        other => panic!("expected Join (unified fallback), got {other:?}"),
    }
}

/// A three-table all-inner nested join is classified as the unified `Join` shape
/// (never an error, never Ineligible): the three leaves in stable tree order and
/// the two collected join conditions, each leaf's Iceberg ident recovered from
/// `TABLE_MAP` (pushdown-planning-join "A three-or-more-table inner join falls
/// back to an N-scan unaccelerated wrapper").
#[test]
fn three_table_inner_join_is_unified_join() {
    let request = three_table_join_request();
    let shape = detect_join(&request, &pd(&request)).expect("all leaves are in TABLE_MAP");
    match shape {
        JoinShape::Join(join) => {
            let names: Vec<&str> = join.tables.iter().map(|t| t.table_name.as_str()).collect();
            assert_eq!(names, ["CUSTOMER", "ORDERS", "LINEITEM"]);
            let idents: Vec<&str> = join
                .tables
                .iter()
                .map(|t| t.table_identifier.as_str())
                .collect();
            assert_eq!(idents, ["lh.customer", "lh.orders", "lh.lineitem"]);
            assert_eq!(join.conditions.len(), 2, "N-1 conditions for N=3 tables");
        }
        other => panic!("expected Join, got {other:?}"),
    }
}

/// A non-inner join node ANYWHERE in the tree (here the nested left node is a
/// left outer join) declines as `Ineligible(NotInnerJoinType)` — a cross-join +
/// conjunctive WHERE cannot reproduce outer-join semantics.
#[test]
fn non_inner_node_in_join_tree_is_ineligible() {
    let mut request = three_table_join_request();
    request["pushdownRequest"]["from"]["left"]["join_type"] = serde_json::json!("left_outer");
    let shape = detect_join(&request, &pd(&request)).expect("shape decline, no Err");
    assert_eq!(
        shape,
        JoinShape::Ineligible(IneligibleJoinReason::NotInnerJoinType)
    );
}

/// A leaf of a multi-table tree absent from `TABLE_MAP` is a hard `Err` (stale
/// virtual schema), identical to the two-table path — never a silent decline.
#[test]
fn multi_table_leaf_absent_from_table_map_is_err() {
    let mut request = three_table_join_request();
    request["schemaMetadataInfo"]["adapterNotes"] = Json::String(
        serde_json::json!({"TABLE_MAP": {"CUSTOMER": "lh.customer", "ORDERS": "lh.orders"}})
            .to_string(),
    );
    let err = detect_join(&request, &pd(&request))
        .expect_err("LINEITEM is absent from TABLE_MAP: must be Err, not a decline");
    assert!(
        err.to_string().contains("LINEITEM"),
        "error must name the unmapped table: {err}"
    );
}

/// A four-table all-inner nested join (NQ3 shape: `part⋈partsupp⋈supplier⋈nation`)
/// is the unified `Join` shape with all four leaves (stable tree order) and the
/// three collected join conditions — the detector generalizes past N=3, never
/// capping at three tables.
#[test]
fn four_table_inner_join_is_unified_join() {
    let request = nq3_join_request();
    let shape = detect_join(&request, &pd(&request)).expect("all leaves are in TABLE_MAP");
    match shape {
        JoinShape::Join(join) => {
            let names: Vec<&str> = join.tables.iter().map(|t| t.table_name.as_str()).collect();
            assert_eq!(names, ["PART", "PARTSUPP", "SUPPLIER", "NATION"]);
            let idents: Vec<&str> = join
                .tables
                .iter()
                .map(|t| t.table_identifier.as_str())
                .collect();
            assert_eq!(
                idents,
                ["lh.part", "lh.partsupp", "lh.supplier", "lh.nation"]
            );
            assert_eq!(join.conditions.len(), 3, "N-1 conditions for N=4 tables");
        }
        other => panic!("expected Join, got {other:?}"),
    }
}

/// `detect_join` is driven by the `from` TREE, not the `involvedTables` count:
/// a two-table `from` yields the unified `Join` shape with exactly those two
/// tables even when `involvedTables` lists more (the old `TooManyTables`
/// defensive belt is gone — the tree is authoritative).
#[test]
fn detect_join_follows_from_tree_not_involved_tables_count() {
    let mut request = join_request(Json::Null, equi_condition());
    request["involvedTables"].as_array_mut().unwrap().push(serde_json::json!({
        "name": "NATION",
        "columns": [{"name": "N_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}],
    }));
    match detect_join(&request, &pd(&request)).expect("tree-driven, no decline") {
        JoinShape::Join(join) => {
            let names: Vec<&str> = join.tables.iter().map(|t| t.table_name.as_str()).collect();
            assert_eq!(names, ["CUSTOMER", "ORDERS"], "only the from-tree leaves");
        }
        other => panic!("expected Join over the two from-tree tables, got {other:?}"),
    }
}

/// An otherwise-eligible join whose virtual table name is absent from
/// `TABLE_MAP` is a hard `Err` (stale virtual schema), not a decline — the
/// same treatment the single-table path gives an unmapped involved table.
#[test]
fn join_with_unmapped_table_is_an_error() {
    let mut request = join_request(Json::Null, equi_condition());
    request["schemaMetadataInfo"]["adapterNotes"] =
        Json::String(serde_json::json!({"TABLE_MAP": {"CUSTOMER": "lh.customer"}}).to_string());
    let err = detect_join(&request, &pd(&request))
        .expect_err("ORDERS is absent from TABLE_MAP: must be Err, not a decline");
    assert!(
        err.to_string().contains("ORDERS"),
        "error must name the unmapped table: {err}"
    );
}

// ---------------------------------------------------------------------------
// Join side selection + broadcast threshold: `select_broadcast_sides`.
// The pure core of the two-table broadcast role/threshold decision — exercised
// without a live Iceberg catalog. `plan_join` resolves each side via
// `resolve_one_join_side` and delegates here, so this covers the decision.
// ---------------------------------------------------------------------------

/// The default `JOIN_BROADCAST_MAX_BYTES` (128 MiB).
const BROADCAST_MAX: u64 = 134_217_728;

/// `total_bytes` is the saturating sum of every file's `file_size_in_bytes`
/// (the Iceberg-manifest size — no Parquet read).
#[test]
fn resolved_side_sums_file_bytes_saturating() {
    assert_eq!(
        resolved_side("ORDERS", vec![("a", 100), ("b", 250), ("c", 4)]).total_bytes,
        354
    );
    // Empty side ⇒ zero bytes.
    assert_eq!(resolved_side("EMPTY", vec![]).total_bytes, 0);
    // A byte total that would overflow u64 saturates to u64::MAX (treated as
    // "far over any threshold"), never wraps.
    assert_eq!(
        resolved_side("HUGE", vec![("x", u64::MAX), ("y", 1)]).total_bytes,
        u64::MAX
    );
}

/// The smaller side by bytes is the dimension; the larger is the fact, and the
/// full resolved payload (files, schema, root, storage, idents) rides along
/// with each role. Here the LEFT argument is smaller.
#[test]
fn dimension_is_left_when_left_side_is_smaller() {
    let customer = resolved_side("CUSTOMER", vec![("c1", 1_000)]);
    let orders = resolved_side("ORDERS", vec![("o1", 50_000), ("o2", 50_000)]);
    let sides = select_broadcast_sides(customer, orders, BROADCAST_MAX);

    assert_eq!(sides.dimension.table_name, "CUSTOMER");
    assert_eq!(sides.fact.table_name, "ORDERS");
    assert_eq!(sides.dimension.total_bytes, 1_000);
    assert_eq!(sides.fact.total_bytes, 100_000);
    assert!(
        sides.broadcast_eligible,
        "1000 bytes is well under the 128 MiB threshold"
    );
    // Resolved payload travels with the role.
    assert_eq!(sides.dimension.table_identifier, "lh.customer");
    assert_eq!(sides.fact.table_identifier, "lh.orders");
    assert_eq!(sides.dimension.files, vec![FileEntry::new("c1", 1_000)]);
    assert_eq!(sides.dimension.table_root, "s3://warehouse/lh/customer");
    assert_eq!(sides.dimension.logical_schema.len(), 1);
    assert_eq!(sides.dimension.effective_storage, sample_storage());
}

/// Reversing the FROM-clause order (larger side first) still selects the
/// smaller side as the dimension — selection is by byte size, not position.
#[test]
fn dimension_is_right_when_right_side_is_smaller() {
    let orders = resolved_side("ORDERS", vec![("o1", 50_000), ("o2", 50_000)]);
    let customer = resolved_side("CUSTOMER", vec![("c1", 1_000)]);
    let sides = select_broadcast_sides(orders, customer, BROADCAST_MAX);

    assert_eq!(sides.dimension.table_name, "CUSTOMER");
    assert_eq!(sides.fact.table_name, "ORDERS");
    assert_eq!(sides.dimension.total_bytes, 1_000);
    assert!(sides.broadcast_eligible);
}

/// The dimension (smaller) side exceeding the threshold is reported as NOT
/// broadcast-eligible — cleanly via the flag, never an error — so the caller
/// builds the deterministic unaccelerated two-scan fallback.
#[test]
fn dimension_over_threshold_is_not_broadcast_eligible() {
    let part = resolved_side("PART", vec![("p1", 200)]);
    let lineitem = resolved_side("LINEITEM", vec![("l1", 900)]);
    // Threshold 100 is below even the smaller side's 200 bytes.
    let sides = select_broadcast_sides(part, lineitem, 100);

    assert_eq!(
        sides.dimension.table_name, "PART",
        "PART (200 bytes) is the smaller side"
    );
    assert_eq!(sides.fact.table_name, "LINEITEM");
    assert!(
        !sides.broadcast_eligible,
        "dimension total 200 > threshold 100: not broadcast-eligible"
    );
}

/// A dimension exactly AT the threshold is eligible (inclusive `<=`); one byte
/// over is not — the boundary the byte-size decision hinges on.
#[test]
fn threshold_boundary_is_inclusive() {
    let at = select_broadcast_sides(
        resolved_side("DIM", vec![("d", 100)]),
        resolved_side("FACT", vec![("f", 10_000)]),
        100,
    );
    assert!(
        at.broadcast_eligible,
        "dimension == threshold must be eligible"
    );

    let over = select_broadcast_sides(
        resolved_side("DIM", vec![("d", 101)]),
        resolved_side("FACT", vec![("f", 10_000)]),
        100,
    );
    assert!(
        !over.broadcast_eligible,
        "dimension == threshold + 1 must not be eligible"
    );
}

/// An empty side (zero files ⇒ zero bytes) is the trivially broadcast-eligible
/// dimension, and selection stays deterministic (documented empty-side edge).
#[test]
fn empty_side_is_the_eligible_dimension() {
    let empty = resolved_side("EMPTYDIM", vec![]);
    let fact = resolved_side("FACT", vec![("f", 5_000)]);
    let sides = select_broadcast_sides(empty, fact, BROADCAST_MAX);

    assert_eq!(sides.dimension.table_name, "EMPTYDIM");
    assert_eq!(sides.dimension.total_bytes, 0);
    assert!(sides.dimension.files.is_empty());
    assert!(sides.broadcast_eligible);
}

/// On an exact byte-size tie (e.g. a self-join, both sides the same table) the
/// FIRST argument is the dimension — deterministic, documented tie-break.
#[test]
fn equal_size_tie_breaks_to_first_argument() {
    let a = resolved_side("SELF_A", vec![("s", 4_242)]);
    let b = resolved_side("SELF_B", vec![("s", 4_242)]);
    let sides = select_broadcast_sides(a, b, BROADCAST_MAX);

    assert_eq!(sides.dimension.table_name, "SELF_A");
    assert_eq!(sides.fact.table_name, "SELF_B");
    assert_eq!(sides.dimension.total_bytes, sides.fact.total_bytes);
}
