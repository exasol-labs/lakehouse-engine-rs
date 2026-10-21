//! The positional-delete fixtures are authored once at stack bring-up by the
//! `spark-iceberg-fixtures` Compose job (`scripts/spark-fixtures/`);
//! `tests/common/pos_delete_fixtures.rs` MUST stay in lockstep with those scripts.

use crate::common::e2e_harness::*;
use crate::common::pos_delete_fixtures::{
    DELETION_VECTOR_CARDINALITIES, DELETION_VECTOR_FILE_ID_RANGES, DELETION_VECTOR_TABLE,
    FILE_GRANULARITY_DELETED_IDS, FILE_GRANULARITY_REMAINING_ROWS, FILE_GRANULARITY_TABLE,
    FILE_GRANULARITY_TOTAL_ROWS, NAMESPACE, ORC_FILE_IDS, PARQUET_FILE_IDS, PARTITION_COL,
    PARTITION_EAST_DELETED_IDS, PARTITION_GRANULARITY_DELETED_IDS,
    PARTITION_GRANULARITY_REMAINING_ROWS, PARTITION_GRANULARITY_TABLE,
    PARTITION_GRANULARITY_TOTAL_ROWS, PARTITION_WEST_DELETED_IDS, UNSUPPORTED_FORMAT_TABLE,
    deletion_vector_deleted, deletion_vector_remaining_ids,
};
use crate::common::seed::{E2E_TABLE, SEED_ROWS_SCORE_GT_15, SEED_TOTAL_ROWS};

use crate::fixtures::{LAKEHOUSE_VS, lakehouse_vs, vs_table};

use lakehouse_engine::adapter::pushdown::shard_count;
use lakehouse_engine::adapter::sharding::partition_files_by_bytes;
use lakehouse_engine::scan::spec::DeleteMechanism;

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

const SAMESHARD_VS_NAME: &str = "POSDEL_SAMESHARD_VS";
const SPLITSHARD_VS_NAME: &str = "POSDEL_SPLITSHARD_VS";
/// Equals the data-file count of `mor_pos_partition` and `mor_dv`, so a 1-node cluster
/// gets exactly one file per shard.
const SPLIT_PARALLELISM_FACTOR: usize = 4;

fn setup_e2e() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        lakehouse_vs();
        let mut conn = exa_conn();
        create_virtual_schema(
            &mut conn,
            &VsProps::new(SAMESHARD_VS_NAME, NAMESPACE).with_parallelism_factor(1),
        );
        create_virtual_schema(
            &mut conn,
            &VsProps::new(SPLITSHARD_VS_NAME, NAMESPACE)
                .with_parallelism_factor(SPLIT_PARALLELISM_FACTOR),
        );
    });
}

fn ids_column(cols: &[Vec<serde_json::Value>]) -> Vec<i64> {
    cols[0].iter().map(parse_int).collect()
}

#[test]
fn fixture_spark_file_granularity_delete_table() {
    setup_e2e();

    let rt = runtime();
    let files = rt.block_on(resolve_fixture_files(NAMESPACE, FILE_GRANULARITY_TABLE));
    assert_eq!(
        files.len(),
        2,
        "mor_pos_file must resolve exactly 2 data files, got {}: {files:?}",
        files.len()
    );

    let total_size: usize = FILE_GRANULARITY_TOTAL_ROWS;
    assert!(
        total_size > 0,
        "sanity: fixture ground truth must be non-empty"
    );

    let mut refs_per_delete_path: HashMap<String, usize> = HashMap::new();
    for entry in &files {
        assert_eq!(
            entry.deletes.len(),
            1,
            "file granularity: data file {} must resolve only its own delete file, got {}",
            entry.path,
            entry.deletes.len()
        );
        for delete in &entry.deletes {
            assert!(
                matches!(delete, DeleteMechanism::IcebergPositionalDelete { .. }),
                "file granularity: delete file for {} must be a Parquet positional delete",
                entry.path
            );
            *refs_per_delete_path
                .entry(
                    delete
                        .object_store_path()
                        .expect("positional delete path")
                        .to_string(),
                )
                .or_insert(0) += 1;
        }
    }
    assert_eq!(
        refs_per_delete_path.len(),
        2,
        "file granularity: Spark must commit exactly 2 distinct delete files \
         (one per data file), got {refs_per_delete_path:?}"
    );
    for (path, count) in &refs_per_delete_path {
        assert_eq!(
            *count, 1,
            "delete file {path} must be resolved by exactly one data file, got {count}"
        );
    }
}

#[test]
fn fixture_spark_partition_granularity_delete_table() {
    setup_e2e();

    let rt = runtime();
    let files = rt.block_on(resolve_fixture_files(
        NAMESPACE,
        PARTITION_GRANULARITY_TABLE,
    ));
    assert_eq!(
        files.len(),
        4,
        "mor_pos_partition must resolve exactly 4 data files, got {}: {files:?}",
        files.len()
    );

    let mut refs_per_delete_path: HashMap<String, usize> = HashMap::new();
    for entry in &files {
        assert_eq!(
            entry.deletes.len(),
            1,
            "partition granularity: data file {} must have exactly 1 associated \
             delete file, got {}",
            entry.path,
            entry.deletes.len()
        );
        assert!(
            matches!(
                entry.deletes[0],
                DeleteMechanism::IcebergPositionalDelete { .. }
            ),
            "partition granularity: delete file for {} must be a Parquet positional delete",
            entry.path
        );
        *refs_per_delete_path
            .entry(
                entry.deletes[0]
                    .object_store_path()
                    .expect("positional delete path")
                    .to_string(),
            )
            .or_insert(0) += 1;
    }
    assert_eq!(
        refs_per_delete_path.len(),
        2,
        "partition granularity: exactly 2 partition-scoped delete files must be \
         committed (one per partition), got {refs_per_delete_path:?}"
    );
    for (path, count) in &refs_per_delete_path {
        assert_eq!(
            *count, 2,
            "partition-scoped delete file {path} must be referenced by exactly \
             2 data files (its partition's two files), got {count}"
        );
    }
}

#[test]
fn e2e_file_granularity_returns_post_delete_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id FROM {} ORDER BY id",
        vs_table(LAKEHOUSE_VS, FILE_GRANULARITY_TABLE)
    );
    let cols = conn.query_columns(&sql);
    let ids = ids_column(&cols);

    assert_eq!(
        ids.len(),
        FILE_GRANULARITY_REMAINING_ROWS,
        "mor_pos_file must return {FILE_GRANULARITY_REMAINING_ROWS} rows post-delete, \
         got {}: {ids:?}",
        ids.len()
    );

    let deleted: HashSet<i64> = FILE_GRANULARITY_DELETED_IDS.iter().copied().collect();
    let expected: Vec<i64> = (1..=FILE_GRANULARITY_TOTAL_ROWS as i64)
        .filter(|id| !deleted.contains(id))
        .collect();
    assert_eq!(
        ids, expected,
        "mor_pos_file post-delete id set must be exactly {expected:?}, got {ids:?}"
    );

    for id in &ids {
        assert!(
            !deleted.contains(id),
            "deleted id {id} must NOT appear in the post-delete result"
        );
    }
}

#[test]
fn e2e_partition_granularity_returns_post_delete_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id FROM {} ORDER BY id",
        vs_table(LAKEHOUSE_VS, PARTITION_GRANULARITY_TABLE)
    );
    let cols = conn.query_columns(&sql);
    let ids = ids_column(&cols);

    assert_eq!(
        ids.len(),
        PARTITION_GRANULARITY_REMAINING_ROWS,
        "mor_pos_partition must return {PARTITION_GRANULARITY_REMAINING_ROWS} rows \
         post-delete, got {}: {ids:?}",
        ids.len()
    );

    let deleted: HashSet<i64> = PARTITION_GRANULARITY_DELETED_IDS.iter().copied().collect();
    let expected: Vec<i64> = (1..=PARTITION_GRANULARITY_TOTAL_ROWS as i64)
        .filter(|id| !deleted.contains(id))
        .collect();
    assert_eq!(
        ids, expected,
        "mor_pos_partition post-delete id set must be exactly {expected:?}, got {ids:?}"
    );
}

#[test]
fn e2e_partition_delete_spans_multiple_partitions() {
    setup_e2e();
    let mut conn = exa_conn();

    let table = vs_table(LAKEHOUSE_VS, PARTITION_GRANULARITY_TABLE);

    let east_sql = format!("SELECT id FROM {table} WHERE {PARTITION_COL} = 'east' ORDER BY id");
    let east_ids = ids_column(&conn.query_columns(&east_sql));
    let east_deleted: HashSet<i64> = PARTITION_EAST_DELETED_IDS.iter().copied().collect();
    let expected_east: Vec<i64> = (1..=10i64)
        .filter(|id| !east_deleted.contains(id))
        .collect();
    assert_eq!(
        east_ids, expected_east,
        "east partition post-delete ids must be exactly {expected_east:?}, got {east_ids:?}"
    );
    for id in &east_ids {
        assert!(
            !east_deleted.contains(id),
            "deleted east id {id} must NOT appear in the east partition result"
        );
    }

    let west_sql = format!("SELECT id FROM {table} WHERE {PARTITION_COL} = 'west' ORDER BY id");
    let west_ids = ids_column(&conn.query_columns(&west_sql));
    let west_deleted: HashSet<i64> = PARTITION_WEST_DELETED_IDS.iter().copied().collect();
    let expected_west: Vec<i64> = (11..=20i64)
        .filter(|id| !west_deleted.contains(id))
        .collect();
    assert_eq!(
        west_ids, expected_west,
        "west partition post-delete ids must be exactly {expected_west:?}, got {west_ids:?}"
    );
    for id in &west_ids {
        assert!(
            !west_deleted.contains(id),
            "deleted west id {id} must NOT appear in the west partition result"
        );
    }

    assert_eq!(
        east_ids.len() + west_ids.len(),
        PARTITION_GRANULARITY_REMAINING_ROWS,
        "east + west partition results must together equal the total post-delete row count"
    );
}

#[test]
fn e2e_partition_delete_invariant_across_fanout() {
    setup_e2e();

    let rt = runtime();
    let files = rt.block_on(resolve_fixture_files(
        NAMESPACE,
        PARTITION_GRANULARITY_TABLE,
    ));
    assert_eq!(
        files.len(),
        4,
        "sanity: mor_pos_partition must have 4 data files"
    );

    let g_same = shard_count(1, 1, files.len());
    assert_eq!(
        g_same, 1,
        "PARALLELISM_FACTOR=1 on a 1-node cluster must yield G=1"
    );
    let same_shards = partition_files_by_bytes(files.clone(), g_same);
    assert_eq!(same_shards.len(), 1, "G=1 must yield exactly 1 shard");
    assert_eq!(
        same_shards[0].len(),
        files.len(),
        "with G=1 every data file (including any two sharing a delete file) \
         must land in the SAME single shard"
    );

    let g_split = shard_count(1, SPLIT_PARALLELISM_FACTOR, files.len());
    assert_eq!(
        g_split,
        files.len(),
        "PARALLELISM_FACTOR={SPLIT_PARALLELISM_FACTOR} on a 1-node cluster with \
         {} files must yield G == file_count",
        files.len()
    );
    let split_shards = partition_files_by_bytes(files.clone(), g_split);
    assert_eq!(
        split_shards.len(),
        files.len(),
        "G == file_count must yield exactly file_count shards"
    );
    for (i, shard) in split_shards.iter().enumerate() {
        assert_eq!(
            shard.len(),
            1,
            "with G == file_count every shard must get exactly 1 file \
             (shard {i} got {}), so every data file lands in a DIFFERENT shard",
            shard.len()
        );
    }

    let deleted: HashSet<i64> = PARTITION_GRANULARITY_DELETED_IDS.iter().copied().collect();
    let expected: Vec<i64> = (1..=PARTITION_GRANULARITY_TOTAL_ROWS as i64)
        .filter(|id| !deleted.contains(id))
        .collect();

    let mut conn = exa_conn();

    let same_sql = format!(
        "SELECT id FROM {} ORDER BY id",
        vs_table(SAMESHARD_VS_NAME, PARTITION_GRANULARITY_TABLE)
    );
    let same_ids = ids_column(&conn.query_columns(&same_sql));
    assert_eq!(
        same_ids, expected,
        "same-shard placement (PARALLELISM_FACTOR=1) must return the exact \
         post-delete id set {expected:?}, got {same_ids:?}"
    );

    let split_sql = format!(
        "SELECT id FROM {} ORDER BY id",
        vs_table(SPLITSHARD_VS_NAME, PARTITION_GRANULARITY_TABLE)
    );
    let split_ids = ids_column(&conn.query_columns(&split_sql));
    assert_eq!(
        split_ids, expected,
        "split-shard placement (PARALLELISM_FACTOR={SPLIT_PARALLELISM_FACTOR}) must \
         return the exact post-delete id set {expected:?}, got {split_ids:?}"
    );

    assert_eq!(
        same_ids, split_ids,
        "post-delete result must be invariant to shard placement: \
         same-shard={same_ids:?} vs split-shard={split_ids:?}"
    );
}

/// Scenario: Positional deletes compose with projection, filter, LIMIT, and pruning
#[test]
fn e2e_deletes_with_projection_filter_limit() {
    setup_e2e();
    let mut conn = exa_conn();

    let table = vs_table(LAKEHOUSE_VS, PARTITION_GRANULARITY_TABLE);
    let sql =
        format!("SELECT id, val FROM {table} WHERE {PARTITION_COL} = 'west' ORDER BY id LIMIT 3");
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, val): {cols:?}");

    let ids = ids_column(&cols);
    assert_eq!(
        ids,
        vec![11, 12, 15],
        "expected west ids [11,12,15], got {ids:?}"
    );

    let vals: Vec<String> = cols[1]
        .iter()
        .map(|v| {
            v.as_str()
                .unwrap_or_else(|| panic!("val not a string: {v:?}"))
                .to_string()
        })
        .collect();
    assert_eq!(
        vals,
        vec![
            "row-11".to_string(),
            "row-12".to_string(),
            "row-15".to_string()
        ],
        "expected vals [row-11,row-12,row-15], got {vals:?}"
    );

    let west_deleted: HashSet<i64> = PARTITION_WEST_DELETED_IDS.iter().copied().collect();
    for id in &ids {
        assert!(
            !west_deleted.contains(id),
            "deleted west id {id} must NOT appear in the projection/filter/limit result"
        );
    }
}

#[test]
fn e2e_deletes_with_single_and_grouped_agg() {
    setup_e2e();
    let mut conn = exa_conn();

    let single_sql = format!(
        "SELECT COUNT(*), SUM(id) FROM {}",
        vs_table(LAKEHOUSE_VS, FILE_GRANULARITY_TABLE)
    );
    let single_cols = conn.query_columns(&single_sql);
    let count = parse_int(&single_cols[0][0]);
    let sum = parse_int(&single_cols[1][0]);
    assert_eq!(
        count, FILE_GRANULARITY_REMAINING_ROWS as i64,
        "COUNT(*) over mor_pos_file must be {FILE_GRANULARITY_REMAINING_ROWS}, got {count}"
    );
    let total: i64 = (1..=FILE_GRANULARITY_TOTAL_ROWS as i64).sum();
    let deleted_sum: i64 = FILE_GRANULARITY_DELETED_IDS.iter().sum();
    let expected_sum = total - deleted_sum;
    assert_eq!(
        sum, expected_sum,
        "SUM(id) over mor_pos_file must be {expected_sum} (={total}-{deleted_sum}), got {sum}"
    );

    let grouped_sql = format!(
        "SELECT {PARTITION_COL}, COUNT(*) FROM {} GROUP BY {PARTITION_COL} ORDER BY {PARTITION_COL}",
        vs_table(LAKEHOUSE_VS, PARTITION_GRANULARITY_TABLE)
    );
    let grouped_cols = conn.query_columns(&grouped_sql);
    assert_eq!(
        grouped_cols[0].len(),
        2,
        "grouped aggregate must return 2 groups (east, west): {grouped_cols:?}"
    );

    let regions: Vec<String> = grouped_cols[0]
        .iter()
        .map(|v| {
            v.as_str()
                .unwrap_or_else(|| panic!("region not a string: {v:?}"))
                .to_string()
        })
        .collect();
    assert_eq!(
        regions,
        vec!["east".to_string(), "west".to_string()],
        "expected groups [east,west] in order, got {regions:?}"
    );

    let counts: Vec<i64> = grouped_cols[1].iter().map(parse_int).collect();
    assert_eq!(
        counts,
        vec![6, 6],
        "east and west must each have 6 post-delete rows, got {counts:?}"
    );
    let grouped_total: i64 = counts.iter().sum();
    assert_eq!(
        grouped_total, PARTITION_GRANULARITY_REMAINING_ROWS as i64,
        "grouped total must equal {PARTITION_GRANULARITY_REMAINING_ROWS}, got {grouped_total}"
    );
}

#[test]
fn fixture_spark_deletion_vector_table() {
    setup_e2e();

    let files = runtime().block_on(resolve_fixture_files(NAMESPACE, DELETION_VECTOR_TABLE));
    assert_eq!(
        files.len(),
        4,
        "mor_dv must resolve 4 data files: {files:?}"
    );

    let mut positional = 0;
    let mut vectors = Vec::new();
    for entry in &files {
        match entry.deletes.as_slice() {
            [DeleteMechanism::IcebergPositionalDelete { .. }] => positional += 1,
            [
                DeleteMechanism::IcebergPuffinDeletionVector {
                    path,
                    content_offset,
                    record_count,
                    ..
                },
            ] => vectors.push((path.as_str(), *content_offset, *record_count)),
            other => panic!("data file {} has unexpected deletes {other:?}", entry.path),
        }
    }
    assert_eq!(
        positional, 1,
        "one data file keeps its v2 positional delete"
    );

    let mut cardinalities: Vec<u64> = vectors.iter().map(|v| v.2).collect();
    cardinalities.sort_unstable();
    assert_eq!(cardinalities, DELETION_VECTOR_CARDINALITIES);

    let (large_path, large_offset, _) = vectors.iter().max_by_key(|v| v.2).unwrap();
    assert!(
        vectors
            .iter()
            .any(|(path, offset, _)| path == large_path && offset != large_offset),
        "the large vector shares its Puffin file with another blob: {vectors:?}"
    );
}

/// Scenario: Iceberg deletion vectors are applied on read
/// Scenario: Iceberg deletion vectors compose with projection, filter, LIMIT, and aggregates
#[test]
fn e2e_deletion_vectors_return_post_delete_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let mut small_expected: Vec<i64> = deletion_vector_remaining_ids()
        .filter(|id| *id < 1000)
        .collect();
    small_expected.sort_unstable();
    let count = deletion_vector_remaining_ids().count() as i64;
    let sum: i64 = deletion_vector_remaining_ids().sum();
    let (_, last) = DELETION_VECTOR_FILE_ID_RANGES[3];
    let top: Vec<i64> = (149_990..=last)
        .filter(|id| !deletion_vector_deleted(*id))
        .take(5)
        .collect();
    let after_range = (150_000..=last)
        .find(|id| !deletion_vector_deleted(*id))
        .unwrap();

    for vs in [LAKEHOUSE_VS, SAMESHARD_VS_NAME, SPLITSHARD_VS_NAME] {
        let table = vs_table(vs, DELETION_VECTOR_TABLE);

        let small = ids_column(&conn.query_columns(&format!(
            "SELECT id FROM {table} WHERE id < 1000 ORDER BY id"
        )));
        assert_eq!(small, small_expected, "{vs}: post-delete ids below 1000");

        let agg = conn.query_columns(&format!("SELECT COUNT(*), SUM(id) FROM {table}"));
        assert_eq!(
            (parse_int(&agg[0][0]), parse_int(&agg[1][0])),
            (count, sum),
            "{vs}: post-delete COUNT(*) and SUM(id)"
        );

        let cols = conn.query_columns(&format!(
            "SELECT id, val FROM {table} WHERE id >= 149990 ORDER BY id LIMIT 5"
        ));
        assert_eq!(ids_column(&cols), top, "{vs}: TopN skips deleted rows");
        let vals: Vec<&str> = cols[1].iter().map(|v| v.as_str().unwrap()).collect();
        let expected_vals: Vec<String> = top.iter().map(|id| format!("row-{id:02}")).collect();
        assert_eq!(
            vals, expected_vals,
            "{vs}: projected values follow their rows"
        );

        let min = conn.query_columns(&format!("SELECT MIN(id) FROM {table} WHERE id >= 150000"));
        assert_eq!(
            parse_int(&min[0][0]),
            after_range,
            "{vs}: a deleted range spanning several row groups stays deleted"
        );
    }
}

// Equality deletes have no E2E fixture (only Flink writes them); their refusal is
// covered by `classify_planned_file`'s unit tests.
/// Scenario: An unsupported delete mechanism fails loud at plan time
/// Scenario: An unsupported delete mechanism on a pruned file does not fail the query
#[test]
fn e2e_unsupported_delete_fails_loud() {
    setup_e2e();
    let mut conn = exa_conn();
    let table = vs_table(LAKEHOUSE_VS, UNSUPPORTED_FORMAT_TABLE);
    let (orc_first, _) = ORC_FILE_IDS;

    for filter in [String::new(), format!("WHERE id >= {orc_first}")] {
        let resp = conn.try_execute(&format!("SELECT id FROM {table} {filter}"));
        assert_eq!(
            resp["status"].as_str(),
            Some("error"),
            "a scan planning a file with an unsupported mechanism must fail ({filter}), got: {resp}"
        );
        let message = resp["exception"]["text"]
            .as_str()
            .or_else(|| resp["message"].as_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        assert!(
            message.contains("lakehouse pushdown declined") && message.contains("orc data files"),
            "error must be the plan-time refusal naming ORC data files ({filter}), got: {resp}"
        );
        for leak in [
            "access_key",
            "secret_key",
            "session_token",
            "secretaccesskey",
            "s3://",
            ".orc",
        ] {
            assert!(
                !message.contains(leak),
                "error must name no credential or file path (found '{leak}'): {resp}"
            );
        }
        assert_eq!(
            resp["responseData"]["results"][0]["resultSet"]["numRows"].as_i64(),
            None,
            "a plan-time failure must not carry a result set: {resp}"
        );
    }

    let (first, last) = PARQUET_FILE_IDS;
    let cols = conn.query_columns(&format!(
        "SELECT id FROM {table} WHERE id >= {first} AND id <= {last} ORDER BY id"
    ));
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    assert_eq!(
        ids,
        (first..=last).collect::<Vec<_>>(),
        "a filter that prunes every file an unsupported mechanism applies to must read the rest"
    );
}

#[test]
fn e2e_delete_free_table_no_regression() {
    setup_e2e();
    let mut conn = exa_conn();

    let table = vs_table(LAKEHOUSE_VS, E2E_TABLE);

    let filter_count = conn.query_row_count(&format!("SELECT id FROM {table} WHERE score > 15.0"));
    assert_eq!(
        filter_count, SEED_ROWS_SCORE_GT_15 as i64,
        "delete-free filter regression: expected {SEED_ROWS_SCORE_GT_15} rows with \
         score > 15.0, got {filter_count}"
    );

    let limit_count = conn.query_row_count(&format!("SELECT id FROM {table} LIMIT 5"));
    assert_eq!(
        limit_count, 5,
        "delete-free LIMIT regression: expected 5 rows, got {limit_count}"
    );

    let agg_sql = format!("SELECT COUNT(*), SUM(score) FROM {table}");
    let agg_cols = conn.query_columns(&agg_sql);
    let total_count = parse_int(&agg_cols[0][0]);
    assert_eq!(
        total_count, SEED_TOTAL_ROWS as i64,
        "delete-free aggregate regression: expected COUNT(*)={SEED_TOTAL_ROWS}, got {total_count}"
    );
    let total_score = agg_cols[1][0]
        .as_f64()
        .or_else(|| agg_cols[1][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("SUM(score) not numeric: {:?}", agg_cols[1][0]));
    // Seed: score = 5.0 * id.
    let n = SEED_TOTAL_ROWS as f64;
    let expected_total_score = 5.0 * (n * (n + 1.0) / 2.0);
    assert!(
        (total_score - expected_total_score).abs() < 1e-6,
        "delete-free aggregate regression: expected SUM(score)≈{expected_total_score}, \
         got {total_score}"
    );
}
