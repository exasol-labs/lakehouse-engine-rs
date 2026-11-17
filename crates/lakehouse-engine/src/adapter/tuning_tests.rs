use super::*;
use crate::adapter::properties::{
    Check, PROP_DF_BATCH_SIZE, PROP_DF_TARGET_PARTITIONS, PROP_DF_THREADING_MODE,
    PROP_DF_THREADS_PER_UDF, PROP_INSTANCE_OVERHEAD_MB, PROP_JOIN_BROADCAST_MAX_BYTES,
    PROP_MEMORY_POOL_FRACTION, PROP_PARALLELISM_FACTOR, PROP_S3_MAX_CONNECTIONS, checked_values,
};

fn checked(props: serde_json::Value) -> TuningProperties {
    checked_values(&props, Check::Strict)
        .unwrap_or_else(|e| panic!("{props} must pass the check: {e}"))
        .tuning
}

fn resolved_tuning(props: serde_json::Value, nr_of_cores: u32) -> Tuning {
    Tuning::resolve(&checked(props), nr_of_cores)
}

#[test]
fn the_parallelism_factor_comes_from_the_property() {
    let tuning = resolved_tuning(serde_json::json!({ PROP_PARALLELISM_FACTOR: "4" }), 1);
    assert_eq!(
        tuning.parallelism_factor, 4,
        "factor must be read from the property"
    );

    let default_factor = resolved_tuning(serde_json::json!({}), 1).parallelism_factor;
    assert_eq!(
        default_factor, DEFAULT_PARALLELISM_FACTOR,
        "must default to {DEFAULT_PARALLELISM_FACTOR} when property absent"
    );
}

#[test]
fn core_count_comes_from_available_parallelism() {
    let nr_of_cores = resolve_nr_of_cores();
    assert!(
        nr_of_cores >= 1,
        "the core count must come from available_parallelism() (>= 1), got {nr_of_cores}"
    );
}

/// Scenario: Adapter uses a core count of 1 when available_parallelism() cannot report one
#[test]
fn core_count_defaults_to_one_when_detection_fails() {
    let detection_failed = Err(std::io::Error::other("platform reports no core count"));
    assert_eq!(
        core_count_or_default(detection_failed),
        1,
        "a failed core-count detection must resolve to 1, not to an unknown sentinel"
    );
}

#[test]
fn core_count_uses_the_detected_count_when_detection_succeeds() {
    let detected = Ok(std::num::NonZeroUsize::new(12).unwrap());
    assert_eq!(
        core_count_or_default(detected),
        12,
        "a reported core count must pass through unchanged, not collapse to the \
         detection-failure default of 1"
    );
}

#[test]
fn default_parallelism_factor_is_one() {
    assert_eq!(
        resolved_tuning(serde_json::json!({}), 1).parallelism_factor,
        1,
        "the scalar scan runs one VM per node, so one shard per node is the default"
    );
}

#[test]
fn df_target_partitions_uses_supplied_value() {
    let tuning = resolved_tuning(
        serde_json::json!({ PROP_DF_THREADING_MODE: "FIXED", PROP_DF_TARGET_PARTITIONS: "4" }),
        8,
    );
    assert_eq!(
        tuning.scan.df_target_partitions, 4,
        "explicit value must be returned"
    );
}

#[test]
fn df_batch_size_uses_supplied_value() {
    let tuning = resolved_tuning(serde_json::json!({ PROP_DF_BATCH_SIZE: "4096" }), 1);
    assert_eq!(
        tuning.scan.df_batch_size, 4096,
        "explicit DATAFUSION_BATCH_SIZE must be returned"
    );

    assert_eq!(
        resolved_tuning(serde_json::json!({}), 1).scan.df_batch_size,
        DEFAULT_DF_BATCH_SIZE,
        "absent property must return DEFAULT_DF_BATCH_SIZE (8192)"
    );
}

#[test]
fn df_threads_per_udf_uses_supplied_value() {
    let tuning = resolved_tuning(
        serde_json::json!({ PROP_DF_THREADING_MODE: "FIXED", PROP_DF_THREADS_PER_UDF: "2" }),
        8,
    );
    assert_eq!(
        tuning.scan.df_threads_per_udf, 2,
        "explicit value must be returned"
    );
}

#[test]
fn resolve_memory_pool_fraction_defaults_and_reads_the_supplied_value() {
    assert_eq!(
        resolved_tuning(serde_json::json!({}), 1)
            .scan
            .memory_pool_fraction,
        DEFAULT_MEMORY_POOL_FRACTION,
        "absent → default 0.6"
    );

    let valid = resolved_tuning(serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "0.5" }), 1);
    assert_eq!(valid.scan.memory_pool_fraction, 0.5);

    let one = resolved_tuning(serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "1.0" }), 1);
    assert_eq!(one.scan.memory_pool_fraction, 1.0);
}

#[test]
fn resolve_instance_overhead_mb_defaults_and_reads_the_supplied_value() {
    assert_eq!(
        resolved_tuning(serde_json::json!({}), 1)
            .scan
            .instance_overhead_mb,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        "absent → default 200"
    );

    let zero = resolved_tuning(serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "0" }), 1);
    assert_eq!(
        zero.scan.instance_overhead_mb, 0,
        "\"0\" is a valid overhead"
    );

    let valid = resolved_tuning(serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "256" }), 1);
    assert_eq!(valid.scan.instance_overhead_mb, 256);
}

#[test]
fn resolve_join_broadcast_max_bytes_defaults_and_reads_the_supplied_value() {
    assert_eq!(
        resolved_tuning(serde_json::json!({}), 1).join_broadcast_max_bytes,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        "absent → default 128 MiB"
    );
    assert_eq!(
        DEFAULT_JOIN_BROADCAST_MAX_BYTES, 134_217_728,
        "default must be exactly 128 MiB"
    );

    let valid = resolved_tuning(
        serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "67108864" }),
        1,
    );
    assert_eq!(valid.join_broadcast_max_bytes, 67_108_864);
}

#[test]
fn threading_mode_parses_case_insensitively() {
    for (value, expected) in [
        ("fixed", ThreadingMode::Fixed),
        ("FiXeD", ThreadingMode::Fixed),
        ("AUTO", ThreadingMode::Auto),
    ] {
        assert_eq!(
            checked(serde_json::json!({ PROP_DF_THREADING_MODE: value })).threading_mode,
            Some(expected),
            "'{value}' must parse to {expected:?}"
        );
    }
}

#[test]
fn threading_mode_defaults_to_auto() {
    assert_eq!(
        resolved_tuning(serde_json::json!({}), 8),
        resolved_tuning(serde_json::json!({ PROP_DF_THREADING_MODE: "AUTO" }), 8),
        "absent property → Auto"
    );
}

#[test]
fn auto_mode_uses_every_core_with_twice_as_many_partitions() {
    let absent = resolved_tuning(serde_json::json!({}), 8);
    assert_eq!(
        (
            absent.scan.df_target_partitions,
            absent.scan.df_threads_per_udf
        ),
        (16, 8),
        "8 cores → 8 threads, 16 target partitions"
    );

    let supplied = resolved_tuning(
        serde_json::json!({
            PROP_DF_THREADING_MODE: "AUTO",
            PROP_DF_TARGET_PARTITIONS: "99",
            PROP_DF_THREADS_PER_UDF: "99",
        }),
        8,
    );
    assert_eq!(
        (
            supplied.scan.df_target_partitions,
            supplied.scan.df_threads_per_udf
        ),
        (16, 8),
        "AUTO ignores supplied values"
    );
}

/// Scenario: AUTO mode yields a single thread on a one-core node
#[test]
fn auto_mode_yields_one_thread_on_one_core() {
    let tuning = resolved_tuning(serde_json::json!({}), 1);
    assert_eq!(
        (
            tuning.scan.df_target_partitions,
            tuning.scan.df_threads_per_udf
        ),
        (2, 1),
        "one core → 1 thread, 2 target partitions"
    );
}

#[test]
fn fixed_mode_uses_supplied_values() {
    let supplied = resolved_tuning(
        serde_json::json!({
            PROP_DF_THREADING_MODE: "FIXED",
            PROP_DF_TARGET_PARTITIONS: "3",
            PROP_DF_THREADS_PER_UDF: "2",
        }),
        16,
    );
    assert_eq!(
        (
            supplied.scan.df_target_partitions,
            supplied.scan.df_threads_per_udf
        ),
        (3, 2),
        "FIXED uses supplied values verbatim"
    );

    let absent = resolved_tuning(serde_json::json!({ PROP_DF_THREADING_MODE: "FIXED" }), 8);
    assert_eq!(
        (
            absent.scan.df_target_partitions,
            absent.scan.df_threads_per_udf
        ),
        (16, 8),
        "absent values fall back to the AUTO values"
    );
}

#[test]
fn resolve_s3_max_connections_fixed_value_wins() {
    assert_eq!(
        resolved_tuning(serde_json::json!({ PROP_S3_MAX_CONNECTIONS: "64" }), 8)
            .scan
            .s3_max_connections,
        64,
        "explicit S3_MAX_CONNECTIONS must be used verbatim"
    );
}

/// Scenario: AUTO derivation yields the single-core budget when the core count cannot be detected
#[test]
fn resolve_s3_max_connections_auto_scales_with_threads() {
    let two_threads_on_eight_cores = resolved_tuning(
        serde_json::json!({ PROP_DF_THREADING_MODE: "FIXED", PROP_DF_THREADS_PER_UDF: "2" }),
        8,
    );
    assert_eq!(
        two_threads_on_eight_cores.scan.s3_max_connections,
        2 * S3_CONNECTIONS_PER_THREAD,
        "an absent value derives from the resolved threads, not the cores"
    );
    assert_eq!(
        resolved_tuning(serde_json::json!({}), 1)
            .scan
            .s3_max_connections,
        S3_CONNECTIONS_PER_THREAD
    );
}
