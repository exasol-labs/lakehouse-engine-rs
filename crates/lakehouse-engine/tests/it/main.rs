//! Every host integration test of this crate links into this one binary, because each separate
//! binary statically links the whole engine (#523). A new test file is a module declared here.

#[path = "../scan_fixture/mod.rs"]
mod scan_fixture;

mod boolean_to_string_casing_test;
mod build_convention;
mod catalog_session_signatures;
mod int96_decode_test;
mod micro_bench;
mod pushdown_public_surface;
mod scan_agg_projection_pruning;
mod scan_batch_loop;
mod scan_column_binding;
mod scan_deletion_vectors;
mod scan_facade_reachability;
mod scan_join_test;
mod scan_name_mapping;
mod scan_no_head_test;
mod scan_parquet_pruning;
mod scan_partition_values;
mod scan_plan_shape;
mod scan_positional_deletes;
mod scan_telemetry;
mod scan_two_arg;
mod shared_type_reexports;
mod timestamp_literal_precision_test;
