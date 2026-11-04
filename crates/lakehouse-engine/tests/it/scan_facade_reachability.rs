//! Compile-time reachability probe for the `scan` module's public surface.
#![allow(unused_imports)]

use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use lakehouse_engine::scan::spec::CommonScanSpec;
use lakehouse_engine::scan::{
    ResolvedScanStorage, build_alias_items, build_grouped_partial_agg_sql,
    build_join_physical_plan, build_partial_agg_sql_filtered, build_raw_scan_physical_plan,
    int96_coerced_parquet_format, register_files, resolve_scan_storage, run_join_scan_with_session,
    run_raw_scan_with_session,
};

const _RESOLVE_SCAN_STORAGE: fn(
    &CommonScanSpec,
    &dyn UdfContext,
) -> Result<ResolvedScanStorage, UdfError> = resolve_scan_storage;
