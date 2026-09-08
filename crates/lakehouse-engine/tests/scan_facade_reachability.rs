//! Compile-time reachability probe for the `scan` module's public surface,
//! from an external-crate vantage.
#![allow(unused_imports)]

use lakehouse_engine::scan::spec::StorageBackend;
use lakehouse_engine::scan::{
    ResolvedScanStorage, build_alias_items, build_grouped_partial_agg_sql,
    build_join_physical_plan, build_partial_agg_sql_filtered, build_raw_scan_physical_plan,
    int96_coerced_parquet_format, register_files, run_join_scan_with_session,
    run_raw_scan_with_session,
};

const _FROM_BACKENDS: fn(StorageBackend, Option<StorageBackend>) -> ResolvedScanStorage =
    ResolvedScanStorage::from_backends;
