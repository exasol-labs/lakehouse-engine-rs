//! Compile-time reachability probe for the `scan` module's public surface.
#![allow(unused_imports)]

use crate::scan::spec::{CommonScanSpec, JoinSpec, ScanStorage};
use crate::scan::{
    FieldIdExprAdapterFactory, FieldIdResolution, PARQUET_FIELD_ID_META_KEY, ResolvedScanStorage,
    build_alias_items, build_grouped_partial_agg_sql, build_join_physical_plan,
    build_partial_agg_sql_filtered, build_raw_scan_physical_plan, int96_coerced_parquet_format,
    reconstruct_abs_uri, register_files, run_join_scan_with_session, run_raw_scan_with_session,
};

const _FROM_BACKENDS: fn(
    crate::scan::spec::StorageBackend,
    Option<crate::scan::spec::StorageBackend>,
) -> ResolvedScanStorage = ResolvedScanStorage::from_backends;

use crate::scan::build_partial_agg_sql;
