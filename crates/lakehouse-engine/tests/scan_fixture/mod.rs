//! The [`ResolvedScanStorage`] an inline-storage fixture spec stands for.
//!
//! One module rather than one copy per binary: thirteen sibling scan binaries in
//! this directory need the identical derivation, and `tests/common/mod.rs` cannot
//! carry it — that module is gated behind `feature = "exasol-e2e"` and its
//! siblings, and none of these binaries is feature-gated. The in-crate
//! `scan/test_support_tests.rs` copy cannot be shared either: a `#[path]` test
//! sibling inside the library crate cannot reach a `tests/` module.

use lakehouse_engine::scan::ResolvedScanStorage;
use lakehouse_engine::scan::spec::{ScanSpec, ScanStorage, StorageBackend};


pub fn resolved_storage(spec: &ScanSpec) -> ResolvedScanStorage {
    ResolvedScanStorage::from_backends(
        inline_backend(&spec.common.storage),
        spec.common
            .join
            .as_ref()
            .map(|join| inline_backend(&join.storage)),
    )
}

fn inline_backend(storage: &ScanStorage) -> StorageBackend {
    match storage {
        ScanStorage::Inline(backend) => backend.clone(),
        other => panic!("this fixture builds an inline storage value, not {other:?}"),
    }
}
