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
