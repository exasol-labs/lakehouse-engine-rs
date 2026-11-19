use super::*;
use lakehouse_catalog::AdlsCred;
use object_store::memory::InMemory;

const LOCATION: &str = "abfss://container@account.dfs.core.windows.net/warehouse/ns/t";
const SAS: &str = "sv=2022-11-02&sr=c&sp=racwdl&sig=SAS_SIGNATURE_SECRET";

fn credentials() -> AdlsCredentials {
    AdlsCredentials::connection(&AdlsCred::Sas(SAS.into())).unwrap()
}

fn storage() -> AdlsStorage {
    AdlsStorage::new(LOCATION, Some(credentials())).unwrap()
}

#[test]
fn a_path_in_the_locations_container_is_its_object_key() {
    let storage = storage();

    let cases = [
        (
            format!("{LOCATION}/data/f.parquet"),
            "warehouse/ns/t/data/f.parquet",
        ),
        (
            "ABFSS://container@account.dfs.core.windows.net/warehouse/ns/t/metadata/m.json"
                .to_string(),
            "warehouse/ns/t/metadata/m.json",
        ),
        (
            "abfss://container@account.dfs.core.windows.net/elsewhere".to_string(),
            "elsewhere",
        ),
    ];

    for (path, key) in cases {
        assert_eq!(storage.key(&path).unwrap().as_ref(), key, "{path}");
    }
}

#[test]
fn a_path_outside_the_locations_container_is_refused_naming_the_path() {
    let storage = storage();

    for path in [
        "abfss://other@account.dfs.core.windows.net/warehouse/ns/t/f",
        "abfss://container@other.dfs.core.windows.net/warehouse/ns/t/f",
        "abfss://container@account.dfs.core.windows.netx/f",
        "s3://container/warehouse/ns/t/f",
        "warehouse/ns/t/f",
    ] {
        let refused = storage.key(path).unwrap_err().to_string();

        assert!(
            refused.contains(&format!(
                "the path {path} lies outside the ADLS container \
                 abfss://container@account.dfs.core.windows.net of the table"
            )),
            "{refused}"
        );
    }
}

#[tokio::test]
async fn a_storage_restored_from_its_serialized_form_refuses_every_request() {
    let factory = AdlsStorageFactory {
        location: LOCATION.into(),
        credentials: Some(credentials()),
    };
    let serialized = serde_json::to_string(&factory).unwrap();
    let restored: AdlsStorageFactory = serde_json::from_str(&serialized).unwrap();
    let storage = restored.build(&StorageConfig::new()).unwrap();

    let refused = storage
        .exists(&format!("{LOCATION}/f"))
        .await
        .unwrap_err()
        .to_string();

    assert!(!serialized.contains("SAS_SIGNATURE_SECRET"), "{serialized}");
    assert!(
        refused.contains("has no credentials and sends no request"),
        "{refused}"
    );
}

#[test]
fn debug_shows_the_container_and_no_credential() {
    let factory = AdlsStorageFactory {
        location: LOCATION.into(),
        credentials: Some(credentials()),
    };

    let shown = format!("{factory:?} {:?}", storage());

    assert!(shown.contains(LOCATION), "{shown}");
    assert!(!shown.contains("SAS_SIGNATURE_SECRET"), "{shown}");
}

/// The storage of [`LOCATION`]'s container over an in-memory store.
fn memory_storage() -> AdlsStorage {
    AdlsStorage {
        scheme: "abfss".into(),
        authority: "container@account.dfs.core.windows.net".into(),
        store: Some(Arc::new(InMemory::new())),
    }
}

fn at(name: &str) -> String {
    format!("{LOCATION}/{name}")
}

#[tokio::test]
async fn a_written_file_reads_back_whole_and_by_range() {
    let storage = memory_storage();
    let path = at("metadata/v1.json");

    storage
        .write(&path, Bytes::from_static(b"hello world"))
        .await
        .unwrap();

    assert!(storage.exists(&path).await.unwrap());
    assert_eq!(storage.metadata(&path).await.unwrap().size, 11);
    assert_eq!(storage.read(&path).await.unwrap(), "hello world");
    let reader = storage.reader(&path).await.unwrap();
    assert_eq!(reader.read(6..11).await.unwrap(), "world");
    assert_eq!(
        storage.new_input(&path).unwrap().read().await.unwrap(),
        "hello world"
    );
}

#[tokio::test]
async fn a_missing_file_does_not_exist_reads_fail_naming_it_and_its_delete_succeeds() {
    let storage = memory_storage();
    let path = at("data/missing.parquet");

    assert!(!storage.exists(&path).await.unwrap());
    let refused = [
        storage.metadata(&path).await.err().unwrap().to_string(),
        storage.read(&path).await.unwrap_err().to_string(),
        storage
            .reader(&path)
            .await
            .unwrap()
            .read(0..1)
            .await
            .unwrap_err()
            .to_string(),
    ];
    for (refused, action) in refused.iter().zip(["look up", "read", "read"]) {
        assert!(
            refused.contains(&format!("could not {action} {path} on ADLS")),
            "{refused}"
        );
    }
    storage.delete(&path).await.unwrap();
}

#[tokio::test]
async fn a_writer_uploads_its_parts_as_one_file_of_their_total_size_and_then_refuses_writes() {
    let storage = memory_storage();
    let path = at("data/f.parquet");
    let mut writer = storage.new_output(&path).unwrap().writer().await.unwrap();

    writer.write(Bytes::from_static(b"ab")).await.unwrap();
    writer.write(Bytes::from_static(b"cd")).await.unwrap();
    let written = writer.close().await.unwrap();
    let after_close = [
        writer.write(Bytes::from_static(b"e")).await.unwrap_err(),
        writer.close().await.err().unwrap(),
    ];

    assert_eq!(written.size, 4);
    assert_eq!(storage.read(&path).await.unwrap(), "abcd");
    for refused in after_close {
        assert!(
            refused
                .to_string()
                .contains(&format!("the ADLS file {path} is already closed")),
            "{refused}"
        );
    }
}

#[tokio::test]
async fn delete_prefix_removes_the_files_under_the_prefix_and_delete_stream_each_listed_file() {
    let storage = memory_storage();
    let paths = ["data/a/1", "data/a/2", "data/ab/1", "data/b/1"].map(at);
    for path in &paths {
        storage.write(path, Bytes::from_static(b"x")).await.unwrap();
    }

    storage.delete_prefix(&at("data/a")).await.unwrap();
    let after_prefix = [
        storage.exists(&paths[0]).await.unwrap(),
        storage.exists(&paths[1]).await.unwrap(),
        storage.exists(&paths[2]).await.unwrap(),
    ];
    storage
        .delete_stream(futures::stream::iter([paths[2].clone(), paths[3].clone()]).boxed())
        .await
        .unwrap();

    assert_eq!(after_prefix, [false, false, true]);
    assert!(!storage.exists(&paths[2]).await.unwrap());
    assert!(!storage.exists(&paths[3]).await.unwrap());
}
