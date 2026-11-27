use super::*;
use object_store::memory::InMemory;

const ROOT: &str = "abfss://container@account.dfs.core.windows.net/";
const LOCATION: &str = "abfss://container@account.dfs.core.windows.net/warehouse/ns/t";

/// A FileIO's storage over in-memory clients, which addresses only [`ROOT`].
fn memory_storage() -> ObjectStorage {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ObjectStorage::new(Arc::new(move |root| {
        if root == ROOT {
            Ok(store.clone())
        } else {
            Err(object_store::Error::Generic {
                store: "test",
                source: format!("{root} is outside {ROOT}").into(),
            })
        }
    }))
}

fn at(name: &str) -> String {
    format!("{LOCATION}/{name}")
}

#[test]
fn a_path_splits_into_its_root_with_a_lowercase_scheme_and_its_object_key() {
    let cases = [
        (at("data/f.parquet"), ROOT, "warehouse/ns/t/data/f.parquet"),
        (
            "ABFSS://container@account.dfs.core.windows.net/warehouse/m.json".to_string(),
            ROOT,
            "warehouse/m.json",
        ),
        ("s3://bucket//a/b".to_string(), "s3://bucket/", "a/b"),
        ("s3://bucket".to_string(), "s3://bucket/", ""),
    ];

    for (path, root, key) in cases {
        let (split_root, split_key) = split_path(&path).unwrap();
        assert_eq!(
            (split_root.as_str(), split_key.as_ref()),
            (root, key),
            "{path}"
        );
    }
}

#[tokio::test]
async fn a_path_without_a_scheme_or_outside_the_addressed_roots_is_refused_naming_it() {
    let storage = memory_storage();

    let no_scheme = storage.exists("warehouse/ns/t/f").await.unwrap_err();
    let outside = storage
        .exists("abfss://other@account.dfs.core.windows.net/f")
        .await
        .unwrap_err();

    assert!(
        no_scheme
            .to_string()
            .contains("the path warehouse/ns/t/f has no scheme"),
        "{no_scheme}"
    );
    assert!(
        outside
            .to_string()
            .contains("could not address abfss://other@account.dfs.core.windows.net/f"),
        "{outside}"
    );
}

#[tokio::test]
async fn a_storage_restored_from_its_serialized_form_refuses_every_request() {
    let factory = ObjectStorageFactory {
        storage: memory_storage(),
    };
    let serialized = serde_json::to_string(&factory).unwrap();
    let restored: ObjectStorageFactory = serde_json::from_str(&serialized).unwrap();
    let storage = restored.build(&StorageConfig::new()).unwrap();

    let refused = storage.exists(&at("f")).await.unwrap_err().to_string();

    assert!(
        refused.contains("has no credentials and sends no request"),
        "{refused}"
    );
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
    assert!(format!("{storage:?}").contains(ROOT));
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
            refused.contains(&format!("could not {action} {path}")),
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
                .contains(&format!("the file {path} is already closed")),
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
