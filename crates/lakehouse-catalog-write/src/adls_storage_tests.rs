use super::*;
use lakehouse_catalog::AdlsCred;

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
