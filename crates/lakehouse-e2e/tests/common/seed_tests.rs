use super::*;
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};

fn get<'a>(props: &'a std::collections::HashMap<String, String>, key: &str) -> Option<&'a str> {
    props.get(key).map(String::as_str)
}

#[test]
fn default_auth_uses_static_seaweedfs_and_injects_no_catalog_auth() {
    let props = seed_catalog_props("http://lk:8181/catalog", "wh", &SeedCatalogAuth::default());

    assert_eq!(
        get(&props, REST_CATALOG_PROP_URI),
        Some("http://lk:8181/catalog")
    );
    assert_eq!(get(&props, REST_CATALOG_PROP_WAREHOUSE), Some("wh"));
    assert_eq!(get(&props, S3_ACCESS_KEY_ID), Some("lhadmin"));
    assert_eq!(get(&props, S3_SECRET_ACCESS_KEY), Some("lhadminsecret123"));
    assert_eq!(get(&props, S3_REGION), Some("us-east-1"));
    assert_eq!(get(&props, S3_PATH_STYLE_ACCESS), Some("true"));
    assert!(
        !props[S3_ENDPOINT].is_empty(),
        "S3 endpoint must default to the host SeaweedFS URL"
    );
    assert!(get(&props, "credential").is_none());
    assert!(get(&props, "oauth2-server-uri").is_none());
    assert!(get(&props, "scope").is_none());
    assert!(get(&props, "token").is_none());
    assert!(get(&props, ADLS_ACCOUNT_NAME).is_none());
    assert!(get(&props, ADLS_ACCOUNT_KEY).is_none());
}

#[test]
fn adls_storage_carries_the_account_key_and_no_s3_property() {
    let auth = SeedCatalogAuth {
        token: Some("bearer-xyz".to_string()),
        storage: SeedStorage::Adls {
            account_name: "lhrsstatic".to_string(),
            account_key: "a2V5".to_string(),
        },
    };
    let props = seed_catalog_props("http://lk:8181/catalog", "wh-azure", &auth);

    assert_eq!(get(&props, ADLS_ACCOUNT_NAME), Some("lhrsstatic"));
    assert_eq!(get(&props, ADLS_ACCOUNT_KEY), Some("a2V5"));
    assert_eq!(get(&props, REST_CATALOG_PROP_WAREHOUSE), Some("wh-azure"));
    // Catalog auth is orthogonal to storage: an ADLS seed still needs its bearer token.
    assert_eq!(get(&props, "token"), Some("bearer-xyz"));

    // `azdls_config_parse` silently discards `s3.*` properties, so a stray one would leak
    // SeaweedFS admin credentials into an Azure run invisibly.
    for s3_prop in [
        S3_ENDPOINT,
        S3_REGION,
        S3_ACCESS_KEY_ID,
        S3_SECRET_ACCESS_KEY,
        S3_PATH_STYLE_ACCESS,
    ] {
        assert!(
            get(&props, s3_prop).is_none(),
            "an ADLS seed must carry no {s3_prop}"
        );
    }
}

#[test]
fn static_bearer_token_is_injected_when_no_client_credentials() {
    let auth = SeedCatalogAuth {
        token: Some("bearer-xyz".to_string()),
        ..Default::default()
    };
    let props = seed_catalog_props("http://lk:8181/catalog", "wh", &auth);

    assert_eq!(get(&props, "token"), Some("bearer-xyz"));
    assert!(get(&props, "credential").is_none());
}

#[test]
fn events_seed_shape_is_identical_for_lakekeeper_and_baseline() {
    // Guards the row count and score formula the Lakekeeper scan assertions rely on.
    let mid = SEED_TOTAL_ROWS / 2;
    let file1 = make_events_batch(1, mid);
    let file2 = make_events_batch(mid + 1, SEED_TOTAL_ROWS);
    assert_eq!(file1.num_rows() + file2.num_rows(), SEED_TOTAL_ROWS);

    let score_col = file1
        .schema()
        .index_of("score")
        .expect("events has a score column");
    let count_gt_15: usize = [&file1, &file2]
        .iter()
        .map(|batch| {
            batch
                .column(score_col)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("score column is Float64")
                .iter()
                .flatten()
                .filter(|&score| score > 15.0)
                .count()
        })
        .sum();
    assert_eq!(count_gt_15, SEED_ROWS_SCORE_GT_15);
}
