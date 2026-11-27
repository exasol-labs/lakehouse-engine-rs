use super::*;
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};

fn get<'a>(props: &'a std::collections::HashMap<String, String>, key: &str) -> Option<&'a str> {
    props.get(key).map(String::as_str)
}

/// The storage factory holds the storage credentials, so the catalog props carry none.
#[test]
fn default_auth_carries_the_catalog_address_and_no_auth_or_storage_property() {
    let props = seed_catalog_props("http://lk:8181/catalog", "wh", &SeedCatalogAuth::default());

    assert_eq!(
        props,
        std::collections::HashMap::from([
            (
                REST_CATALOG_PROP_URI.to_string(),
                "http://lk:8181/catalog".to_string()
            ),
            (REST_CATALOG_PROP_WAREHOUSE.to_string(), "wh".to_string()),
        ])
    );
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
