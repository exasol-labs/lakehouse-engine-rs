use aws_sdk_glue::config::retry::RetryMode;

use super::*;

/// Scenario: Every listing follows its continuation tokens and stops on an empty page
#[test]
fn a_listing_continues_only_on_a_non_empty_token_after_a_non_empty_page() {
    assert_eq!(
        next_page_token(Some("tables-1"), 1),
        Some("tables-1".to_string()),
        "a short page with a token does not end the listing"
    );
    assert_eq!(
        next_page_token(Some("tables-2"), 0),
        None,
        "an empty page ends the listing although it carries a token"
    );
    assert_eq!(next_page_token(Some(""), 3), None);
    assert_eq!(next_page_token(None, 3), None);
}

/// Scenario: The CatalogId is sent only when the CONNECTION names one
#[test]
fn catalog_id_is_set_only_for_a_non_empty_warehouse() {
    assert_eq!(catalog_id(""), None);
    assert_eq!(catalog_id("   "), None);
    assert_eq!(catalog_id("123456789012"), Some("123456789012".to_string()));
}

/// Scenario: Every Glue call retries within a bounded time
#[test]
fn every_call_retries_five_attempts_within_thirty_seconds() {
    let creds = ConnectionCreds {
        access_key: "AKID".to_string(),
        secret_key: "SECRET".to_string(),
        ..ConnectionCreds::default()
    };

    let config = glue_config(
        "https://glue.eu-west-1.amazonaws.com",
        &creds,
        "eu-west-1".to_string(),
    )
    .build();

    let retry = config.retry_config().expect("a retry policy");
    assert_eq!(retry.mode(), RetryMode::Standard);
    assert_eq!(retry.max_attempts(), 5);
    assert_eq!(
        config
            .timeout_config()
            .and_then(|timeouts| timeouts.operation_timeout()),
        Some(Duration::from_secs(30))
    );
}
