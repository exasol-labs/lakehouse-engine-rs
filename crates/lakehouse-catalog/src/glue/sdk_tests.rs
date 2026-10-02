use aws_sdk_glue::config::retry::RetryMode;

use super::*;

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
    let client = glue_client(
        "https://glue.eu-west-1.amazonaws.com",
        &ConnectionCreds::default(),
        "eu-west-1".to_string(),
    );

    let retry = client.config().retry_config().expect("a retry policy");
    assert_eq!(retry.mode(), RetryMode::Standard);
    assert_eq!(retry.max_attempts(), 5);
    assert_eq!(
        client
            .config()
            .timeout_config()
            .and_then(|timeouts| timeouts.operation_timeout()),
        Some(Duration::from_secs(30))
    );
}
