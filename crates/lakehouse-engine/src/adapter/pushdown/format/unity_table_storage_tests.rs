use super::*;

/// Two DISTINCT sentinel credential values, so a leak names which half of the
/// static credential escaped.
const SENTINEL_ACCESS_KEY: &str = "AKIA-SENTINEL-ACCESS-0001";
const SENTINEL_SECRET_KEY: &str = "sentinel-secret-value-0002";

const TABLE_ROOT: &str = "s3://bucket/cat/sch/orders";

/// Every effective-storage secret is masked, and only the secrets are: redaction is
/// the single guard between an object-store error that echoes a credential verbatim
/// and the text Exasol surfaces, so it must mask each value it was handed while
/// leaving the rest of the message readable enough to act on.
#[test]
fn redacted_masks_every_effective_storage_secret_in_a_raised_error() {
    let secrets = [SENTINEL_ACCESS_KEY, SENTINEL_SECRET_KEY];
    let raised = UdfError::User(format!(
        "failed to resolve the current Delta version for table root '{TABLE_ROOT}': \
         signature mismatch for {SENTINEL_ACCESS_KEY} signed with {SENTINEL_SECRET_KEY}"
    ));

    let message = match redacted(raised, &secrets) {
        UdfError::User(message) => message,
        other => panic!("redaction must answer a user error, got {other:?}"),
    };

    assert!(
        !message.contains(SENTINEL_ACCESS_KEY),
        "the access key must not survive redaction: {message}"
    );
    assert!(
        !message.contains(SENTINEL_SECRET_KEY),
        "the secret key must not survive redaction: {message}"
    );
    assert!(
        message.starts_with("failed to resolve the current Delta version"),
        "the non-secret text must survive verbatim: {message}"
    );
    assert!(
        message.contains(TABLE_ROOT),
        "the table root the read failed on must survive: {message}"
    );
}
