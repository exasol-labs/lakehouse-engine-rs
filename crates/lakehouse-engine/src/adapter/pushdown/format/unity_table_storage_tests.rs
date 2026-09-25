use super::*;
use crate::adapter::pushdown::test_support::{SENTINEL_ACCESS_KEY, SENTINEL_SECRET_KEY};

/// Every effective-storage secret is masked, and only the secrets are.
#[test]
fn redacted_masks_every_effective_storage_secret_in_a_raised_error() {
    let readable = "failed to resolve the current Delta version for table root \
                    's3://bucket/cat/sch/orders': signature mismatch for ";
    let raised = UdfError::User(format!(
        "{readable}{SENTINEL_ACCESS_KEY} signed with {SENTINEL_SECRET_KEY}"
    ));

    let message = match redacted(raised, &[SENTINEL_ACCESS_KEY, SENTINEL_SECRET_KEY]) {
        UdfError::User(message) => message,
        other => panic!("redaction must answer a user error, got {other:?}"),
    };

    assert!(message.starts_with(readable), "{message}");
    assert!(
        !message.contains(SENTINEL_ACCESS_KEY) && !message.contains(SENTINEL_SECRET_KEY),
        "{message}"
    );
}
