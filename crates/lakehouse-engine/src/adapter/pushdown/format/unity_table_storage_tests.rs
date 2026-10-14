use super::*;
use crate::adapter::pushdown::test_support::{
    SENTINEL_ACCESS_KEY, SENTINEL_SECRET_KEY, user_message,
};

#[test]
fn redacted_masks_every_effective_storage_secret_in_a_raised_error() {
    let readable = "failed to resolve the current Delta version for table root \
                    's3://bucket/cat/sch/orders': signature mismatch for ";
    let raised = UdfError::User(format!(
        "{readable}{SENTINEL_ACCESS_KEY} signed with {SENTINEL_SECRET_KEY}"
    ));

    let message = user_message(redacted(
        raised,
        &[SENTINEL_ACCESS_KEY, SENTINEL_SECRET_KEY],
    ));

    assert!(message.starts_with(readable), "{message}");
    assert!(
        !message.contains(SENTINEL_ACCESS_KEY) && !message.contains(SENTINEL_SECRET_KEY),
        "{message}"
    );
}
