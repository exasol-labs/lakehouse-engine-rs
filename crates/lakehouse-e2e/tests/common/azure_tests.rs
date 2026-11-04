use super::{
    CONTAINER_NAME_PREFIX, MAX_CONTAINER_NAME_LEN, MIN_CONTAINER_NAME_LEN, azure_failure,
    delete_reached_desired_state, derive_container_name, is_name_collision,
};
use azure_core::error::ErrorKind;
use azure_core::http::StatusCode;
use azure_storage_blob::models::StorageErrorCode;

const FIXED_MILLIS: u128 = 1_762_000_000_000;

/// The segment's alphabet and separators are `cloud_fixture`'s; this checks the prefix join
/// and Azure's length range.
#[test]
fn container_name_is_azure_and_lakekeeper_legal() {
    let ninety_chars = "A".repeat(90);
    for user in ["", "ÜBER_user", ninety_chars.as_str()] {
        let name = derive_container_name(user, FIXED_MILLIS);
        assert!(
            (MIN_CONTAINER_NAME_LEN..=MAX_CONTAINER_NAME_LEN).contains(&name.len()),
            "user {user:?}: name {name:?} must be {MIN_CONTAINER_NAME_LEN} to \
             {MAX_CONTAINER_NAME_LEN} characters"
        );
        assert!(
            name.starts_with(&format!("{CONTAINER_NAME_PREFIX}-"))
                && !name.contains("--")
                && name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "user {user:?}: name {name:?} must join the {CONTAINER_NAME_PREFIX} prefix by one \
             hyphen and hold only [a-z0-9-]"
        );
    }

    assert_eq!(
        derive_container_name(&ninety_chars, FIXED_MILLIS).len(),
        MAX_CONTAINER_NAME_LEN,
        "an over-long user is truncated to exactly the remaining budget"
    );
}

#[test]
fn failure_without_a_service_response_carries_no_error_code() {
    let failures = [
        azure_core::Error::with_message(ErrorKind::Connection, "connection refused"),
        azure_core::Error::with_message(ErrorKind::Io, "read timed out"),
        azure_core::Error::with_message(ErrorKind::Credential, "token request rejected"),
        ErrorKind::HttpResponse {
            status: StatusCode::NotFound,
            error_code: Some("ContainerNotFound".to_string()),
            raw_response: None,
        }
        .into_error(),
    ];

    for failure in failures {
        let rendered = failure.to_string();
        let (code, description) = azure_failure(failure);

        assert!(
            code.is_none(),
            "{rendered}: a failure carrying no service response must yield no Azure error \
             code, got {code:?}"
        );
        assert!(
            !description.is_empty(),
            "{rendered}: the failure must still be described, or teardown reports a leak \
             with no reason"
        );
    }
}

#[test]
fn container_guard_keys_each_spec_clause_on_exactly_one_code() {
    let unmapped = StorageErrorCode::UnknownValue("SomethingNew".to_string());

    assert!(
        delete_reached_desired_state(Some(&StorageErrorCode::ContainerNotFound)),
        "a container already absent at delete time SHALL be treated as deleted"
    );
    for other in [
        Some(&StorageErrorCode::ContainerAlreadyExists),
        Some(&unmapped),
        None,
    ] {
        assert!(
            !delete_reached_desired_state(other),
            "{other:?} is not an absent container: treating it as the desired end state \
             would report a surviving container as cleaned up"
        );
    }

    assert!(
        is_name_collision(Some(&StorageErrorCode::ContainerAlreadyExists)),
        "a name collision at create time SHALL fail the run"
    );
    for other in [
        Some(&StorageErrorCode::ContainerNotFound),
        Some(&unmapped),
        None,
    ] {
        assert!(
            !is_name_collision(other),
            "{other:?} is not a name collision: it must fail with its own description \
             instead of one blaming the container name"
        );
    }
}
