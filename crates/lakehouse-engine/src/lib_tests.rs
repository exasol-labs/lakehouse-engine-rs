use exasol_udf_sdk::test_support::DefaultsCtx;

use super::ENGINE_VERSION;

#[test]
fn engine_version_is_a_plain_semver_string() {
    assert!(
        !ENGINE_VERSION.is_empty(),
        "ENGINE_VERSION must not be empty"
    );
    let parts: Vec<&str> = ENGINE_VERSION.split('.').collect();
    assert_eq!(
        parts.len(),
        3,
        "ENGINE_VERSION must be a plain X.Y.Z string, got {ENGINE_VERSION:?}"
    );
    for part in parts {
        assert!(
            !part.is_empty() && part.chars().all(|c| c.is_ascii_digit()),
            "each ENGINE_VERSION component must be all-digits, got {part:?} in {ENGINE_VERSION:?}"
        );
    }
}

#[test]
fn lakehouse_version_returns_the_engine_version() {
    let result = super::lakehouse_version(&mut DefaultsCtx).unwrap();
    assert_eq!(result.as_deref(), Some(ENGINE_VERSION));
}
