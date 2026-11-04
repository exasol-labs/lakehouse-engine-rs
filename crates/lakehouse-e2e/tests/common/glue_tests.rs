use super::super::cloud_fixture::panic_message;
use super::{
    ACCESS_KEY_ID_VAR, ASSUME_ROLE_ARN_VAR, ASSUME_ROLE_BASE_ACCESS_KEY_ID_VAR,
    ASSUME_ROLE_BASE_SECRET_ACCESS_KEY_VAR, ASSUME_ROLE_EXTERNAL_ID_VAR, DATABASE_PREFIX,
    FIXTURE_BUCKET_VAR, GlueEnv, MAX_DATABASE_NAME_LEN, REGION_VAR, SECRET_ACCESS_KEY_VAR,
    database_name, derive_run_id,
};

const FIXED_MILLIS: u128 = 1_762_000_000_000;

const VALUES: [(&str, &str); 8] = [
    (ACCESS_KEY_ID_VAR, "AKIAACCESSKEYSENTINEL"),
    (SECRET_ACCESS_KEY_VAR, "secret/access+key/sentinel"),
    (REGION_VAR, "eu-sentinel-1"),
    (FIXTURE_BUCKET_VAR, "bucket-sentinel"),
    (ASSUME_ROLE_BASE_ACCESS_KEY_ID_VAR, "AKIABASEKEYSENTINEL"),
    (
        ASSUME_ROLE_BASE_SECRET_ACCESS_KEY_VAR,
        "base/secret+key/sentinel",
    ),
    (
        ASSUME_ROLE_ARN_VAR,
        "arn:aws:iam::000000000000:role/sentinel",
    ),
    (ASSUME_ROLE_EXTERNAL_ID_VAR, "external-id-sentinel"),
];

fn value_of(name: &str) -> Option<String> {
    VALUES
        .iter()
        .find(|(var, _)| *var == name)
        .map(|(_, value)| value.to_string())
}

/// specs/testing.md § Per-run cloud resources
#[test]
fn run_id_is_a_legal_glue_database_name() {
    let three_hundred_chars = "A".repeat(300);
    for user in ["", "ÜBER-user", three_hundred_chars.as_str()] {
        let name = database_name(&derive_run_id(user, FIXED_MILLIS));
        assert!(
            name.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "user {user:?}: name {name:?} must contain only lowercase letters, digits, and _"
        );
        assert!(
            name.starts_with(DATABASE_PREFIX) && !name.contains("__"),
            "user {user:?}: name {name:?} must keep the {DATABASE_PREFIX} prefix the sweep \
             matches, joined by a single underscore"
        );
    }
    assert_eq!(
        database_name(&derive_run_id(&three_hundred_chars, FIXED_MILLIS)).len(),
        MAX_DATABASE_NAME_LEN,
        "an over-long user is truncated to exactly Glue's limit"
    );
}

/// specs/testing.md § Failure contract
#[test]
fn glue_env_reads_all_eight_variables_up_front() {
    let env = GlueEnv::from_lookup(value_of);
    assert_eq!(
        [
            env.access_key_id.as_str(),
            env.secret_access_key(),
            env.region.as_str(),
            env.fixture_bucket.as_str(),
            env.assume_role_base_access_key_id.as_str(),
            env.assume_role_base_secret_access_key.as_str(),
            env.assume_role_arn.as_str(),
            env.assume_role_external_id.as_str(),
        ],
        VALUES.map(|(_, value)| value)
    );

    for (missing, _) in VALUES {
        let message = panic_message(|| {
            GlueEnv::from_lookup(|name| (name != missing).then(|| value_of(name)).flatten());
        });
        assert!(
            message.contains(missing),
            "the panic for a missing {missing} must name the variable, got: {message}"
        );
        for (_, value) in VALUES {
            assert!(
                !message.contains(value),
                "the panic for a missing {missing} must echo no variable value, got: {message}"
            );
        }
    }
}

/// specs/testing.md § Credentials in tests
#[test]
fn glue_env_redacts_both_secret_keys_and_the_external_id() {
    let env = GlueEnv::from_lookup(value_of);
    let redacted = env.redact(&VALUES.map(|(_, value)| value).join(" "));
    for secret in [
        SECRET_ACCESS_KEY_VAR,
        ASSUME_ROLE_BASE_SECRET_ACCESS_KEY_VAR,
        ASSUME_ROLE_EXTERNAL_ID_VAR,
    ]
    .map(|name| value_of(name).unwrap())
    {
        assert!(!redacted.contains(&secret), "{secret} leaked: {redacted}");
    }
}
