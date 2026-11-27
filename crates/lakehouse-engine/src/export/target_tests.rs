use super::*;
use crate::export::test_support::{CONNECTION, connection_object, merged};
use exasol_udf_sdk::test_support::TestContext;

const SECRET: &str = "sentinel-secret-key";

fn context(address: &str, password: serde_json::Value) -> TestContext {
    TestContext::set(vec![])
        .with_script_schema("LHVS")
        .with_current_user("EXPORTER")
        .with_connection(
            CONNECTION,
            connection_object(address, "", &password.to_string()),
        )
}

fn static_keys() -> serde_json::Value {
    serde_json::json!({
        "warehouse": "wh",
        "endpoint": "https://s3.example.com",
        "path_style": true,
        "region": "us-east-1",
        "access_key": "AKID",
        "secret_key": SECRET,
    })
}

fn with(password: serde_json::Value, extra: serde_json::Value) -> serde_json::Value {
    merged(password, extra)
}

fn refusal_of(address: &str, password: serde_json::Value, allow_http: bool) -> String {
    match read_connection(&context(address, password), CONNECTION, allow_http) {
        Err(error) => error.to_string(),
        Ok(_) => panic!("expected a refusal"),
    }
}

#[test]
fn a_supported_connection_is_read_with_its_address_and_credentials() {
    let connection = read_connection(
        &context("https://catalog.example.com", static_keys()),
        CONNECTION,
        false,
    )
    .expect("a supported CONNECTION");

    assert_eq!(connection.address, "https://catalog.example.com");
    assert_eq!(connection.creds.warehouse, "wh");
    assert_eq!(connection.creds.secret_key, SECRET);
    assert!(!connection.allow_http);
}

#[test]
fn a_connection_the_shared_checks_refuse_is_refused_naming_the_connection_and_the_field() {
    let cases = [
        (
            "a shared check: Azure storage with an IAM role",
            serde_json::json!({
                "warehouse": "wh",
                "account_name": "acct",
                "account_key": SECRET,
                "aws_assume_role_arn": "arn:aws:iam::1:role/r",
            }),
            vec![
                "CONNECTION 'LAKEHOUSE_CATALOG_CREDS'",
                "aws_assume_role_arn",
            ],
        ),
        (
            "a shared check: half an OAuth2 client",
            serde_json::json!({ "warehouse": "wh", "client_id": "id" }),
            vec!["CONNECTION 'LAKEHOUSE_CATALOG_CREDS'", "client_secret"],
        ),
    ];

    for (case, password, fragments) in cases {
        let message = refusal_of("https://catalog.example.com", password, true);
        assert!(
            message.starts_with("LAKEHOUSE_EXPORT: "),
            "{case}: {message}"
        );
        for fragment in fragments {
            assert!(
                message.contains(fragment),
                "{case}: {message} lacks {fragment}"
            );
        }
        assert!(!message.contains(SECRET), "{case}: {message}");
    }
}

#[test]
fn an_azure_connection_is_read_with_its_account_key_or_sas_token() {
    for (field, value) in [("account_key", SECRET), ("sas_token", "sv=1&sig=s")] {
        let mut password = serde_json::json!({ "warehouse": "wh", "account_name": "acct" });
        password[field] = value.into();

        let connection = read_connection(
            &context("https://catalog.example.com", password),
            CONNECTION,
            false,
        )
        .unwrap_or_else(|error| panic!("{field}: {error}"));

        assert_eq!(
            connection.creds.account_name.as_deref(),
            Some("acct"),
            "{field}"
        );
    }
}

#[test]
fn a_password_that_is_no_json_object_is_refused_naming_the_connection() {
    let message = refusal_of(
        "https://catalog.example.com",
        serde_json::json!(["not", "an", "object"]),
        false,
    );

    assert!(
        message
            .contains("CONNECTION 'LAKEHOUSE_CATALOG_CREDS' password is not a valid JSON object"),
        "{message}"
    );
}

#[test]
fn plain_http_is_refused_per_field_without_allow_http_and_accepted_with_it() {
    let role = serde_json::json!({ "aws_assume_role_arn": "arn:aws:iam::1:role/r" });
    let cases = [
        ("address", "http://catalog.example.com", static_keys()),
        (
            "oauth2_server_uri",
            "https://catalog.example.com",
            with(
                static_keys(),
                serde_json::json!({
                    "client_id": "id",
                    "client_secret": "s",
                    "oauth2_server_uri": "http://auth.example.com/token",
                }),
            ),
        ),
        (
            "aws_sts_endpoint",
            "https://catalog.example.com",
            with(
                with(static_keys(), role),
                serde_json::json!({ "aws_sts_endpoint": "HTTP://sts.example.com" }),
            ),
        ),
        (
            "endpoint",
            "https://catalog.example.com",
            with(
                static_keys(),
                serde_json::json!({ "endpoint": "http://s3.example.com" }),
            ),
        ),
    ];

    for (field, address, password) in cases {
        let message = refusal_of(address, password.clone(), false);
        for fragment in [
            format!("field {field} of CONNECTION 'LAKEHOUSE_CATALOG_CREDS'"),
            "plain HTTP".to_string(),
            "ALLOW_HTTP = 'true'".to_string(),
        ] {
            assert!(
                message.contains(&fragment),
                "{field}: {message} lacks {fragment}"
            );
        }
        assert!(!message.contains("example.com"), "{field}: {message}");

        read_connection(&context(address, password), CONNECTION, true)
            .unwrap_or_else(|error| panic!("{field} with ALLOW_HTTP: {error}"));
    }
}
