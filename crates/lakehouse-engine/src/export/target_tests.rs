use super::*;
use exasol_udf_sdk::connection::ConnectionObject;
use exasol_udf_sdk::test_support::TestContext;

const CONNECTION: &str = "EXPORT_CONN";
const SECRET: &str = "sentinel-secret-key";

fn context(address: &str, password: serde_json::Value) -> TestContext {
    TestContext::set(vec![])
        .with_script_schema("LHVS")
        .with_current_user("EXPORTER")
        .with_connection(
            CONNECTION,
            ConnectionObject {
                kind: "password".to_string(),
                address: address.to_string(),
                user: String::new(),
                password: password.to_string(),
            },
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

fn with(mut password: serde_json::Value, extra: serde_json::Value) -> serde_json::Value {
    let fields = password.as_object_mut().expect("an object");
    fields.extend(extra.as_object().cloned().unwrap_or_default());
    password
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
fn an_unwritable_connection_is_refused_naming_the_connection_and_the_field() {
    let cases = [
        (
            "no warehouse",
            serde_json::json!({ "token": "t" }),
            vec!["CONNECTION 'EXPORT_CONN'", "warehouse"],
        ),
        (
            "Azure account key",
            serde_json::json!({ "warehouse": "wh", "account_name": "acct", "account_key": SECRET }),
            vec![
                "CONNECTION 'EXPORT_CONN'",
                "account_name, account_key",
                "S3-compatible",
            ],
        ),
        (
            "Azure SAS token",
            serde_json::json!({ "warehouse": "wh", "account_name": "acct", "sas_token": SECRET }),
            vec!["CONNECTION 'EXPORT_CONN'", "sas_token", "S3-compatible"],
        ),
        (
            "SigV4 signing",
            with(static_keys(), serde_json::json!({ "use_sigv4": true })),
            vec!["CONNECTION 'EXPORT_CONN'", "use_sigv4", "SigV4"],
        ),
        (
            "a shared check: half an OAuth2 client",
            serde_json::json!({ "warehouse": "wh", "client_id": "id" }),
            vec!["CONNECTION 'EXPORT_CONN'", "client_secret"],
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
fn a_password_that_is_no_json_object_is_refused_naming_the_connection() {
    let message = refusal_of(
        "https://catalog.example.com",
        serde_json::json!(["not", "an", "object"]),
        false,
    );

    assert!(
        message.contains("CONNECTION 'EXPORT_CONN' password is not a valid JSON object"),
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
            format!("field {field} of CONNECTION 'EXPORT_CONN'"),
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
