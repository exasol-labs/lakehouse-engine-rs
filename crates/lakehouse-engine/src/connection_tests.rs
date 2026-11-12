use super::*;

#[test]
fn token_parsed_from_json() {
    let json = serde_json::json!({
        "warehouse": "wh",
        "token": "my-secret-token"
    });
    let creds = parse_creds(&json);

    assert_eq!(creds.token.as_deref(), Some("my-secret-token"));
    assert_eq!(creds.client_id, None);
    assert_eq!(creds.client_secret, None);
    assert_eq!(creds.oauth2_server_uri, None);
    assert_eq!(creds.scope, None);
}

#[test]
fn oauth_client_creds_parsed_from_json() {
    let json = serde_json::json!({
        "warehouse": "wh",
        "client_id": "my-client-id",
        "client_secret": "my-client-secret",
        "oauth2_server_uri": "https://auth.example.com/token",
        "scope": "catalog:read"
    });
    let creds = parse_creds(&json);

    assert_eq!(creds.client_id.as_deref(), Some("my-client-id"));
    assert_eq!(creds.client_secret.as_deref(), Some("my-client-secret"));
    assert_eq!(
        creds.oauth2_server_uri.as_deref(),
        Some("https://auth.example.com/token")
    );
    assert_eq!(creds.scope.as_deref(), Some("catalog:read"));
    assert_eq!(creds.token, None);
}

#[test]
fn oauth_optional_fields_absent_when_not_supplied() {
    let json = serde_json::json!({
        "warehouse": "wh",
        "client_id": "my-client-id",
        "client_secret": "my-client-secret"
    });
    let creds = parse_creds(&json);

    assert_eq!(creds.oauth2_server_uri, None);
    assert_eq!(creds.scope, None);
}

#[test]
fn new_auth_fields_default_to_none() {
    let json = serde_json::json!({
        "warehouse": "wh",
        "endpoint": "http://s3.example.com",
        "region": "us-east-1",
        "access_key": "AKID",
        "secret_key": "SECRET"
    });
    let creds = parse_creds(&json);

    assert_eq!(creds.token, None);
    assert_eq!(creds.client_id, None);
    assert_eq!(creds.client_secret, None);
    assert_eq!(creds.oauth2_server_uri, None);
    assert_eq!(creds.scope, None);
}
