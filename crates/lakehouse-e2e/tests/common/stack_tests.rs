use super::CatalogConnectionPassword;

fn base_password() -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        warehouse: "s3://warehouse/".to_string(),
        endpoint: "http://seaweedfs:8333".to_string(),
        region: "us-east-1".to_string(),
        access_key: "lhadmin".to_string(),
        secret_key: "lhadminsecret123".to_string(),
        path_style: true,
        ..Default::default()
    }
}

#[test]
fn omits_catalog_auth_fields_when_absent() {
    let json_str = base_password().to_sql_password_json();
    let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    for key in [
        "token",
        "client_id",
        "client_secret",
        "oauth2_server_uri",
        "scope",
        "account_name",
        "account_key",
        "aws_assume_role_arn",
        "aws_external_id",
        "aws_sts_endpoint",
    ] {
        assert!(
            parsed.get(key).is_none(),
            "expected key '{key}' to be absent, found: {parsed}"
        );
    }
}

#[test]
fn serializes_token_auth_field_when_present() {
    let password = CatalogConnectionPassword {
        token: Some("bearer-token".to_string()),
        ..base_password()
    };
    let json_str = password.to_sql_password_json();
    let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    assert_eq!(parsed["token"], "bearer-token");
}

#[test]
fn serializes_oauth2_auth_fields_when_present() {
    let password = CatalogConnectionPassword {
        client_id: Some("my-client".to_string()),
        client_secret: Some("my-secret".to_string()),
        oauth2_server_uri: Some("https://idp.example.com/token".to_string()),
        scope: Some("catalog".to_string()),
        ..base_password()
    };
    let json_str = password.to_sql_password_json();
    let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    assert_eq!(parsed["client_id"], "my-client");
    assert_eq!(parsed["client_secret"], "my-secret");
    assert_eq!(parsed["oauth2_server_uri"], "https://idp.example.com/token");
    assert_eq!(parsed["scope"], "catalog");
}

#[test]
fn serializes_azure_account_fields_when_present() {
    let password = CatalogConnectionPassword {
        account_name: Some("mystorageacct".to_string()),
        account_key: Some("base64-encoded-key".to_string()),
        ..base_password()
    };
    let json_str = password.to_sql_password_json();
    let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    assert_eq!(parsed["account_name"], "mystorageacct");
    assert_eq!(parsed["account_key"], "base64-encoded-key");
}

#[test]
fn serializes_assume_role_fields_when_present() {
    let password = CatalogConnectionPassword {
        aws_assume_role_arn: Some(
            "arn:aws:iam::123456789012:role/lakehouse-assume-role-demo".to_string(),
        ),
        aws_external_id: Some("lh+ext=id/2026:demo@example".to_string()),
        aws_sts_endpoint: Some("http://seaweedfs:8333".to_string()),
        ..base_password()
    };
    let json_str = password.to_sql_password_json();
    let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    assert_eq!(
        parsed["aws_assume_role_arn"],
        "arn:aws:iam::123456789012:role/lakehouse-assume-role-demo"
    );
    assert_eq!(parsed["aws_external_id"], "lh+ext=id/2026:demo@example");
    assert_eq!(parsed["aws_sts_endpoint"], "http://seaweedfs:8333");
}
