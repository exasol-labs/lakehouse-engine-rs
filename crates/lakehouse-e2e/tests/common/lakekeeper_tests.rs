use super::*;

#[test]
fn lakekeeper_connection_password_static_populates_oauth_and_static_s3() {
    let pw = lakekeeper_connection_password(WAREHOUSE_STATIC, false);

    assert_eq!(pw.warehouse, WAREHOUSE_STATIC);
    assert!(!pw.use_vended_credentials);
    assert_eq!(pw.client_id.as_deref(), Some(OAUTH_CLIENT_ID));
    assert_eq!(pw.client_secret.as_deref(), Some(OAUTH_CLIENT_SECRET));
    assert_eq!(
        pw.oauth2_server_uri.as_deref(),
        Some("http://keycloak:8080/realms/iceberg/protocol/openid-connect/token")
    );
    assert_eq!(pw.endpoint, "http://seaweedfs:8333");
    assert_eq!(pw.region, S3_REGION);
    assert_eq!(pw.access_key, STATIC_ACCESS_KEY);
    assert_eq!(pw.secret_key, STATIC_SECRET_KEY);
    assert!(pw.path_style);
    assert_eq!(pw.session_token, None);
    // SigV4 and OAuth are mutually exclusive.
    assert!(!pw.use_sigv4);
}

#[test]
fn lakekeeper_connection_password_vended_omits_static_s3() {
    let pw = lakekeeper_connection_password(WAREHOUSE_VENDED, true);

    assert_eq!(pw.warehouse, WAREHOUSE_VENDED);
    assert!(pw.use_vended_credentials);
    assert_eq!(pw.client_id.as_deref(), Some(OAUTH_CLIENT_ID));
    assert_eq!(pw.client_secret.as_deref(), Some(OAUTH_CLIENT_SECRET));
    assert_eq!(
        pw.oauth2_server_uri.as_deref(),
        Some("http://keycloak:8080/realms/iceberg/protocol/openid-connect/token")
    );
    assert_eq!(pw.endpoint, "");
    assert_eq!(pw.region, "");
    assert_eq!(pw.access_key, "");
    assert_eq!(pw.secret_key, "");
    assert!(!pw.use_sigv4);
    assert_eq!(pw.account_name, None);
    assert_eq!(pw.account_key, None);
    assert_eq!(pw.session_token, None);
    assert!(pw.path_style);

    let json_str = pw.to_sql_password_json();
    let parsed: serde_json::Value =
        serde_json::from_str(&json_str).expect("password serializes to valid JSON");
    assert!(
        parsed.get("account_name").is_none() && parsed.get("account_key").is_none(),
        "the vended branch must not surface either backend's storage credential"
    );
}

#[test]
fn lakekeeper_connection_password_serializes_expected_json() {
    let pw = lakekeeper_connection_password(WAREHOUSE_VENDED, true);
    let json_str = pw.to_sql_password_json();
    let parsed: serde_json::Value =
        serde_json::from_str(&json_str).expect("password serializes to valid JSON");

    assert_eq!(parsed["warehouse"], WAREHOUSE_VENDED);
    assert_eq!(parsed["use_vended_credentials"], true);
    assert_eq!(parsed["client_id"], OAUTH_CLIENT_ID);
    assert_eq!(parsed["client_secret"], OAUTH_CLIENT_SECRET);
    assert_eq!(
        parsed["oauth2_server_uri"],
        "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token"
    );
}

#[test]
fn warehouse_profiles_carry_documented_names() {
    assert_eq!(WarehouseProfile::static_creds().name(), WAREHOUSE_STATIC);
    assert_eq!(WarehouseProfile::vended().name(), WAREHOUSE_VENDED);
}

#[test]
fn adls_warehouse_matches_lakekeeper_profile_shape() {
    let static_profile = AdlsWarehouseProfile::static_creds("lhrs-e2e-user-42", "acct", "a2V5");
    let vended_profile = AdlsWarehouseProfile::vended("lhrs-e2e-user-42", "acct", "a2V5");

    assert_eq!(
        static_profile.name(),
        "lhrs-e2e-user-42-static",
        "the warehouse carries the container's per-run suffix"
    );
    assert_eq!(
        vended_profile.name(),
        "lhrs-e2e-user-42-vended",
        "the warehouse carries the container's per-run suffix"
    );
    assert!(
        !vended_profile.name().starts_with(static_profile.name())
            && !static_profile.name().starts_with(vended_profile.name()),
        "neither warehouse name may be a prefix of the other — Lakekeeper's \
         key-prefix isolation between the two sibling warehouses depends on it"
    );

    let static_storage = static_profile.storage_profile();
    assert_eq!(static_storage["type"], "adls");
    assert_eq!(static_storage["account-name"], "acct");
    assert_eq!(static_storage["filesystem"], "lhrs-e2e-user-42");
    assert_eq!(static_storage["key-prefix"], static_profile.name());
    assert_eq!(
        static_storage["sas-enabled"], false,
        "the static-credential warehouse reads the account key directly, so \
         Lakekeeper's own true default must be overridden off"
    );

    let vended_storage = vended_profile.storage_profile();
    assert_eq!(vended_storage["type"], "adls");
    assert_eq!(vended_storage["account-name"], "acct");
    assert_eq!(vended_storage["filesystem"], "lhrs-e2e-user-42");
    assert_eq!(vended_storage["key-prefix"], vended_profile.name());
    assert_eq!(
        vended_storage["sas-enabled"], true,
        "the vended-credential warehouse matches Lakekeeper v0.13.1's own \
         sas-enabled default, so Lakekeeper mints a SAS token per request"
    );

    let static_credential = static_profile.storage_credential();
    let vended_credential = vended_profile.storage_credential();
    assert_eq!(static_credential["type"], "az");
    assert_eq!(static_credential["credential-type"], "shared-access-key");
    assert_eq!(static_credential["key"], "a2V5");
    assert_eq!(
        static_credential, vended_credential,
        "both modes register the same account key; Lakekeeper mints the \
         vended SAS from it rather than needing a separate credential shape"
    );
}

#[test]
fn adls_connection_password_is_unambiguously_azure() {
    let pw = lakekeeper_adls_connection_password("lhrs-e2e-user-42-static", "acct", "a2V5");

    assert_eq!(pw.warehouse, "lhrs-e2e-user-42-static");
    assert_eq!(pw.account_name.as_deref(), Some("acct"));
    assert_eq!(pw.account_key.as_deref(), Some("a2V5"));
    assert_eq!(pw.client_id.as_deref(), Some(OAUTH_CLIENT_ID));
    assert_eq!(pw.client_secret.as_deref(), Some(OAUTH_CLIENT_SECRET));
    assert_eq!(
        pw.oauth2_server_uri.as_deref(),
        Some("http://keycloak:8080/realms/iceberg/protocol/openid-connect/token")
    );
    assert_eq!(pw.endpoint, "");
    assert_eq!(pw.region, "");
    assert_eq!(pw.access_key, "");
    assert_eq!(pw.secret_key, "");
    assert_eq!(pw.session_token, None);
    assert!(!pw.use_sigv4);
    assert!(!pw.use_vended_credentials);

    let parsed: serde_json::Value = serde_json::from_str(&pw.to_sql_password_json())
        .expect("password serializes to valid JSON");
    assert_eq!(parsed["account_name"], "acct");
    assert_eq!(parsed["account_key"], "a2V5");
    assert_eq!(parsed["endpoint"], "");
    assert_eq!(parsed["region"], "");
}
