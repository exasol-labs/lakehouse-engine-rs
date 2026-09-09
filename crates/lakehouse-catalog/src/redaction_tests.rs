use super::*;

#[test]
fn redact_removes_credential_values() {
    let msg = r#"error: access_key=AKIAIOSFODNN7EXAMPLE secret_key=wJalrXUtnFEMI"#;
    let safe = redact_credentials(msg);
    assert!(
        !safe.contains("AKIAIOSFODNN7EXAMPLE"),
        "key must be redacted"
    );
    assert!(!safe.contains("wJalrXUtnFEMI"), "secret must be redacted");
    assert!(safe.contains("access_key"));
    assert!(safe.contains("secret_key"));
}

#[test]
fn redact_no_false_positives_on_clean_message() {
    let msg = "failed to read object: 404 Not Found";
    let safe = redact_credentials(msg);
    assert_eq!(safe, msg, "clean messages should pass through unchanged");
}

#[test]
fn redact_secret_values_strips_literal_credential_values() {
    // S3 signature-style error shape that embeds the raw access key without
    // a recognizable `access_key=` label — the label heuristic misses this.
    let secret = "wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY";
    let msg = format!(
        "<Error><Code>SignatureDoesNotMatch</Code><AWSAccessKeyId>AKIAIOSFODNN7EXAMPLE</AWSAccessKeyId><StringToSign>{secret}</StringToSign></Error>"
    );
    let secrets = ["AKIAIOSFODNN7EXAMPLE", secret];
    let safe = redact_secret_values(&msg, &secrets);
    assert!(
        !safe.contains("AKIAIOSFODNN7EXAMPLE"),
        "access key value must be redacted: {safe}"
    );
    assert!(
        !safe.contains(secret),
        "secret key value must be redacted: {safe}"
    );
    assert!(safe.contains("[REDACTED]"), "redaction marker must appear");
}

// ---------------------------------------------------------------------------
// Task 4.4 — Extended redaction: bearer token + SigV4 Authorization + vended STS
// ---------------------------------------------------------------------------
#[test]
fn redact_credentials_strips_authorization_header() {
    let auth_value = "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20231201/us-east-1/glue/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123";
    let msg = format!("request failed: Authorization={auth_value}");
    let safe = redact_credentials(&msg);
    assert!(
        !safe.contains(auth_value),
        "Authorization value must be redacted: {safe}"
    );
    assert!(
        safe.contains("Authorization"),
        "Authorization label must be preserved: {safe}"
    );
}
#[test]
fn redact_credentials_strips_bearer_token() {
    let msg = "catalog error: Bearer my-secret-oauth-token-value";
    let safe = redact_credentials(msg);
    assert!(
        !safe.contains("my-secret-oauth-token-value"),
        "Bearer token must be redacted: {safe}"
    );
}
#[test]
fn redact_credentials_strips_vended_sts_keys() {
    let msg = r#"s3.access-key-id=VENDED_AKID s3.secret-access-key=VENDED_SK s3.session-token=VENDED_TOK"#;
    let safe = redact_credentials(msg);
    assert!(
        !safe.contains("VENDED_AKID"),
        "vended access key must be redacted: {safe}"
    );
    assert!(
        !safe.contains("VENDED_SK"),
        "vended secret key must be redacted: {safe}"
    );
    assert!(
        !safe.contains("VENDED_TOK"),
        "vended session token must be redacted: {safe}"
    );
    // Labels must be preserved so the error is still readable.
    assert!(
        safe.contains("s3.access-key-id"),
        "label must be preserved: {safe}"
    );
}
#[test]
fn redact_credentials_strips_azure_account_key_and_sas_labels() {
    let msg = concat!(
        "account_key=STATIC_ACCOUNT_KEY_VALUE ",
        "sas_token=STATIC_SAS_TOKEN_VALUE ",
        "adls.account-key=CONFIG_ACCOUNT_KEY_VALUE ",
        "adls.sas-token=CONFIG_SAS_TOKEN_VALUE ",
        "azure_storage_access_key=ENV_ACCOUNT_KEY_VALUE ",
        "azure_storage_sas_key=ENV_SAS_KEY_VALUE ",
        "https://acct.blob.core.windows.net/c/f?sv=2023&sig=SAS_SIGNATURE_VALUE"
    );
    let safe = redact_credentials(msg);
    for secret in [
        "STATIC_ACCOUNT_KEY_VALUE",
        "STATIC_SAS_TOKEN_VALUE",
        "CONFIG_ACCOUNT_KEY_VALUE",
        "CONFIG_SAS_TOKEN_VALUE",
        "ENV_ACCOUNT_KEY_VALUE",
        "ENV_SAS_KEY_VALUE",
        "SAS_SIGNATURE_VALUE",
    ] {
        assert!(!safe.contains(secret), "secret must be redacted: {safe}");
    }
    for label in [
        "account_key",
        "sas_token",
        "adls.account-key",
        "adls.sas-token",
    ] {
        assert!(safe.contains(label), "label must be preserved: {safe}");
    }
}

#[test]
fn redact_credentials_handles_serialized_sas_correctly() {
    let wire = r#"failed to build store from config {"sas":"STATIC_SAS_WIRE_VALUE"}"#;
    let safe = redact_credentials(wire);
    assert!(!safe.contains("STATIC_SAS_WIRE_VALUE"), "{safe}");
    assert!(safe.contains("\"sas\":"), "{safe}");

    let realistic = r#"failed to build store from config {"sas":"sv=2023-11-03&ss=b&srt=sco&sp=rwdlacx&se=2026-01-01T00:00:00Z&sig=REALSIGNATURE"}"#;
    let safe = redact_credentials(realistic);
    assert!(!safe.contains("REALSIGNATURE"), "{safe}");
    assert!(!safe.contains("sv=2023-11-03"), "{safe}");

    let prose = "the SAS token approach was assessed and found sas-isfactory overall";
    assert_eq!(redact_credentials(prose), prose);
}
#[test]
fn redact_error_text_removes_a_sas_token_whole_unlike_the_inverted_order() {
    let sas = "sv=2023-11-03&ss=b&srt=sco&sp=rwdlacx&se=2026-01-01T00:00:00Z&sig=SIG_VALUE";
    let raw = format!("failed to open https://acct.blob.core.windows.net/c/f?{sas}");

    let safe = redact_error_text(&raw, &[sas]);
    assert!(!safe.contains(sas), "SAS literal must be gone: {safe}");
    assert!(
        !safe.contains("sp=rwdlacx"),
        "permission field must not survive: {safe}"
    );
    assert!(
        !safe.contains("SIG_VALUE"),
        "signature must not survive: {safe}"
    );

    let inverted = redact_secret_values(&redact_credentials(&raw), &[sas]);
    assert!(
        inverted.contains("sp=rwdlacx"),
        "inverted order is expected to leak the permission field: {inverted}"
    );
}
#[test]
fn redact_credentials_redacts_all_occurrences_of_repeated_label() {
    // Two occurrences of "access_key" with distinct values — both must vanish.
    let msg = "access_key=FIRST_KEY_VALUE, access_key=SECOND_KEY_VALUE";
    let safe = redact_credentials(msg);
    assert!(
        !safe.contains("FIRST_KEY_VALUE"),
        "first occurrence must be redacted: {safe}"
    );
    assert!(
        !safe.contains("SECOND_KEY_VALUE"),
        "second occurrence must be redacted: {safe}"
    );
    // Labels themselves should still be visible so the error is readable.
    assert!(
        safe.contains("access_key"),
        "label must be preserved: {safe}"
    );
}
#[test]
fn redact_credentials_strips_x_amz_security_token() {
    let msg = "X-Amz-Security-Token=AQoDYXdzEJr_STS_TOKEN_VALUE (403)";
    let safe = redact_credentials(msg);
    assert!(
        !safe.contains("AQoDYXdzEJr_STS_TOKEN_VALUE"),
        "security token value must be redacted: {safe}"
    );
}

// ---------------------------------------------------------------------------
// Task 4.3 — No credential in error text
// ---------------------------------------------------------------------------
#[test]
fn catalog_error_message_strips_credentials() {
    let msg = "GET failed: access_key=AKID_SECRET_VALUE region=us-east-1";
    let safe = redact_credentials(msg);
    assert!(
        !safe.contains("AKID_SECRET_VALUE"),
        "credential value must be redacted: {safe}"
    );
    assert!(
        safe.contains("access_key"),
        "label must be preserved: {safe}"
    );
}
#[test]
fn redact_credentials_does_not_panic_on_length_changing_unicode_casefold() {
    let msg = "İİİİİsig=ütoken";
    let safe = redact_credentials(msg);
    assert!(
        !safe.contains("sig=ü"),
        "sig= value must be redacted: {safe}"
    );
}
#[test]
fn redact_credentials_strips_unity_vended_and_oauth_secrets() {
    let msg = concat!(
        "access_key_id=UC_AKID_VALUE ",
        "secret_access_key=UC_SECRET_VALUE ",
        "session_token=UC_SESSION_VALUE ",
        "sas_token=UC_SAS_VALUE ",
        "gcp_oauth_token=UC_GCP_TOKEN_VALUE ",
        "client_secret=UC_CLIENT_SECRET_VALUE ",
        "catalog=unity"
    );
    let safe = redact_credentials(msg);
    for secret in [
        "UC_AKID_VALUE",
        "UC_SECRET_VALUE",
        "UC_SESSION_VALUE",
        "UC_SAS_VALUE",
        "UC_GCP_TOKEN_VALUE",
        "UC_CLIENT_SECRET_VALUE",
    ] {
        assert!(!safe.contains(secret), "secret must be redacted: {safe}");
    }
    assert!(
        safe.contains("catalog=unity"),
        "non-secret context must be preserved: {safe}"
    );
}
