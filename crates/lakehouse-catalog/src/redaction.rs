//! Credential redaction for error messages surfaced to callers.
//!
//! `lakehouse-engine`'s scan and pushdown paths route every externally-visible
//! error string through these functions before it reaches Exasol, so no
//! credential value or credential-shaped substring survives into surfaced
//! SQL/error text.

/// Replace each non-empty literal secret value with "[REDACTED]".
///
/// Catches credential leaks that the label-based heuristic misses — e.g. an S3
/// XML error or signature mismatch that echoes the access/secret key verbatim
/// without a recognizable `access_key=` label.
pub fn redact_secret_values(s: &str, secrets: &[&str]) -> String {
    let mut result = s.to_string();
    for secret in secrets {
        if !secret.is_empty() && result.contains(secret) {
            result = result.replace(secret, "[REDACTED]");
        }
    }
    result
}

/// Replace credential-shaped substrings with "[REDACTED]".
///
/// Catches common S3 credential patterns, SigV4 Authorization headers, vended
/// STS keys, and Azure ADLS static-credential patterns, without pulling in a
/// regex crate.
pub fn redact_credentials(s: &str) -> String {
    // Heuristic: anything that looks like an AWS key (long alphanum after
    // known key names) is replaced. We keep this simple and conservative.
    let patterns = [
        // S3 credential field names (static and vended)
        "access_key",
        "secret_key",
        "session_token",
        // Iceberg REST vended credential config keys
        "s3.access-key-id",
        "s3.secret-access-key",
        "s3.session-token",
        // SigV4 / HTTP header patterns
        "Authorization",
        "Bearer ",
        "X-Amz-Security-Token",
        "X-Amz-Credential",
        // AWS environment variable names
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "account_key",
        "sas_token",
        "adls.account-key",
        "adls.sas-token",
        "azure_storage_access_key",
        "azure_storage_sas_key",
        "sig=",
    ];
    const REDACTED: &str = "[REDACTED]";
    let mut result = s.to_string();
    for pat in patterns {
        // Redact ALL occurrences of the pattern, not just the first.
        // A single label that appears multiple times in one error string (e.g.
        // a debug dump that prints every header) would otherwise leak on the
        // 2nd+ occurrence if we stopped after the first match. The cursor
        // advances past each redaction so the re-emitted label is never
        // re-matched (which would loop forever).
        let pat_lower = pat.to_lowercase();
        let mut from = 0;
        while let Some(rel) = result[from..].to_lowercase().find(&pat_lower) {
            let idx = from + rel;
            let after = idx + pat.len();
            // Find the end of the value (next quote, whitespace, comma, or ampersand).
            let end = result[after..]
                .find(['"', '\'', ' ', '\n', ',', '&', '\r'])
                .map(|i| after + i)
                .unwrap_or(result.len());
            result = format!("{}{}{REDACTED}{}", &result[..idx], pat, &result[end..]);
            from = after + REDACTED.len();
            if from >= result.len() {
                break;
            }
        }
    }
    result
}

/// Strip credentials from an external error string before it is surfaced.
///
/// The single owner of the two-pass composition: [`redact_secret_values`] FIRST,
/// [`redact_credentials`] SECOND. The order is load-bearing, not stylistic — an
/// Azure SAS token carries its own `sig=` label, so a label-first pass rewrites
/// the middle of the token and leaves the value pass unable to match the literal,
/// surfacing the token's permission and expiry fields verbatim. Value-first
/// removes the whole token, and the label pass then still catches credential
/// shapes whose literal value the caller never held.
pub fn redact_error_text(msg: &str, secrets: &[&str]) -> String {
    redact_credentials(&redact_secret_values(msg, secrets))
}

#[cfg(test)]
mod tests {
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

    /// Scenario: Authorization header value (SigV4) is redacted from error messages.
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

    /// Scenario: Bearer token is redacted from error messages.
    #[test]
    fn redact_credentials_strips_bearer_token() {
        let msg = "catalog error: Bearer my-secret-oauth-token-value";
        let safe = redact_credentials(msg);
        assert!(
            !safe.contains("my-secret-oauth-token-value"),
            "Bearer token must be redacted: {safe}"
        );
    }

    /// Scenario: Vended STS keys (Iceberg config map keys) are redacted.
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

    /// Scenario: Azure ADLS static-credential labels (field names, iceberg
    /// storage config keys, and the SAS-URL signature query parameter) are
    /// redacted, mirroring the AWS-label coverage above.
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

    /// Scenario: `redact_error_text` runs the value pass BEFORE the label pass, so a
    /// SAS token — which carries its own `sig=` label — is removed whole.
    ///
    /// Pins the ORDER, not just the outcome: the inverted composition is asserted to
    /// still leak the token's `sp=` permission field, so swapping the two calls inside
    /// `redact_error_text` fails here instead of silently passing.
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

    /// Scenario: A label that appears MORE than once in the same error string is
    /// fully redacted on every occurrence — not just the first.
    ///
    /// Without the `while let` loop the second `access_key` would remain visible.
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

    /// Scenario: X-Amz-Security-Token (vended session token header) is redacted.
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

    /// Scenario: a catalog error message has its credential-shaped values removed.
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
}
