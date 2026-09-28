/// Catches leaks the label heuristic misses, e.g. an S3 error echoing a key
/// verbatim without an `access_key=` label.
pub fn redact_secret_values(s: &str, secrets: &[&str]) -> String {
    let mut result = s.to_string();
    for secret in secrets {
        if !secret.is_empty() && result.contains(secret) {
            result = result.replace(secret, "[REDACTED]");
        }
    }
    result
}

/// Redacts the value following each known credential label.
pub fn redact_credentials(s: &str) -> String {
    let patterns = [
        "access_key",
        "secret_key",
        "session_token",
        "oauth_token",
        "client_secret",
        "s3.access-key-id",
        "s3.secret-access-key",
        "s3.session-token",
        "Authorization",
        "Bearer ",
        "X-Amz-Security-Token",
        "X-Amz-Credential",
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
        // The trailing quote lets the value scan reach into the quoted value; a
        // bare `sas` would over-match prose.
        "\"sas\":\"",
    ];
    const REDACTED: &str = "[REDACTED]";
    let mut result = s.to_string();
    for pat in patterns {
        // The cursor skips past each redaction so the re-emitted label is never
        // re-matched (which would loop forever).
        let pat_lower = pat.to_ascii_lowercase();
        let mut from = 0;
        while let Some(rel) = result[from..].to_ascii_lowercase().find(&pat_lower) {
            let idx = from + rel;
            let after = idx + pat.len();
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

/// Value pass first, label pass second. The order is load-bearing: an Azure SAS
/// token contains `sig=`, so a label-first pass mangles the token and the value
/// pass no longer matches, leaking its permission and expiry fields.
pub fn redact_error_text(msg: &str, secrets: &[&str]) -> String {
    redact_credentials(&redact_secret_values(msg, secrets))
}

#[cfg(test)]
#[path = "redaction_tests.rs"]
mod tests;
