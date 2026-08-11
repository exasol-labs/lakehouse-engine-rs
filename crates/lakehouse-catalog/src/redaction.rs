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
        // Unity Catalog vended GCP token + OAuth M2M client secret
        "oauth_token",
        "client_secret",
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
        let pat_lower = pat.to_ascii_lowercase();
        let mut from = 0;
        while let Some(rel) = result[from..].to_ascii_lowercase().find(&pat_lower) {
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
#[path = "redaction_tests.rs"]
mod tests;
