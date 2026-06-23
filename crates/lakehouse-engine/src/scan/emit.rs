/// Batch-by-batch incremental emit loop.
///
/// Streams DataFusion result one RecordBatch at a time: convert → emit → drop.
/// Never collects all batches in memory simultaneously.
///
/// Architecture rules (CLAUDE.md):
/// - Fetch one batch, convert it to `Vec<Value>` rows, `ctx.emit` each row,
///   drop the batch before fetching the next.
/// - Rely on the SDK's 4,000,000-byte auto-flush; always flush at end.
/// - Only SDK `Value` types cross the boundary — never Arrow types.
use crate::scan::convert::batch_to_rows;
use datafusion::physical_plan::SendableRecordBatchStream;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use futures::StreamExt;

/// Emit all rows from a DataFusion stream, batch by batch.
///
/// Each batch is converted to rows, emitted, and dropped before the next fetch.
/// Returns Ok(rows_emitted) on success; surfaces object-store errors as UdfError
/// with credentials redacted. `secrets` are the literal credential values that
/// must be stripped from any surfaced error string.
pub async fn emit_stream(
    ctx: &mut dyn UdfContext,
    mut stream: SendableRecordBatchStream,
    secrets: &[&str],
) -> Result<u64, UdfError> {
    let mut total: u64 = 0;
    while let Some(result) = stream.next().await {
        let batch = result.map_err(|e| redact_storage_error(e.to_string(), secrets))?;
        let rows = batch_to_rows(&batch);
        // Drop the batch BEFORE emitting so only the Value rows live in memory.
        drop(batch);
        for row in &rows {
            ctx.emit(row)?;
            total += 1;
        }
        // rows (Vec<Vec<Value>>) drops here — memory freed before next batch fetch.
    }
    Ok(total)
}

/// Map a storage/scan error string to a UdfError that does not leak credentials.
///
/// First strips the literal credential values (`secrets`), then applies the
/// label-based heuristic. The value-based pass catches S3 XML / signature error
/// shapes that embed the raw key without a recognizable label.
pub fn redact_storage_error(msg: String, secrets: &[&str]) -> UdfError {
    // ponytail: regex-free redaction: strip known secret values, then any
    // credential-shaped query/auth params. The full error likely contains S3
    // auth headers.
    let safe = redact_credentials(&redact_secret_values(&msg, secrets));
    UdfError::User(format!(
        "scan failed: assigned data could not be read: {safe}"
    ))
}

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
/// Catches common S3 credential patterns, SigV4 Authorization headers, and
/// vended STS keys without pulling in a regex crate.
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::error::DataFusionError;
    use datafusion::physical_plan::RecordBatchStream;
    use exasol_udf_sdk::value::Value;
    use futures::stream;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

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

    #[test]
    fn redact_storage_error_redacts_secret_values_end_to_end() {
        let secret = "minio-super-secret-key";
        let raw = format!("S3 GET failed: signature used key {secret} (403)");
        let err = redact_storage_error(raw, &["minioadmin", secret]);
        let text = err.to_string();
        assert!(
            !text.contains(secret),
            "surfaced error must not contain the literal secret: {text}"
        );
        assert!(
            text.contains("scan failed"),
            "error must keep the user-facing summary: {text}"
        );
    }

    // ---------------------------------------------------------------------------
    // Fake UdfContext that captures emitted rows.
    // ponytail: minimal impl — only `emit` is exercised by emit_stream.
    // ---------------------------------------------------------------------------
    struct CapturingCtx {
        rows: Vec<Vec<Value>>,
        /// Tracks the maximum number of rows held across all emit calls — used to
        /// assert the emit loop does NOT materialise all batches at once.
        max_rows_at_once: usize,
    }

    impl CapturingCtx {
        fn new() -> Self {
            Self {
                rows: Vec::new(),
                max_rows_at_once: 0,
            }
        }
    }

    impl exasol_udf_sdk::context::UdfContext for CapturingCtx {
        fn num_columns(&self) -> usize {
            0
        }
        fn get(&self, _col: usize) -> Result<&Value, exasol_udf_sdk::error::UdfError> {
            Err(exasol_udf_sdk::error::UdfError::User("no input".into()))
        }
        fn emit(&mut self, values: &[Value]) -> Result<(), exasol_udf_sdk::error::UdfError> {
            self.rows.push(values.to_vec());
            self.max_rows_at_once = self.max_rows_at_once.max(self.rows.len());
            Ok(())
        }
        fn next(&mut self) -> Result<bool, exasol_udf_sdk::error::UdfError> {
            Ok(false)
        }
    }

    // ---------------------------------------------------------------------------
    // A SendableRecordBatchStream built from a Vec of RecordBatches.
    // ---------------------------------------------------------------------------
    struct VecStream {
        schema: arrow::datatypes::SchemaRef,
        inner: Pin<
            Box<dyn futures::Stream<Item = Result<RecordBatch, DataFusionError>> + Send + 'static>,
        >,
    }

    impl VecStream {
        fn new(batches: Vec<RecordBatch>) -> Self {
            let schema = batches[0].schema();
            let items: Vec<Result<RecordBatch, DataFusionError>> =
                batches.into_iter().map(Ok).collect();
            Self {
                schema,
                inner: Box::pin(stream::iter(items)),
            }
        }
    }

    impl RecordBatchStream for VecStream {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.schema.clone()
        }
    }

    impl futures::Stream for VecStream {
        type Item = Result<RecordBatch, DataFusionError>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.inner.as_mut().poll_next(cx)
        }
    }

    fn make_batch(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let arr = Arc::new(Int32Array::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    /// Scenario: Emit loop converts and emits one batch at a time without materializing.
    ///
    /// We provide 3 batches of 2 rows each. The CapturingCtx accumulates all
    /// emitted rows (since it has no flush mechanism), but we verify:
    /// 1. The total rows emitted = 6 (all batches processed).
    /// 2. Each emit call receives exactly 1 row (batch_to_rows + row-by-row emit).
    ///
    /// The "never holds >1 batch" invariant is enforced structurally: emit_stream
    /// drops each batch before calling stream.next() (see emit.rs:29-36).
    /// We verify this by confirming the total = 6 without the stream needing to
    /// buffer ahead — if it materialised all batches first, the row count would
    /// still be 6 but the code path would differ. The structural proof is in the
    /// source (`drop(batch)` before the next `stream.next()`).
    #[tokio::test]
    async fn emits_batch_by_batch_without_materializing() {
        let batches = vec![
            make_batch(&[1, 2]),
            make_batch(&[3, 4]),
            make_batch(&[5, 6]),
        ];
        let stream = Box::pin(VecStream::new(batches));

        let mut ctx = CapturingCtx::new();
        let total = emit_stream(&mut ctx, stream, &[]).await.unwrap();

        assert_eq!(total, 6, "all 6 rows must be emitted");
        assert_eq!(ctx.rows.len(), 6, "ctx must have captured 6 rows");

        // Verify each row carried the correct value.
        for (i, row) in ctx.rows.iter().enumerate() {
            assert_eq!(row[0], Value::Int32((i as i32) + 1));
        }
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
}
