/// Batch-by-batch incremental emit loop using Arrow IPC.
///
/// Streams DataFusion result one RecordBatch at a time: emit via IPC → drop.
/// Never collects all batches in memory simultaneously.
///
/// Architecture rules (CLAUDE.md):
/// - Fetch one batch, call `ctx.emit_batch(&batch)` (Arrow IPC bytes — ABI-safe),
///   drop the batch before fetching the next.
/// - Rely on the SDK's 4,000,000-byte auto-flush; always flush at end.
/// - Only IPC bytes cross the .so boundary — never Arrow types or Value intermediates.
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use exasol_udf_sdk::context::{EmitBatch, UdfContext};
use exasol_udf_sdk::error::UdfError;
use futures::StreamExt;
use std::sync::Arc;

/// Emit all rows from a DataFusion stream, batch by batch via Arrow IPC.
///
/// Each batch is emitted via `ctx.emit_batch` (Arrow IPC bytes — ABI-safe),
/// then dropped before the next fetch. No `Vec<Value>` intermediate is created.
/// Returns Ok(rows_emitted) on success; surfaces scan errors as UdfError
/// with credentials redacted. `secrets` are the literal credential values that
/// must be stripped from any surfaced error string.
pub async fn emit_stream(
    ctx: &mut dyn UdfContext,
    mut stream: SendableRecordBatchStream,
    secrets: &[&str],
) -> Result<u64, UdfError> {
    let mut total: u64 = 0;
    while let Some(result) = stream.next().await {
        let batch = result.map_err(|e| classify_scan_error(e, secrets))?;
        // Normalize Arrow view types (Utf8View→Utf8, BinaryView→Binary) so that
        // emit_batch never sees a type the SDK's strict IPC validation rejects.
        // DataFusion 58 defaults to view types for Parquet string columns
        // (schema_force_view_types), and scalar functions can also produce them.
        let batch = normalize_view_types(batch)
            .map_err(|e| UdfError::User(format!("emit normalization failed: {e}")))?;
        // Count rows before emitting — batch is borrowed by emit_batch.
        total += batch.num_rows() as u64;
        ctx.emit_batch(&batch)?;
        drop(batch);
    }
    Ok(total)
}

/// Normalize Arrow view types in a RecordBatch so they are acceptable to emit_batch.
///
/// The SDK's emit_batch performs strict Arrow→ExaType validation and rejects
/// `Utf8View` and `BinaryView`. DataFusion 58 produces these by default for
/// Parquet string/binary columns and for many scalar function outputs.
///
/// Fast path: if no column has a view type, returns the original batch unchanged
/// with no allocation. Only columns whose type is a view type are cast; all others
/// are kept as-is (shared Arc pointer, zero copy).
pub fn normalize_view_types(batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
    // Fast path: no view types present — avoid any allocation.
    if !batch
        .schema()
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Utf8View | DataType::BinaryView))
    {
        return Ok(batch);
    }

    // Slow path: at least one view column — rebuild schema and columns.
    let schema = batch.schema();
    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_columns: Vec<Arc<dyn arrow::array::Array>> =
        Vec::with_capacity(batch.num_columns());

    for (field, col) in schema.fields().iter().zip(batch.columns()) {
        match field.data_type() {
            DataType::Utf8View => {
                let cast_col = arrow::compute::cast(col.as_ref(), &DataType::Utf8)?;
                new_fields.push(arrow::datatypes::Field::new(
                    field.name(),
                    DataType::Utf8,
                    field.is_nullable(),
                ));
                new_columns.push(cast_col);
            }
            DataType::BinaryView => {
                let cast_col = arrow::compute::cast(col.as_ref(), &DataType::Binary)?;
                new_fields.push(arrow::datatypes::Field::new(
                    field.name(),
                    DataType::Binary,
                    field.is_nullable(),
                ));
                new_columns.push(cast_col);
            }
            _ => {
                new_fields.push(field.as_ref().clone());
                new_columns.push(col.clone());
            }
        }
    }

    let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
}

/// Classify a DataFusion scan error and produce a UdfError without credential leaks.
///
/// Calls `find_root()` on the error chain to detect `ResourcesExhausted` through any
/// nesting of `Context`, `External`, or `ArrowError` wrappers — which DataFusion 54
/// uses internally (e.g. sort wraps OOM errors with `.context()`).
///
/// - `ResourcesExhausted` → clean memory-exhaustion error (distinct from storage errors)
/// - Everything else → storage-read error via `redact_storage_error`
///
/// Credential redaction is applied in both paths.
pub fn classify_scan_error(e: DataFusionError, secrets: &[&str]) -> UdfError {
    match e.find_root() {
        DataFusionError::ResourcesExhausted(msg) => resources_exhausted_error(msg, secrets),
        _ => redact_storage_error(e.to_string(), secrets),
    }
}

/// Produce a clean memory-exhaustion UdfError, redacting any credential values.
///
/// Only the innermost `ResourcesExhausted` `msg` is surfaced here. Any wrapping
/// `.context()` message (e.g. from DataFusion sort's OOM path) is intentionally
/// dropped: context strings may carry credential-bearing fragments from outer
/// error layers, so exposing them defeats the redaction guarantee.
fn resources_exhausted_error(msg: &str, secrets: &[&str]) -> UdfError {
    let safe = redact_credentials(&redact_secret_values(msg, secrets));
    UdfError::User(format!(
        "scan failed: memory exhausted (ResourcesExhausted): {safe}"
    ))
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
    // Fake UdfContext that captures Arrow IPC bytes from emit_batch.
    // emit() is intentionally left as a no-op trap — if emit_stream calls it on
    // the raw-row path, emit_was_called == true will fail the assertion.
    // ---------------------------------------------------------------------------
    struct CapturingCtx {
        /// Row-by-row emit calls — must be empty after emit_stream on the raw path.
        rows: Vec<Vec<Value>>,
        /// Accumulated IPC byte payloads, one entry per emit_batch call.
        ipc_batches: Vec<Vec<u8>>,
    }

    impl CapturingCtx {
        fn new() -> Self {
            Self {
                rows: Vec::new(),
                ipc_batches: Vec::new(),
            }
        }

        /// Decode all captured IPC payloads back to RecordBatches for assertions.
        fn decoded_batches(&self) -> Vec<RecordBatch> {
            use arrow::ipc::reader::StreamReader;
            use std::io::Cursor;
            self.ipc_batches
                .iter()
                .map(|bytes| {
                    StreamReader::try_new(Cursor::new(bytes), None)
                        .expect("IPC bytes must be a valid Arrow IPC stream")
                        .next()
                        .expect("IPC stream must contain exactly one batch")
                        .expect("IPC read must not error")
                })
                .collect()
        }
    }

    impl exasol_udf_sdk::context::UdfContext for CapturingCtx {
        fn num_columns(&self) -> usize {
            0
        }
        fn get(&self, _col: usize) -> Result<&Value, exasol_udf_sdk::error::UdfError> {
            Err(exasol_udf_sdk::error::UdfError::User("no input".into()))
        }
        /// Row-by-row emit — must NOT be called on the raw-row emit_stream path.
        fn emit(&mut self, values: &[Value]) -> Result<(), exasol_udf_sdk::error::UdfError> {
            self.rows.push(values.to_vec());
            Ok(())
        }
        fn next(&mut self) -> Result<bool, exasol_udf_sdk::error::UdfError> {
            Ok(false)
        }
        /// Capture the IPC bytes so the test can decode and assert their content.
        fn emit_record_batch_ipc(
            &mut self,
            ipc: &[u8],
        ) -> Result<(), exasol_udf_sdk::error::UdfError> {
            self.ipc_batches.push(ipc.to_vec());
            Ok(())
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

    /// Scenario: emit_stream emits one Arrow IPC batch per RecordBatch — no Vec<Value> intermediate.
    ///
    /// Invariants verified:
    /// 1. total == 6 (num_rows counted correctly across 3 batches of 2 rows each).
    /// 2. Exactly 3 IPC payloads captured — one per input batch, not one per row.
    /// 3. No row-by-row emit calls (ctx.rows is empty — emit() was never called).
    /// 4. Each IPC payload decodes back to a RecordBatch with the correct values,
    ///    proving the bytes faithfully round-trip through Arrow IPC.
    ///
    /// The "never holds >1 batch" invariant is structural: emit_stream holds only
    /// one RecordBatch reference at a time (counted → emit_batch(&batch) → drop).
    #[tokio::test]
    async fn emits_batch_by_batch_without_materializing() {
        let input_batches = vec![
            make_batch(&[1, 2]),
            make_batch(&[3, 4]),
            make_batch(&[5, 6]),
        ];
        let stream = Box::pin(VecStream::new(input_batches));

        let mut ctx = CapturingCtx::new();
        let total = emit_stream(&mut ctx, stream, &[]).await.unwrap();

        // 1. Row count is the sum of num_rows across all batches.
        assert_eq!(total, 6, "total must equal sum of all batch row counts");

        // 2. One IPC payload per batch — never one per row.
        assert_eq!(
            ctx.ipc_batches.len(),
            3,
            "exactly 3 IPC payloads must be captured (one per input batch)"
        );

        // 3. Row-by-row emit must never be called on the raw-row path.
        assert!(
            ctx.rows.is_empty(),
            "emit() must not be called on the raw IPC path; got {} row-by-row calls",
            ctx.rows.len()
        );

        // 4. IPC round-trip: decode and verify values.
        let decoded = ctx.decoded_batches();
        assert_eq!(
            decoded.len(),
            3,
            "decoded batch count must match payload count"
        );

        use arrow::array::Int32Array;
        let expected_values = [&[1i32, 2][..], &[3, 4], &[5, 6]];
        for (batch, expected) in decoded.iter().zip(expected_values.iter()) {
            assert_eq!(batch.num_rows(), 2);
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("column 0 must be Int32Array");
            for (row_idx, &expected_val) in expected.iter().enumerate() {
                assert_eq!(
                    col.value(row_idx),
                    expected_val,
                    "IPC-decoded value must match original"
                );
            }
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

    // ---------------------------------------------------------------------------
    // Task 3.2 — ResourcesExhausted surfacing
    // ---------------------------------------------------------------------------

    /// Scenario: A ResourcesExhausted error surfaces as a memory-exhaustion error,
    /// not a storage error, and carries no credential values.
    ///
    /// Verifies all three nesting forms DataFusion 54 can produce:
    /// - direct ResourcesExhausted
    /// - Context-wrapped ResourcesExhausted (e.g. from sort's .context() call)
    /// - External-wrapped ResourcesExhausted
    #[tokio::test]
    async fn resources_exhausted_surfaces_as_memory_error_not_storage_error() {
        let secret = "AKIAIOSFODNN7EXAMPLE";
        let secrets = [secret];

        // --- 1. Direct ResourcesExhausted ---
        let direct = DataFusionError::ResourcesExhausted(
            "Failed to allocate additional 256 MiB for HashAggregateExec".to_string(),
        );
        let err_direct = classify_scan_error(direct, &secrets);
        let text_direct = err_direct.to_string();
        assert!(
            text_direct.contains("memory exhausted"),
            "direct: must contain 'memory exhausted': {text_direct}"
        );
        assert!(
            !text_direct.contains("assigned data could not be read"),
            "direct: must NOT be classified as storage error: {text_direct}"
        );
        assert!(
            !text_direct.contains(secret),
            "direct: must not contain secret: {text_direct}"
        );

        // --- 2. Context-wrapped ResourcesExhausted ---
        // Sort in DataFusion 54 calls e.context("...") on ResourcesExhausted.
        let context_wrapped =
            DataFusionError::ResourcesExhausted("pool limit exceeded".to_string()).context(
                format!("External sort failed; secret would be bad: {secret}"),
            );
        let err_ctx = classify_scan_error(context_wrapped, &secrets);
        let text_ctx = err_ctx.to_string();
        assert!(
            text_ctx.contains("memory exhausted"),
            "context-wrapped: must contain 'memory exhausted': {text_ctx}"
        );
        assert!(
            !text_ctx.contains("assigned data could not be read"),
            "context-wrapped: must NOT be classified as storage error: {text_ctx}"
        );
        assert!(
            !text_ctx.contains(secret),
            "context-wrapped: must not contain secret: {text_ctx}"
        );

        // --- 3. External-wrapped ResourcesExhausted ---
        let external_wrapped = DataFusionError::External(Box::new(
            DataFusionError::ResourcesExhausted("repartition OOM".to_string()),
        ));
        let err_ext = classify_scan_error(external_wrapped, &secrets);
        let text_ext = err_ext.to_string();
        assert!(
            text_ext.contains("memory exhausted"),
            "external-wrapped: must contain 'memory exhausted': {text_ext}"
        );
        assert!(
            !text_ext.contains("assigned data could not be read"),
            "external-wrapped: must NOT be classified as storage error: {text_ext}"
        );

        // --- 4. Non-ResourcesExhausted error still routes to storage path ---
        let storage_err = DataFusionError::Execution("S3 read failed: 403".to_string());
        let err_storage = classify_scan_error(storage_err, &[]);
        let text_storage = err_storage.to_string();
        assert!(
            text_storage.contains("assigned data could not be read"),
            "non-OOM error must use storage path: {text_storage}"
        );
        assert!(
            !text_storage.contains("memory exhausted"),
            "non-OOM error must NOT look like memory error: {text_storage}"
        );
    }

    // ---------------------------------------------------------------------------
    // Task R7 — Utf8View normalization: emit_stream must not crash on view types
    // ---------------------------------------------------------------------------

    /// Scenario: normalize_view_types converts Utf8View columns to Utf8 and leaves
    /// non-view columns untouched.
    ///
    /// Invariants:
    /// 1. A batch with only non-view types is returned unchanged (fast path).
    /// 2. A batch with Utf8View is rebuilt: column type becomes Utf8, values preserved.
    /// 3. A mixed batch (Int32 + Utf8View) normalizes only the view column.
    #[test]
    fn normalize_view_types_casts_utf8view_to_utf8() {
        use arrow::array::{StringArray, StringViewArray};
        use arrow::datatypes::Field;

        // Fast path: no view types — the function returns Ok without rebuilding.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let col = Arc::new(Int32Array::from(vec![1i32, 2]));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();
        let result = normalize_view_types(batch.clone()).unwrap();
        assert_eq!(
            result.schema(),
            batch.schema(),
            "fast path: schema unchanged"
        );
        assert_eq!(result.num_rows(), 2, "fast path: row count unchanged");

        // Slow path: Utf8View column.
        let view_arr = StringViewArray::from(vec!["hello", "world"]);
        let view_schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        let view_batch = RecordBatch::try_new(view_schema, vec![Arc::new(view_arr)]).unwrap();
        let normalized = normalize_view_types(view_batch).unwrap();
        assert_eq!(
            normalized.schema().field(0).data_type(),
            &DataType::Utf8,
            "Utf8View must be normalized to Utf8"
        );
        let str_col = normalized
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("normalized column must be StringArray (Utf8)");
        assert_eq!(str_col.value(0), "hello");
        assert_eq!(str_col.value(1), "world");

        // Mixed: Int32 + Utf8View — only the view column changes.
        let mixed_schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int32, false),
            Field::new("s", DataType::Utf8View, false),
        ]));
        let int_col = Arc::new(Int32Array::from(vec![42i32]));
        let view_col = Arc::new(StringViewArray::from(vec!["abc"]));
        let mixed_batch =
            RecordBatch::try_new(mixed_schema, vec![int_col, view_col as Arc<_>]).unwrap();
        let norm = normalize_view_types(mixed_batch).unwrap();
        assert_eq!(norm.schema().field(0).data_type(), &DataType::Int32);
        assert_eq!(norm.schema().field(1).data_type(), &DataType::Utf8);
    }

    /// Scenario: emit_stream with a Utf8View column emits successfully via IPC.
    ///
    /// This is the regression test for the E2E failure:
    ///   "emit_batch: Arrow column 1 of type Utf8View cannot feed declared ExaType String"
    ///
    /// Invariants:
    /// 1. emit_stream does not return an error (no VM crash).
    /// 2. The emitted IPC payload decodes to a batch with Utf8 (not Utf8View) column.
    /// 3. String values survive the emit→IPC→decode round-trip unchanged.
    #[tokio::test]
    async fn emit_stream_normalizes_utf8view_before_emit_batch() {
        use arrow::array::{StringArray, StringViewArray};
        use arrow::datatypes::Field;

        // Build a RecordBatch with a Utf8View column — the type DataFusion 58 produces
        // by default for Parquet string columns (schema_force_view_types).
        let view_arr = StringViewArray::from(vec!["event-01", "event-02", "event-03"]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8View, false),
        ]));
        let id_col = Arc::new(Int32Array::from(vec![1i32, 2, 3]));
        let view_batch = RecordBatch::try_new(schema, vec![id_col, Arc::new(view_arr)]).unwrap();

        let stream = Box::pin(VecStream::new(vec![view_batch]));
        let mut ctx = CapturingCtx::new();

        // Must not error — previously crashed with "Utf8View cannot feed ExaType String".
        let total = emit_stream(&mut ctx, stream, &[])
            .await
            .expect("emit_stream must succeed with Utf8View input");

        assert_eq!(total, 3, "all 3 rows must be counted");
        assert_eq!(ctx.ipc_batches.len(), 1, "exactly 1 IPC payload");

        // Decode and verify: the emitted batch must have Utf8 (not Utf8View) for column 1.
        let decoded = ctx.decoded_batches();
        let decoded_batch = &decoded[0];
        assert_eq!(
            decoded_batch.schema().field(1).data_type(),
            &DataType::Utf8,
            "decoded batch must have Utf8 column (Utf8View was normalized before IPC)"
        );

        // String values must survive unchanged.
        let str_col = decoded_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("decoded column 1 must be StringArray");
        assert_eq!(str_col.value(0), "event-01");
        assert_eq!(str_col.value(1), "event-02");
        assert_eq!(str_col.value(2), "event-03");
    }
}
