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
///
/// `exa_types` is the declared Exasol EMITS type string for each output column
/// (positionally aligned). Every column is coerced to the Arrow type that ExaType
/// accepts before `emit_batch` — DataFusion's physical Parquet type can diverge
/// from the Iceberg logical type the VS declared, and `emit_batch` rejects ANY
/// mismatch. Pass `&[]` to fall back to view-type normalization only.
pub async fn emit_stream(
    ctx: &mut dyn UdfContext,
    mut stream: SendableRecordBatchStream,
    secrets: &[&str],
    exa_types: &[String],
) -> Result<u64, UdfError> {
    let mut total: u64 = 0;
    while let Some(result) = stream.next().await {
        let batch = result.map_err(|e| classify_scan_error(e, secrets))?;
        // Coerce each column to the Arrow type its declared EMITS ExaType accepts,
        // so emit_batch's strict Arrow→ExaType validation never rejects a column.
        // Generalizes the old Utf8View→Utf8 normalization across the full mapping.
        let batch = coerce_batch_to_exa_types(batch, exa_types)
            .map_err(|e| UdfError::User(format!("emit type coercion failed: {e}")))?;
        // Count rows before emitting — batch is borrowed by emit_batch.
        total += batch.num_rows() as u64;
        ctx.emit_batch(&batch)?;
        drop(batch);
    }
    Ok(total)
}

/// Coerce every column of a RecordBatch to the Arrow type the engine's strict
/// `emit_batch` IPC feed accepts for its declared EMITS ExaType.
///
/// `exa_types[i]` is the Exasol EMITS type string the VS declared for output
/// column `i` (the SAME list the adapter put in the EMITS clause), positionally
/// aligned with the batch columns. For each column:
///
/// - A concrete target ([`exasol_type_to_arrow`] returns `Some`) → cast the
///   column to that Arrow type (Int32→Decimal128, Float32→Float64, narrow
///   Decimal→wide Decimal, etc.). DataFusion's Parquet scan can produce a
///   different physical Arrow type than the Iceberg logical type the VS declared
///   (e.g. an Iceberg `int` widened to `long`), and `emit_batch` rejects ANY such
///   mismatch — so the postcondition is that the fed Arrow type maps back to the
///   declared ExaType.
/// - The string family ([`exasol_type_to_arrow`] returns `None`, i.e. VARCHAR /
///   CHAR) → cast to `Utf8`. Incompatible source types were already pre-cast to a
///   string by the scan SQL (`CAST(col AS VARCHAR)`); this also subsumes the old
///   `Utf8View`/`BinaryView` normalization.
///
/// Fast path: a column already of the target type is kept as-is (shared `Arc`,
/// zero copy). When `exa_types` is empty or shorter than the column count (a spec
/// that predates `emit_exa_types`), unmatched columns fall back to view-type
/// normalization so a `Utf8View` column still does not crash `emit_batch`.
pub fn coerce_batch_to_exa_types(
    batch: RecordBatch,
    exa_types: &[String],
) -> Result<RecordBatch, ArrowError> {
    let schema = batch.schema();

    // Decide the target Arrow type for each column up front.
    let targets: Vec<DataType> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            target_arrow_type(exa_types.get(i).map(String::as_str), field.data_type())
        })
        .collect();

    // Fast path: every column already matches its target — no allocation.
    if schema
        .fields()
        .iter()
        .zip(&targets)
        .all(|(f, t)| f.data_type() == t)
    {
        return Ok(batch);
    }

    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_columns: Vec<Arc<dyn arrow::array::Array>> =
        Vec::with_capacity(batch.num_columns());

    for ((field, col), target) in schema.fields().iter().zip(batch.columns()).zip(&targets) {
        if field.data_type() == target {
            new_fields.push(field.as_ref().clone());
            new_columns.push(col.clone());
        } else {
            let cast_col = arrow::compute::cast(col.as_ref(), target)?;
            new_fields.push(arrow::datatypes::Field::new(
                field.name(),
                target.clone(),
                field.is_nullable(),
            ));
            new_columns.push(cast_col);
        }
    }

    let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
}

/// Resolve the target Arrow type for one output column.
///
/// `declared` is the Exasol EMITS type string for this column (None when the
/// spec carries no declared type for this position). `source` is the column's
/// current Arrow type, used only for the no-declared-type fallback.
fn target_arrow_type(declared: Option<&str>, source: &DataType) -> DataType {
    if let Some(exa) = declared {
        match crate::types::mapping::exasol_type_to_arrow(exa) {
            // Concrete numeric / temporal / boolean target.
            Some(t) => t,
            // String family (VARCHAR / CHAR): feed Utf8.
            None => DataType::Utf8,
        }
    } else {
        // No declared type for this column: preserve the source type, but still
        // normalize view types so emit_batch does not reject them.
        match source {
            DataType::Utf8View => DataType::Utf8,
            DataType::BinaryView => DataType::Binary,
            other => other.clone(),
        }
    }
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
        let total = emit_stream(&mut ctx, stream, &[], &[]).await.unwrap();

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

    /// Scenario: with NO declared types (`&[]`), `coerce_batch_to_exa_types`
    /// normalizes Utf8View → Utf8 and leaves non-view columns untouched — the
    /// backward-compatible fallback for specs that predate `emit_exa_types`.
    ///
    /// Invariants:
    /// 1. A batch with only non-view types is returned unchanged (fast path).
    /// 2. A batch with Utf8View is rebuilt: column type becomes Utf8, values preserved.
    /// 3. A mixed batch (Int32 + Utf8View) normalizes only the view column.
    #[test]
    fn coerce_batch_empty_types_normalizes_utf8view_to_utf8() {
        use arrow::array::{StringArray, StringViewArray};
        use arrow::datatypes::Field;

        // Fast path: no view types — the batch is returned unchanged.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let col = Arc::new(Int32Array::from(vec![1i32, 2]));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();
        let result = coerce_batch_to_exa_types(batch.clone(), &[]).unwrap();
        assert_eq!(
            result.schema(),
            batch.schema(),
            "fast path: schema unchanged"
        );
        assert_eq!(result.num_rows(), 2, "fast path: row count unchanged");

        // Utf8View column → Utf8.
        let view_arr = StringViewArray::from(vec!["hello", "world"]);
        let view_schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        let view_batch = RecordBatch::try_new(view_schema, vec![Arc::new(view_arr)]).unwrap();
        let normalized = coerce_batch_to_exa_types(view_batch, &[]).unwrap();
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
        let norm = coerce_batch_to_exa_types(mixed_batch, &[]).unwrap();
        assert_eq!(norm.schema().field(0).data_type(), &DataType::Int32);
        assert_eq!(norm.schema().field(1).data_type(), &DataType::Utf8);
    }

    // ---------------------------------------------------------------------------
    // Coerce-to-declared-ExaType — table-driven over the full mapping
    // ---------------------------------------------------------------------------

    /// Scenario: `coerce_batch_to_exa_types` casts EVERY output column to the Arrow
    /// type the declared EMITS ExaType accepts, across the full mapping table —
    /// reproducing Exasol's DECIMAL precision binning (Int32 / Int64 / Decimal128).
    ///
    /// This is the generalized fix for BOTH live bench failures:
    ///   "Arrow column 0 of type Int32 cannot feed declared ExaType Int64"
    ///   "Arrow column 0 of type Decimal128(10, 0) cannot feed declared ExaType Int64"
    ///
    /// Each case provides a source Arrow array (the type DataFusion's Parquet scan
    /// or aggregate might actually produce) and the declared Exasol EMITS type
    /// string; the coerced column's Arrow type must equal the canonical target for
    /// that ExaType (`exasol_type_to_arrow`), and must NOT remain the source type.
    #[test]
    fn coerce_batch_casts_every_column_to_declared_exatype() {
        use crate::types::mapping::exasol_type_to_arrow;
        use arrow::array::{
            Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
            StringViewArray, UInt32Array,
        };
        use arrow::datatypes::Field;

        // (column name, source Arrow array, declared Exasol EMITS type)
        // First live failure: Iceberg `int` declared DECIMAL(10,0) (ExaType Int64),
        // but DataFusion produced Arrow Int32 → must cast Int32→Int64.
        let int32_to_int64: Arc<dyn arrow::array::Array> =
            Arc::new(Int32Array::from(vec![1, 2, 3]));
        // Second live failure: COUNT(*) declared DECIMAL(10,0) (ExaType Int64),
        // produced as Decimal128(10,0) → must cast Decimal128→Int64.
        let dec10_count_to_int64: Arc<dyn arrow::array::Array> = Arc::new(
            Decimal128Array::from(vec![5i128, 7, 9])
                .with_precision_and_scale(10, 0)
                .unwrap(),
        );
        // Small scale-0 DECIMAL declared DECIMAL(5,0) → ExaType Int32.
        let int64_to_int32: Arc<dyn arrow::array::Array> =
            Arc::new(Int64Array::from(vec![1i64, 2, 3]));
        // UInt32 declared DECIMAL(20,0) (p>18 → ExaType Numeric/Decimal128).
        let uint32_to_dec20: Arc<dyn arrow::array::Array> =
            Arc::new(UInt32Array::from(vec![10u32, 20, 30]));
        // Float32 declared DOUBLE PRECISION.
        let f32_to_double: Arc<dyn arrow::array::Array> =
            Arc::new(Float32Array::from(vec![1.5f32, 2.5, 3.5]));
        // Float64 already matches DOUBLE PRECISION (fast path).
        let f64_double: Arc<dyn arrow::array::Array> =
            Arc::new(Float64Array::from(vec![1.0f64, 2.0, 3.0]));
        // Decimal width divergence (scale>0): DECIMAL(10,2) declared DECIMAL(20,2).
        let dec_narrow_to_wide: Arc<dyn arrow::array::Array> = Arc::new(
            Decimal128Array::from(vec![100i128, 200, 300])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        let date: Arc<dyn arrow::array::Array> = Arc::new(Date32Array::from(vec![0, 1, 2]));
        let utf8view_to_varchar: Arc<dyn arrow::array::Array> =
            Arc::new(StringViewArray::from(vec!["a", "b", "c"]));

        let cases: Vec<(&str, Arc<dyn arrow::array::Array>, &str)> = vec![
            ("c_int32_to_int64", int32_to_int64, "DECIMAL(10,0)"),
            ("c_count_to_int64", dec10_count_to_int64, "DECIMAL(10,0)"),
            ("c_int32_bin", int64_to_int32, "DECIMAL(5,0)"),
            ("c_uint_dec20", uint32_to_dec20, "DECIMAL(20,0)"),
            ("c_f32", f32_to_double, "DOUBLE PRECISION"),
            ("c_f64", f64_double, "DOUBLE PRECISION"),
            ("c_dec_scaled", dec_narrow_to_wide, "DECIMAL(20,2)"),
            ("c_date", date, "DATE"),
            ("c_str", utf8view_to_varchar, "VARCHAR(2000000)"),
        ];

        let fields: Vec<Field> = cases
            .iter()
            .map(|(name, col, _)| Field::new(*name, col.data_type().clone(), true))
            .collect();
        let columns: Vec<Arc<dyn arrow::array::Array>> =
            cases.iter().map(|(_, col, _)| col.clone()).collect();
        let exa_types: Vec<String> = cases.iter().map(|(_, _, t)| t.to_string()).collect();

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns).unwrap();

        let coerced = coerce_batch_to_exa_types(batch, &exa_types)
            .expect("coercion must succeed for all mapping cases");

        for (idx, (name, _, declared)) in cases.iter().enumerate() {
            let got = coerced.schema().field(idx).data_type().clone();
            match exasol_type_to_arrow(declared) {
                Some(expected) => assert_eq!(
                    got, expected,
                    "column {name} (declared {declared}) must coerce to {expected:?}, got {got:?}"
                ),
                None => assert_eq!(
                    got,
                    DataType::Utf8,
                    "column {name} (declared {declared}) must coerce to Utf8, got {got:?}"
                ),
            }
        }

        // Explicit bin assertions for the two live-failure columns.
        assert_eq!(
            coerced.schema().field(0).data_type(),
            &DataType::Int64,
            "Int32 declared DECIMAL(10,0) must become Int64 (1st live failure)"
        );
        assert_eq!(
            coerced.schema().field(1).data_type(),
            &DataType::Int64,
            "Decimal128(10,0) COUNT(*) declared DECIMAL(10,0) must become Int64 (2nd live failure)"
        );
        assert_eq!(
            coerced.schema().field(2).data_type(),
            &DataType::Int32,
            "Int64 declared DECIMAL(5,0) must become Int32 (small-precision bin)"
        );

        // Row count and values must survive the Int32→Int64 cast.
        assert_eq!(coerced.num_rows(), 3);
        let c0 = coerced
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c_int32_to_int64 must now be Int64");
        assert_eq!(c0.value(0), 1);
        assert_eq!(c0.value(2), 3);
        // COUNT(*) values survive Decimal128→Int64.
        let c1 = coerced
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c_count_to_int64 must now be Int64");
        assert_eq!(c1.value(0), 5);
        assert_eq!(c1.value(2), 9);
    }

    /// Scenario: when `exa_types` is empty (no declared schema carried), the batch
    /// still falls back to view-type normalization so a Utf8View column does not
    /// crash `emit_batch`. Backward-compatible with specs that lack `emit_exa_types`.
    #[test]
    fn coerce_batch_empty_types_falls_back_to_view_normalization() {
        use arrow::array::StringViewArray;
        use arrow::datatypes::Field;

        let view = Arc::new(StringViewArray::from(vec!["x", "y"]));
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8View, true)]));
        let batch = RecordBatch::try_new(schema, vec![view]).unwrap();

        let coerced = coerce_batch_to_exa_types(batch, &[]).expect("empty types must not error");
        assert_eq!(
            coerced.schema().field(0).data_type(),
            &DataType::Utf8,
            "empty types must still normalize Utf8View to Utf8"
        );
    }

    /// Scenario: emit_stream coerces each column to its declared EMITS ExaType
    /// before emit_batch — end-to-end through the IPC round-trip.
    ///
    /// This is the regression test for BOTH live E2E failures:
    ///   "Arrow column 0 of type Int32 cannot feed declared ExaType Int64"
    ///   "Arrow column 1 of type Utf8View cannot feed declared ExaType String"
    ///
    /// The source batch is `Int32` + `Utf8View` (what DataFusion's Parquet scan
    /// produces); the declared EMITS types are `DECIMAL(10,0)` (which Exasol bins
    /// to ExaType Int64) and `VARCHAR(2000000)` (string). After emit_stream the
    /// decoded IPC batch must have `Int64` and `Utf8`, with values preserved.
    ///
    /// Invariants:
    /// 1. emit_stream does not return an error (no VM crash).
    /// 2. Column 0 is coerced Int32 → Int64 (DECIMAL(10,0) bins to ExaType Int64).
    /// 3. Column 1 is coerced Utf8View → Utf8; string values survive unchanged.
    #[tokio::test]
    async fn emit_stream_coerces_columns_to_declared_exatypes_before_emit_batch() {
        use arrow::array::{Int64Array, StringArray, StringViewArray};
        use arrow::datatypes::Field;

        // Build a RecordBatch with Int32 + Utf8View — what DataFusion 58 produces
        // for an Iceberg `int` column, and a string column (schema_force_view_types).
        let view_arr = StringViewArray::from(vec!["event-01", "event-02", "event-03"]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8View, false),
        ]));
        let id_col = Arc::new(Int32Array::from(vec![1i32, 2, 3]));
        let view_batch = RecordBatch::try_new(schema, vec![id_col, Arc::new(view_arr)]).unwrap();

        let stream = Box::pin(VecStream::new(vec![view_batch]));
        let mut ctx = CapturingCtx::new();

        // Declared EMITS types: DECIMAL(10,0) (Exasol bins p≤18,s=0 → ExaType Int64)
        // and VARCHAR. This is the exact shape of the live Q1 failure.
        let exa_types = vec!["DECIMAL(10,0)".to_string(), "VARCHAR(2000000)".to_string()];

        // Must not error — previously crashed with the two "cannot feed" errors.
        let total = emit_stream(&mut ctx, stream, &[], &exa_types)
            .await
            .expect("emit_stream must succeed and coerce to declared ExaTypes");

        assert_eq!(total, 3, "all 3 rows must be counted");
        assert_eq!(ctx.ipc_batches.len(), 1, "exactly 1 IPC payload");

        let decoded = ctx.decoded_batches();
        let decoded_batch = &decoded[0];

        // Column 0: Int32 coerced to Int64 (DECIMAL(10,0) bins to ExaType Int64).
        assert_eq!(
            decoded_batch.schema().field(0).data_type(),
            &DataType::Int64,
            "column 0 must be coerced to Int64 (the DECIMAL(10,0) ExaType target)"
        );
        let int_col = decoded_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("decoded column 0 must be Int64Array");
        assert_eq!(int_col.value(0), 1);
        assert_eq!(int_col.value(2), 3);

        // Column 1: Utf8View coerced to Utf8 (VARCHAR target).
        assert_eq!(
            decoded_batch.schema().field(1).data_type(),
            &DataType::Utf8,
            "column 1 must be coerced to Utf8 (Utf8View)"
        );
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
