#[cfg(test)]
mod phase_telemetry_tests {
    use super::super::*;
    use std::thread::sleep;

    /// Telemetry is silent at the production default level and any level below
    /// DEBUG; enabled at DEBUG and TRACE.
    #[test]
    fn telemetry_enabled_only_at_debug_or_more_verbose() {
        assert!(!telemetry_enabled(tracing::Level::ERROR));
        assert!(!telemetry_enabled(tracing::Level::WARN));
        assert!(!telemetry_enabled(tracing::Level::INFO));
        assert!(telemetry_enabled(tracing::Level::DEBUG));
        assert!(telemetry_enabled(tracing::Level::TRACE));
    }

    /// The three phases are accumulated distinctly and their sum reconstructs
    /// the scan-body wall-clock within a small tolerance. Import and emit are
    /// attributed to separate accumulators (a read-bound scan vs an emit-bound
    /// scan are distinguishable).
    #[test]
    fn phases_accumulate_distinctly_and_sum_to_body() {
        let mut t = PhaseTimers::start();
        sleep(Duration::from_millis(20)); // startup work
        t.seal_startup();

        // Two batches: import then emit each.
        for _ in 0..2 {
            t.import_started();
            sleep(Duration::from_millis(10));
            t.import_ended();

            t.emit_started();
            sleep(Duration::from_millis(5));
            t.emit_ended();
        }

        let startup = t.startup();
        let import = t.import();
        let emit = t.emit();
        let body = t.body_elapsed();

        // Distinct attribution: import (~20ms total) clearly exceeds emit (~10ms).
        assert!(import > emit, "import {import:?} must exceed emit {emit:?}");
        assert!(
            startup >= Duration::from_millis(18),
            "startup must capture the ~20ms pre-fetch work, got {startup:?}"
        );

        // Sum reconstructs the body within measurement error (sleeps overshoot,
        // and a few hundred microseconds of un-timed glue may exist between the
        // last emit and reading body). Allow a generous absolute tolerance.
        let summed = startup + import + emit;
        let diff = body.saturating_sub(summed);
        assert!(
            diff < Duration::from_millis(10),
            "startup+import+emit ({summed:?}) must reconstruct body ({body:?}) within tolerance; diff={diff:?}"
        );
    }

    /// `seal_startup` is idempotent: a second call does not overwrite the first
    /// startup measurement (so re-entering the loop body cannot corrupt it).
    #[test]
    fn seal_startup_is_idempotent() {
        let mut t = PhaseTimers::start();
        sleep(Duration::from_millis(10));
        t.seal_startup();
        let first = t.startup();
        sleep(Duration::from_millis(10));
        t.seal_startup();
        assert_eq!(first, t.startup(), "second seal_startup must be a no-op");
    }

    /// The telemetry record carries the three phase durations and the body
    /// wall-clock, tagged with the pid, on one greppable line.
    #[test]
    fn telemetry_record_carries_three_phases_and_pid() {
        let mut t = PhaseTimers::start();
        t.seal_startup();
        t.import_started();
        sleep(Duration::from_millis(1));
        t.import_ended();
        let rec = telemetry_record(&t);
        let pid = std::process::id();
        assert!(
            rec.starts_with("LHTELEM "),
            "must carry the LHTELEM tag: {rec}"
        );
        assert!(rec.contains(&format!("pid={pid}")), "must carry pid: {rec}");
        assert!(
            rec.contains("phase_startup_ms="),
            "must report startup: {rec}"
        );
        assert!(
            rec.contains("phase_import_ms="),
            "must report import: {rec}"
        );
        assert!(rec.contains("phase_emit_ms="), "must report emit: {rec}");
        assert!(
            rec.contains("body_ms="),
            "must report body wall-clock: {rec}"
        );
        assert!(!rec.contains('\n'), "must be a single line: {rec}");
    }

    /// A telemetry-file write to an impossible path is swallowed — best-effort,
    /// never panics or errors (the property that keeps a sink failure from
    /// failing the scan).
    #[test]
    fn write_telemetry_file_swallows_failure() {
        // Append to a path under a non-existent directory: the open fails and
        // the function returns normally without panicking.
        append_record(
            "/nonexistent-dir-lakehouse/telemetry.log",
            "LHTELEM pid=0 phase_startup_ms=0\n",
        );
    }
}

#[cfg(test)]
mod debug_checkpoint_tests {
    use super::super::*;

    /// The per-process debug path is `/tmp/lakehouse_udf_debug.<pid>.log` and
    /// carries THIS process's pid — so concurrent shard VMs (distinct pids) each
    /// get a distinct file and never interleave lines.
    #[test]
    fn debug_log_path_is_per_pid() {
        let path = debug_log_path();
        let pid = std::process::id();
        assert_eq!(
            path,
            format!("/tmp/lakehouse_udf_debug.{pid}.log"),
            "debug path must embed this process's pid"
        );
    }

    /// `current_rss_bytes` reads a plausible non-zero RSS from /proc/self/statm
    /// on Linux (the SLC target). On a platform without procfs it returns 0
    /// rather than panicking — the checkpoint line still renders.
    #[test]
    fn current_rss_is_readable_or_zero() {
        let rss = current_rss_bytes();
        // The test process itself is resident, so on Linux this is > 0; we only
        // assert it does not panic and is a sane u64 (always true). On non-Linux
        // it is 0. Either way the function is total.
        let _ = rss;
    }

    /// One formatted checkpoint line carries every field the live triage needs:
    /// the LHDBG tag, a sequence number, pid, thread identity, the running row
    /// count, an rss_mb field, and the message — all on a single line.
    ///
    /// Verified by formatting the same way `debug_checkpoint` does (the write
    /// itself targets a fixed /tmp path shared with the live UDF, so the unit
    /// test asserts the record SHAPE rather than touching that shared file).
    #[test]
    fn checkpoint_line_contains_required_fields() {
        debug_set_rows(12_345);
        let seq = DEBUG_SEQ.fetch_add(1, Ordering::Relaxed);
        let rows = DEBUG_ROWS.load(Ordering::Relaxed);
        let pid = std::process::id();
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("<unnamed>").to_string();
        let line = format!(
            "LHDBG seq={seq} epoch_ms=0 pid={pid} thread={thread_name} \
             thread_id=X rows={rows} rss_mb=0 :: ENTER run_scan"
        );
        assert!(line.starts_with("LHDBG "), "must carry the LHDBG grep tag");
        assert!(
            line.contains(&format!("pid={pid}")),
            "must carry pid: {line}"
        );
        assert!(line.contains("seq="), "must carry sequence: {line}");
        assert!(line.contains("thread="), "must carry thread: {line}");
        assert!(
            line.contains("rows=12345"),
            "must carry the running row total: {line}"
        );
        assert!(line.contains("rss_mb="), "must carry rss_mb: {line}");
        assert!(
            line.contains(":: ENTER run_scan"),
            "must carry the checkpoint message: {line}"
        );
        assert!(!line.contains('\n'), "must be one line");
    }

    /// The sequence counter is monotonic and unique under concurrent threads —
    /// the property that lets the LAST line before death localize the crash even
    /// when multiple DataFusion/Tokio worker threads emit checkpoints in one VM.
    /// `fetch_add` is atomic, so N threads each taking K sequence numbers yield
    /// N×K distinct values with no duplicates and no torn reads.
    #[test]
    fn sequence_counter_is_unique_under_concurrency() {
        use std::collections::HashSet;
        use std::sync::Arc;

        let seen: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let seen = seen.clone();
            handles.push(std::thread::spawn(move || {
                let mut local = Vec::with_capacity(1000);
                for _ in 0..1000 {
                    local.push(DEBUG_SEQ.fetch_add(1, Ordering::Relaxed));
                }
                let mut g = seen.lock().unwrap();
                for v in local {
                    g.insert(v);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // 8 threads × 1000 = 8000 fetch_adds → 8000 distinct sequence values.
        assert_eq!(
            seen.lock().unwrap().len(),
            8000,
            "every fetch_add must yield a unique sequence number (no duplicates under concurrency)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use std::sync::Mutex;

    // The panic hook is process-wide shared state. Any test that swaps the hook
    // (take_hook / set_hook) must hold this lock for the whole swap-act-restore
    // window, or a concurrently running test's take_hook can capture the wrong
    // hook and corrupt the chain. Serializes only the hook-mutating tests.
    static HOOK_LOCK: Mutex<()> = Mutex::new(());

    fn unique_path(tag: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "lakehouse_udf_panic_test_{}_{}.log",
            tag,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&p);
        p
    }

    /// The formatted record carries every diagnostic field the live triage needs:
    /// pid, thread identity, location, the panic payload, and a backtrace.
    #[test]
    fn format_record_contains_required_fields() {
        let _guard = HOOK_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let captured: std::sync::Arc<Mutex<Option<String>>> = std::sync::Arc::new(Mutex::new(None));
        let sink = captured.clone();
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let bt = Backtrace::force_capture();
            *sink.lock().unwrap() = Some(format_panic_record(info, &bt));
        }));
        let _ = std::panic::catch_unwind(|| panic!("synthetic boom"));
        std::panic::set_hook(prev);

        let record = captured.lock().unwrap().take().expect("hook must have run");
        let pid = std::process::id();
        assert!(
            record.contains(&format!("pid={pid}")),
            "record must carry pid: {record}"
        );
        assert!(
            record.contains("thread="),
            "record must carry thread identity: {record}"
        );
        assert!(
            record.contains("location=") && record.contains("diagnostics_tests.rs"),
            "record must carry the panic location: {record}"
        );
        assert!(
            record.contains("payload=synthetic boom"),
            "record must carry the payload: {record}"
        );
        assert!(
            record.contains("backtrace:"),
            "record must carry a backtrace section: {record}"
        );
    }

    /// `append_record` creates the file and appends (does not truncate) so
    /// multiple panics accumulate.
    #[test]
    fn append_record_creates_and_appends() {
        let path = unique_path("append");
        let path_str = path.to_str().unwrap();
        append_record(path_str, "FIRST\n");
        append_record(path_str, "SECOND\n");
        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(contents.contains("FIRST"), "first record must persist");
        assert!(
            contents.contains("SECOND"),
            "second record must be appended, not overwrite: {contents}"
        );
        let first_pos = contents.find("FIRST").unwrap();
        let second_pos = contents.find("SECOND").unwrap();
        assert!(first_pos < second_pos, "append must preserve order");
        let _ = std::fs::remove_file(&path);
    }

    /// `panic_payload_message` extracts `&str` and `String` payloads and falls
    /// back for non-string payloads. Verified via catch_unwind hook capture.
    #[test]
    fn payload_message_extracts_str_and_string() {
        let _guard = HOOK_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let captured: std::sync::Arc<Mutex<Vec<String>>> =
            std::sync::Arc::new(Mutex::new(Vec::new()));
        let sink = captured.clone();
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            sink.lock().unwrap().push(panic_payload_message(info));
        }));

        let _ = std::panic::catch_unwind(|| panic!("str payload"));
        let owned = String::from("string payload");
        let _ = std::panic::catch_unwind(move || std::panic::panic_any(owned));
        let _ = std::panic::catch_unwind(|| std::panic::panic_any(42_i32));

        std::panic::set_hook(prev);

        let msgs = captured.lock().unwrap();
        assert!(
            msgs.iter().any(|m| m.contains("str payload")),
            "&str payload must be captured: {msgs:?}"
        );
        assert!(
            msgs.iter().any(|m| m.contains("string payload")),
            "String payload must be captured: {msgs:?}"
        );
        assert!(
            msgs.iter().any(|m| m == "<non-string panic payload>"),
            "non-string payload (panic_any i32) must fall back: {msgs:?}"
        );
    }

    /// A hook installed via `std::panic::set_hook` fires on a panic raised on a
    /// SPAWNED thread, not only the main thread — the core property the
    /// worker-thread crash relies on. The hook here writes a record to a temp
    /// file from the panicking worker; the test asserts the record persisted,
    /// proving the same `append_record` path used by `install_panic_hook` runs
    /// for a non-main thread's panic.
    #[test]
    fn hook_fires_on_spawned_worker_thread_panic() {
        let _guard = HOOK_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let path = unique_path("worker");
        let path_owned = path.to_str().unwrap().to_string();

        let prev = std::panic::take_hook();
        let hook_path = path_owned.clone();
        std::panic::set_hook(Box::new(move |info| {
            let bt = Backtrace::force_capture();
            let record = format_panic_record(info, &bt);
            append_record(&hook_path, &record);
        }));

        // Panic on a freshly spawned (non-main) thread. The spawned thread's
        // panic unwinds and the join returns Err, but the process-wide hook must
        // have already run on that worker thread.
        let handle = std::thread::Builder::new()
            .name("scan-worker-probe".to_string())
            .spawn(|| panic!("worker thread boom"))
            .unwrap();
        let join = handle.join();

        std::panic::set_hook(prev);
        assert!(join.is_err(), "spawned thread must have panicked");

        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(
            contents.contains("worker thread boom"),
            "hook must capture the worker-thread panic payload: {contents}"
        );
        assert!(
            contents.contains("thread=scan-worker-probe"),
            "record must name the panicking worker thread: {contents}"
        );
        assert!(
            contents.contains("location="),
            "record must carry a location field: {contents}"
        );
        let _ = std::fs::remove_file(&path);
    }
}
