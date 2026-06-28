// DIAGNOSTIC: process-wide panic capture for the scan UDF.
//
// The scan UDF entry point wraps only its own (main) run thread in
// `catch_unwind`. A panic on a DataFusion / Tokio WORKER thread (parquet
// decode, async I/O) is NOT seen by that guard: with `panic = "unwind"` the
// panicking worker thread aborts the whole VM process *after* its frames
// unwind, so the engine reports an `err_zombie` VM crash with no Rust panic
// text. A process-wide panic hook (`std::panic::set_hook`) fires on the
// panicking thread itself, synchronously, BEFORE that unwind/abort — for ANY
// thread — so it is the only seam that can capture a worker-thread panic.
//
// This module installs such a hook. It records the panic to stderr (the SLC /
// exaudfclient may log it) AND appends it to a fixed, retrievable file in the
// COS container, then chains the previously-installed hook so abort behavior is
// unchanged. It is intentionally minimal and clearly marked so it can be kept
// as an observability feature or removed cleanly.

use std::backtrace::Backtrace;
use std::fs::File;
use std::io::Write;
use std::panic::PanicHookInfo;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, Once, OnceLock};

/// Fixed, retrievable path the panic hook appends to inside the COS container.
///
/// Stable on purpose: it is fetched from the container after a live run.
pub const PANIC_LOG_PATH: &str = "/tmp/lakehouse_udf_panic.log";

static INSTALL: Once = Once::new();

// ============================================================================
// DIAGNOSTIC (temporary): per-process checkpoint trail for the scan UDF.
//
// Purpose: prove EXACTLY where/why the scan VM dies on Q3 (engine-enforced
// per-instance MEMORY kill — an allocation abort crossing the ~4 GB address
// space: no Rust panic, no core, err_zombie — so the panic hook above never
// fires). The mechanism is a coarse checkpoint trail written to a per-PID file
// where the LAST line before the VM dies localizes the crash, and the
// per-batch RSS sequence reveals streaming (flat RSS) vs accumulation (climbing
// RSS).
//
// Concurrency model (must hold under the bench's concurrent config — multiple
// shard VMs per node AND a multi-thread DataFusion runtime within a VM; do NOT
// rely on NR_OF_CORES=1):
//   * Per-PROCESS file `/tmp/lakehouse_udf_debug.<pid>.log` — one file per VM
//     process, so concurrent shard VMs (separate processes) never interleave.
//   * Intra-VM threads (Tokio workers, DataFusion decode threads) share one PID
//     → one file. A process-global `Mutex<Option<File>>` guards a single
//     long-lived append handle; each checkpoint is ONE `write_all` of one
//     `<4 KB` line, then `flush()` + `sync_all()`, all under the lock. O_APPEND
//     already makes each `write_all` atomic at the kernel level; the lock only
//     serializes the (write, flush, sync) triple so records never tear.
//   * The lock is held for the few microseconds of a line write — never across
//     batch decode/emit — so it does NOT serialize DataFusion work or secretly
//     reduce concurrency.
//   * Crash-proof: `flush()` + `sync_all()` per line so the last checkpoint is
//     on disk before a hard abort. All I/O is best-effort (errors swallowed) so
//     a logging failure never itself crashes the VM.
//
// Removability: this whole block (and its call sites, all marked
// `// DIAGNOSTIC (temporary):`) can be deleted wholesale without touching the
// scan logic.
// ============================================================================

/// Per-process debug-checkpoint log path: `/tmp/lakehouse_udf_debug.<pid>.log`.
///
/// One file per VM process so concurrent shard VMs never interleave lines.
pub fn debug_log_path() -> String {
    format!("/tmp/lakehouse_udf_debug.{}.log", std::process::id())
}

/// Monotonic per-process checkpoint sequence counter.
static DEBUG_SEQ: AtomicU64 = AtomicU64::new(0);

/// Cumulative rows emitted so far this process (best-effort, updated by the
/// per-batch checkpoint so a checkpoint line can carry the running row total
/// even when called from a site that does not itself know the count).
static DEBUG_ROWS: AtomicU64 = AtomicU64::new(0);

/// The single long-lived append handle to the per-process debug file, guarded
/// for intra-VM multi-thread safety. `OnceLock` lazily opens it on first use.
fn debug_writer() -> &'static Mutex<Option<File>> {
    static WRITER: OnceLock<Mutex<Option<File>>> = OnceLock::new();
    WRITER.get_or_init(|| {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(debug_log_path())
            .ok();
        Mutex::new(file)
    })
}

/// Record the cumulative rows-emitted counter (called by the per-batch hook).
pub fn debug_set_rows(total: u64) {
    DEBUG_ROWS.store(total, Ordering::Relaxed);
}

/// Current resident set size (RSS) in bytes, read from `/proc/self/statm`.
///
/// `/proc/self/statm` field 2 (0-indexed field 1) is the resident page count;
/// multiply by the page size. Returns `0` if unreadable (best-effort) so the
/// checkpoint line still renders. Page size is fixed at 4096 on the SLC's
/// x86-64 Linux; reading it dynamically would pull in libc, which this module
/// deliberately avoids.
fn current_rss_bytes() -> u64 {
    const PAGE_SIZE: u64 = 4096;
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
        return 0;
    };
    statm
        .split_ascii_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u64>().ok())
        .map(|pages| pages * PAGE_SIZE)
        .unwrap_or(0)
}

/// Write one diagnostic checkpoint line, tagged with pid, thread identity, a
/// monotonic sequence number, cumulative rows emitted, and current RSS (MB).
///
/// `msg` is the checkpoint label plus any inline detail (kept on one line,
/// `<4 KB`). Each call is a single `write_all` under the writer lock, then
/// `flush` + `sync_all`, so it survives a hard abort and never tears under
/// concurrent threads. Also mirrored to stderr so a future SLC output-redirect
/// would surface it live.
pub fn debug_checkpoint(msg: &str) {
    let seq = DEBUG_SEQ.fetch_add(1, Ordering::Relaxed);
    let rows = DEBUG_ROWS.load(Ordering::Relaxed);
    let rss_mb = current_rss_bytes() / (1024 * 1024);
    let pid = std::process::id();
    let epoch_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let thread = std::thread::current();
    let thread_name = thread.name().unwrap_or("<unnamed>").to_string();
    let thread_id = format!("{:?}", thread.id());

    // One line, single write. Keep it greppable: leading "LHDBG" tag + key=val.
    let line = format!(
        "LHDBG seq={seq} epoch_ms={epoch_ms} pid={pid} thread={thread_name} \
         thread_id={thread_id} rows={rows} rss_mb={rss_mb} :: {msg}\n"
    );

    // (a) stderr mirror — best-effort.
    let _ = write!(std::io::stderr(), "{line}");
    let _ = std::io::stderr().flush();

    // (b) per-process file — single locked write + flush + sync so the last
    //     line is durable before a hard VM abort.
    if let Ok(mut guard) = debug_writer().lock()
        && let Some(file) = guard.as_mut()
    {
        let _ = file.write_all(line.as_bytes());
        let _ = file.flush();
        let _ = file.sync_all();
    }
}

/// Install the process-wide diagnostic panic hook exactly once.
///
/// Idempotent (guarded by a `Once`): repeated calls after the first are no-ops,
/// so it is safe to call at the top of every UDF entry without re-chaining.
/// Must be called before any DataFusion / Tokio worker thread is spawned so the
/// hook is in place when a worker panics.
pub fn install_panic_hook() {
    INSTALL.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            // Capture a backtrace without requiring RUST_BACKTRACE in the env.
            let backtrace = Backtrace::force_capture();
            let record = format_panic_record(info, &backtrace);

            // (a) stderr — best-effort; the SLC may forward it.
            let _ = write!(std::io::stderr(), "{record}");
            let _ = std::io::stderr().flush();

            // (b) fixed retrievable file — best-effort, flushed+synced because
            // the process may abort immediately after this hook returns.
            append_record(PANIC_LOG_PATH, &record);

            // Chain the previously-installed hook so behavior is otherwise
            // unchanged (default stderr message + the engine's abort).
            previous(info);
        }));
    });
}

/// Format a single panic record: pid, timestamp, thread, location, payload,
/// and the captured backtrace. Self-contained and allocation-only — never
/// panics, so it is safe to call from inside the panic hook.
fn format_panic_record(info: &PanicHookInfo<'_>, backtrace: &Backtrace) -> String {
    let pid = std::process::id();
    let epoch_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let thread = std::thread::current();
    let thread_name = thread.name().unwrap_or("<unnamed>").to_string();
    let thread_id = format!("{:?}", thread.id());

    let location = info
        .location()
        .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
        .unwrap_or_else(|| "<unknown location>".to_string());

    let message = panic_payload_message(info);

    format!(
        "==== LAKEHOUSE UDF PANIC ====\n\
         pid={pid} epoch_secs={epoch_secs} thread={thread_name} thread_id={thread_id}\n\
         location={location}\n\
         payload={message}\n\
         backtrace:\n{backtrace}\n\
         =============================\n"
    )
}

/// Extract the panic payload as a string, covering the common `&str` / `String`
/// payload shapes and falling back to a placeholder for non-string payloads.
fn panic_payload_message(info: &PanicHookInfo<'_>) -> String {
    let payload = info.payload();
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Append a record to the panic log, flushing and syncing immediately.
///
/// Best-effort: all I/O errors are swallowed because this runs inside the panic
/// hook (a panic here would abort with no information) and the process may be
/// torn down right after. Append mode + per-record open keeps concurrent
/// worker-thread panics from clobbering each other's records.
fn append_record(path: &str, record: &str) {
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = file.write_all(record.as_bytes());
        let _ = file.flush();
        let _ = file.sync_all();
    }
}

// ============================================================================
// Phase telemetry (Task 4): opt-in, config-gated, monotonic-clock phase timing.
//
// Layers on the per-process checkpoint infra above. Exactly three phases are
// timed with a monotonic clock (`std::time::Instant`, not wall-clock, so a
// clock adjustment cannot perturb a measurement):
//   (a) startup       — UDF entry through runtime/session/plan build, up to the
//                        first batch fetch.
//   (b) import         — cumulative time awaiting batches from the DataFusion
//                        stream (the S3 read + Parquet decode the stream drives).
//   (c) send-back/emit — cumulative time turning batches back into Exasol output
//                        (column coercion + emit_batch/emit flush).
//
// Accounting model: startup is sealed when the first import interval opens; from
// then on every wall-clock instant of the scan body is attributed to either an
// import interval (awaiting `stream.next()`) or an emit interval (everything
// from a batch arriving until the next fetch begins — coercion is attributed to
// emit). So `startup + import + emit` reconstructs the scan-body wall-clock to
// within measurement error, with no phase silently omitted.
//
// Telemetry is best-effort and MUST NOT alter results: every write is wrapped
// and its failure ignored, and the timers never change the streaming discipline
// (fetch one batch, emit, drop before the next).
// ============================================================================

use std::time::{Duration, Instant};

/// Return `true` when the resolved debug level enables phase telemetry.
///
/// Telemetry emits only at `DEBUG` (or more verbose); the production default
/// (`INFO`) is silent. Mirrors the `udf_log!` level ordering
/// (`ERROR < WARN < INFO < DEBUG < TRACE`).
pub fn telemetry_enabled(level: tracing::Level) -> bool {
    level >= tracing::Level::DEBUG
}

/// Monotonic phase-timing accumulators for one scan invocation.
///
/// Construct with [`PhaseTimers::start`] at the top of the scan body. Call
/// [`PhaseTimers::seal_startup`] just before the first batch fetch, then wrap
/// each fetch with [`import`](Self::import_started)/[`import_ended`] and each
/// emit with [`emit_started`](Self::emit_started)/[`emit_ended`]. The three
/// durations are read back via the accessors at completion.
#[derive(Debug)]
pub struct PhaseTimers {
    body_start: Instant,
    startup: Duration,
    startup_sealed: bool,
    import: Duration,
    emit: Duration,
    mark: Option<Instant>,
}

impl PhaseTimers {
    /// Begin timing the scan body. The startup phase clock starts now.
    pub fn start() -> Self {
        Self {
            body_start: Instant::now(),
            startup: Duration::ZERO,
            startup_sealed: false,
            import: Duration::ZERO,
            emit: Duration::ZERO,
            mark: None,
        }
    }

    /// Seal the startup phase: record entry→now as startup. Idempotent — only
    /// the first call (just before the first batch fetch) takes effect.
    pub fn seal_startup(&mut self) {
        if !self.startup_sealed {
            self.startup = self.body_start.elapsed();
            self.startup_sealed = true;
        }
    }

    /// Mark the start of awaiting a batch from the stream.
    pub fn import_started(&mut self) {
        self.mark = Some(Instant::now());
    }

    /// Accumulate the elapsed await time into the import phase.
    pub fn import_ended(&mut self) {
        if let Some(m) = self.mark.take() {
            self.import += m.elapsed();
        }
    }

    /// Mark the start of an emit (coercion + emit_batch/flush).
    pub fn emit_started(&mut self) {
        self.mark = Some(Instant::now());
    }

    /// Accumulate the elapsed emit time into the send-back phase.
    pub fn emit_ended(&mut self) {
        if let Some(m) = self.mark.take() {
            self.emit += m.elapsed();
        }
    }

    /// Startup phase duration (entry → first batch fetch).
    pub fn startup(&self) -> Duration {
        self.startup
    }

    /// Cumulative object-storage import duration.
    pub fn import(&self) -> Duration {
        self.import
    }

    /// Cumulative send-back/emit duration.
    pub fn emit(&self) -> Duration {
        self.emit
    }

    /// Total scan-body wall-clock since [`start`](Self::start).
    pub fn body_elapsed(&self) -> Duration {
        self.body_start.elapsed()
    }
}

/// Format the single per-VM phase-telemetry record.
///
/// One greppable line (`LHTELEM` tag) carrying the pid (so per-shard timings are
/// attributable even before the SLC's fd-tagging is applied) and the three phase
/// durations in milliseconds plus the reconstructed scan-body wall-clock.
pub fn telemetry_record(timers: &PhaseTimers) -> String {
    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
    format!(
        "LHTELEM pid={} phase_startup_ms={:.3} phase_import_ms={:.3} phase_emit_ms={:.3} body_ms={:.3}",
        std::process::id(),
        ms(timers.startup()),
        ms(timers.import()),
        ms(timers.emit()),
        ms(timers.body_elapsed()),
    )
}

/// Best-effort append of one telemetry record to the per-process telemetry file.
///
/// Distinct from the per-PID debug-checkpoint file so a benchmark can collect
/// the telemetry line directly. Every error is swallowed — a telemetry-write
/// failure MUST NOT fail the scan.
pub fn telemetry_file_path() -> String {
    format!("/tmp/lakehouse_udf_telemetry.{}.log", std::process::id())
}

/// Write one phase-telemetry record, best-effort, to the per-process telemetry
/// file. Returns nothing and never errors — a failed write is ignored so the
/// scan is never affected by a telemetry sink problem.
pub fn write_telemetry_file(record: &str) {
    let mut line = String::with_capacity(record.len() + 1);
    line.push_str(record);
    line.push('\n');
    append_record(&telemetry_file_path(), &line);
}

#[cfg(test)]
mod phase_telemetry_tests {
    use super::*;
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
    use super::*;

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
    use super::*;
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
            record.contains("location=") && record.contains("diagnostics.rs"),
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
