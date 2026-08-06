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

use datafusion::execution::cache::cache_manager::FileMetadataCacheEntry;
use object_store::path::Path;
use std::backtrace::Backtrace;
use std::collections::{HashMap, HashSet};
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

// ============================================================================
// Footer re-fetch observable (task 1.7b): make a metadata-cache eviction that
// re-fetches a positional-delete data file's Parquet footer visible instead of
// silent (issue #165 proposed-change item 3).
//
// `PositionalDeleteScanTable::partitioned_files` records, via
// `record_access_plan_cached_footer`, every data-file path whose footer it
// fetched and cached while building the base access plan. At scan completion
// the report site (`scan/mod.rs`) reads the session `FileMetadataCache`'s
// `list_entries()` snapshot and passes it to `footer_refetch_count`, which
// counts a recorded path as a re-fetch when the opener's own lookup did not
// find it (absent from the map — evicted, or never admitted because the entry
// exceeded the whole cache limit) or — only where the scan shape guarantees the
// opener opens every assigned file — found it but never read it back
// (`hits == 0`, which `put` resets to on every re-insert). `footer_refetch_count`
// documents why a pushed LIMIT or a join makes that second reading ambiguous.
//
// `reset_access_plan_cached_footers` MUST run at the start of every scan
// invocation (`run_scan_dispatch`), because a pooled UDF process serves many
// invocations off the same fixed per-node VM in sequence; without the reset, a
// later invocation would report an earlier invocation's recorded paths.
// ============================================================================

/// Process-global record of the data-file footer paths access-plan
/// construction cached this scan invocation, guarded the same way
/// [`debug_writer`] guards its file handle: a `Mutex`, lazily created behind a
/// `OnceLock` since `HashSet::new()` is not itself a `const fn`.
fn access_plan_cached_footers() -> &'static Mutex<HashSet<Path>> {
    static PATHS: OnceLock<Mutex<HashSet<Path>>> = OnceLock::new();
    PATHS.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Record that `path`'s Parquet footer was fetched and cached during
/// access-plan construction. Called once per delete-carrying data file,
/// immediately after the `fetch_metadata()` call that cached its entry — the
/// only site that knows which footers it cached.
pub fn record_access_plan_cached_footer(path: &Path) {
    if let Ok(mut paths) = access_plan_cached_footers().lock() {
        paths.insert(path.clone());
    }
}

/// Clear the recorded set. MUST be called at the start of every scan
/// invocation so a later invocation on the same pooled UDF process never
/// reports an earlier invocation's recorded paths.
pub fn reset_access_plan_cached_footers() {
    if let Ok(mut paths) = access_plan_cached_footers().lock() {
        paths.clear();
    }
}

/// Whether the scan shape guarantees the Parquet opener reaches every file
/// access-plan construction cached a footer for — the condition that decides
/// whether a cached entry's `hits == 0` is readable as a re-fetch at all (see
/// [`footer_refetch_count`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenerCoverage {
    /// The opener opens every assigned file: no pushed LIMIT can end the stream
    /// early and no join can leave one side unpolled.
    EveryAssignedFile,
    /// The stream can finish before the opener reaches every assigned file.
    MayStopEarly,
}

/// Count how many of the recorded access-plan-cached footers the Parquet opener
/// had to fetch again, given `entries` (`FileMetadataCache::list_entries()`) and
/// how much of the file list the scan shape guarantees the opener reaches.
///
/// A recorded path ABSENT from the map is always a re-fetch: its entry was
/// evicted, or never admitted because it exceeded the whole cache limit, so the
/// opener's own lookup necessarily fetched the footer a second time.
///
/// A recorded path PRESENT with `hits == 0` is ambiguous, and `coverage`
/// resolves it. `put` counts no hit (`datafusion-execution-54.1.0/src/cache/
/// file_metadata_cache.rs:75`), so `hits == 0` is BOTH the state of an entry the
/// opener looked up, missed, and re-`put`, AND the state of an entry
/// access-plan construction cached for a file the opener never opened at all —
/// and the opener does not always reach every assigned file. A pushed LIMIT ends
/// the stream as soon as the remaining row budget reaches zero, before the later
/// files of the group are opened (`datafusion-datasource-54.1.0/src/file_stream/
/// scan_state.rs:166-186`, which returns `ScanAndReturn::Done` at that point),
/// and an inner join whose build side comes back empty never polls the probe
/// side. Under [`OpenerCoverage::MayStopEarly`] `hits == 0` is therefore NOT
/// counted: an unopened footer was fetched once, not twice, and nothing was
/// lost. Only under [`OpenerCoverage::EveryAssignedFile`] does `hits == 0` mean
/// the opener looked and missed.
///
/// A path the opener genuinely read back carries `hits >= 1`, so a scan whose
/// footers all stayed cached reports zero under either coverage.
pub fn footer_refetch_count(
    entries: &HashMap<Path, FileMetadataCacheEntry>,
    coverage: OpenerCoverage,
) -> u64 {
    let Ok(paths) = access_plan_cached_footers().lock() else {
        return 0;
    };
    paths
        .iter()
        .filter(|path| match entries.get(*path) {
            Some(entry) => entry.hits == 0 && coverage == OpenerCoverage::EveryAssignedFile,
            None => true,
        })
        .count() as u64
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

#[cfg(test)]
#[path = "diagnostics_tests.rs"]
mod test_modules;

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
