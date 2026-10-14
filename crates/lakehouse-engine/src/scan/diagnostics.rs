//! A panic on a DataFusion/Tokio worker thread bypasses the entry point's `catch_unwind` and
//! aborts the VM (`err_zombie`) with no Rust panic text. A process-wide panic hook fires on the
//! panicking thread before that abort, so it is the only seam that can capture it; the hook logs
//! to stderr and [`PANIC_LOG_PATH`], then chains the previous hook.

use datafusion::execution::cache::cache_manager::FileMetadataCacheEntry;
use object_store::path::Path;
use std::backtrace::Backtrace;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::panic::PanicHookInfo;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, Once, OnceLock};

/// Stable on purpose: fetched from the container after a live run.
pub const PANIC_LOG_PATH: &str = "/tmp/lakehouse_udf_panic.log";

static INSTALL: Once = Once::new();

// Per-process checkpoint trail: the last line before a VM dies localizes the crash, and the
// per-batch RSS sequence separates streaming from accumulation. One file per PID so concurrent
// shard VMs never interleave; intra-VM threads share one locked append handle, held only for a
// single line write (write + flush + sync_all, so the last line survives a hard abort). All I/O
// is best-effort so logging never crashes the VM.

pub fn debug_log_path() -> String {
    format!("/tmp/lakehouse_udf_debug.{}.log", std::process::id())
}

static DEBUG_SEQ: AtomicU64 = AtomicU64::new(0);

/// Lets a checkpoint carry the running row total from sites that do not know it.
static DEBUG_ROWS: AtomicU64 = AtomicU64::new(0);

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

pub fn debug_set_rows(total: u64) {
    DEBUG_ROWS.store(total, Ordering::Relaxed);
}

// Footer re-fetch observable (#165): data-file footers cached during access-plan construction
// are recorded here and compared against the session `FileMetadataCache` at scan end.
// `reset_access_plan_cached_footers` MUST run at the start of every invocation, because a pooled
// UDF process serves many invocations in sequence.

fn access_plan_cached_footers() -> &'static Mutex<HashSet<Path>> {
    static PATHS: OnceLock<Mutex<HashSet<Path>>> = OnceLock::new();
    PATHS.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Called right after the `fetch_metadata()` that cached the entry, the only site that knows.
pub fn record_access_plan_cached_footer(path: &Path) {
    if let Ok(mut paths) = access_plan_cached_footers().lock() {
        paths.insert(path.clone());
    }
}

/// MUST run at the start of every scan invocation; see the section note above.
pub fn reset_access_plan_cached_footers() {
    if let Ok(mut paths) = access_plan_cached_footers().lock() {
        paths.clear();
    }
}

/// Decides whether a cached entry's `hits == 0` is readable as a re-fetch (see [`footer_refetch_count`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenerCoverage {
    /// No pushed LIMIT can end the stream early and no join can leave one side unpolled.
    EveryAssignedFile,
    MayStopEarly,
}

/// A recorded path absent from `entries` is always a re-fetch (evicted or never admitted).
///
/// A present path with `hits == 0` is ambiguous: `put` counts no hit
/// (`datafusion-execution-54.1.0/src/cache/file_metadata_cache.rs:75`), so it is both a
/// looked-up-missed-re-put entry and one the opener never opened. A pushed LIMIT ends the stream
/// before later files open (`datafusion-datasource-54.1.0/src/file_stream/scan_state.rs:166-186`)
/// and an inner join with an empty build side never polls the probe side, so `hits == 0` counts
/// only under [`OpenerCoverage::EveryAssignedFile`].
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

/// Field 1 of `/proc/self/statm` is the resident page count. Page size is fixed at 4096 on the
/// SLC's x86-64 Linux, since reading it dynamically would pull in libc. `0` if unreadable.
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

/// One `write_all` under the writer lock then `flush` + `sync_all`, so a line survives a hard
/// abort and never tears. Mirrored to stderr for an SLC output redirect.
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

    let line = format!(
        "LHDBG seq={seq} epoch_ms={epoch_ms} pid={pid} thread={thread_name} \
         thread_id={thread_id} rows={rows} rss_mb={rss_mb} :: {msg}\n"
    );

    let _ = write!(std::io::stderr(), "{line}");
    let _ = std::io::stderr().flush();

    // Synced so the last line is durable before a hard VM abort.
    if let Ok(mut guard) = debug_writer().lock()
        && let Some(file) = guard.as_mut()
    {
        let _ = file.write_all(line.as_bytes());
        let _ = file.flush();
        let _ = file.sync_all();
    }
}

/// Idempotent. Must run before any DataFusion/Tokio worker thread is spawned.
pub fn install_panic_hook() {
    INSTALL.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let backtrace = Backtrace::force_capture();
            let record = format_panic_record(info, &backtrace);

            let _ = write!(std::io::stderr(), "{record}");
            let _ = std::io::stderr().flush();

            // Synced: the process may abort right after this hook returns.
            append_record(PANIC_LOG_PATH, &record);

            // Keeps the default stderr message and the engine's abort.
            previous(info);
        }));
    });
}

/// Never panics, so it is safe inside the panic hook.
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

/// Best-effort: a panic inside the panic hook would abort with no information. Per-record
/// append-mode open keeps concurrent worker-thread panics from clobbering each other.
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

// Phase telemetry: startup (entry to first batch fetch), import (awaiting `stream.next()`), and
// emit (coercion + emit) are timed on a monotonic clock. Startup seals when the first import
// opens; afterwards every instant is import or emit, so the three reconstruct the scan body.
// Best-effort and MUST NOT alter results or the streaming discipline.

use std::time::{Duration, Instant};

/// Only at `DEBUG` or more verbose; the production default `INFO` is silent.
pub fn telemetry_enabled(level: tracing::Level) -> bool {
    level >= tracing::Level::DEBUG
}

/// Call [`PhaseTimers::seal_startup`] just before the first batch fetch, then bracket each fetch
/// with `import_started`/`import_ended` and each emit with `emit_started`/`emit_ended`.
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

    /// Idempotent: only the first call takes effect.
    pub fn seal_startup(&mut self) {
        if !self.startup_sealed {
            self.startup = self.body_start.elapsed();
            self.startup_sealed = true;
        }
    }

    pub fn import_started(&mut self) {
        self.mark = Some(Instant::now());
    }

    pub fn import_ended(&mut self) {
        if let Some(m) = self.mark.take() {
            self.import += m.elapsed();
        }
    }

    pub fn emit_started(&mut self) {
        self.mark = Some(Instant::now());
    }

    pub fn emit_ended(&mut self) {
        if let Some(m) = self.mark.take() {
            self.emit += m.elapsed();
        }
    }

    pub fn startup(&self) -> Duration {
        self.startup
    }

    pub fn import(&self) -> Duration {
        self.import
    }

    pub fn emit(&self) -> Duration {
        self.emit
    }

    pub fn body_elapsed(&self) -> Duration {
        self.body_start.elapsed()
    }
}

/// Carries the pid so per-shard timings are attributable before the SLC's fd-tagging.
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

/// Separate from the debug-checkpoint file so a benchmark can collect the telemetry line directly.
pub fn telemetry_file_path() -> String {
    format!("/tmp/lakehouse_udf_telemetry.{}.log", std::process::id())
}

/// Best-effort: a failed write never affects the scan.
pub fn write_telemetry_file(record: &str) {
    let mut line = String::with_capacity(record.len() + 1);
    line.push_str(record);
    line.push('\n');
    append_record(&telemetry_file_path(), &line);
}

#[cfg(test)]
#[path = "diagnostics_tests.rs"]
mod tests;
