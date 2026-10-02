//! Shared by the cloud harnesses that own a per-run resource (`azure`, `glue`).

use std::future::Future;
use std::time::{SystemTime, UNIX_EPOCH};

/// Takes `value` as a parameter so the panic path is testable without mutating the shared
/// process environment. Panics name only the variable, never the value.
pub fn require_var(suite: &str, name: &str, value: Option<&str>) -> String {
    let Some(value) = value else {
        panic!("the {suite} suite requires environment variable {name}, which is not set");
    };
    let value = value.trim();
    assert!(
        !value.is_empty(),
        "the {suite} suite requires environment variable {name}, which is set but empty"
    );
    value.to_string()
}

/// `{user}{separator}{millis}` from `$USER` and the clock, at most `max_len` characters.
pub fn per_run_segment(separator: char, max_len: usize) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the UNIX epoch")
        .as_millis();
    derive_run_segment(
        &std::env::var("USER").unwrap_or_default(),
        millis,
        separator,
        max_len,
    )
}

/// The user is folded into `[a-z0-9]` plus `separator`, with no leading, trailing, or doubled
/// separator, since both Azure containers and Glue databases reject anything else.
pub fn derive_run_segment(user: &str, millis: u128, separator: char, max_len: usize) -> String {
    let suffix = millis.to_string();
    let budget = max_len.saturating_sub(separator.len_utf8() + suffix.len());
    let user_segment = sanitize_segment(user, budget, separator);
    if user_segment.is_empty() {
        suffix
    } else {
        format!("{user_segment}{separator}{suffix}")
    }
}

fn sanitize_segment(raw: &str, max_len: usize, separator: char) -> String {
    let mut segment = String::new();
    for character in raw.chars() {
        let legal = if character.is_ascii_alphanumeric() {
            character.to_ascii_lowercase()
        } else {
            separator
        };
        if legal == separator && segment.ends_with(separator) {
            continue;
        }
        if segment.len() == max_len {
            break;
        }
        segment.push(legal);
    }
    segment.trim_matches(separator).to_string()
}

/// `Drop` can fire inside a caller's `block_on`, where nesting a runtime panics, so `teardown`
/// runs on its own thread and runtime. Never panics, so an unwinding test keeps its failure.
pub fn run_teardown_off_runtime<F>(thread_name: &str, teardown: F) -> Result<F::Output, String>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // `thread::spawn` panics when the OS refuses a thread, which would abort an unwinding test.
    let spawned = std::thread::Builder::new()
        .name(thread_name.to_string())
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                // Not `enable_all`: it silently skips IO when tokio's `net` feature is off.
                .enable_io()
                .enable_time()
                .build()
                .map(|runtime| runtime.block_on(teardown))
                .map_err(|error| format!("build the teardown runtime: {error}"))
        });
    match spawned {
        Ok(handle) => handle
            .join()
            .unwrap_or_else(|_| Err("the teardown thread panicked".to_string())),
        Err(error) => Err(format!("the teardown thread could not be spawned: {error}")),
    }
}

pub fn panic_message(body: impl FnOnce() + std::panic::UnwindSafe) -> String {
    let payload = std::panic::catch_unwind(body).expect_err("expected the body to panic");
    super::stack::panic_payload_message(&*payload)
        .expect("panic payload was neither String nor &str")
}

mod cloud_fixture_tests {
    use super::{derive_run_segment, panic_message, require_var, run_teardown_off_runtime};

    const FIXED_MILLIS: u128 = 1_762_000_000_000;
    const MAX_LEN: usize = 40;

    #[test]
    fn run_segment_folds_the_user_into_a_legal_segment_for_either_separator() {
        let suffix = FIXED_MILLIS.to_string();
        let budget = MAX_LEN - 1 - suffix.len();
        let over_long = "A".repeat(90);
        // The separator lands on the last budgeted character, so truncation must drop it.
        let truncated_at_a_separator = format!("{}.tail", "a".repeat(budget - 1));

        for separator in ['-', '_'] {
            let sep = separator.to_string();
            let segment = |user: &str| derive_run_segment(user, FIXED_MILLIS, separator, MAX_LEN);

            for user in [
                "",
                "-",
                "___",
                "Antoni.Reus",
                "a..b",
                "ÜBER-user",
                "9",
                over_long.as_str(),
                truncated_at_a_separator.as_str(),
            ] {
                let segment = segment(user);
                assert!(
                    segment.len() <= MAX_LEN,
                    "{separator:?}, user {user:?}: {segment:?} exceeds {MAX_LEN} characters"
                );
                assert!(
                    segment
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == separator),
                    "{separator:?}, user {user:?}: {segment:?} must hold only [a-z0-9] and \
                     {separator:?}"
                );
                assert!(
                    !segment.contains(&sep.repeat(2)) && !segment.starts_with(separator),
                    "{separator:?}, user {user:?}: {segment:?} must not double or lead with the \
                     separator"
                );
                assert!(
                    segment.ends_with(&suffix),
                    "{separator:?}, user {user:?}: {segment:?} must keep the millisecond suffix"
                );
            }

            assert_eq!(
                segment(""),
                suffix,
                "an empty user leaves no segment rather than a leading separator"
            );
            assert_eq!(
                segment(&sep.repeat(3)),
                suffix,
                "a user of only separators leaves no segment"
            );
            assert_eq!(
                segment("Antoni.Reus"),
                format!("antoni{sep}reus{sep}{suffix}")
            );
            assert_eq!(
                segment("a..b"),
                format!("a{sep}b{sep}{suffix}"),
                "consecutive illegal characters collapse to one separator"
            );
            assert_eq!(
                segment("ÜBER-user"),
                format!("ber{sep}user{sep}{suffix}"),
                "a multi-byte character maps to one separator, trimmed at the segment start"
            );
            assert_eq!(
                segment(&over_long).len(),
                MAX_LEN,
                "an over-long user is truncated to exactly the remaining budget"
            );
            assert_eq!(
                segment(&truncated_at_a_separator),
                format!("{}{sep}{suffix}", "a".repeat(budget - 1)),
                "truncation on a separator drops it instead of leaving a doubled one"
            );
        }
    }

    #[test]
    fn missing_variable_fails_loud() {
        for absent in [None, Some(""), Some("   ")] {
            let message = panic_message(|| {
                require_var("probe-e2e", "PROBE_SECRET", absent);
            });
            assert!(
                message.contains("PROBE_SECRET") && message.contains("probe-e2e"),
                "panic for {absent:?} must name the suite and the variable, got: {message}"
            );
            assert!(
                !message.contains("   "),
                "panic for {absent:?} must not echo the value, got: {message}"
            );
        }
    }

    #[test]
    fn present_variable_is_read_without_surrounding_whitespace() {
        assert_eq!(
            require_var("probe-e2e", "PROBE_KEY", Some(" a2V5\n")),
            "a2V5",
            "a value sourced from test.env or a CI secret may carry a line ending"
        );
    }

    #[test]
    fn teardown_reports_a_panicking_future_instead_of_panicking() {
        assert_eq!(
            run_teardown_off_runtime("probe-teardown", async { 7 }),
            Ok(7)
        );
        let outcome: Result<(), String> =
            run_teardown_off_runtime("probe-teardown", async { panic!("deliberate panic") });
        assert_eq!(outcome, Err("the teardown thread panicked".to_string()));
    }
}
