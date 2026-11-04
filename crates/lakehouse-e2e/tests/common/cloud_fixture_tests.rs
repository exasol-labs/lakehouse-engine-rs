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
