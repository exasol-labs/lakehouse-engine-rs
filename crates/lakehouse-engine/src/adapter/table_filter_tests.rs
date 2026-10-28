use super::*;

fn matches(entry: &str, name: &str) -> bool {
    let patterns = parse_patterns(entry).unwrap_or_else(|e| panic!("'{entry}' must parse: {e}"));
    TableFilter::new(Some(patterns), None).exposes(name)
}

/// Scenario: A pattern matches the whole virtual table name without regard to case
#[test]
fn star_and_question_mark_follow_the_glob_rules() {
    assert!(matches("*", ""));
    assert!(matches("*", &"A".repeat(500)));
    assert!(matches("A*", "A"));
    assert!(matches("?", "A"));
    assert!(!matches("?", ""));
    assert!(!matches("?", "AB"));
    assert!(matches("A*B*C", "AXBYBZC"));
    assert!(!matches("A*B*C", "AXBYBZ"));
}

/// Scenario: A pattern matches the whole virtual table name without regard to case
#[test]
fn other_characters_match_only_themselves() {
    assert!(matches(r"[A]\B*", r"[A]\BX"));
    assert!(!matches(r"[A]\B*", "AB"));
}

/// Scenario: A pattern matches the whole virtual table name without regard to case
#[test]
fn a_pattern_matches_the_whole_name() {
    assert!(matches("ORDERS", "ORDERS"));
    assert!(!matches("ORDERS", "ORDERS_2024"));
    assert!(!matches("ORDERS", "STAGING__ORDERS"));
}

/// Scenario: A pattern matches the whole virtual table name without regard to case
#[test]
fn entries_are_trimmed_and_uppercased() {
    assert!(matches(" orders_?0* ", "ORDERS_2024"));
}

#[test]
fn blank_entries_are_rejected() {
    for value in ["A,", " , "] {
        assert_eq!(
            parse_patterns(value).err().as_deref(),
            Some("a comma-separated list of non-empty table name patterns"),
            "{value:?}"
        );
    }
}

#[test]
fn many_stars_do_not_backtrack_exponentially() {
    let pattern = format!("{}B", "*A".repeat(10));
    assert!(!matches(&pattern, &"A".repeat(128)));
}
