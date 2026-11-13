use super::*;
use crate::adapter::pushdown::test_support::{col, like, str_lit};
use serde_json::json;

fn pattern(pattern: &str, escape: Option<char>) -> LikePattern {
    LikePattern::parse(pattern, escape).expect("the pattern parses")
}

fn prefix(prefix: &str, exact: bool) -> Option<LikeBound> {
    Some(LikeBound::Prefix {
        prefix: prefix.into(),
        exact,
    })
}

/// Scenario: A LIKE pattern prunes by its literal prefix
#[test]
fn a_like_pattern_bounds_by_its_literal_prefix() {
    assert_eq!(
        pattern("abc", None).bound(),
        Some(LikeBound::Equal("abc".into()))
    );
    assert_eq!(pattern("ab%", None).bound(), prefix("ab", true));
    assert_eq!(pattern("ab%%", None).bound(), prefix("ab", true));
    assert_eq!(pattern("ab%c", None).bound(), prefix("ab", false));
    assert_eq!(pattern("ab_", None).bound(), prefix("ab", false));
    assert_eq!(pattern("%ab", None).bound(), None);
    assert_eq!(pattern("_", None).bound(), None);
}

/// Scenario: A LIKE pattern prunes by its literal prefix
#[test]
fn an_escaped_wildcard_is_a_literal_character() {
    assert_eq!(pattern("a!_b%", Some('!')).bound(), prefix("a_b", true));
    assert_eq!(
        pattern("a!%", Some('!')).bound(),
        Some(LikeBound::Equal("a%".into()))
    );
    assert_eq!(
        pattern("a!!", Some('!')).bound(),
        Some(LikeBound::Equal("a!".into()))
    );
    assert!(
        LikePattern::parse("a!", Some('!')).is_none(),
        "a trailing escape escapes nothing"
    );
}

/// Scenario: A LIKE pattern prunes by its literal prefix
#[test]
fn a_like_pattern_matches_as_exasol_does() {
    let cases = [
        ("a%", "a", true),
        ("a%", "ab", true),
        ("a%", "ba", false),
        ("_", "a", true),
        ("_", "", false),
        ("_", "ab", false),
        ("%b%", "abc", true),
        ("a%c", "abbbc", true),
        ("a%c", "abcd", false),
        ("%", "", true),
        ("ä_", "äö", true),
    ];
    for (like, value, matches) in cases {
        assert_eq!(
            pattern(like, None).matches(value),
            matches,
            "{value} LIKE {like}"
        );
    }
    assert!(pattern("!_", Some('!')).matches("_"));
    assert!(!pattern("!_", Some('!')).matches("a"));
}

#[test]
fn like_operands_need_a_bare_column_and_string_literals() {
    let escaped = |escape: Json| {
        let mut node = like(col("S"), "a%");
        node["escapeChar"] = escape;
        node
    };
    assert!(like_operands(&like(col("S"), "a%")).is_some());
    assert!(like_operands(&escaped(str_lit("!"))).is_some());
    assert!(like_operands(&escaped(str_lit("!!"))).is_none());
    assert!(like_operands(&escaped(json!({"type": "literal_null"}))).is_none());
    assert!(like_operands(&like(str_lit("x"), "a%")).is_none());
}
