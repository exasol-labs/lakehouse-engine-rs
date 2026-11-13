use super::*;
use crate::adapter::pushdown::test_support::{col, like, like_escape, str_lit};
use serde_json::json;

fn pattern(pattern: &str, escape: char) -> LikePattern {
    LikePattern::parse(pattern, escape).expect("the pattern parses")
}

fn prefix(prefix: &str, exact: bool) -> Option<LikeBound> {
    Some(LikeBound::Prefix {
        prefix: prefix.into(),
        exact,
    })
}

fn bound_of(node: &Json) -> Option<LikeBound> {
    like_operands(node)?.1.bound()
}

#[test]
fn a_like_pattern_bounds_by_its_literal_prefix() {
    assert_eq!(
        pattern("abc", '\\').bound(),
        Some(LikeBound::Equal("abc".into()))
    );
    assert_eq!(pattern("ab%", '\\').bound(), prefix("ab", true));
    assert_eq!(pattern("ab%%", '\\').bound(), prefix("ab", true));
    assert_eq!(pattern("ab%c", '\\').bound(), prefix("ab", false));
    assert_eq!(pattern("ab_", '\\').bound(), prefix("ab", false));
    assert_eq!(pattern("%ab", '\\').bound(), None);
    assert_eq!(pattern("_", '\\').bound(), None);
}

#[test]
fn an_escaped_character_is_a_literal_character() {
    assert_eq!(pattern("a!_b%", '!').bound(), prefix("a_b", true));
    assert_eq!(
        pattern("a!%", '!').bound(),
        Some(LikeBound::Equal("a%".into()))
    );
    assert_eq!(
        pattern("a!!", '!').bound(),
        Some(LikeBound::Equal("a!".into()))
    );
    assert_eq!(
        pattern("x!a", '!').bound(),
        Some(LikeBound::Equal("xa".into())),
        "an escaped ordinary character is that character"
    );
    assert!(
        LikePattern::parse("a!", '!').is_none(),
        "a trailing escape escapes nothing"
    );
}

/// A plain `LIKE` arrives without `escapeChar`, but Exasol still escapes with `\`.
#[test]
fn a_plain_like_escapes_with_the_default_backslash() {
    assert_eq!(bound_of(&like(col("S"), r"x\_%")), prefix("x_", true));
    assert_eq!(
        bound_of(&like(col("S"), r"x\a")),
        Some(LikeBound::Equal("xa".into()))
    );
    assert_eq!(
        bound_of(&like_escape(col("S"), r"x\_%", "!")),
        prefix(r"x\", false),
        "an explicit escape replaces the default"
    );
}

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
        (r"x\_%", "x_1", true),
        (r"x\_%", "xa1", false),
    ];
    for (like, value, matches) in cases {
        assert_eq!(
            pattern(like, '\\').matches(value),
            matches,
            "{value} LIKE {like}"
        );
    }
    assert!(pattern("!_", '!').matches("_"));
    assert!(!pattern("!_", '!').matches("a"));
}

#[test]
fn like_operands_need_a_bare_column_and_string_literals() {
    let mut null_escape = like(col("S"), "a%");
    null_escape["escapeChar"] = json!({"type": "literal_null"});
    assert!(like_operands(&like(col("S"), "a%")).is_some());
    assert!(like_operands(&like_escape(col("S"), "a%", "!")).is_some());
    assert!(like_operands(&like_escape(col("S"), "a%", "!!")).is_none());
    assert!(like_operands(&null_escape).is_none());
    assert!(like_operands(&like(str_lit("x"), "a%")).is_none());
}

/// An empty fold must answer no constraint, since the formats' empty AND is true and their
/// empty OR is false, which would prune every file.
#[test]
fn a_fold_with_nothing_translated_constrains_nothing() {
    let join = |parts: Vec<String>| parts.join(" ");
    assert_eq!(fold_and(std::iter::empty(), join), None);
    assert_eq!(fold_or(std::iter::empty(), join), None);
    assert_eq!(fold_and([None, None], join), None);
    assert_eq!(fold_or([None, None], join), None);
}
