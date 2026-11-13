use serde_json::Value as Json;
use std::cmp::Ordering;
use vs_expression::ColumnNode;

#[derive(Clone, Copy, Debug)]
pub(in crate::adapter::pushdown) enum Comparison {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

impl Comparison {
    pub(in crate::adapter::pushdown) fn from_node_type(kind: &str) -> Option<Self> {
        Some(match kind {
            "predicate_equal" => Self::Equal,
            "predicate_notequal" => Self::NotEqual,
            "predicate_less" => Self::Less,
            "predicate_lessequal" => Self::LessEqual,
            "predicate_greater" => Self::Greater,
            "predicate_greaterequal" => Self::GreaterEqual,
            _ => return None,
        })
    }

    pub(in crate::adapter::pushdown) fn node_type(self) -> &'static str {
        match self {
            Self::Equal => "predicate_equal",
            Self::NotEqual => "predicate_notequal",
            Self::Less => "predicate_less",
            Self::LessEqual => "predicate_lessequal",
            Self::Greater => "predicate_greater",
            Self::GreaterEqual => "predicate_greaterequal",
        }
    }

    pub(in crate::adapter::pushdown) fn with_operands_swapped(self) -> Self {
        match self {
            Self::Less => Self::Greater,
            Self::LessEqual => Self::GreaterEqual,
            Self::Greater => Self::Less,
            Self::GreaterEqual => Self::LessEqual,
            symmetric => symmetric,
        }
    }

    pub(super) fn holds(self, ordering: Ordering) -> bool {
        match self {
            Self::Equal => ordering == Ordering::Equal,
            Self::NotEqual => ordering != Ordering::Equal,
            Self::Less => ordering == Ordering::Less,
            Self::LessEqual => ordering != Ordering::Greater,
            Self::Greater => ordering == Ordering::Greater,
            Self::GreaterEqual => ordering != Ordering::Less,
        }
    }
}

fn column_name(node: &Json) -> Option<&str> {
    ColumnNode::parse(node).map(|column| column.name)
}

/// Exasol reads `''` as NULL, so an empty string constrains nothing.
pub(super) fn non_empty_str(value: &Json) -> Option<&str> {
    let s = value.as_str()?;
    (!s.is_empty()).then_some(s)
}

/// An AND/OR node's operands; `None` when empty, since an empty OR would prune every file.
pub(super) fn operands(node: &Json) -> Option<&[Json]> {
    non_empty(node.get("expressions")?)
}

/// The comparison is normalized to read with the column on the left.
pub(super) fn comparison_operands(
    node: &Json,
    comparison: Comparison,
) -> Option<(&str, &Json, Comparison)> {
    let left = node.get("left")?;
    let right = node.get("right")?;
    if let Some(column) = column_name(left) {
        return Some((column, right, comparison));
    }
    Some((
        column_name(right)?,
        left,
        comparison.with_operands_swapped(),
    ))
}

pub(super) fn subject_column(node: &Json) -> Option<&str> {
    column_name(node.get("expression")?)
}

/// `None` when empty, since an empty IN would prune every file.
pub(super) fn in_operands(node: &Json) -> Option<(&str, &[Json])> {
    Some((subject_column(node)?, non_empty(node.get("arguments")?)?))
}

pub(super) fn between_operands(node: &Json) -> Option<(&str, Option<&Json>, Option<&Json>)> {
    Some((subject_column(node)?, node.get("left"), node.get("right")))
}

fn non_empty(array: &Json) -> Option<&[Json]> {
    let items = array.as_array()?;
    (!items.is_empty()).then_some(items.as_slice())
}

/// A pruning predicate implied by the filter node it translates. Only an exact one may be negated,
/// because `NOT` over a widened predicate narrows it and prunes files holding matching rows (#466).
#[derive(Debug, PartialEq)]
pub(super) struct Translated<P> {
    pub(super) predicate: P,
    pub(super) exact: bool,
}

impl<P> Translated<P> {
    pub(super) fn exact(predicate: P) -> Self {
        Self {
            predicate,
            exact: true,
        }
    }

    pub(super) fn negated(self, not: impl FnOnce(P) -> P) -> Option<Self> {
        self.exact.then(|| Self::exact(not(self.predicate)))
    }
}

/// An untranslatable conjunct is dropped, which only widens; `None` when none is left.
pub(super) fn fold_and<P>(
    children: impl IntoIterator<Item = Option<Translated<P>>>,
    and: impl FnOnce(Vec<P>) -> P,
) -> Option<Translated<P>> {
    let mut predicates = Vec::new();
    let mut exact = true;
    for child in children {
        match child {
            Some(translated) => {
                exact &= translated.exact;
                predicates.push(translated.predicate);
            }
            None => exact = false,
        }
    }
    (!predicates.is_empty()).then(|| Translated {
        predicate: and(predicates),
        exact,
    })
}

/// One untranslatable disjunct forfeits the whole OR, since dropping it would narrow.
pub(super) fn fold_or<P>(
    children: impl IntoIterator<Item = Option<Translated<P>>>,
    or: impl FnOnce(Vec<P>) -> P,
) -> Option<Translated<P>> {
    let translated: Vec<Translated<P>> = children.into_iter().collect::<Option<_>>()?;
    if translated.is_empty() {
        return None;
    }
    let exact = translated.iter().all(|child| child.exact);
    Some(Translated {
        predicate: or(translated
            .into_iter()
            .map(|child| child.predicate)
            .collect()),
        exact,
    })
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum LikeToken {
    Char(char),
    AnyOne,
    AnyMany,
}

/// A `LIKE` pattern with its escapes resolved.
#[derive(Debug)]
pub(super) struct LikePattern(Vec<LikeToken>);

/// What a `LIKE` pattern implies for a column: equality when it has no wildcard, else a literal
/// prefix that is exact only when nothing but `%` follows it.
#[derive(Debug, PartialEq)]
pub(super) enum LikeBound {
    Equal(String),
    Prefix { prefix: String, exact: bool },
}

/// Exasol sends `escapeChar` only for an explicit `ESCAPE`. A plain `LIKE` takes the session's
/// `DEFAULT_LIKE_ESCAPE_CHARACTER`, which the adapter cannot read, so it assumes the default.
const DEFAULT_LIKE_ESCAPE: char = '\\';

/// The column and pattern of a `LIKE` over a bare column and a string pattern.
pub(super) fn like_operands(node: &Json) -> Option<(&str, LikePattern)> {
    fn string(literal: &Json) -> Option<&str> {
        (literal.get("type")?.as_str()? == "literal_string").then_some(())?;
        literal.get("value")?.as_str()
    }
    let escape = match node.get("escapeChar") {
        None => DEFAULT_LIKE_ESCAPE,
        Some(escape) => {
            let mut chars = string(escape)?.chars();
            let escape = chars.next()?;
            chars.next().is_none().then_some(escape)?
        }
    };
    let pattern = LikePattern::parse(string(node.get("pattern")?)?, escape)?;
    Some((subject_column(node)?, pattern))
}

impl LikePattern {
    fn parse(pattern: &str, escape: char) -> Option<Self> {
        let mut chars = pattern.chars();
        let mut tokens = Vec::new();
        while let Some(c) = chars.next() {
            tokens.push(match c {
                c if c == escape => LikeToken::Char(chars.next()?),
                '%' => LikeToken::AnyMany,
                '_' => LikeToken::AnyOne,
                c => LikeToken::Char(c),
            });
        }
        Some(Self(tokens))
    }

    /// `None` when the pattern starts with a wildcard, which constrains nothing.
    pub(super) fn bound(&self) -> Option<LikeBound> {
        let literal = self
            .0
            .iter()
            .position(|token| !matches!(token, LikeToken::Char(_)))
            .unwrap_or(self.0.len());
        let prefix: String = self.0[..literal]
            .iter()
            .filter_map(|token| match token {
                LikeToken::Char(c) => Some(*c),
                _ => None,
            })
            .collect();
        let rest = &self.0[literal..];
        if rest.is_empty() {
            return Some(LikeBound::Equal(prefix));
        }
        (!prefix.is_empty()).then(|| LikeBound::Prefix {
            prefix,
            exact: rest.iter().all(|token| *token == LikeToken::AnyMany),
        })
    }

    pub(super) fn matches(&self, value: &str) -> bool {
        let value: Vec<char> = value.chars().collect();
        let (mut at, mut token) = (0, 0);
        let mut backtrack = None;
        while at < value.len() {
            match self.0.get(token) {
                Some(LikeToken::AnyMany) => {
                    backtrack = Some((token, at));
                    token += 1;
                }
                Some(LikeToken::AnyOne) => (at, token) = (at + 1, token + 1),
                Some(LikeToken::Char(c)) if *c == value[at] => (at, token) = (at + 1, token + 1),
                _ => match backtrack {
                    Some((many, from)) => {
                        backtrack = Some((many, from + 1));
                        (at, token) = (from + 1, many + 1);
                    }
                    None => return false,
                },
            }
        }
        self.0[token..]
            .iter()
            .all(|token| *token == LikeToken::AnyMany)
    }
}

#[cfg(test)]
#[path = "filter_json_tests.rs"]
mod tests;
