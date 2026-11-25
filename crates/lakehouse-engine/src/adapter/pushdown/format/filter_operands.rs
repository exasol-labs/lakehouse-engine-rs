use std::cmp::Ordering;
use vs_expression::{CompareOp, Expr, Scalar};

pub(super) fn holds(op: CompareOp, ordering: Ordering) -> bool {
    match op {
        CompareOp::Equal => ordering == Ordering::Equal,
        CompareOp::NotEqual => ordering != Ordering::Equal,
        CompareOp::Less => ordering == Ordering::Less,
        CompareOp::LessEqual => ordering != Ordering::Greater,
        CompareOp::Greater => ordering == Ordering::Greater,
        CompareOp::GreaterEqual => ordering != Ordering::Less,
    }
}

pub(super) fn column_name(node: &Expr) -> Option<&str> {
    node.as_column().map(|column| column.name.as_str())
}

/// Exasol reads `''` as NULL, so an empty string constrains nothing.
pub(super) fn non_empty_str(value: &Scalar) -> Option<&str> {
    let s = value.as_str()?;
    (!s.is_empty()).then_some(s)
}

/// An AND/OR node's operands; `None` when empty, since an empty OR would prune every file.
pub(super) fn operands(items: &[Expr]) -> Option<&[Expr]> {
    (!items.is_empty()).then_some(items)
}

/// The comparison is normalized to read with the column on the left.
pub(super) fn comparison_operands<'a>(
    left: &'a Expr,
    right: &'a Expr,
    comparison: CompareOp,
) -> Option<(&'a str, &'a Expr, CompareOp)> {
    if let Some(column) = column_name(left) {
        return Some((column, right, comparison));
    }
    Some((column_name(right)?, left, comparison.mirrored()))
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
pub(super) fn like_operands<'a>(
    expression: &'a Expr,
    pattern: &Expr,
    escape: Option<&Expr>,
) -> Option<(&'a str, LikePattern)> {
    let escape = match escape {
        None => DEFAULT_LIKE_ESCAPE,
        Some(escape) => {
            let mut chars = escape.as_string_literal()?.chars();
            let escape = chars.next()?;
            chars.next().is_none().then_some(escape)?
        }
    };
    let pattern = LikePattern::parse(pattern.as_string_literal()?, escape)?;
    Some((column_name(expression)?, pattern))
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
#[path = "filter_operands_tests.rs"]
mod tests;
