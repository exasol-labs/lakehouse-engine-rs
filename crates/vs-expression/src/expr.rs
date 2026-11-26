//! The typed expression tree of a pushdown request. [`crate::parse_request`] is its only
//! reader of request JSON; every renderer, walker, and rewrite works on this tree.
//!
//! Every walk visits a node's children in the order SQL writes them, which is the order of the
//! emitted columns and aggregate slots.

use std::convert::Infallible;

use crate::exasol_type::ExasolType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Literal(Literal),
    Column(Column),
    Compare {
        op: CompareOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    /// `expression IN (values)`.
    InList {
        expression: Box<Expr>,
        values: Vec<Expr>,
    },
    Between {
        expression: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    Like {
        expression: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
    },
    Scalar(ScalarCall),
    /// `EXTRACT(field FROM argument)`, `field` as Exasol spells it.
    Extract {
        field: String,
        argument: Box<Expr>,
    },
    Case(CaseExpr),
    Cast {
        target: ExasolType,
        argument: Box<Expr>,
    },
    JsonValue(JsonValueCall),
    Aggregate(AggregateCall),
}

/// A literal, its value checked at parse. Exasol sends a boolean as a JSON boolean, every other
/// value as text, and a NULL of any type, `''` included, as `literal_null`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal {
    Null,
    Bool(bool),
    String(String),
    /// `literal_double` in the text Exasol sent, such as `1.0000000000000001e+300`.
    Double(String),
    /// `literal_exactnumeric` in the text Exasol sent, such as `-5` or `0.1`.
    ExactNumeric(String),
    Date(String),
    Timestamp(String),
    /// `literal_timestamp_utc`, a UTC timestamp value.
    TimestampUtc(String),
    /// `literal_timestamputc`, Exasol's `TIMESTAMP WITH LOCAL TIME ZONE` literal.
    LocalTimeZoneTimestamp(String),
    /// A hex string retyped as bytes for a binary column, never sent by Exasol. A `width` types it
    /// `FixedSizeBinary`.
    Binary {
        hex: String,
        width: Option<u64>,
    },
}

/// A `column` node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// As the request spells it; callers pick [`Self::upper_name`] or [`Self::ascii_upper_name`].
    pub name: String,
    pub table_name: Option<String>,
    pub table_alias: Option<String>,
}

impl Column {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table_name: None,
            table_alias: None,
        }
    }

    /// A column qualified by a relation alias only, as a rewritten join leg names it.
    pub fn aliased(name: impl Into<String>, alias: Option<&str>) -> Self {
        Self {
            table_alias: alias.map(str::to_string),
            ..Self::new(name)
        }
    }

    /// Full-Unicode uppercase (`ß` → `SS`), the fold of every declared column name.
    pub fn upper_name(&self) -> String {
        self.name.to_uppercase()
    }

    /// The join-leg collectors' fold. It agrees with [`Self::upper_name`] only because every
    /// declared name is already Unicode-uppercased; repair that premise, never unify the folds.
    pub fn ascii_upper_name(&self) -> String {
        self.name.to_ascii_uppercase()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

impl CompareOp {
    pub fn sql(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::NotEqual => "<>",
            Self::Less => "<",
            Self::LessEqual => "<=",
            Self::Greater => ">",
            Self::GreaterEqual => ">=",
        }
    }

    /// The operator with its operands swapped: `a < b` is `b > a`.
    pub fn mirrored(self) -> Self {
        match self {
            Self::Less => Self::Greater,
            Self::LessEqual => Self::GreaterEqual,
            Self::Greater => Self::Less,
            Self::GreaterEqual => Self::LessEqual,
            op => op,
        }
    }
}

/// A `function_scalar` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalarCall {
    /// Uppercased.
    pub name: String,
    pub arguments: Vec<Expr>,
}

impl ScalarCall {
    pub fn new(name: &str, arguments: Vec<Expr>) -> Self {
        Self {
            name: name.to_uppercase(),
            arguments,
        }
    }
}

/// A `CASE`. With a `basis` it compares the basis with each WHEN value; `results` holds one THEN
/// per WHEN plus an optional trailing ELSE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseExpr {
    pub basis: Option<Box<Expr>>,
    pub whens: Vec<Expr>,
    pub results: Vec<Expr>,
}

impl CaseExpr {
    /// The ELSE result, when `results` holds one more than `whens`.
    pub fn else_result(&self) -> Option<&Expr> {
        (self.results.len() == self.whens.len() + 1).then(|| &self.results[self.whens.len()])
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonValueCall {
    pub document: Box<Expr>,
    pub path: Box<Expr>,
    /// The type a RETURNING clause states.
    pub returning: Option<ExasolType>,
    pub on_empty: Option<JsonBehavior>,
    pub on_error: Option<JsonBehavior>,
}

/// An `ON EMPTY` or `ON ERROR` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonBehavior {
    Null,
    Error,
    Default(Box<Expr>),
}

/// A `function_aggregate` call. No argument means `<NAME>(*)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateCall {
    /// Uppercased.
    pub name: String,
    pub arguments: Vec<Expr>,
    pub distinct: bool,
}

/// The children of `$node` as `&` or, given `mut`, `&mut` references: one definition, so the
/// shared and the mutable walks visit the same children in the same order.
macro_rules! children_of {
    ($node:expr $(, $m:tt)?) => {{
        let mut children = Vec::new();
        match $node {
            Expr::Literal(_) | Expr::Column(_) => {}
            Expr::Compare { left, right, .. } => children.extend([&$($m)? **left, &$($m)? **right]),
            Expr::And(items) | Expr::Or(items) => children.extend(items),
            Expr::Not(inner) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                children.push(&$($m)? **inner);
            }
            Expr::InList { expression, values } => {
                children.push(&$($m)? **expression);
                children.extend(values);
            }
            Expr::Between {
                expression,
                low,
                high,
            } => children.extend([&$($m)? **expression, &$($m)? **low, &$($m)? **high]),
            Expr::Like {
                expression,
                pattern,
                escape,
            } => {
                children.extend([&$($m)? **expression, &$($m)? **pattern]);
                if let Some(escape) = escape {
                    children.push(&$($m)? **escape);
                }
            }
            Expr::Scalar(call) => children.extend(&$($m)? call.arguments),
            Expr::Extract { argument, .. } | Expr::Cast { argument, .. } => {
                children.push(&$($m)? **argument);
            }
            Expr::Case(case) => {
                if let Some(basis) = &$($m)? case.basis {
                    children.push(&$($m)? **basis);
                }
                // Each WHEN before its THEN, the ELSE last.
                let mut results = (&$($m)? case.results).into_iter();
                for when in &$($m)? case.whens {
                    children.push(when);
                    children.extend(results.next());
                }
                children.extend(results);
            }
            Expr::JsonValue(call) => {
                children.extend([&$($m)? *call.document, &$($m)? *call.path]);
                for behavior in [&$($m)? call.on_empty, &$($m)? call.on_error] {
                    if let Some(JsonBehavior::Default(expression)) = behavior {
                        children.push(&$($m)? **expression);
                    }
                }
            }
            Expr::Aggregate(call) => children.extend(&$($m)? call.arguments),
        }
        children
    }};
}

impl Expr {
    pub fn compare(op: CompareOp, left: Expr, right: Expr) -> Self {
        Self::Compare {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn as_column(&self) -> Option<&Column> {
        match self {
            Self::Column(column) => Some(column),
            _ => None,
        }
    }

    pub fn as_literal(&self) -> Option<&Literal> {
        match self {
            Self::Literal(literal) => Some(literal),
            _ => None,
        }
    }

    /// The value of a `literal_string`.
    pub fn as_string_literal(&self) -> Option<&str> {
        match self {
            Self::Literal(Literal::String(value)) => Some(value),
            _ => None,
        }
    }

    pub fn is_aggregate(&self) -> bool {
        matches!(self, Self::Aggregate(_))
    }

    /// The child expressions in the order SQL writes them (see the module doc).
    pub fn children(&self) -> Vec<&Expr> {
        children_of!(self)
    }

    /// [`Self::children`], mutable.
    fn children_mut(&mut self) -> Vec<&mut Expr> {
        children_of!(self, mut)
    }

    /// Every column node at any depth, pre-order.
    pub fn columns(&self) -> Vec<&Column> {
        match self {
            Self::Column(column) => vec![column],
            _ => self
                .children()
                .into_iter()
                .flat_map(Expr::columns)
                .collect(),
        }
    }

    /// Whether `predicate` holds for any node at any depth.
    pub fn any(&self, predicate: &impl Fn(&Expr) -> bool) -> bool {
        predicate(self)
            || self
                .children()
                .into_iter()
                .any(|child| child.any(predicate))
    }

    /// Copies the tree, replacing each subtree for which `substitute` returns `Some`; the walk
    /// does not descend into a replaced subtree.
    pub fn substitute(&self, substitute: &mut impl FnMut(&Expr) -> Option<Expr>) -> Expr {
        let mut copy = self.clone();
        let Ok(()) = copy.try_rewrite(&mut |node| Ok::<_, Infallible>(substitute(node)));
        copy
    }

    /// Fallible in-place rewrite of every node, pre-order: `rewrite` returns the node in place of
    /// `node`, or `None` to keep it and descend into its children.
    pub fn try_rewrite<E>(
        &mut self,
        rewrite: &mut impl FnMut(&Expr) -> Result<Option<Expr>, E>,
    ) -> Result<(), E> {
        if let Some(replaced) = rewrite(self)? {
            *self = replaced;
            return Ok(());
        }
        for child in self.children_mut() {
            child.try_rewrite(rewrite)?;
        }
        Ok(())
    }

    /// Copies the tree with every column rewritten by `f`.
    pub fn map_columns(&self, f: &mut impl FnMut(&Column) -> Column) -> Expr {
        self.substitute(&mut |node| node.as_column().map(|column| Expr::Column(f(column))))
    }

    /// Copies the tree without any column's `tableAlias`.
    pub fn strip_table_alias(&self) -> Expr {
        self.map_columns(&mut |column| Column {
            table_alias: None,
            ..column.clone()
        })
    }

    /// Copies the tree, setting each column's `tableAlias` to `f`'s answer.
    pub fn with_table_aliases(&self, f: &mut impl FnMut(&Column) -> String) -> Expr {
        self.map_columns(&mut |column| Column {
            table_alias: Some(f(column)),
            ..column.clone()
        })
    }

    /// The top-level conjuncts, nested `AND` nodes flattened. Each `OR` or `NOT` stays one
    /// conjunct.
    pub fn conjuncts(&self) -> Vec<&Expr> {
        match self {
            Self::And(items) => items.iter().flat_map(Expr::conjuncts).collect(),
            other => vec![other],
        }
    }

    /// `None` when nothing is kept, the bare conjunct for one, else their `AND`.
    pub fn conjoin(mut kept: Vec<Expr>) -> Option<Expr> {
        match kept.len() {
            0 => None,
            1 => kept.pop(),
            _ => Some(Self::And(kept)),
        }
    }
}

#[cfg(test)]
#[path = "expr_tests.rs"]
mod tests;
