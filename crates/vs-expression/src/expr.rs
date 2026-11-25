//! The typed expression tree of a pushdown request. [`crate::parse_expression`] is its only
//! reader of request JSON; every renderer, walker, and rewrite works on this tree.
//!
//! Every walk visits a node's children in the alphabetical order of their wire keys (`arguments`,
//! `basis`, `escapeChar`, `expression`, `left`, `pattern`, `results`, `right`), the order the
//! golden fixtures pin for the emitted columns and aggregate slots.

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
    /// A node only the scan's typed pass writes, rendered only in the DataFusion dialect.
    Scan(ScanNode),
}

/// A literal as Exasol sent it: its kind and its raw value, which renders as sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Literal {
    pub kind: LiteralKind,
    pub value: Scalar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiteralKind {
    Null,
    Bool,
    String,
    Double,
    ExactNumeric,
    Date,
    Timestamp,
    /// `literal_timestamp_utc`, a UTC timestamp value.
    TimestampUtc,
    /// `literal_timestamputc`, Exasol's `TIMESTAMP WITH LOCAL TIME ZONE` literal.
    LocalTimeZoneTimestamp,
    /// A hex string retyped as bytes for a binary column, never sent by Exasol. A `width` types it
    /// `FixedSizeBinary`.
    Binary {
        width: Option<u64>,
    },
}

/// A literal's value. Absent and JSON null are both `Null`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scalar {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
}

impl Scalar {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(text) => Some(text),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// The value as SQL text: a string as is, a number or boolean in its JSON spelling, NULL empty.
    pub fn text(&self) -> String {
        match self {
            Self::Null => String::new(),
            Self::Bool(value) => value.to_string(),
            Self::Number(number) => number.to_string(),
            Self::String(text) => text.clone(),
        }
    }
}

impl Literal {
    pub fn new(kind: LiteralKind, value: Scalar) -> Self {
        Self { kind, value }
    }

    pub fn null() -> Self {
        Self::new(LiteralKind::Null, Scalar::Null)
    }

    pub fn string(value: impl Into<String>) -> Self {
        Self::new(LiteralKind::String, Scalar::String(value.into()))
    }

    pub fn bool(value: bool) -> Self {
        Self::new(LiteralKind::Bool, Scalar::Bool(value))
    }

    /// Exasol's NULL: a `literal_null`, or any literal without a value.
    pub fn is_null(&self) -> bool {
        self.kind == LiteralKind::Null || self.value.is_null()
    }

    /// A `literal_bool` holds `true`, `"true"`, `"TRUE"`, or `1`; anything else is FALSE.
    pub fn is_true(&self) -> bool {
        match &self.value {
            Scalar::Bool(value) => *value,
            Scalar::String(text) => text == "true" || text == "TRUE",
            Scalar::Number(number) => number.as_i64() == Some(1),
            Scalar::Null => false,
        }
    }
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
    /// The fractional digits a `TO_TIMESTAMP` keeps, which only the typed pass knows.
    pub(crate) timestamp_digits: Option<u8>,
}

impl ScalarCall {
    pub fn new(name: &str, arguments: Vec<Expr>) -> Self {
        Self {
            name: name.to_uppercase(),
            arguments,
            timestamp_digits: None,
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

/// A node the typed pass writes. Only this crate builds or reads one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanNode(pub(crate) ScanKind);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScanKind {
    /// An integer operand converted to text.
    IntegerText(Box<Expr>),
    /// A boolean operand converted to Exasol's `TRUE`/`FALSE`.
    BooleanText(Box<Expr>),
    /// A boolean operand converted to Exasol's 1 or 0, which DataFusion does not do
    /// (apache/datafusion#16797).
    BooleanNumber(Box<Expr>),
    /// A string function's numeric argument as the `Int64` DataFusion requires, rounded as Exasol
    /// rounds it.
    IntegerArgument {
        argument: Box<Expr>,
        round: bool,
        negative_is_null: bool,
    },
    /// The seconds of its argument with `fraction` fractional digits.
    Second { argument: Box<Expr>, fraction: u8 },
    /// Its text argument with an empty string read as NULL, because Exasol has no empty string
    /// and DataFusion keeps one as a value (#203).
    EmptyAsNull(Box<Expr>),
}

impl ScanKind {
    pub(crate) fn argument(&self) -> &Expr {
        match self {
            Self::IntegerText(argument)
            | Self::BooleanText(argument)
            | Self::BooleanNumber(argument)
            | Self::EmptyAsNull(argument)
            | Self::IntegerArgument { argument, .. }
            | Self::Second { argument, .. } => argument,
        }
    }

    pub(crate) fn argument_mut(&mut self) -> &mut Expr {
        match self {
            Self::IntegerText(argument)
            | Self::BooleanText(argument)
            | Self::BooleanNumber(argument)
            | Self::EmptyAsNull(argument)
            | Self::IntegerArgument { argument, .. }
            | Self::Second { argument, .. } => argument,
        }
    }
}

impl Expr {
    pub(crate) fn scan(kind: ScanKind) -> Self {
        Self::Scan(ScanNode(kind))
    }

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
            Self::Literal(Literal {
                kind: LiteralKind::String,
                value,
            }) => value.as_str(),
            _ => None,
        }
    }

    pub fn is_aggregate(&self) -> bool {
        matches!(self, Self::Aggregate(_))
    }

    /// The child expressions in the alphabetical order of their wire keys (see the module doc).
    pub fn children(&self) -> Vec<&Expr> {
        let mut children = Vec::new();
        match self {
            Self::Literal(_) | Self::Column(_) => {}
            Self::Compare { left, right, .. } => children.extend([&**left, &**right]),
            Self::And(items) | Self::Or(items) => children.extend(items),
            Self::Not(inner) | Self::IsNull(inner) | Self::IsNotNull(inner) => {
                children.push(&**inner);
            }
            Self::InList { expression, values } => {
                children.extend(values);
                children.push(&**expression);
            }
            Self::Between {
                expression,
                low,
                high,
            } => children.extend([&**expression, &**low, &**high]),
            Self::Like {
                expression,
                pattern,
                escape,
            } => {
                children.extend(escape.as_deref());
                children.extend([&**expression, &**pattern]);
            }
            Self::Scalar(call) => children.extend(&call.arguments),
            Self::Extract { argument, .. } | Self::Cast { argument, .. } => {
                children.push(&**argument);
            }
            Self::Case(case) => {
                children.extend(&case.whens);
                children.extend(case.basis.as_deref());
                children.extend(&case.results);
            }
            Self::JsonValue(call) => {
                children.extend([&*call.document, &*call.path]);
                for behavior in [&call.on_empty, &call.on_error] {
                    if let Some(JsonBehavior::Default(expression)) = behavior {
                        children.push(&**expression);
                    }
                }
            }
            Self::Aggregate(call) => children.extend(&call.arguments),
            Self::Scan(ScanNode(kind)) => children.push(kind.argument()),
        }
        children
    }

    /// [`Self::children`], mutable.
    fn children_mut(&mut self) -> Vec<&mut Expr> {
        let mut children: Vec<&mut Expr> = Vec::new();
        match self {
            Self::Literal(_) | Self::Column(_) => {}
            Self::Compare { left, right, .. } => children.extend([&mut **left, &mut **right]),
            Self::And(items) | Self::Or(items) => children.extend(items),
            Self::Not(inner) | Self::IsNull(inner) | Self::IsNotNull(inner) => {
                children.push(&mut **inner);
            }
            Self::InList { expression, values } => {
                children.extend(values);
                children.push(&mut **expression);
            }
            Self::Between {
                expression,
                low,
                high,
            } => children.extend([&mut **expression, &mut **low, &mut **high]),
            Self::Like {
                expression,
                pattern,
                escape,
            } => {
                children.extend(escape.as_deref_mut());
                children.extend([&mut **expression, &mut **pattern]);
            }
            Self::Scalar(call) => children.extend(&mut call.arguments),
            Self::Extract { argument, .. } | Self::Cast { argument, .. } => {
                children.push(&mut **argument);
            }
            Self::Case(case) => {
                children.extend(&mut case.whens);
                children.extend(case.basis.as_deref_mut());
                children.extend(&mut case.results);
            }
            Self::JsonValue(call) => {
                children.extend([&mut *call.document, &mut *call.path]);
                for behavior in [&mut call.on_empty, &mut call.on_error] {
                    if let Some(JsonBehavior::Default(expression)) = behavior {
                        children.push(&mut **expression);
                    }
                }
            }
            Self::Aggregate(call) => children.extend(&mut call.arguments),
            Self::Scan(ScanNode(kind)) => children.push(kind.argument_mut()),
        }
        children
    }

    /// Visits every node, pre-order.
    pub fn walk<'a>(&'a self, f: &mut impl FnMut(&'a Expr)) {
        f(self);
        for child in self.children() {
            child.walk(f);
        }
    }

    /// Visits every column node at any depth.
    pub fn walk_columns<'a>(&'a self, f: &mut impl FnMut(&'a Column)) {
        self.walk(&mut |node| {
            if let Self::Column(column) = node {
                f(column);
            }
        });
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
        if let Some(replaced) = substitute(self) {
            return replaced;
        }
        let mut copy = self.clone();
        copy.substitute_children(substitute);
        copy
    }

    fn substitute_children(&mut self, substitute: &mut impl FnMut(&Expr) -> Option<Expr>) {
        for child in self.children_mut() {
            match substitute(child) {
                Some(replaced) => *child = replaced,
                None => child.substitute_children(substitute),
            }
        }
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

/// Takes `node` out of its place, leaving a NULL literal.
pub(crate) fn take(node: &mut Expr) -> Expr {
    std::mem::replace(node, Expr::Literal(Literal::null()))
}

#[cfg(test)]
#[path = "expr_tests.rs"]
mod tests;
