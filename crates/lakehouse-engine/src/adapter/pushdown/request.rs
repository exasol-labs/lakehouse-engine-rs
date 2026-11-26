//! The planner's model of the adapter request Exasol sends for a pushdown. `vs-expression` reads
//! the request JSON once into plain data, and this module checks that data against what Exasol
//! validates in the result, so planning never re-reads a key and never panics on a malformed request.

use crate::types::exasol_type::ExasolType;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
pub(super) use vs_expression::AggregationType;
use vs_expression::{Expr, Literal, ParsedPushdown, ParsedRequest, parse_request};
pub(crate) use vs_expression::{FromClause, InvolvedTable, JoinKind, SortElement};

/// The pushdown request with the tables it involves.
#[derive(Debug, Clone)]
pub(crate) struct AdapterRequest {
    involved_tables: Vec<InvolvedTable>,
    pushdown: PushdownRequest,
}

impl AdapterRequest {
    pub(crate) fn parse(request: &Json) -> Result<Self, UdfError> {
        let ParsedRequest {
            involved_tables,
            pushdown,
        } = parse_request(request)?;
        Ok(Self {
            involved_tables,
            pushdown: PushdownRequest::checked(pushdown)?,
        })
    }

    pub(crate) fn pushdown(&self) -> &PushdownRequest {
        &self.pushdown
    }

    pub(crate) fn involved_tables(&self) -> &[InvolvedTable] {
        &self.involved_tables
    }

    /// The involved table named `name`, as a join leaf or the FROM table names it.
    pub(crate) fn involved_table(&self, name: &str) -> Option<&InvolvedTable> {
        self.involved_tables.iter().find(|table| table.name == name)
    }
}

/// A clause of the request that can hold an expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Clause {
    SelectList,
    From,
    Filter,
    GroupBy,
    Having,
    OrderBy,
}

impl Clause {
    /// In the order SQL writes the clauses, which is the order of the emitted columns and
    /// aggregate slots.
    pub(super) const ALL: [Self; 6] = [
        Self::SelectList,
        Self::From,
        Self::Filter,
        Self::GroupBy,
        Self::Having,
        Self::OrderBy,
    ];

    /// The clauses an Exasol wrapper renders over a fan-out, its WHERE aside.
    pub(super) const WRAPPER: [Self; 4] =
        [Self::SelectList, Self::GroupBy, Self::OrderBy, Self::Having];
}

/// What Exasol validates positionally in the result of the pushed query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum OutputContract {
    /// `selectList` absent or JSON null, Exasol's `SELECT *`: every declared column, in order.
    WholeRow,
    /// `selectList` `[]`: Exasol needs no column and accepts exactly one, whose value it ignores.
    AnyOneColumn,
    /// One output column per `selectList` item.
    Items(Vec<Expr>),
}

/// [`AdapterRequest::parse`] is the only way to one, so every holder has passed the strict
/// checks, and no consumer re-reads `selectList` or `selectListDataTypes`.
///
/// Its `orderBy` holds no constant key that Exasol pushed, because a constant does not change the order
/// and Exasol reads a bare number there as a select-list position. An `orderBy` of constants only with
/// a non-zero offset holds one `FALSE` key instead.
#[derive(Debug, Clone)]
pub struct PushdownRequest {
    from: Option<FromClause>,
    select_types: Vec<ExasolType>,
    output: OutputContract,
    filter: Option<Expr>,
    group_by: Vec<Expr>,
    having: Option<Expr>,
    order_by: Vec<SortElement>,
    limit: Option<u64>,
    offset: u64,
    aggregation_type: Option<AggregationType>,
}

impl PushdownRequest {
    /// Exasol sends an aggregating request at least a `literal_null` item (#627, verified live),
    /// so `[]` beside `aggregationType` fails like any other malformed select list.
    fn checked(parsed: ParsedPushdown) -> Result<Self, UdfError> {
        let select_types = parsed.select_types.ok_or_else(|| {
            UdfError::User(
                "the pushdown request carries no selectListDataTypes array; the adapter needs \
                 Exasol 8.29 or later"
                    .into(),
            )
        })?;
        let output = match parsed.select_list {
            None => OutputContract::WholeRow,
            Some(items) if items.is_empty() => {
                if parsed.aggregation_type.is_some() {
                    return Err(UdfError::User(
                        "the pushdown request aggregates under an empty selectList".into(),
                    ));
                }
                OutputContract::AnyOneColumn
            }
            Some(items) if items.len() != select_types.len() => {
                return Err(UdfError::User(format!(
                    "the pushdown request declares {} selectListDataTypes entries for {} selectList items",
                    select_types.len(),
                    items.len()
                )));
            }
            Some(items) => OutputContract::Items(items),
        };
        let mut request = Self {
            from: parsed.from,
            select_types,
            output,
            filter: parsed.filter,
            group_by: parsed.group_by,
            having: parsed.having,
            order_by: parsed.order_by,
            limit: parsed.limit,
            offset: parsed.offset,
            aggregation_type: parsed.aggregation_type,
        };
        request.drop_constant_sort_keys();
        Ok(request)
    }

    pub(super) fn output(&self) -> &OutputContract {
        &self.output
    }

    /// The select-list items, `None` unless the contract is [`OutputContract::Items`].
    pub(super) fn select_items(&self) -> Option<&[Expr]> {
        match &self.output {
            OutputContract::Items(items) => Some(items),
            OutputContract::WholeRow | OutputContract::AnyOneColumn => None,
        }
    }

    /// `row`, the declared columns a `SELECT *` returns. Exasol validates them positionally
    /// against `selectListDataTypes`, so another width is an adapter bug.
    pub(super) fn whole_row<T>(&self, row: Vec<T>) -> Result<Vec<T>, UdfError> {
        if row.len() != self.select_types.len() {
            return Err(UdfError::User(format!(
                "internal adapter error: the pushdown query returns {} column(s), Exasol expects {}",
                row.len(),
                self.select_types.len()
            )));
        }
        Ok(row)
    }

    /// One type per select-list item, or per table column for `SELECT *`.
    pub fn select_types(&self) -> &[ExasolType] {
        &self.select_types
    }

    /// `None` only for an index past the list, which the count check rules out for every
    /// select-list item; the consumer then declines rather than guess a type.
    pub fn select_type(&self, select_index: usize) -> Option<ExasolType> {
        self.select_types.get(select_index).copied()
    }

    /// [`Self::select_type`] when a scan UDF can emit it. `None` means the scan has no value of
    /// the item's type, so Exasol must compute the item over the scan output.
    pub fn emittable_select_type(&self, select_index: usize) -> Option<ExasolType> {
        self.select_type(select_index)
            .filter(ExasolType::is_udf_emittable)
    }

    pub(super) fn from(&self) -> Option<&FromClause> {
        self.from.as_ref()
    }

    /// The whole WHERE as Exasol sent it. Which of its conjuncts a scan applies and which an
    /// Exasol wrapper applies is a `WherePlacement`'s answer, never this tree's.
    pub(super) fn filter(&self) -> Option<&Expr> {
        self.filter.as_ref()
    }

    pub(super) fn group_by(&self) -> &[Expr] {
        &self.group_by
    }

    pub(super) fn having(&self) -> Option<&Expr> {
        self.having.as_ref()
    }

    pub(super) fn order_by(&self) -> &[SortElement] {
        &self.order_by
    }

    pub(super) fn limit(&self) -> Option<u64> {
        self.limit
    }

    /// Exasol normalises `OFFSET 0` away, so an absent key and zero are the same request.
    pub(super) fn offset(&self) -> u64 {
        self.offset
    }

    pub(super) fn aggregation_type(&self) -> Option<AggregationType> {
        self.aggregation_type
    }

    /// Every expression of the `which` clauses, clause by clause in `which` order.
    pub(super) fn expressions(&self, which: &[Clause]) -> Vec<&Expr> {
        let mut trees = Vec::new();
        for &clause in which {
            match clause {
                Clause::Filter => trees.extend(&self.filter),
                Clause::From => trees.extend(self.from.iter().flat_map(FromClause::conditions)),
                Clause::GroupBy => trees.extend(&self.group_by),
                Clause::Having => trees.extend(&self.having),
                Clause::OrderBy => trees.extend(self.order_by.iter().map(|e| &e.expression)),
                Clause::SelectList => trees.extend(self.select_items().into_iter().flatten()),
            }
        }
        trees
    }

    /// This request with `rewrite` applied to each expression of the `which` clauses, its select
    /// list and checked types kept.
    pub(super) fn map_expressions(
        &self,
        which: &[Clause],
        mut rewrite: impl FnMut(&Expr) -> Expr,
    ) -> Self {
        let Ok(rewritten) = self.try_map_expressions(which, |_, _, tree| {
            *tree = rewrite(tree);
            Ok::<_, std::convert::Infallible>(())
        });
        rewritten
    }

    /// [`Self::map_expressions`] with an in-place rewrite that can fail and is told which clause
    /// it rewrites and the tree's index in that clause.
    pub(super) fn try_map_expressions<E>(
        &self,
        which: &[Clause],
        mut rewrite: impl FnMut(Clause, usize, &mut Expr) -> Result<(), E>,
    ) -> Result<Self, E> {
        let mut request = self.clone();
        for &clause in which {
            let mut index = 0;
            let mut each = |tree: &mut Expr| {
                let result = rewrite(clause, index, tree);
                index += 1;
                result
            };
            match clause {
                Clause::Filter => request.filter.iter_mut().try_for_each(&mut each)?,
                Clause::From => {
                    if let Some(from) = &mut request.from {
                        from.try_map_conditions(&mut each)?;
                    }
                }
                Clause::GroupBy => request.group_by.iter_mut().try_for_each(&mut each)?,
                Clause::Having => request.having.iter_mut().try_for_each(&mut each)?,
                Clause::OrderBy => request
                    .order_by
                    .iter_mut()
                    .try_for_each(|element| each(&mut element.expression))?,
                Clause::SelectList => {
                    if let OutputContract::Items(items) = &mut request.output {
                        items.iter_mut().try_for_each(&mut each)?;
                    }
                }
            }
        }
        Ok(request)
    }

    /// This request with `from` and `filter` in place of its own.
    pub(super) fn with_from_and_filter(&self, from: FromClause, filter: Option<Expr>) -> Self {
        Self {
            from: Some(from),
            filter,
            ..self.clone()
        }
    }

    /// Exasol rejects `OFFSET` without `ORDER BY` but accepts `ORDER BY FALSE`, which no
    /// select-list position can name, so a windowed all-constant `orderBy` keeps one `FALSE` key.
    /// Only the clock fold turns a sort key constant after the parse, so it drops them again.
    pub(super) fn drop_constant_sort_keys(&mut self) {
        let Some(first) = self.order_by.first().cloned() else {
            return;
        };
        self.order_by
            .retain(|element| !matches!(element.expression, Expr::Literal(_)));
        if self.order_by.is_empty() && self.offset != 0 {
            self.order_by.push(SortElement {
                expression: Expr::Literal(Literal::Bool(false)),
                ..first
            });
        }
    }
}

#[cfg(test)]
#[path = "request_tests.rs"]
mod tests;
