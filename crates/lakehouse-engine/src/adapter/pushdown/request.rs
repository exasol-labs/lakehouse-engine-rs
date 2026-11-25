//! The adapter request Exasol sends for a pushdown, parsed once into a typed model. This module is
//! the only reader of the request JSON: which key holds a clause, that a JSON-null clause is an
//! absent one, and which field a malformed request lacks are decided here, so planning never
//! re-reads a key and never panics on a malformed request.

use crate::types::exasol_type::{ExasolType, exasol_type_from_json};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use vs_expression::{Expr, Literal, RequestNode, invalid_field};

/// One `involvedTables` entry: a virtual table the pushed query reads.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InvolvedTable {
    pub(crate) name: String,
    /// The table's identifier note, as Exasol sent it.
    pub(crate) adapter_notes: Option<String>,
    pub(crate) columns: Vec<InvolvedColumn>,
}

/// One declared column of an involved table.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InvolvedColumn {
    pub(crate) name: String,
    pub(crate) data_type: ExasolType,
    /// The column's declaration note, as Exasol sent it.
    pub(crate) adapter_notes: Option<String>,
}

/// The pushdown request with the tables it involves.
#[derive(Debug, Clone)]
pub(crate) struct AdapterRequest {
    involved_tables: Vec<InvolvedTable>,
    pushdown: PushdownRequest,
}

impl AdapterRequest {
    pub(crate) fn parse(request: &Json) -> Result<Self, UdfError> {
        let pushdown =
            PushdownRequest::parse(request.get("pushdownRequest").unwrap_or(&Json::Null))?;
        let involved_tables = RequestNode::object(request, "")?
            .objects("involvedTables")?
            .iter()
            .map(involved_table)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            involved_tables,
            pushdown,
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

fn involved_table(table: &RequestNode) -> Result<InvolvedTable, UdfError> {
    Ok(InvolvedTable {
        name: table.text("name")?.to_string(),
        adapter_notes: note(table),
        columns: table
            .objects("columns")?
            .iter()
            .filter_map(involved_column)
            .collect::<Result<_, _>>()?,
    })
}

/// `None` for a column without a name, which no clause can reference. A missing or invalid
/// `dataType` fails the request: the adapter declared every column itself, so no defaulted type
/// may stand in for it.
fn involved_column(column: &RequestNode) -> Option<Result<InvolvedColumn, UdfError>> {
    let name = column.get("name")?.as_str()?;
    let parsed = column
        .get("dataType")
        .ok_or_else(|| UdfError::User(format!("involved column {name} declares no dataType")))
        .and_then(exasol_type_from_json)
        .map(|data_type| InvolvedColumn {
            name: name.to_string(),
            data_type,
            adapter_notes: note(column),
        });
    Some(parsed)
}

fn note(node: &RequestNode) -> Option<String> {
    node.get("adapterNotes")
        .and_then(Json::as_str)
        .map(str::to_string)
}

/// A clause of the request that can hold an expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Clause {
    Filter,
    From,
    GroupBy,
    Having,
    OrderBy,
    SelectList,
}

impl Clause {
    /// In the alphabetical order of the request's wire keys, the order the golden fixtures pin for
    /// the emitted columns and aggregate slots.
    pub(super) const ALL: [Self; 6] = [
        Self::Filter,
        Self::From,
        Self::GroupBy,
        Self::Having,
        Self::OrderBy,
        Self::SelectList,
    ];

    /// The clauses an Exasol wrapper renders over a fan-out, its WHERE aside.
    pub(super) const WRAPPER: [Self; 4] =
        [Self::SelectList, Self::GroupBy, Self::OrderBy, Self::Having];
}

/// Exasol's `aggregationType`, which it sets on every request that aggregates, including one whose
/// select list it stripped of every aggregate (#627).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AggregationType {
    SingleGroup,
    GroupBy,
}

/// What Exasol validates positionally in the result of the pushed query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OutputContract {
    /// `selectList` absent or JSON null, Exasol's `SELECT *`: every declared column, in order.
    WholeRow,
    /// `selectList` `[]`: Exasol needs no column and accepts exactly one, whose value it ignores.
    AnyOneColumn,
    /// One output column per `selectList` item.
    Items,
}

/// The `FROM` tree: a table, or a join over its two sides.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FromClause {
    Table {
        name: String,
        alias: Option<String>,
    },
    Join {
        kind: JoinKind,
        left: Box<FromClause>,
        right: Box<FromClause>,
        condition: Expr,
    },
}

impl FromClause {
    /// The join conditions in the alphabetical order of the join's wire keys: a node's
    /// `condition` before its `left` and `right` sides.
    fn conditions(&self) -> Vec<&Expr> {
        match self {
            Self::Table { .. } => Vec::new(),
            Self::Join {
                left,
                right,
                condition,
                ..
            } => std::iter::once(condition)
                .chain(left.conditions())
                .chain(right.conditions())
                .collect(),
        }
    }

    fn try_map_conditions<E>(
        &mut self,
        rewrite: &mut impl FnMut(&mut Expr) -> Result<(), E>,
    ) -> Result<(), E> {
        if let Self::Join {
            left,
            right,
            condition,
            ..
        } = self
        {
            rewrite(condition)?;
            left.try_map_conditions(rewrite)?;
            right.try_map_conditions(rewrite)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

/// One `orderBy` element.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SortElement {
    pub(crate) expression: Expr,
    pub(crate) ascending: bool,
    pub(crate) nulls_last: bool,
}

/// [`PushdownRequest::parse`] is the only constructor, so every holder has passed the strict
/// checks, and no consumer re-reads `selectList` or `selectListDataTypes`.
///
/// Its `orderBy` holds no constant key that Exasol pushed, because a constant does not change the order
/// and Exasol reads a bare number there as a select-list position. An `orderBy` of constants only with
/// a non-zero offset holds one `FALSE` key instead.
#[derive(Debug, Clone)]
pub struct PushdownRequest {
    from: Option<FromClause>,
    select_list: Vec<Expr>,
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
    pub fn parse(json: &Json) -> Result<Self, UdfError> {
        let entries = json
            .get("selectListDataTypes")
            .and_then(Json::as_array)
            .ok_or_else(|| {
                UdfError::User(
                    "the pushdown request carries no selectListDataTypes array; the adapter \
                     needs Exasol 8.29 or later"
                        .into(),
                )
            })?;
        let node = RequestNode::object(json, "")?;
        let select_types = entries
            .iter()
            .enumerate()
            .map(|(i, entry)| {
                exasol_type_from_json(entry).map_err(|error| {
                    invalid_field(
                        &format!("selectListDataTypes[{i}]"),
                        &format!("is invalid: {error}"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let aggregation_type = match node.optional_text("aggregationType")? {
            None => None,
            Some("single_group") => Some(AggregationType::SingleGroup),
            Some("group_by") => Some(AggregationType::GroupBy),
            Some(other) => {
                return Err(node.invalid(
                    "aggregationType",
                    &format!("names the aggregation type {other}"),
                ));
            }
        };
        let (output, select_list) = match node.get("selectList") {
            None => (OutputContract::WholeRow, Vec::new()),
            Some(Json::Array(items)) if items.is_empty() => {
                if aggregation_type.is_some() {
                    return Err(UdfError::User(
                        "the pushdown request aggregates under an empty selectList".into(),
                    ));
                }
                (OutputContract::AnyOneColumn, Vec::new())
            }
            Some(Json::Array(items)) if items.len() != select_types.len() => {
                return Err(UdfError::User(format!(
                    "the pushdown request declares {} selectListDataTypes entries for {} selectList items",
                    select_types.len(),
                    items.len()
                )));
            }
            Some(_) => (OutputContract::Items, node.list("selectList")?),
        };
        let limit = node.optional_field("limit")?;
        let limit_field = |key| limit.as_ref().map(|limit| limit.whole_number(key));
        let mut request = Self {
            from: node
                .optional_field("from")?
                .map(|from| parse_from(&from))
                .transpose()?,
            select_list,
            select_types,
            output,
            filter: node.optional_child("filter")?,
            group_by: node.list("groupBy")?,
            having: node.optional_child("having")?,
            order_by: node
                .objects("orderBy")?
                .iter()
                .map(sort_element)
                .collect::<Result<_, _>>()?,
            limit: limit_field("numElements").transpose()?.flatten(),
            offset: limit_field("offset").transpose()?.flatten().unwrap_or(0),
            aggregation_type,
        };
        request.drop_constant_sort_keys();
        Ok(request)
    }

    pub(super) fn output(&self) -> OutputContract {
        self.output
    }

    /// The select-list items, `None` unless the contract is [`OutputContract::Items`].
    pub(super) fn select_items(&self) -> Option<&[Expr]> {
        match self.output {
            OutputContract::Items => Some(&self.select_list),
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
                Clause::SelectList => trees.extend(&self.select_list),
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
                Clause::SelectList => request.select_list.iter_mut().try_for_each(&mut each)?,
            }
        }
        request.drop_constant_sort_keys();
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
    /// A rewrite can turn a sort key constant, such as a folded clock function, so every rewrite
    /// drops them again.
    fn drop_constant_sort_keys(&mut self) {
        let Some(first) = self.order_by.first().cloned() else {
            return;
        };
        self.order_by
            .retain(|element| !matches!(element.expression, Expr::Literal(_)));
        if self.order_by.is_empty() && self.offset != 0 {
            self.order_by.push(SortElement {
                expression: Expr::Literal(Literal::bool(false)),
                ..first
            });
        }
    }
}

fn sort_element(element: &RequestNode) -> Result<SortElement, UdfError> {
    Ok(SortElement {
        expression: element.child("expression")?,
        ascending: element.flag("isAscending")?,
        nulls_last: element.flag("nullsLast")?,
    })
}

fn parse_from(node: &RequestNode) -> Result<FromClause, UdfError> {
    match node.text("type")? {
        "table" => Ok(FromClause::Table {
            name: node.text("name")?.to_string(),
            alias: node.optional_text("alias")?.map(str::to_string),
        }),
        "join" => {
            let kind = match node.text("join_type")? {
                "inner" => JoinKind::Inner,
                "left_outer" => JoinKind::LeftOuter,
                "right_outer" => JoinKind::RightOuter,
                "full_outer" => JoinKind::FullOuter,
                other => {
                    return Err(node.invalid(
                        "join_type",
                        &format!("names the join type {other}, which the adapter does not plan"),
                    ));
                }
            };
            let side = |key| parse_from(&node.field(key)?).map(Box::new);
            Ok(FromClause::Join {
                kind,
                left: side("left")?,
                right: side("right")?,
                condition: node.child("condition")?,
            })
        }
        other => Err(node.invalid(
            "type",
            &format!("names the FROM node type {other}, which the adapter does not plan"),
        )),
    }
}

#[cfg(test)]
#[path = "request_tests.rs"]
mod tests;
