//! The adapter request Exasol sends for a pushdown, read once into plain data. [`parse_request`]
//! is the only reader of the request JSON: which key holds a clause, that a JSON-null clause is an
//! absent one, and which field a malformed request lacks are decided here and in the expression
//! parser it calls, so no caller meets the JSON.

use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use crate::exasol_type::ExasolType;
use crate::expr::Expr;
use crate::parse::RequestNode;

/// The request as read: the tables the pushed query involves and its clauses.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedRequest {
    pub involved_tables: Vec<InvolvedTable>,
    pub pushdown: ParsedPushdown,
}

/// One `involvedTables` entry: a virtual table the pushed query reads.
#[derive(Debug, Clone, PartialEq)]
pub struct InvolvedTable {
    pub name: String,
    /// The table's identifier note, as Exasol sent it.
    pub adapter_notes: Option<String>,
    pub columns: Vec<InvolvedColumn>,
}

/// One declared column of an involved table.
#[derive(Debug, Clone, PartialEq)]
pub struct InvolvedColumn {
    pub name: String,
    pub data_type: ExasolType,
    /// The column's declaration note, as Exasol sent it.
    pub adapter_notes: Option<String>,
}

/// The `pushdownRequest` object, each clause as Exasol sent it. An absent clause and a JSON-null
/// one read alike.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedPushdown {
    pub from: Option<FromClause>,
    /// `None` for an absent `selectList`, which an empty one is not.
    pub select_list: Option<Vec<Expr>>,
    /// `None` for an absent `selectListDataTypes`.
    pub select_types: Option<Vec<ExasolType>>,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<SortElement>,
    pub limit: Option<u64>,
    /// Exasol normalises `OFFSET 0` away, so an absent key and zero are the same request.
    pub offset: u64,
    pub aggregation_type: Option<AggregationType>,
}

/// Exasol's `aggregationType`, which it sets on every request that aggregates, including one whose
/// select list it stripped of every aggregate (#627).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationType {
    SingleGroup,
    GroupBy,
}

/// The `FROM` tree: a table, or a join over its two sides.
#[derive(Debug, Clone, PartialEq)]
pub enum FromClause {
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
    /// The join conditions in the order SQL writes them: those of a join's sides before its own.
    pub fn conditions(&self) -> Vec<&Expr> {
        match self {
            Self::Table { .. } => Vec::new(),
            Self::Join {
                left,
                right,
                condition,
                ..
            } => left
                .conditions()
                .into_iter()
                .chain(right.conditions())
                .chain([condition])
                .collect(),
        }
    }

    /// `rewrite` applied to each join condition, in the order of [`Self::conditions`].
    pub fn try_map_conditions<E>(
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
            left.try_map_conditions(rewrite)?;
            right.try_map_conditions(rewrite)?;
            rewrite(condition)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

/// One `orderBy` element.
#[derive(Debug, Clone, PartialEq)]
pub struct SortElement {
    pub expression: Expr,
    pub ascending: bool,
    pub nulls_last: bool,
}

/// The adapter request of a pushdown. A malformed field fails with an error that names it.
pub fn parse_request(request: &Json) -> Result<ParsedRequest, UdfError> {
    let request = RequestNode::object(request, "")?;
    let pushdown = pushdown(&request.root("pushdownRequest")?)?;
    let involved_tables = request
        .objects("involvedTables")?
        .iter()
        .map(involved_table)
        .collect::<Result<_, _>>()?;
    Ok(ParsedRequest {
        involved_tables,
        pushdown,
    })
}

fn pushdown(node: &RequestNode) -> Result<ParsedPushdown, UdfError> {
    let limit = node.optional_field("limit")?;
    let limit_field = |key| limit.as_ref().map(|limit| limit.whole_number(key));
    Ok(ParsedPushdown {
        from: node
            .optional_field("from")?
            .map(|from| from_clause(&from))
            .transpose()?,
        select_list: node.optional_list("selectList")?,
        select_types: node.optional_data_types("selectListDataTypes")?,
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
        aggregation_type: aggregation_type(node)?,
    })
}

fn aggregation_type(node: &RequestNode) -> Result<Option<AggregationType>, UdfError> {
    Ok(match node.optional_text("aggregationType")? {
        None => None,
        Some("single_group") => Some(AggregationType::SingleGroup),
        Some("group_by") => Some(AggregationType::GroupBy),
        Some(other) => {
            return Err(node.invalid(
                "aggregationType",
                &format!("names the aggregation type {other}"),
            ));
        }
    })
}

fn from_clause(node: &RequestNode) -> Result<FromClause, UdfError> {
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
            let side = |key| from_clause(&node.field(key)?).map(Box::new);
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

fn sort_element(element: &RequestNode) -> Result<SortElement, UdfError> {
    Ok(SortElement {
        expression: element.child("expression")?,
        ascending: element.flag("isAscending")?,
        nulls_last: element.flag("nullsLast")?,
    })
}

fn involved_table(table: &RequestNode) -> Result<InvolvedTable, UdfError> {
    Ok(InvolvedTable {
        name: table.text("name")?.to_string(),
        adapter_notes: note(table)?,
        columns: table
            .objects("columns")?
            .iter()
            .map(involved_column)
            .collect::<Result<_, _>>()?,
    })
}

/// A missing or invalid `dataType` fails the request: the adapter declared every column itself, so
/// no defaulted type may stand in for it.
fn involved_column(column: &RequestNode) -> Result<InvolvedColumn, UdfError> {
    Ok(InvolvedColumn {
        name: column.text("name")?.to_string(),
        data_type: column.data_type("dataType")?,
        adapter_notes: note(column)?,
    })
}

fn note(node: &RequestNode) -> Result<Option<String>, UdfError> {
    Ok(node.optional_text("adapterNotes")?.map(str::to_string))
}

#[cfg(test)]
#[path = "request_tests.rs"]
mod tests;
