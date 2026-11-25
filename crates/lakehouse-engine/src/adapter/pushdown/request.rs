//! The `pushdownRequest` object Exasol sends. Planning reads and rewrites its clauses only through
//! [`PushdownRequest`], so which key holds a clause, and that a JSON-null clause is an absent one,
//! is decided here once.

use crate::types::exasol_type::{ExasolType, exasol_type_from_json};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

/// A clause of the request that can hold a `column` node.
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
    /// In the request object's key order, so a walk over them meets columns as a walk over the
    /// whole object did.
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

    fn key(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::From => "from",
            Self::GroupBy => "groupBy",
            Self::Having => "having",
            Self::OrderBy => "orderBy",
            Self::SelectList => "selectList",
        }
    }
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

/// [`PushdownRequest::parse`] is the only constructor, so every holder has passed the strict
/// checks, and no consumer re-reads `selectList` or `selectListDataTypes`.
///
/// Its `orderBy` holds no constant key that Exasol pushed, because a constant does not change the order
/// and Exasol reads a bare number there as a select-list position. An `orderBy` of constants only with
/// a non-zero offset holds one `FALSE` key instead.
#[derive(Debug, Clone)]
pub struct PushdownRequest {
    json: Json,
    select_types: Vec<ExasolType>,
    output: OutputContract,
}

impl PushdownRequest {
    /// Exasol sends an aggregating request at least a `literal_null` item (#627, verified live),
    /// so `[]` beside `aggregationType` fails like any other malformed select list.
    pub fn parse(mut json: Json) -> Result<Self, UdfError> {
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
        let select_types = entries
            .iter()
            .map(exasol_type_from_json)
            .collect::<Result<Vec<_>, _>>()?;
        let output = match json.get("selectList") {
            None | Some(Json::Null) => OutputContract::WholeRow,
            Some(Json::Array(items)) if items.is_empty() => {
                if aggregation_type(&json).is_some() {
                    return Err(UdfError::User(
                        "the pushdown request aggregates under an empty selectList".into(),
                    ));
                }
                OutputContract::AnyOneColumn
            }
            Some(Json::Array(items)) => {
                if items.len() != select_types.len() {
                    return Err(UdfError::User(format!(
                        "the pushdown request declares {} selectListDataTypes entries for {} selectList items",
                        select_types.len(),
                        items.len()
                    )));
                }
                OutputContract::Items
            }
            Some(_) => {
                return Err(UdfError::User(
                    "the pushdown request's selectList is neither an array nor absent".into(),
                ));
            }
        };
        drop_constant_sort_keys(&mut json);
        Ok(Self {
            json,
            select_types,
            output,
        })
    }

    pub(super) fn output(&self) -> OutputContract {
        self.output
    }

    /// The select-list items, `None` unless the contract is [`OutputContract::Items`].
    pub(super) fn select_items(&self) -> Option<&[Json]> {
        match self.output {
            OutputContract::Items => self
                .clause(Clause::SelectList)
                .and_then(Json::as_array)
                .map(Vec::as_slice),
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

    /// The `FROM` tree: a `table` node, or a `join` node over its legs.
    pub(super) fn from(&self) -> Option<&Json> {
        self.clause(Clause::From)
    }

    /// The whole WHERE as Exasol sent it. Which of its conjuncts a scan applies and which an
    /// Exasol wrapper applies is a `WherePlacement`'s answer, never this tree's.
    pub(super) fn filter(&self) -> Option<&Json> {
        self.clause(Clause::Filter)
    }

    pub(super) fn group_by(&self) -> &[Json] {
        self.array(Clause::GroupBy)
    }

    pub(super) fn having(&self) -> Option<&Json> {
        self.clause(Clause::Having)
    }

    /// The sort elements, each an `expression` beside its direction flags.
    pub(super) fn order_by(&self) -> &[Json] {
        self.array(Clause::OrderBy)
    }

    pub(super) fn limit(&self) -> Option<u64> {
        limit_field(&self.json, "numElements")
    }

    /// Exasol normalises `OFFSET 0` away, so an absent key and zero are the same request.
    pub(super) fn offset(&self) -> u64 {
        limit_field(&self.json, "offset").unwrap_or(0)
    }

    pub(super) fn aggregation_type(&self) -> Option<AggregationType> {
        aggregation_type(&self.json)
    }

    /// The values of the `which` clauses that the request carries.
    pub(super) fn clauses<'a>(&'a self, which: &'a [Clause]) -> impl Iterator<Item = &'a Json> {
        which.iter().filter_map(|&clause| self.clause(clause))
    }

    /// This request with `rewrite` applied to each of the `which` clauses it carries, its select
    /// list and checked types kept.
    pub(super) fn map_clauses(
        &self,
        which: &[Clause],
        mut rewrite: impl FnMut(&Json) -> Json,
    ) -> Self {
        let Ok(rewritten) = self.try_map_clauses(which, |_, value| {
            *value = rewrite(value);
            Ok::<_, std::convert::Infallible>(())
        });
        rewritten
    }

    /// [`Self::map_clauses`] with an in-place rewrite that can fail and is told which clause it
    /// rewrites.
    pub(super) fn try_map_clauses<E>(
        &self,
        which: &[Clause],
        mut rewrite: impl FnMut(Clause, &mut Json) -> Result<(), E>,
    ) -> Result<Self, E> {
        let mut json = self.json.clone();
        for &clause in which {
            if let Some(value) = json.get_mut(clause.key()).filter(|value| !value.is_null()) {
                rewrite(clause, value)?;
            }
        }
        Ok(self.rebuilt(json))
    }

    /// This request with `clause` replaced by `value`, its select list and checked types kept.
    pub(super) fn with_clause(&self, clause: Clause, value: Json) -> Self {
        let mut json = self.json.clone();
        json[clause.key()] = value;
        self.rebuilt(json)
    }

    fn clause(&self, clause: Clause) -> Option<&Json> {
        self.json.get(clause.key()).filter(|value| !value.is_null())
    }

    fn array(&self, clause: Clause) -> &[Json] {
        self.clause(clause)
            .and_then(Json::as_array)
            .map_or(&[], Vec::as_slice)
    }

    /// A rewrite can turn a sort key constant, such as a folded clock function, so the drop runs
    /// on every rewrite.
    fn rebuilt(&self, mut json: Json) -> Self {
        debug_assert!(
            self.output != OutputContract::Items
                || json
                    .get(Clause::SelectList.key())
                    .and_then(Json::as_array)
                    .is_some_and(|items| items.len() == self.select_types.len()),
            "a clause rewrite must keep the select list's checked shape"
        );
        drop_constant_sort_keys(&mut json);
        Self {
            json,
            select_types: self.select_types.clone(),
            output: self.output,
        }
    }
}

fn limit_field(json: &Json, field: &str) -> Option<u64> {
    json.get("limit")
        .and_then(|limit| limit.get(field))
        .and_then(Json::as_u64)
}

fn aggregation_type(json: &Json) -> Option<AggregationType> {
    match json.get("aggregationType").and_then(Json::as_str)? {
        "single_group" => Some(AggregationType::SingleGroup),
        "group_by" => Some(AggregationType::GroupBy),
        _ => None,
    }
}

/// Exasol rejects `OFFSET` without `ORDER BY` but accepts `ORDER BY FALSE`, which no select-list
/// position can name, so a windowed all-constant `orderBy` keeps one `FALSE` key.
fn drop_constant_sort_keys(json: &mut Json) {
    let offset = limit_field(json, "offset").unwrap_or(0);
    let Some(keys) = json.get_mut("orderBy").and_then(Json::as_array_mut) else {
        return;
    };
    let Some(first) = keys.first().cloned() else {
        return;
    };
    keys.retain(|key| !is_constant_sort_key(key));
    if !keys.is_empty() {
        return;
    }
    if offset == 0 {
        if let Some(request) = json.as_object_mut() {
            request.remove("orderBy");
        }
        return;
    }
    let mut false_key = first;
    false_key["expression"] = serde_json::json!({"type": "literal_bool", "value": false});
    keys.push(false_key);
}

/// Every `literal_*` node is constant, including types the select list cannot project.
fn is_constant_sort_key(key: &Json) -> bool {
    key.get("expression")
        .and_then(|expression| expression.get("type"))
        .and_then(Json::as_str)
        .is_some_and(|node_type| node_type.starts_with("literal_"))
}

#[cfg(test)]
#[path = "request_tests.rs"]
mod tests;
