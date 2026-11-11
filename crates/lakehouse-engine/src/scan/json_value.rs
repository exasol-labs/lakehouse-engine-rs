//! `JSON_VALUE` in the scan (#574): each document parses once into a `serde_json::Value`, also
//! when several calls read it, paths run in lax mode over item sequences, and the result converts
//! to the `RETURNING` type. docs/semantic-differences.md lists what this parse changes.

use arrow::array::{Array, ArrayRef, AsArray, BooleanBuilder, StringBuilder, StructArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::ScalarValue;
use datafusion::common::alias::AliasGenerator;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Aggregate, ColumnarValue, Expr, Filter, LogicalPlan, Projection, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility, col, lit,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use vs_expression::{JSON_VALUE_BOOLEAN_FN, JSON_VALUE_TEXT_FN, JsonPathStep, parse_json_path};

/// The call that evaluates every `JSON_VALUE` of one plan node over one document.
const JSON_VALUES_FN: &str = "lakehouse_json_values";
/// One call's field of a [`JSON_VALUES_FN`] result.
const JSON_FIELD_FN: &str = "lakehouse_json_field";

/// What one path selects in one document.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Item<'a> {
    /// No item: `ON EMPTY` applies.
    Empty,
    /// Not JSON, several items, or a non-scalar item: `ON ERROR` applies.
    Error,
    Null,
    /// A string's text, or the text of a number, `true`, or `false`.
    Text(Cow<'a, str>),
}

/// `None` for a document that is not strict JSON.
fn parse(document: &str) -> Option<Value> {
    serde_json::from_str(document).ok()
}

/// Lax mode: a member step looks into the objects of an array, an index step treats a non-array
/// as a one-item array, and a structural mismatch selects nothing.
fn select<'a>(root: Option<&'a Value>, path: &[JsonPathStep]) -> Item<'a> {
    let Some(root) = root else {
        return Item::Error;
    };
    let mut items = vec![root];
    for step in path {
        items = items
            .into_iter()
            .flat_map(|item| match (step, item) {
                (JsonPathStep::Member(name), Value::Object(members)) => {
                    members.get(name).into_iter().collect()
                }
                (JsonPathStep::Member(name), Value::Array(elements)) => elements
                    .iter()
                    .filter_map(|element| element.as_object()?.get(name))
                    .collect(),
                (JsonPathStep::Index(index), Value::Array(elements)) => {
                    elements.get(*index as usize).into_iter().collect()
                }
                (JsonPathStep::Index(0), item) => vec![item],
                _ => Vec::new(),
            })
            .collect();
    }
    match items.as_slice() {
        [] => Item::Empty,
        [Value::Null] => Item::Null,
        [Value::String(text)] => Item::Text(Cow::Borrowed(text)),
        [Value::Bool(value)] => Item::Text(Cow::Borrowed(if *value { "true" } else { "false" })),
        [Value::Number(number)] => Item::Text(Cow::Owned(number.to_string())),
        _ => Item::Error,
    }
}

/// Exasol's text to BOOLEAN cast: spaces trimmed, case ignored, and digits read as a number.
fn boolean_from_text(text: &str) -> Option<bool> {
    let text = text.trim_matches(' ');
    if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
        return match text.trim_start_matches('0') {
            "" => Some(false),
            "1" => Some(true),
            _ => None,
        };
    }
    match text.to_ascii_uppercase().as_str() {
        "TRUE" | "T" | "YES" | "Y" => Some(true),
        "FALSE" | "F" | "NO" | "N" => Some(false),
        _ => None,
    }
}

/// `Err` means `ON ERROR` applies. Exasol reads the empty string as NULL.
fn text_value(text: &str, max_chars: usize) -> std::result::Result<Option<&str>, ()> {
    match text {
        "" => Ok(None),
        text if text.chars().count() > max_chars => Err(()),
        text => Ok(Some(text)),
    }
}

/// A NULL document or item gives NULL, and a failed conversion gives `on_error`.
fn resolve<'i, T: Copy>(
    item: Option<&'i Item<'_>>,
    convert: impl Fn(&'i str) -> std::result::Result<Option<T>, ()>,
    on_empty: Option<T>,
    on_error: Option<T>,
) -> Option<T> {
    match item? {
        Item::Null => None,
        Item::Empty => on_empty,
        Item::Error => on_error,
        Item::Text(text) => convert(text).unwrap_or(on_error),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Returning {
    Text,
    Boolean,
}

/// The `RETURNING` conversion of one call, with its `ON EMPTY` and `ON ERROR` constants.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Conversion {
    Text {
        max_chars: usize,
        on_empty: Option<String>,
        on_error: Option<String>,
    },
    Boolean {
        on_empty: Option<bool>,
        on_error: Option<bool>,
    },
}

/// One call's constants: `(path, max_chars, on_empty, on_error)` for text and
/// `(path, on_empty, on_error)` for BOOLEAN.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct JsonCall {
    path: Vec<JsonPathStep>,
    conversion: Conversion,
}

impl JsonCall {
    fn from_constants(returning: Returning, constants: &[&ScalarValue]) -> Result<Self> {
        let invalid = || {
            DataFusionError::Execution(format!("invalid JSON_VALUE scan arguments {constants:?}"))
        };
        let text = |value: &ScalarValue| match value {
            ScalarValue::Null => Ok(None),
            other => other
                .try_as_str()
                .map(|text| text.map(str::to_string))
                .ok_or_else(invalid),
        };
        let boolean = |value: &ScalarValue| match value {
            ScalarValue::Boolean(value) => Ok(*value),
            ScalarValue::Null => Ok(None),
            _ => Err(invalid()),
        };
        let (path, conversion) = match (returning, constants) {
            (Returning::Text, [path, max_chars, on_empty, on_error]) => {
                let ScalarValue::UInt64(Some(max_chars)) = max_chars.cast_to(&DataType::UInt64)?
                else {
                    return Err(invalid());
                };
                let conversion = Conversion::Text {
                    max_chars: max_chars as usize,
                    on_empty: text(on_empty)?,
                    on_error: text(on_error)?,
                };
                (path, conversion)
            }
            (Returning::Boolean, [path, on_empty, on_error]) => {
                let conversion = Conversion::Boolean {
                    on_empty: boolean(on_empty)?,
                    on_error: boolean(on_error)?,
                };
                (path, conversion)
            }
            _ => return Err(invalid()),
        };
        let path = text(path)?
            .as_deref()
            .and_then(parse_json_path)
            .ok_or_else(invalid)?;
        Ok(Self { path, conversion })
    }

    fn data_type(&self) -> DataType {
        match self.conversion {
            Conversion::Text { .. } => DataType::Utf8,
            Conversion::Boolean { .. } => DataType::Boolean,
        }
    }
}

/// One call's output, with the conversion it applies to each item.
enum Column<'c> {
    Text {
        max_chars: usize,
        on_empty: Option<&'c str>,
        on_error: Option<&'c str>,
        values: StringBuilder,
    },
    Boolean {
        on_empty: Option<bool>,
        on_error: Option<bool>,
        values: BooleanBuilder,
    },
}

impl<'c> Column<'c> {
    fn new(call: &'c JsonCall) -> Self {
        match &call.conversion {
            Conversion::Text {
                max_chars,
                on_empty,
                on_error,
            } => Self::Text {
                max_chars: *max_chars,
                on_empty: on_empty.as_deref(),
                on_error: on_error.as_deref(),
                values: StringBuilder::new(),
            },
            Conversion::Boolean { on_empty, on_error } => Self::Boolean {
                on_empty: *on_empty,
                on_error: *on_error,
                values: BooleanBuilder::new(),
            },
        }
    }

    /// `item` is `None` for a NULL document.
    fn append(&mut self, item: Option<&Item>) {
        match self {
            Self::Text {
                max_chars,
                on_empty,
                on_error,
                values,
            } => values.append_option(resolve(
                item,
                |text| text_value(text, *max_chars),
                *on_empty,
                *on_error,
            )),
            Self::Boolean {
                on_empty,
                on_error,
                values,
            } => values.append_option(resolve(
                item,
                |text| boolean_from_text(text).map(Some).ok_or(()),
                *on_empty,
                *on_error,
            )),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Text { mut values, .. } => Arc::new(values.finish()),
            Self::Boolean { mut values, .. } => Arc::new(values.finish()),
        }
    }
}

/// Reads string arrays in place, so a document column is not copied per batch.
fn for_each_document(documents: &ArrayRef, mut visit: impl FnMut(Option<&str>)) -> Result<()> {
    match documents.data_type() {
        DataType::Utf8 => documents.as_string::<i32>().iter().for_each(visit),
        DataType::LargeUtf8 => documents.as_string::<i64>().iter().for_each(visit),
        DataType::Utf8View => documents.as_string_view().iter().for_each(visit),
        _ => cast(documents, &DataType::Utf8)?
            .as_string::<i32>()
            .iter()
            .for_each(&mut visit),
    }
    Ok(())
}

/// One column per call, parsing each document once.
fn evaluate(calls: &[JsonCall], documents: &ArrayRef) -> Result<Vec<ArrayRef>> {
    let mut columns: Vec<Column> = calls.iter().map(Column::new).collect();
    for_each_document(documents, |document| {
        // Exasol reads the empty string as NULL.
        let root = document.filter(|d| !d.is_empty()).map(parse);
        for (call, column) in calls.iter().zip(&mut columns) {
            let item = root.as_ref().map(|root| select(root.as_ref(), &call.path));
            column.append(item.as_ref());
        }
    })?;
    Ok(columns.into_iter().map(Column::finish).collect())
}

fn constants(args: &ScalarFunctionArgs) -> Result<Vec<&ScalarValue>> {
    args.args[1..]
        .iter()
        .map(|arg| match arg {
            ColumnarValue::Scalar(value) => Ok(value),
            ColumnarValue::Array(_) => Err(DataFusionError::Execution(
                "JSON_VALUE scan arguments after the document must be constants".into(),
            )),
        })
        .collect()
}

/// One `JSON_VALUE` call, as the adapter renders it.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonUdf {
    name: &'static str,
    returning: Returning,
    signature: Signature,
}

impl ScalarUDFImpl for JsonUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(match self.returning {
            Returning::Text => DataType::Utf8,
            Returning::Boolean => DataType::Boolean,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let call = JsonCall::from_constants(self.returning, &constants(&args)?)?;
        let documents = args.args[0].to_array(args.number_rows)?;
        let [values] =
            <[ArrayRef; 1]>::try_from(evaluate(&[call], &documents)?).expect("one column per call");
        Ok(ColumnarValue::Array(values))
    }
}

/// Every call of one plan node over one document, as a struct with one field per call.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonValuesUdf {
    calls: Vec<JsonCall>,
    signature: Signature,
}

impl JsonValuesUdf {
    fn field_name(index: usize) -> String {
        format!("c{index}")
    }

    fn fields(&self) -> Fields {
        self.calls
            .iter()
            .enumerate()
            .map(|(index, call)| Field::new(Self::field_name(index), call.data_type(), true))
            .collect()
    }
}

impl ScalarUDFImpl for JsonValuesUdf {
    fn name(&self) -> &str {
        JSON_VALUES_FN
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(self.fields()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let documents = args.args[0].to_array(args.number_rows)?;
        let columns = evaluate(&self.calls, &documents)?;
        Ok(ColumnarValue::Array(Arc::new(StructArray::try_new(
            self.fields(),
            columns,
            None,
        )?)))
    }
}

/// `(values, index)`: field `index` of a [`JSON_VALUES_FN`] result. DataFusion moves a
/// `get_field` toward the scan and so copies the evaluation into every field, but not this call.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonFieldUdf {
    data_type: DataType,
    signature: Signature,
}

impl JsonFieldUdf {
    fn call(values: Expr, index: usize, data_type: DataType) -> Expr {
        let udf = Self {
            data_type,
            signature: Signature::any(2, Volatility::Immutable),
        };
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::from(udf)),
            vec![values, lit(index as u64)],
        ))
    }
}

impl ScalarUDFImpl for JsonFieldUdf {
    fn name(&self) -> &str {
        JSON_FIELD_FN
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarValue::UInt64(Some(index)) = constants(&args)?[0] else {
            return Err(DataFusionError::Execution(format!("{JSON_FIELD_FN} index")));
        };
        let values = args.args[0].to_array(args.number_rows)?;
        Ok(ColumnarValue::Array(Arc::clone(
            values.as_struct().column(*index as usize),
        )))
    }
}

/// The document and constants of a rendered `JSON_VALUE` call.
fn json_call(expr: &Expr) -> Option<(&Expr, JsonCall)> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return None;
    };
    let returning = match func.name() {
        JSON_VALUE_TEXT_FN => Returning::Text,
        JSON_VALUE_BOOLEAN_FN => Returning::Boolean,
        _ => return None,
    };
    let (document, constants) = args.split_first()?;
    let constants: Vec<&ScalarValue> = constants
        .iter()
        .map(|arg| match arg {
            Expr::Literal(value, _) => Some(value),
            _ => None,
        })
        .collect::<Option<_>>()?;
    let call = JsonCall::from_constants(returning, &constants).ok()?;
    Some((document, call))
}

/// The calls of `plan`'s expressions that share their document with another call, each mapped to
/// its field of the `lakehouse_json_values` call that evaluates them all, and those calls, aliased.
fn shared_calls(
    plan: &LogicalPlan,
    aliases: &AliasGenerator,
) -> Result<(HashMap<Expr, Expr>, Vec<Expr>)> {
    let mut by_document: Vec<(Expr, Vec<(Expr, JsonCall)>)> = Vec::new();
    plan.apply_expressions(|expr| {
        expr.apply(|node| {
            let Some((document, call)) = json_call(node) else {
                return Ok(TreeNodeRecursion::Continue);
            };
            match by_document.iter_mut().find(|(d, _)| d == document) {
                Some((_, calls)) if calls.iter().any(|(c, _)| c == node) => {}
                Some((_, calls)) => calls.push((node.clone(), call)),
                None => by_document.push((document.clone(), vec![(node.clone(), call)])),
            }
            // The rewrite replaces the whole call, so a call inside its document is not shared.
            Ok(TreeNodeRecursion::Jump)
        })
    })?;
    let mut fields = HashMap::new();
    let mut evaluations = Vec::new();
    for (document, calls) in by_document.into_iter().filter(|(_, c)| c.len() > 1) {
        let (exprs, calls): (Vec<Expr>, Vec<JsonCall>) = calls.into_iter().unzip();
        let udf = JsonValuesUdf {
            calls,
            signature: Signature::any(1, Volatility::Immutable),
        };
        let alias = aliases.next("__lakehouse_json");
        for (index, (expr, call)) in exprs.into_iter().zip(&udf.calls).enumerate() {
            let field = JsonFieldUdf::call(col(alias.as_str()), index, call.data_type());
            fields.insert(expr, field);
        }
        evaluations.push(
            Expr::ScalarFunction(ScalarFunction::new_udf(
                Arc::new(ScalarUDF::from(udf)),
                vec![document],
            ))
            .alias(alias),
        );
    }
    Ok((fields, evaluations))
}

fn columns_of(plan: &LogicalPlan) -> impl Iterator<Item = Expr> {
    plan.schema().columns().into_iter().map(Expr::Column)
}

/// Evaluates the calls of one plan node that read one document as one `lakehouse_json_values`
/// call in a projection below the node, so the document parses once per row. DataFusion's
/// common subexpression pass does not merge them.
#[derive(Debug)]
struct ShareJsonDocuments;

impl OptimizerRule for ShareJsonDocuments {
    fn name(&self) -> &str {
        "lakehouse_share_json_documents"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let (LogicalPlan::Projection(Projection { input, .. })
        | LogicalPlan::Filter(Filter { input, .. })
        | LogicalPlan::Aggregate(Aggregate { input, .. })) = &plan
        else {
            return Ok(Transformed::no(plan));
        };
        let (fields, evaluations) = shared_calls(&plan, config.alias_generator())?;
        if fields.is_empty() {
            return Ok(Transformed::no(plan));
        }
        let parsed = Projection::try_new(
            columns_of(input).chain(evaluations).collect(),
            Arc::clone(input),
        )?;
        let names = NamePreserver::new(&plan);
        let exprs = plan
            .expressions()
            .into_iter()
            .map(|expr| {
                let name = names.save(&expr);
                let rewritten = expr.transform_down(|node| {
                    Ok(match fields.get(&node) {
                        Some(field) => {
                            Transformed::new(field.clone(), true, TreeNodeRecursion::Jump)
                        }
                        None => Transformed::no(node),
                    })
                })?;
                Ok(name.restore(rewritten.data))
            })
            .collect::<Result<Vec<_>>>()?;
        let rewritten = plan.with_new_exprs(exprs, vec![LogicalPlan::Projection(parsed)])?;
        // A filter passes its input's columns through, so the evaluated calls are dropped again.
        let rewritten = match plan {
            LogicalPlan::Filter(_) => LogicalPlan::Projection(Projection::try_new(
                columns_of(&plan).collect(),
                Arc::new(rewritten),
            )?),
            _ => rewritten,
        };
        Ok(Transformed::yes(rewritten))
    }
}

pub(super) fn register_json_value_udfs(ctx: &SessionContext) {
    for (name, returning, arity) in [
        (JSON_VALUE_TEXT_FN, Returning::Text, 5),
        (JSON_VALUE_BOOLEAN_FN, Returning::Boolean, 4),
    ] {
        ctx.register_udf(ScalarUDF::from(JsonUdf {
            name,
            returning,
            signature: Signature::any(arity, Volatility::Immutable),
        }));
    }
    ctx.add_optimizer_rule(Arc::new(ShareJsonDocuments));
}

#[cfg(test)]
#[path = "json_value_tests.rs"]
mod tests;
