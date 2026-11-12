//! `JSON_VALUE` in the scan (#574): paths run in lax mode over item sequences, the result converts
//! to the `RETURNING` type, and one read of a document serves every call of a plan node on it.
//! A read stops once no path can select more, unless a call over the document anywhere in the
//! query has a `DEFAULT ... ON ERROR`, which reads the whole document as Exasol does
//! (docs/semantic-differences.md).

use arrow::array::{Array, ArrayRef, AsArray, BooleanBuilder, StringBuilder, StructArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::ScalarValue;
use datafusion::common::alias::AliasGenerator;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Aggregate, ColumnarValue, Expr, Filter, LogicalPlan, Projection, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility, col, lit,
};
use datafusion::optimizer::{AnalyzerRule, ApplyOrder, OptimizerConfig, OptimizerRule};
use jiter::{Jiter, Peek};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use vs_expression::{JSON_VALUE_BOOLEAN_FN, JSON_VALUE_TEXT_FN, JsonPathStep, parse_json_path};

/// The twins of the rendered calls that read the whole document, set by [`StrictDocuments`].
const JSON_VALUE_TEXT_STRICT_FN: &str = "lakehouse_json_value_text_strict";
const JSON_VALUE_BOOLEAN_STRICT_FN: &str = "lakehouse_json_value_boolean_strict";
/// The call that evaluates every `JSON_VALUE` of one plan node over one document.
const JSON_VALUES_FN: &str = "lakehouse_json_values";
/// One call's field of a [`JSON_VALUES_FN`] result.
const JSON_FIELD_FN: &str = "lakehouse_json_field";

/// jiter skips a value at most this deep, so a path walk stops at the same depth.
const MAX_DEPTH: usize = 200;

/// What one path selects in one document.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Item<'a> {
    /// No item: `ON EMPTY` applies.
    Empty,
    /// Not JSON, several items, or a non-scalar item: `ON ERROR` applies.
    Error,
    Null,
    /// A string's text, or the source text of a number, `true`, or `false`.
    Text(Cow<'a, str>),
}

impl<'a> Item<'a> {
    fn add(&mut self, item: Item<'a>) {
        *self = match self {
            Item::Empty => item,
            _ => Item::Error,
        };
    }
}

/// One path's next step: `(path, step)`.
type Cursor = (usize, usize);

#[derive(Debug, PartialEq, Eq)]
enum Flow {
    Continue,
    /// No path can select more, so the rest of the document is not read.
    Done,
}

/// A document that is not JSON, or nested deeper than [`MAX_DEPTH`].
struct Invalid;

impl From<jiter::JiterError> for Invalid {
    fn from(_: jiter::JiterError) -> Self {
        Invalid
    }
}

/// One read of a document for several paths. A strict read validates the whole document and finds
/// duplicate members after a selected one. A fast read stops once no path can select more, and
/// takes only the first member of a name in each object.
struct Walk<'a, 'p> {
    doc: &'a str,
    jiter: Jiter<'a>,
    paths: &'p [&'p [JsonPathStep]],
    items: Vec<Item<'a>>,
    strict: bool,
}

fn step<'p>(paths: &[&'p [JsonPathStep]], (path, step): Cursor) -> Option<&'p JsonPathStep> {
    paths[path].get(step)
}

/// The text of `doc` that `text` is, which jiter returns for a string without escapes.
fn borrowed<'a>(doc: &'a str, text: &str) -> Option<&'a str> {
    let start = (text.as_ptr() as usize).checked_sub(doc.as_ptr() as usize)?;
    doc.get(start..start + text.len())
}

impl<'a> Walk<'a, '_> {
    fn scalar(&mut self, peek: Peek) -> Result<Item<'a>, Invalid> {
        let start = self.jiter.current_index();
        match peek {
            Peek::Null => {
                self.jiter.known_null()?;
                return Ok(Item::Null);
            }
            Peek::String => {
                let doc = self.doc;
                let text = self.jiter.known_str()?;
                return Ok(Item::Text(match borrowed(doc, text) {
                    Some(text) => Cow::Borrowed(text),
                    None => Cow::Owned(text.to_string()),
                }));
            }
            Peek::True | Peek::False => {
                self.jiter.known_bool(peek)?;
            }
            _ => {
                self.jiter.known_number_bytes(peek)?;
            }
        }
        Ok(Item::Text(Cow::Borrowed(
            &self.doc[start..self.jiter.current_index()],
        )))
    }

    /// `finish`: no cursor of the whole walk is pending after this value.
    fn value(
        &mut self,
        peek: Peek,
        cursors: Vec<Cursor>,
        finish: bool,
        depth: usize,
    ) -> Result<Flow, Invalid> {
        if depth > MAX_DEPTH {
            return Err(Invalid);
        }
        let is_array = peek == Peek::Array;
        let container = is_array || peek == Peek::Object;
        let (mut complete, mut active) = (Vec::new(), Vec::new());
        for mut cursor in cursors {
            // Lax mode reads a non-array as a one-item array.
            while !is_array && step(self.paths, cursor) == Some(&JsonPathStep::Index(0)) {
                cursor.1 += 1;
            }
            match step(self.paths, cursor) {
                None => complete.push(cursor.0),
                Some(JsonPathStep::Member(_)) if container => active.push(cursor),
                Some(JsonPathStep::Index(_)) if is_array => active.push(cursor),
                Some(_) => {}
            }
        }
        let done = if finish && !self.strict {
            Flow::Done
        } else {
            Flow::Continue
        };
        if container {
            for &path in &complete {
                self.items[path].add(Item::Error);
            }
        } else if !complete.is_empty() {
            let item = self.scalar(peek)?;
            for &path in &complete {
                self.items[path].add(item.clone());
            }
            return Ok(done);
        }
        if active.is_empty() {
            if done == Flow::Continue {
                self.jiter.known_skip(peek)?;
            }
            return Ok(done);
        }
        let flow = if is_array {
            self.array(active, finish, depth)?
        } else {
            self.object(active, finish, depth)?
        };
        Ok(match flow {
            Flow::Done => Flow::Done,
            Flow::Continue => done,
        })
    }

    fn object(
        &mut self,
        mut pending: Vec<Cursor>,
        finish: bool,
        depth: usize,
    ) -> Result<Flow, Invalid> {
        let paths = self.paths;
        let mut key = self.jiter.known_object()?;
        while let Some(name) = key {
            let matched: Vec<Cursor> = pending
                .iter()
                .filter(|&&c| matches!(step(paths, c), Some(JsonPathStep::Member(m)) if m == name))
                .map(|&(path, step)| (path, step + 1))
                .collect();
            if matched.is_empty() {
                self.jiter.next_skip()?;
            } else {
                if !self.strict {
                    pending.retain(|&(path, step)| !matched.contains(&(path, step + 1)));
                }
                let peek = self.jiter.peek()?;
                let finish = finish && pending.is_empty();
                if self.value(peek, matched, finish, depth + 1)? == Flow::Done {
                    return Ok(Flow::Done);
                }
            }
            key = self.jiter.next_key()?;
        }
        Ok(Flow::Continue)
    }

    fn array(&mut self, cursors: Vec<Cursor>, finish: bool, depth: usize) -> Result<Flow, Invalid> {
        let mut element = self.jiter.known_array()?;
        let mut index = 0;
        while let Some(peek) = element {
            let mut matched = Vec::new();
            let mut later = false;
            for &cursor in &cursors {
                match step(self.paths, cursor) {
                    // Lax mode applies a member step to each object of an array.
                    Some(JsonPathStep::Member(_)) => {
                        later = true;
                        if peek == Peek::Object {
                            matched.push(cursor);
                        }
                    }
                    Some(JsonPathStep::Index(i)) if *i as usize == index => {
                        matched.push((cursor.0, cursor.1 + 1));
                    }
                    Some(JsonPathStep::Index(i)) => later |= *i as usize > index,
                    None => {}
                }
            }
            if matched.is_empty() {
                self.jiter.known_skip(peek)?;
            } else if self.value(peek, matched, finish && !later, depth + 1)? == Flow::Done {
                return Ok(Flow::Done);
            }
            index += 1;
            element = self.jiter.array_step()?;
        }
        Ok(Flow::Continue)
    }
}

/// What each of `paths` selects in `doc`.
fn select<'a>(doc: &'a str, paths: &[&[JsonPathStep]], strict: bool) -> Vec<Item<'a>> {
    let mut walk = Walk {
        doc,
        jiter: Jiter::new(doc.as_bytes()),
        paths,
        items: vec![Item::Empty; paths.len()],
        strict,
    };
    let cursors = (0..paths.len()).map(|path| (path, 0)).collect();
    let read = walk.jiter.peek().map_err(Invalid::from).and_then(|peek| {
        match walk.value(peek, cursors, true, 0)? {
            Flow::Done => Ok(()),
            Flow::Continue => Ok(walk.jiter.finish()?),
        }
    });
    match read {
        Ok(()) => walk.items,
        Err(Invalid) => vec![Item::Error; paths.len()],
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
/// `(path, on_empty, on_error)` for BOOLEAN, and whether it reads the whole document.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct JsonCall {
    path: Vec<JsonPathStep>,
    conversion: Conversion,
    strict: bool,
}

impl JsonCall {
    /// A `strict` function reads the whole document, as does a call with a `DEFAULT ... ON
    /// ERROR`: Exasol sends `NULL ON ERROR` also without the clause, so only a `DEFAULT` asks.
    fn from_constants(
        returning: Returning,
        strict: bool,
        constants: &[&ScalarValue],
    ) -> Result<Self> {
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
        let (path, conversion, on_error) = match (returning, constants) {
            (Returning::Text, [path, max_chars, on_empty, on_error]) => {
                let ScalarValue::UInt64(Some(max_chars)) = max_chars.cast_to(&DataType::UInt64)?
                else {
                    return Err(invalid());
                };
                let on_error = text(on_error)?;
                let has_default = on_error.is_some();
                let conversion = Conversion::Text {
                    max_chars: max_chars as usize,
                    on_empty: text(on_empty)?,
                    on_error,
                };
                (path, conversion, has_default)
            }
            (Returning::Boolean, [path, on_empty, on_error]) => {
                let on_error = boolean(on_error)?;
                let conversion = Conversion::Boolean {
                    on_empty: boolean(on_empty)?,
                    on_error,
                };
                (path, conversion, on_error.is_some())
            }
            _ => return Err(invalid()),
        };
        let path = text(path)?
            .as_deref()
            .and_then(parse_json_path)
            .ok_or_else(invalid)?;
        Ok(Self {
            path,
            conversion,
            strict: strict || on_error,
        })
    }

    fn returning(&self) -> Returning {
        match self.conversion {
            Conversion::Text { .. } => Returning::Text,
            Conversion::Boolean { .. } => Returning::Boolean,
        }
    }

    fn data_type(&self) -> DataType {
        match self.returning() {
            Returning::Text => DataType::Utf8,
            Returning::Boolean => DataType::Boolean,
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

/// One column per call, reading each document once for all of them.
fn evaluate(calls: &[JsonCall], documents: &ArrayRef) -> Result<Vec<ArrayRef>> {
    let mut columns: Vec<Column> = calls.iter().map(Column::new).collect();
    let paths: Vec<&[JsonPathStep]> = calls.iter().map(|call| call.path.as_slice()).collect();
    let strict = calls.iter().any(|call| call.strict);
    for_each_document(documents, |document| {
        // Exasol reads the empty string as NULL.
        let items = document
            .filter(|d| !d.is_empty())
            .map(|d| select(d, &paths, strict));
        for (index, column) in columns.iter_mut().enumerate() {
            column.append(items.as_ref().map(|items| &items[index]));
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

/// One `JSON_VALUE` call, as the adapter renders it or as [`StrictDocuments`] makes it strict.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonUdf {
    name: &'static str,
    returning: Returning,
    strict: bool,
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
        let call = JsonCall::from_constants(self.returning, self.strict, &constants(&args)?)?;
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
    fn fields(&self) -> Fields {
        self.calls
            .iter()
            .enumerate()
            .map(|(index, call)| Field::new(format!("c{index}"), call.data_type(), true))
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
    let (returning, strict) = match func.name() {
        JSON_VALUE_TEXT_FN => (Returning::Text, false),
        JSON_VALUE_BOOLEAN_FN => (Returning::Boolean, false),
        JSON_VALUE_TEXT_STRICT_FN => (Returning::Text, true),
        JSON_VALUE_BOOLEAN_STRICT_FN => (Returning::Boolean, true),
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
    let call = JsonCall::from_constants(returning, strict, &constants).ok()?;
    Some((document, call))
}

/// The calls of `plan`'s expressions that share their document with another call, each mapped to
/// its field of the [`JSON_VALUES_FN`] call that evaluates them all, and those calls, aliased.
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

/// Evaluates the calls of one plan node that read one document as one [`JSON_VALUES_FN`] call in
/// a projection below the node, so the document is read once per row. DataFusion's common
/// subexpression pass does not merge them.
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

fn json_udf(returning: Returning, strict: bool) -> ScalarUDF {
    let (name, arity) = match (returning, strict) {
        (Returning::Text, false) => (JSON_VALUE_TEXT_FN, 5),
        (Returning::Boolean, false) => (JSON_VALUE_BOOLEAN_FN, 4),
        (Returning::Text, true) => (JSON_VALUE_TEXT_STRICT_FN, 5),
        (Returning::Boolean, true) => (JSON_VALUE_BOOLEAN_STRICT_FN, 4),
    };
    ScalarUDF::from(JsonUdf {
        name,
        returning,
        strict,
        signature: Signature::any(arity, Volatility::Immutable),
    })
}

/// Makes every call over a document strict when one call over it anywhere in the plan is, before
/// the optimizer moves calls between plan nodes, so a call's read does not depend on its node.
#[derive(Debug)]
struct StrictDocuments;

impl AnalyzerRule for StrictDocuments {
    fn name(&self) -> &str {
        "lakehouse_strict_json_documents"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let mut strict_documents: Vec<Expr> = Vec::new();
        plan.apply_with_subqueries(|node| {
            node.apply_expressions(|expr| {
                expr.apply(|node| {
                    if let Some((document, call)) = json_call(node)
                        && call.strict
                        && !strict_documents.contains(document)
                    {
                        strict_documents.push(document.clone());
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })
        })?;
        if strict_documents.is_empty() {
            return Ok(plan);
        }
        let strict = |expr: Expr| -> Result<Transformed<Expr>> {
            let Some((document, call)) = json_call(&expr) else {
                return Ok(Transformed::no(expr));
            };
            if call.strict || !strict_documents.contains(document) {
                return Ok(Transformed::no(expr));
            }
            let Expr::ScalarFunction(ScalarFunction { args, .. }) = expr else {
                unreachable!("json_call matches only scalar functions");
            };
            let udf = Arc::new(json_udf(call.returning(), true));
            Ok(Transformed::yes(Expr::ScalarFunction(
                ScalarFunction::new_udf(udf, args),
            )))
        };
        plan.transform_up_with_subqueries(|node| {
            let names = NamePreserver::new(&node);
            node.map_expressions(|expr| {
                let name = names.save(&expr);
                expr.transform_up(strict)?
                    .map_data(|expr| Ok(name.restore(expr)))
            })?
            .map_data(LogicalPlan::recompute_schema)
        })
        .map(|transformed| transformed.data)
    }
}

pub(super) fn register_json_value_udfs(ctx: &SessionContext) {
    for returning in [Returning::Text, Returning::Boolean] {
        for strict in [false, true] {
            ctx.register_udf(json_udf(returning, strict));
        }
    }
    ctx.add_analyzer_rule(Arc::new(StrictDocuments));
    ctx.add_optimizer_rule(Arc::new(ShareJsonDocuments));
}

#[cfg(test)]
#[path = "json_value_tests.rs"]
mod tests;
