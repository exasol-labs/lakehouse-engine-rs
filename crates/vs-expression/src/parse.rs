//! The reader of a pushdown request's JSON, crate-private so that [`crate::parse_request`] is the
//! only way in. A node the [`Expr`] model cannot represent fails here, with an error that names its
//! field, so no renderer or planner meets one.

use exasol_udf_sdk::error::UdfError;
use serde_json::{Map, Value as Json};

use crate::exasol_type::{ExasolType, exasol_type_from_json};
use crate::expr::{
    AggregateCall, CaseExpr, Column, CompareOp, Expr, JsonBehavior, JsonValueCall, Literal,
    ScalarCall,
};
use crate::scalar_functions::{Arguments, MANY, aggregate_arguments, scalar_arguments};

/// The node at `path` of the pushdown request, such as `filter` or `selectList[2]`.
pub(crate) fn parse_expression(node: &Json, path: &str) -> Result<Expr, UdfError> {
    RequestNode::at(node, path)?.expr()
}

/// A malformed request: what is wrong with the field at `path`.
fn invalid_field(path: &str, problem: &str) -> UdfError {
    UdfError::User(format!("the pushdown request's {path} {problem}"))
}

/// One JSON object of the request and where it sits; every field it reads fails naming its path.
/// The request root sits at the empty path, so its fields read by their bare key. No accessor
/// hands out the JSON itself, so a caller reads a field only as a checked value.
pub(crate) struct RequestNode<'a> {
    map: &'a Map<String, Json>,
    path: String,
}

impl<'a> RequestNode<'a> {
    fn at(node: &'a Json, path: &str) -> Result<Self, UdfError> {
        Self::of(node, path, "is not an expression node")
    }

    /// The object `node` at `path`, such as `from` or `orderBy[0]`.
    pub(crate) fn object(node: &'a Json, path: &str) -> Result<Self, UdfError> {
        Self::of(node, path, "is not an object")
    }

    fn of(node: &'a Json, path: &str, problem: &str) -> Result<Self, UdfError> {
        match node {
            Json::Object(map) => Ok(Self {
                map,
                path: path.to_string(),
            }),
            _ => Err(invalid_field(path, problem)),
        }
    }

    fn field_path(&self, key: &str) -> String {
        match self.path.as_str() {
            "" => key.to_string(),
            path => format!("{path}.{key}"),
        }
    }

    pub(crate) fn invalid(&self, key: &str, problem: &str) -> UdfError {
        invalid_field(&self.field_path(key), problem)
    }

    /// `None` for an absent or JSON-null field.
    fn get(&self, key: &str) -> Option<&'a Json> {
        self.map.get(key).filter(|value| !value.is_null())
    }

    pub(crate) fn text(&self, key: &str) -> Result<&'a str, UdfError> {
        self.optional_text(key)?
            .ok_or_else(|| self.invalid(key, "is missing"))
    }

    pub(crate) fn optional_text(&self, key: &str) -> Result<Option<&'a str>, UdfError> {
        match self.get(key) {
            None => Ok(None),
            Some(Json::String(text)) => Ok(Some(text)),
            Some(_) => Err(self.invalid(key, "is not a string")),
        }
    }

    pub(crate) fn flag(&self, key: &str) -> Result<bool, UdfError> {
        match self.get(key) {
            None => Err(self.invalid(key, "is missing")),
            Some(Json::Bool(value)) => Ok(*value),
            Some(_) => Err(self.invalid(key, "is not a boolean")),
        }
    }

    /// `None` when the field is absent.
    pub(crate) fn whole_number(&self, key: &str) -> Result<Option<u64>, UdfError> {
        self.get(key)
            .map(|value| {
                value
                    .as_u64()
                    .ok_or_else(|| self.invalid(key, "is not a whole number"))
            })
            .transpose()
    }

    /// The object at `key`, which must be present.
    pub(crate) fn field(&self, key: &str) -> Result<Self, UdfError> {
        self.optional_field(key)?
            .ok_or_else(|| self.invalid(key, "is missing"))
    }

    pub(crate) fn optional_field(&self, key: &str) -> Result<Option<Self>, UdfError> {
        self.get(key)
            .map(|value| Self::object(value, &self.field_path(key)))
            .transpose()
    }

    /// The object at `key` as a request root of its own, so its fields read by their bare key.
    pub(crate) fn root(&self, key: &str) -> Result<Self, UdfError> {
        Ok(Self {
            map: self.field(key)?.map,
            path: String::new(),
        })
    }

    /// The objects of the array at `key`; an absent array is empty.
    pub(crate) fn objects(&self, key: &str) -> Result<Vec<Self>, UdfError> {
        self.array(key)?
            .iter()
            .enumerate()
            .map(|(i, item)| Self::object(item, &format!("{}[{i}]", self.field_path(key))))
            .collect()
    }

    pub(crate) fn child(&self, key: &str) -> Result<Expr, UdfError> {
        let value = self
            .get(key)
            .ok_or_else(|| self.invalid(key, "is missing"))?;
        parse_expression(value, &self.field_path(key))
    }

    fn boxed(&self, key: &str) -> Result<Box<Expr>, UdfError> {
        self.child(key).map(Box::new)
    }

    pub(crate) fn optional_child(&self, key: &str) -> Result<Option<Expr>, UdfError> {
        self.get(key)
            .map(|value| parse_expression(value, &self.field_path(key)))
            .transpose()
    }

    /// The expressions of the array at `key`; an absent array is empty.
    pub(crate) fn list(&self, key: &str) -> Result<Vec<Expr>, UdfError> {
        self.array(key)?
            .iter()
            .enumerate()
            .map(|(i, item)| parse_expression(item, &format!("{}[{i}]", self.field_path(key))))
            .collect()
    }

    /// [`Self::list`], `None` for an absent array, which an empty one is not.
    pub(crate) fn optional_list(&self, key: &str) -> Result<Option<Vec<Expr>>, UdfError> {
        self.get(key).map(|_| self.list(key)).transpose()
    }

    fn array(&self, key: &str) -> Result<&'a [Json], UdfError> {
        match self.get(key) {
            None => Ok(&[]),
            Some(Json::Array(items)) => Ok(items),
            Some(_) => Err(self.invalid(key, "is not an array")),
        }
    }

    fn required_list(&self, key: &str) -> Result<Vec<Expr>, UdfError> {
        if self.get(key).is_none() {
            return Err(self.invalid(key, "is missing"));
        }
        self.list(key)
    }

    /// The one argument of a call that takes exactly one.
    fn single_argument(&self) -> Result<Box<Expr>, UdfError> {
        let mut arguments = self.list("arguments")?;
        match arguments.len() {
            1 => Ok(Box::new(arguments.remove(0))),
            n => Err(self.invalid("arguments", &format!("holds {n} arguments, not 1"))),
        }
    }

    pub(crate) fn data_type(&self, key: &str) -> Result<ExasolType, UdfError> {
        let value = self
            .get(key)
            .ok_or_else(|| self.invalid(key, "is missing"))?;
        exasol_type_from_json(value).map_err(|err| self.invalid(key, &format!("is invalid: {err}")))
    }

    /// The types of the `dataType` array at `key`, `None` for an absent array.
    pub(crate) fn optional_data_types(
        &self,
        key: &str,
    ) -> Result<Option<Vec<ExasolType>>, UdfError> {
        if self.get(key).is_none() {
            return Ok(None);
        }
        self.array(key)?
            .iter()
            .enumerate()
            .map(|(i, entry)| {
                exasol_type_from_json(entry).map_err(|err| {
                    self.invalid(&format!("{key}[{i}]"), &format!("is invalid: {err}"))
                })
            })
            .collect::<Result<_, _>>()
            .map(Some)
    }

    fn expr(&self) -> Result<Expr, UdfError> {
        let kind = self.text("type")?;
        if let Some(literal) = self.literal(kind)? {
            return Ok(Expr::Literal(literal));
        }
        if let Some(op) = compare_op(kind) {
            return Ok(Expr::compare(op, self.child("left")?, self.child("right")?));
        }
        Ok(match kind {
            "column" => Expr::Column(Column {
                name: self.text("name")?.to_string(),
                table_name: self.optional_text("tableName")?.map(str::to_string),
                table_alias: self.optional_text("tableAlias")?.map(str::to_string),
            }),
            "predicate_and" => Expr::And(self.required_list("expressions")?),
            "predicate_or" => Expr::Or(self.required_list("expressions")?),
            "predicate_not" => Expr::Not(self.boxed("expression")?),
            "predicate_is_null" => Expr::IsNull(self.boxed("expression")?),
            "predicate_is_not_null" => Expr::IsNotNull(self.boxed("expression")?),
            "predicate_in_constlist" => Expr::InList {
                expression: self.boxed("expression")?,
                values: self.required_list("arguments")?,
            },
            "predicate_between" => Expr::Between {
                expression: self.boxed("expression")?,
                low: self.boxed("left")?,
                high: self.boxed("right")?,
            },
            "predicate_like" => Expr::Like {
                expression: self.boxed("expression")?,
                pattern: self.boxed("pattern")?,
                escape: self.optional_child("escapeChar")?.map(Box::new),
            },
            "function_scalar" => self.scalar()?,
            "function_scalar_cast" => self.cast()?,
            "function_scalar_extract" => Expr::Extract {
                field: self.text("toExtract")?.to_string(),
                argument: self.single_argument()?,
            },
            "function_scalar_case" => self.case()?,
            "function_scalar_json_value" => self.json_value()?,
            "function_aggregate" => self.aggregate()?,
            other => {
                return Err(self.invalid(
                    "type",
                    &format!("names the node type {other}, which the adapter does not translate"),
                ));
            }
        })
    }

    /// The literal of node type `kind`, `None` for any other node type.
    fn literal(&self, kind: &str) -> Result<Option<Literal>, UdfError> {
        let text = |literal: fn(String) -> Literal| {
            let value = self.text("value")?;
            Ok(Some(literal(value.to_string())))
        };
        match kind {
            "literal_null" => Ok(Some(Literal::Null)),
            "literal_bool" => Ok(Some(Literal::Bool(self.flag("value")?))),
            "literal_string" => text(Literal::String),
            "literal_double" => text(Literal::Double),
            "literal_exactnumeric" => text(Literal::ExactNumeric),
            "literal_date" => text(Literal::Date),
            "literal_timestamp" => text(Literal::Timestamp),
            "literal_timestamp_utc" => text(Literal::TimestampUtc),
            "literal_timestamputc" => text(Literal::LocalTimeZoneTimestamp),
            _ => Ok(None),
        }
    }

    /// Exasol may send a CAST as a `function_scalar` named `CAST`, beside `function_scalar_cast`.
    fn scalar(&self) -> Result<Expr, UdfError> {
        let name = self.text("name")?;
        if name.eq_ignore_ascii_case("CAST") {
            return self.cast();
        }
        let takes = scalar_arguments(name);
        let arguments = self.call_arguments("scalar function", name, takes)?;
        Ok(Expr::Scalar(ScalarCall::new(name, arguments)))
    }

    fn aggregate(&self) -> Result<Expr, UdfError> {
        let name = self.text("name")?;
        let takes = aggregate_arguments(name);
        Ok(Expr::Aggregate(AggregateCall {
            arguments: self.call_arguments("aggregate function", name, takes)?,
            name: name.to_uppercase(),
            distinct: match self.get("distinct") {
                None => false,
                Some(Json::Bool(distinct)) => *distinct,
                Some(_) => return Err(self.invalid("distinct", "is not a boolean")),
            },
        }))
    }

    /// The arguments of a call of `name`, whose argument counts are `takes`, `None` for a function
    /// the adapter does not translate. A missing `arguments` is a call without any.
    fn call_arguments(
        &self,
        function: &str,
        name: &str,
        takes: Option<Arguments>,
    ) -> Result<Vec<Expr>, UdfError> {
        let Some(takes) = takes else {
            return Err(self.invalid(
                "name",
                &format!("names the {function} {name}, which the adapter does not translate"),
            ));
        };
        let arguments = self.list("arguments")?;
        if !takes.contains(&arguments.len()) {
            return Err(self.invalid(
                "arguments",
                &format!(
                    "holds {} arguments, but {} takes {}",
                    arguments.len(),
                    name.to_uppercase(),
                    argument_counts(&takes)
                ),
            ));
        }
        Ok(arguments)
    }

    fn cast(&self) -> Result<Expr, UdfError> {
        Ok(Expr::Cast {
            target: self.data_type("dataType")?,
            argument: self.single_argument()?,
        })
    }

    fn case(&self) -> Result<Expr, UdfError> {
        let case = CaseExpr {
            basis: self.optional_child("basis")?.map(Box::new),
            whens: self.required_list("arguments")?,
            results: self.required_list("results")?,
        };
        let (whens, results) = (case.whens.len(), case.results.len());
        if results != whens && results != whens + 1 {
            return Err(self.invalid(
                "results",
                &format!("holds {results} results for {whens} WHEN values, not one per WHEN plus an optional ELSE"),
            ));
        }
        Ok(Expr::Case(case))
    }

    fn json_value(&self) -> Result<Expr, UdfError> {
        let mut arguments = self.list("arguments")?;
        if arguments.len() != 2 {
            return Err(self.invalid("arguments", "does not hold a document and a path"));
        }
        let path = Box::new(arguments.remove(1));
        let document = Box::new(arguments.remove(0));
        Ok(Expr::JsonValue(JsonValueCall {
            document,
            path,
            returning: self
                .get("returningDataType")
                .map(|_| self.data_type("returningDataType"))
                .transpose()?,
            on_empty: self.behavior("emptyBehavior")?,
            on_error: self.behavior("errorBehavior")?,
        }))
    }

    fn behavior(&self, key: &str) -> Result<Option<JsonBehavior>, UdfError> {
        let Some(value) = self.get(key) else {
            return Ok(None);
        };
        let path = self.field_path(key);
        let behavior = RequestNode::at(value, &path)?;
        Ok(Some(match behavior.text("type")? {
            "NULL" => JsonBehavior::Null,
            "ERROR" => JsonBehavior::Error,
            "DEFAULT" => JsonBehavior::Default(behavior.boxed("expression")?),
            other => {
                return Err(behavior.invalid("type", &format!("names the behavior {other}")));
            }
        }))
    }
}

fn compare_op(kind: &str) -> Option<CompareOp> {
    Some(match kind {
        "predicate_equal" => CompareOp::Equal,
        "predicate_notequal" => CompareOp::NotEqual,
        "predicate_less" => CompareOp::Less,
        "predicate_lessequal" => CompareOp::LessEqual,
        "predicate_greater" => CompareOp::Greater,
        "predicate_greaterequal" => CompareOp::GreaterEqual,
        _ => return None,
    })
}

fn argument_counts(takes: &Arguments) -> String {
    match (*takes.start(), *takes.end()) {
        (fewest, MANY) => format!("at least {fewest}"),
        (fewest, most) if fewest == most => fewest.to_string(),
        (fewest, most) => format!("{fewest} to {most}"),
    }
}

#[cfg(test)]
#[path = "parse_tests.rs"]
mod tests;
