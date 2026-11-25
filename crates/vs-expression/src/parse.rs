//! The one reader of a pushdown request's expression JSON. A node the [`Expr`] model cannot
//! represent fails here, with an error that names its field, so no renderer or planner meets one.

use exasol_udf_sdk::error::UdfError;
use serde_json::{Map, Value as Json};

use crate::exasol_type::{ExasolType, exasol_type_from_json};
use crate::expr::{
    AggregateCall, CaseExpr, Column, CompareOp, Expr, JsonBehavior, JsonValueCall, Literal,
    LiteralKind, Scalar, ScalarCall,
};

/// The node at `path` of the pushdown request, such as `filter` or `selectList[2]`.
pub fn parse_expression(node: &Json, path: &str) -> Result<Expr, UdfError> {
    RequestNode::at(node, path)?.expr()
}

/// A malformed request: what is wrong with the field at `path`.
pub fn invalid_field(path: &str, problem: &str) -> UdfError {
    UdfError::User(format!("the pushdown request's {path} {problem}"))
}

/// One JSON object of the request and where it sits; every field it reads fails naming its path.
/// The request root sits at the empty path, so its fields read by their bare key.
pub struct RequestNode<'a> {
    map: &'a Map<String, Json>,
    path: String,
}

impl<'a> RequestNode<'a> {
    fn at(node: &'a Json, path: &str) -> Result<Self, UdfError> {
        Self::of(node, path, "is not an expression node")
    }

    /// The object `node` at `path`, such as `from` or `orderBy[0]`.
    pub fn object(node: &'a Json, path: &str) -> Result<Self, UdfError> {
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

    pub fn invalid(&self, key: &str, problem: &str) -> UdfError {
        invalid_field(&self.field_path(key), problem)
    }

    /// `None` for an absent or JSON-null field.
    pub fn get(&self, key: &str) -> Option<&'a Json> {
        self.map.get(key).filter(|value| !value.is_null())
    }

    pub fn text(&self, key: &str) -> Result<&'a str, UdfError> {
        self.optional_text(key)?
            .ok_or_else(|| self.invalid(key, "is missing"))
    }

    pub fn optional_text(&self, key: &str) -> Result<Option<&'a str>, UdfError> {
        match self.get(key) {
            None => Ok(None),
            Some(Json::String(text)) => Ok(Some(text)),
            Some(_) => Err(self.invalid(key, "is not a string")),
        }
    }

    pub fn flag(&self, key: &str) -> Result<bool, UdfError> {
        match self.get(key) {
            None => Err(self.invalid(key, "is missing")),
            Some(Json::Bool(value)) => Ok(*value),
            Some(_) => Err(self.invalid(key, "is not a boolean")),
        }
    }

    /// `None` when the field is absent.
    pub fn whole_number(&self, key: &str) -> Result<Option<u64>, UdfError> {
        self.get(key)
            .map(|value| {
                value
                    .as_u64()
                    .ok_or_else(|| self.invalid(key, "is not a whole number"))
            })
            .transpose()
    }

    /// The object at `key`, which must be present.
    pub fn field(&self, key: &str) -> Result<Self, UdfError> {
        self.optional_field(key)?
            .ok_or_else(|| self.invalid(key, "is missing"))
    }

    pub fn optional_field(&self, key: &str) -> Result<Option<Self>, UdfError> {
        self.get(key)
            .map(|value| Self::object(value, &self.field_path(key)))
            .transpose()
    }

    /// The objects of the array at `key`; an absent array is empty.
    pub fn objects(&self, key: &str) -> Result<Vec<Self>, UdfError> {
        self.array(key)?
            .iter()
            .enumerate()
            .map(|(i, item)| Self::object(item, &format!("{}[{i}]", self.field_path(key))))
            .collect()
    }

    pub fn child(&self, key: &str) -> Result<Expr, UdfError> {
        let value = self
            .get(key)
            .ok_or_else(|| self.invalid(key, "is missing"))?;
        parse_expression(value, &self.field_path(key))
    }

    fn boxed(&self, key: &str) -> Result<Box<Expr>, UdfError> {
        self.child(key).map(Box::new)
    }

    pub fn optional_child(&self, key: &str) -> Result<Option<Expr>, UdfError> {
        self.get(key)
            .map(|value| parse_expression(value, &self.field_path(key)))
            .transpose()
    }

    /// The expressions of the array at `key`; an absent array is empty.
    pub fn list(&self, key: &str) -> Result<Vec<Expr>, UdfError> {
        self.array(key)?
            .iter()
            .enumerate()
            .map(|(i, item)| parse_expression(item, &format!("{}[{i}]", self.field_path(key))))
            .collect()
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

    fn data_type(&self, key: &str) -> Result<ExasolType, UdfError> {
        let value = self
            .get(key)
            .ok_or_else(|| self.invalid(key, "is missing"))?;
        exasol_type_from_json(value).map_err(|err| self.invalid(key, &format!("is invalid: {err}")))
    }

    fn expr(&self) -> Result<Expr, UdfError> {
        let kind = self.text("type")?;
        if let Some(kind) = literal_kind(kind) {
            return self.literal(kind);
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
            "function_aggregate" => Expr::Aggregate(AggregateCall {
                name: self.text("name")?.to_uppercase(),
                arguments: self.list("arguments")?,
                distinct: match self.get("distinct") {
                    None => false,
                    Some(Json::Bool(distinct)) => *distinct,
                    Some(_) => return Err(self.invalid("distinct", "is not a boolean")),
                },
            }),
            other => {
                return Err(self.invalid(
                    "type",
                    &format!("names the node type {other}, which the adapter does not translate"),
                ));
            }
        })
    }

    fn literal(&self, kind: LiteralKind) -> Result<Expr, UdfError> {
        let value = match self.get("value") {
            None => Scalar::Null,
            Some(Json::Bool(value)) => Scalar::Bool(*value),
            Some(Json::Number(number)) => Scalar::Number(number.clone()),
            Some(Json::String(text)) => Scalar::String(text.clone()),
            Some(_) => return Err(self.invalid("value", "is not a scalar")),
        };
        Ok(Expr::Literal(Literal::new(kind, value)))
    }

    /// Exasol may send a CAST as a `function_scalar` named `CAST`, beside `function_scalar_cast`.
    fn scalar(&self) -> Result<Expr, UdfError> {
        let name = self.text("name")?;
        if name.eq_ignore_ascii_case("CAST") {
            return self.cast();
        }
        Ok(Expr::Scalar(ScalarCall::new(name, self.list("arguments")?)))
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

fn literal_kind(kind: &str) -> Option<LiteralKind> {
    Some(match kind {
        "literal_null" => LiteralKind::Null,
        "literal_bool" => LiteralKind::Bool,
        "literal_string" => LiteralKind::String,
        "literal_double" => LiteralKind::Double,
        "literal_exactnumeric" => LiteralKind::ExactNumeric,
        "literal_date" => LiteralKind::Date,
        "literal_timestamp" => LiteralKind::Timestamp,
        "literal_timestamp_utc" => LiteralKind::TimestampUtc,
        "literal_timestamputc" => LiteralKind::LocalTimeZoneTimestamp,
        _ => return None,
    })
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

#[cfg(test)]
#[path = "parse_tests.rs"]
mod tests;
