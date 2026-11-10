//! The WITH options and clauses of an export statement.

use super::refusal;
use crate::exasol::{
    CATALOG_CONNECTION, catalog_connection_name, parse_boolean, parse_whole_number,
};
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::spec::{ExportSpec, Parameter};
use std::num::NonZeroUsize;

const TABLE_NAME: &str = "TABLE_NAME";
const WRITERS_PER_NODE: &str = "WRITERS_PER_NODE";
const SINGLE_COMMIT: &str = "SINGLE_COMMIT";
pub(super) const ALLOW_HTTP: &str = "ALLOW_HTTP";

/// How the export spreads the source rows over writer invocations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WriterLayout {
    /// `SINGLE_COMMIT = 'true'`: one writer for the whole cluster.
    SingleCommit,
    /// `WRITERS_PER_NODE` writers per node, 1 when the option is absent.
    PerNode(usize),
}

/// An export statement the engine accepts: no `TRUNCATE`, `REPLACE`, or `CREATED BY`, no `AT` clause, a
/// `CATALOG_CONNECTION` option, and a valid `TABLE_NAME`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ExportOptions {
    pub(super) connection_name: String,
    pub(super) table_name: String,
    pub(super) writers: WriterLayout,
    /// The user's consent to plain-HTTP requests, `false` unless `ALLOW_HTTP = 'true'`.
    pub(super) allow_http: bool,
}

impl ExportOptions {
    /// Fails with a `UdfError::User` that names the refused clause or option and its
    /// accepted form. It never echoes a value of an inline CONNECTION.
    pub(super) fn from_spec(spec: &ExportSpec) -> Result<Self, UdfError> {
        refuse_rewrite_clauses(spec)?;
        refuse_at_clause(spec)?;
        let with_options = WithOptions::collect(&spec.parameters)?;
        let connection_name = with_options.connection_name()?;
        let table_name = check_table_name(with_options.table_name)?.to_string();
        let writers = with_options.writer_layout()?;
        let allow_http = with_options
            .allow_http
            .map(parse_allow_http)
            .transpose()?
            .unwrap_or(false);
        Ok(Self {
            connection_name,
            table_name,
            writers,
            allow_http,
        })
    }
}

fn refuse_rewrite_clauses(spec: &ExportSpec) -> Result<(), UdfError> {
    let clauses = [
        (spec.has_truncate, "TRUNCATE"),
        (spec.has_replace, "REPLACE"),
        (spec.created_by.is_some(), "CREATED BY"),
    ];
    match clauses.into_iter().find(|(given, _)| *given) {
        Some((_, clause)) => Err(refusal(format_args!(
            "{clause} is not supported, because the export never truncates or replaces a table; remove {clause} from the EXPORT statement"
        ))),
        None => Ok(()),
    }
}

// `AT` makes Exasol check the CONNECTION privilege without handing the script the
// credentials, so the connection travels as a WITH option instead.
fn refuse_at_clause(spec: &ExportSpec) -> Result<(), UdfError> {
    if spec.connection_name.is_none() && spec.connection_information.is_none() {
        return Ok(());
    }
    Err(refusal(format_args!(
        "the AT clause is not supported; remove it and name the CONNECTION with {CATALOG_CONNECTION} = '<connection>' in the WITH options"
    )))
}

struct WithOptions<'a> {
    catalog_connection: Option<&'a str>,
    table_name: Option<&'a str>,
    writers_per_node: Option<&'a str>,
    single_commit: Option<&'a str>,
    allow_http: Option<&'a str>,
}

impl<'a> WithOptions<'a> {
    fn collect(parameters: &'a [Parameter]) -> Result<Self, UdfError> {
        let mut options = Self {
            catalog_connection: None,
            table_name: None,
            writers_per_node: None,
            single_commit: None,
            allow_http: None,
        };
        for parameter in parameters {
            let slot = match parameter.key.as_str() {
                CATALOG_CONNECTION => &mut options.catalog_connection,
                TABLE_NAME => &mut options.table_name,
                WRITERS_PER_NODE => &mut options.writers_per_node,
                SINGLE_COMMIT => &mut options.single_commit,
                ALLOW_HTTP => &mut options.allow_http,
                unknown => {
                    return Err(refusal(format_args!(
                        "unknown option {unknown}; the accepted options are {CATALOG_CONNECTION}, {TABLE_NAME}, {WRITERS_PER_NODE}, {SINGLE_COMMIT}, and {ALLOW_HTTP}"
                    )));
                }
            };
            *slot = Some(parameter.value.as_str());
        }
        Ok(options)
    }

    fn connection_name(&self) -> Result<String, UdfError> {
        catalog_connection_name(self.catalog_connection)
            .map(str::to_string)
            .ok_or_else(|| {
                refusal(format_args!(
                    "option {CATALOG_CONNECTION} is required; set {CATALOG_CONNECTION} = '<connection>', naming the CONNECTION object the virtual schema uses"
                ))
            })
    }

    fn writer_layout(&self) -> Result<WriterLayout, UdfError> {
        let per_node = self
            .writers_per_node
            .map(parse_writers_per_node)
            .transpose()?;
        let single_commit = self.single_commit.map(parse_single_commit).transpose()?;
        match (single_commit, per_node) {
            (Some(true), Some(_)) => Err(refusal(format_args!(
                "{WRITERS_PER_NODE} cannot be combined with {SINGLE_COMMIT} = 'true', which runs one writer for the whole cluster; remove one of the two options"
            ))),
            (Some(true), None) => Ok(WriterLayout::SingleCommit),
            (_, per_node) => Ok(WriterLayout::PerNode(per_node.unwrap_or(1))),
        }
    }
}

/// The `TABLE_NAME` value, when it is a namespace and a table name joined by `.`.
pub(super) fn check_table_name(value: Option<&str>) -> Result<&str, UdfError> {
    let form = "'<namespace>.<table>', a namespace of one or more '.'-separated levels and a table name, with no empty part";
    let Some(value) = value else {
        return Err(refusal(format_args!(
            "option {TABLE_NAME} is required; set {TABLE_NAME} = {form}"
        )));
    };
    let parts: Vec<&str> = value.split('.').collect();
    if parts.len() < 2 || parts.iter().any(|part| part.is_empty()) {
        return Err(refusal(format_args!(
            "{TABLE_NAME} = '{value}' is not of the form {form}"
        )));
    }
    Ok(value)
}

fn parse_writers_per_node(value: &str) -> Result<usize, UdfError> {
    parse_whole_number::<NonZeroUsize>(value)
        .map(NonZeroUsize::get)
        .map_err(|expected| {
            refusal(format_args!(
                "{WRITERS_PER_NODE} = '{value}' is not {expected}, for example {WRITERS_PER_NODE} = '4'"
            ))
        })
}

fn parse_allow_http(value: &str) -> Result<bool, UdfError> {
    parse_boolean(value).ok_or_else(|| {
        refusal(format_args!(
            "{ALLOW_HTTP} = '{value}' is not 'true' or 'false'"
        ))
    })
}

fn parse_single_commit(value: &str) -> Result<bool, UdfError> {
    parse_boolean(value).ok_or_else(|| {
        refusal(format_args!(
            "{SINGLE_COMMIT} = '{value}' is not 'true' or 'false'"
        ))
    })
}

#[cfg(test)]
#[path = "options_tests.rs"]
mod tests;
