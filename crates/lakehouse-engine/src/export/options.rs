use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::spec::{ExportSpec, Parameter};

use super::refusal;

const TABLE_NAME: &str = "TABLE_NAME";
const WRITERS_PER_NODE: &str = "WRITERS_PER_NODE";
const SINGLE_COMMIT: &str = "SINGLE_COMMIT";

/// How the export spreads the source rows over writer invocations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WriterLayout {
    /// `SINGLE_COMMIT = 'true'`: one writer for the whole cluster.
    SingleCommit,
    /// `WRITERS_PER_NODE` writers per node, 1 when the option is absent.
    PerNode(usize),
}

/// An export statement the engine accepts: append-only, `AT` a named CONNECTION, with a
/// valid `TABLE_NAME`. It keeps only what the writer SELECT needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ExportOptions {
    pub(super) connection_name: String,
    pub(super) writers: WriterLayout,
}

impl ExportOptions {
    /// Fails with a `UdfError::User` that names the refused clause or option and its
    /// accepted form. It never echoes a value of an inline CONNECTION.
    pub(super) fn from_spec(spec: &ExportSpec) -> Result<Self, UdfError> {
        refuse_rewrite_clauses(spec)?;
        let connection_name = named_connection(spec)?;
        let with_options = WithOptions::collect(&spec.parameters)?;
        check_table_name(with_options.table_name)?;
        let writers = with_options.writer_layout()?;
        Ok(Self {
            connection_name,
            writers,
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
            "{clause} is not supported, because the export only appends rows; remove {clause} from the EXPORT statement"
        ))),
        None => Ok(()),
    }
}

fn named_connection(spec: &ExportSpec) -> Result<String, UdfError> {
    if let Some(name) = &spec.connection_name {
        return Ok(name.clone());
    }
    let problem = if spec.connection_information.is_some() {
        "an inline AT '<address>' USER '<user>' IDENTIFIED BY '<password>' clause is not supported"
    } else {
        "the EXPORT statement names no CONNECTION"
    };
    Err(refusal(format_args!(
        "{problem}; add AT <connection>, naming the CONNECTION object the virtual schema uses"
    )))
}

struct WithOptions<'a> {
    table_name: Option<&'a str>,
    writers_per_node: Option<&'a str>,
    single_commit: Option<&'a str>,
}

impl<'a> WithOptions<'a> {
    fn collect(parameters: &'a [Parameter]) -> Result<Self, UdfError> {
        let mut options = Self {
            table_name: None,
            writers_per_node: None,
            single_commit: None,
        };
        for parameter in parameters {
            let slot = match parameter.key.as_str() {
                TABLE_NAME => &mut options.table_name,
                WRITERS_PER_NODE => &mut options.writers_per_node,
                SINGLE_COMMIT => &mut options.single_commit,
                unknown => {
                    return Err(refusal(format_args!(
                        "unknown option {unknown}; the accepted options are {TABLE_NAME}, {WRITERS_PER_NODE}, and {SINGLE_COMMIT}"
                    )));
                }
            };
            *slot = Some(parameter.value.as_str());
        }
        Ok(options)
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

fn check_table_name(value: Option<&str>) -> Result<(), UdfError> {
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
    Ok(())
}

fn parse_writers_per_node(value: &str) -> Result<usize, UdfError> {
    let all_digits = !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit());
    match value.parse::<usize>() {
        Ok(writers) if all_digits && writers >= 1 => Ok(writers),
        _ => Err(refusal(format_args!(
            "{WRITERS_PER_NODE} = '{value}' is not an integer of at least 1 that fits in 64 bits, for example {WRITERS_PER_NODE} = '4'"
        ))),
    }
}

fn parse_single_commit(value: &str) -> Result<bool, UdfError> {
    if value.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if value.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(refusal(format_args!(
            "{SINGLE_COMMIT} = '{value}' is not 'true' or 'false'"
        )))
    }
}

#[cfg(test)]
#[path = "options_tests.rs"]
mod tests;
