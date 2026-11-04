use super::EXPORT_SCRIPT_NAME;
use super::options::WriterLayout;

/// Exasol distributes groups round-robin up to this count; above it they are
/// hash-partitioned (unbalanced).
const MAX_WRITER_COUNT: usize = 300;

/// `TYPEOF` patterns of the types Exasol does not pass to a UDF reliably.
const UNSUPPORTED_TYPE_PATTERNS: [&str; 4] = [
    "GEOMETRY%",
    "HASHTYPE%",
    "INTERVAL%",
    "%WITH LOCAL TIME ZONE",
];

/// The SELECT the export hook answers with. It calls the export script itself as the
/// writer, so `run` sees the writer spec first and then the source columns.
pub(super) struct WriterSelect<'a> {
    pub(super) script_schema: &'a str,
    pub(super) writer_spec: &'a str,
    /// Quoted column references as Exasol passes them, for example `"T"."A"`.
    pub(super) source_columns: &'a [String],
    pub(super) writers: WriterLayout,
    pub(super) node_count: usize,
}

impl WriterSelect<'_> {
    /// Exasol binds `DUAL` to the export source. Every source row passes a type guard and
    /// reaches exactly one of G writers through the `MOD(ROWNUM, G)` group key.
    pub(super) fn render(&self) -> String {
        let arguments = std::iter::once(string_literal(self.writer_spec))
            .chain(self.source_columns.iter().cloned())
            .collect::<Vec<_>>()
            .join(", ");
        let guards = self
            .source_columns
            .iter()
            .map(|column| type_guard(column))
            .collect::<Vec<_>>()
            .join(" AND ");
        // The key stays top-level: an export SELECT over a subquery of DUAL fails.
        format!(
            "SELECT {}.{}({arguments}) FROM DUAL WHERE {guards} GROUP BY MOD(ROWNUM, {})",
            quote_identifier(self.script_schema),
            quote_identifier(EXPORT_SCRIPT_NAME),
            self.group_count(),
        )
    }

    fn group_count(&self) -> usize {
        match self.writers {
            WriterLayout::SingleCommit => 1,
            WriterLayout::PerNode(per_node) => writer_count(self.node_count, per_node),
        }
    }
}

fn writer_count(node_count: usize, per_node: usize) -> usize {
    node_count
        .saturating_mul(per_node)
        .clamp(1, MAX_WRITER_COUNT)
}

// Exasol has no error function, so the guard fails the cast of its message. The message
// concatenates TYPEOF, because Exasol folds a constant CAST eagerly and fails every export.
fn type_guard(column: &str) -> String {
    let column_type = format!("TYPEOF({column})");
    let unsupported = UNSUPPORTED_TYPE_PATTERNS
        .iter()
        .map(|pattern| format!("{column_type} LIKE {}", string_literal(pattern)))
        .collect::<Vec<_>>()
        .join(" OR ");
    let message_head = string_literal(&format!(
        "{EXPORT_SCRIPT_NAME}: column \"{}\" has type ",
        column_display_name(column)
    ));
    let message_tail =
        string_literal(", which the export does not support; cast it to a supported type");
    format!(
        "(CASE WHEN {unsupported} THEN CAST({message_head} || {column_type} || {message_tail} \
         AS DECIMAL(1,0)) ELSE 0 END) = 0"
    )
}

/// The last quoted identifier of `reference`, unescaped; `reference` itself when it holds
/// no quoted identifier.
fn column_display_name(reference: &str) -> String {
    let mut last = None;
    let mut chars = reference.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '"' {
            continue;
        }
        let mut identifier = String::new();
        while let Some(c) = chars.next() {
            match c {
                '"' if chars.peek() == Some(&'"') => {
                    chars.next();
                    identifier.push('"');
                }
                '"' => break,
                c => identifier.push(c),
            }
        }
        last = Some(identifier);
    }
    last.unwrap_or_else(|| reference.to_string())
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn string_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

#[cfg(test)]
#[path = "sql_tests.rs"]
mod tests;
