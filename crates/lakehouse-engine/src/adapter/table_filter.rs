//! The glob rules of `TABLE_INCLUDE` and `TABLE_EXCLUDE`, owned here so the value check and the
//! listing cannot disagree on what a pattern means.

const WILDCARD_RUN: char = '*';
const WILDCARD_ONE: char = '?';
const EXPECTED_LIST: &str = "a comma-separated list of non-empty table name patterns";

/// An uppercased glob over a virtual table name. Only `*` and `?` are special: `[`, `]`, and `\`
/// match themselves, which no glob crate allows without escaping every pattern first.
#[derive(Debug, PartialEq)]
pub(super) struct Pattern(Vec<char>);

impl Pattern {
    /// Matches the whole name. One backtrack point, the last `*`, bounds the run time to the
    /// product of the pattern and name lengths.
    fn matches(&self, name: &str) -> bool {
        let pattern = &self.0;
        let name: Vec<char> = name.chars().collect();
        let (mut p, mut n) = (0, 0);
        let mut last_run: Option<(usize, usize)> = None;
        while n < name.len() {
            match pattern.get(p) {
                Some(&WILDCARD_RUN) => {
                    p += 1;
                    last_run = Some((p, n));
                }
                Some(&c) if c == WILDCARD_ONE || c == name[n] => {
                    p += 1;
                    n += 1;
                }
                _ => {
                    let Some((run_p, run_n)) = last_run else {
                        return false;
                    };
                    p = run_p;
                    n = run_n + 1;
                    last_run = Some((run_p, n));
                }
            }
        }
        pattern[p..].iter().all(|&c| c == WILDCARD_RUN)
    }
}

/// Splits a property value into patterns, with the `Err` text the value check reports as the
/// expectation. Each entry is trimmed and uppercased with the fold of the virtual table name.
pub(super) fn parse_patterns(raw: &str) -> Result<Vec<Pattern>, String> {
    raw.split(',')
        .map(|entry| match entry.trim() {
            "" => Err(EXPECTED_LIST.to_string()),
            entry => Ok(Pattern(entry.to_uppercase().chars().collect())),
        })
        .collect()
}

/// Which virtual tables a schema exposes: every table, unless `TABLE_INCLUDE` narrows the set,
/// and never one that `TABLE_EXCLUDE` matches.
#[derive(Debug, Default, PartialEq)]
pub(super) struct TableFilter {
    include: Option<Vec<Pattern>>,
    exclude: Vec<Pattern>,
}

impl TableFilter {
    pub(super) fn new(include: Option<Vec<Pattern>>, exclude: Option<Vec<Pattern>>) -> Self {
        Self {
            include,
            exclude: exclude.unwrap_or_default(),
        }
    }

    /// `virtual_table_name` is the uppercased name that `flatten_table_name` returns.
    pub(super) fn exposes(&self, virtual_table_name: &str) -> bool {
        let included = self
            .include
            .as_ref()
            .is_none_or(|patterns| patterns.iter().any(|p| p.matches(virtual_table_name)));
        included && !self.exclude.iter().any(|p| p.matches(virtual_table_name))
    }
}

#[cfg(test)]
#[path = "table_filter_tests.rs"]
mod tests;
