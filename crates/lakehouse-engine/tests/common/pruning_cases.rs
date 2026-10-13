//! The pruning case table, checked against the native oracle that names each row's file. Each
//! binary provisions its format's copy of the fixture; these checks are shared by every format.

use super::e2e_harness::{
    SCAN_SCRIPT_NAME, isolated_pushdown_statement, parse_int, value_to_string,
};
use super::exasol_ws::ExaConn;
use super::pruning_fixture::{
    K_SPLIT, PAGE_SPLIT_FILES, PRUNING_FILES, PRUNING_ORACLE_TABLE, PRUNING_TABLE,
    ROW_GROUP_SPLIT_FILES, labels_named,
};
use super::raw_parquet::get_fixture_object;

use parquet::file::metadata::ParquetMetaData;
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};
use parquet::file::statistics::Statistics;

use std::collections::BTreeSet;

use Placement::{OuterWhere, Scan};

pub const VS_ICEBERG: &str = "PRUNING_ICEBERG";
pub const VS_HIVE: &str = "PRUNING_HIVE";
pub const VS_DELTA: &str = "PRUNING_DELTA";

#[derive(Clone, Copy, Debug)]
enum Placement {
    Scan,
    OuterWhere,
}

impl Placement {
    fn holds(self, plan: &str) -> bool {
        let scan_filter = plan.contains(SCAN_FILTER_MARKER);
        match self {
            Placement::Scan => scan_filter,
            Placement::OuterWhere => plan.contains(OUTER_WHERE_ALIAS) && !scan_filter,
        }
    }
}

const SCAN_FILTER_MARKER: &str = "\"filter\":\"";
const OUTER_WHERE_ALIAS: &str = "LHS_T0";

#[derive(Clone, Copy, Debug)]
pub enum Format {
    Iceberg,
    Hive,
    Delta,
}

impl Format {
    fn table(self) -> String {
        let vs = match self {
            Format::Iceberg => VS_ICEBERG,
            Format::Hive => VS_HIVE,
            Format::Delta => VS_DELTA,
        };
        format!("{vs}.{}", PRUNING_TABLE.to_uppercase())
    }
}

fn sorted_ids(conn: &mut ExaConn, sql: &str) -> Vec<i64> {
    let mut ids: Vec<i64> = conn.query_columns(sql)[0].iter().map(parse_int).collect();
    ids.sort();
    ids
}

fn oracle_labels(conn: &mut ExaConn, predicate: &str) -> BTreeSet<String> {
    conn.query_columns(&format!(
        "SELECT DISTINCT FILE_LABEL FROM {PRUNING_ORACLE_TABLE} WHERE {predicate}"
    ))[0]
        .iter()
        .map(value_to_string)
        .collect()
}

/// With its page index, so a caller can check the page layout.
pub fn read_footer(uri: &str) -> ParquetMetaData {
    let options = ReadOptionsBuilder::new().with_page_index().build();
    SerializedFileReader::new_with_options(get_fixture_object(uri), options)
        .unwrap_or_else(|e| panic!("open the Parquet footer of {uri}: {e}"))
        .metadata()
        .clone()
}

fn assert_pruning_shape(label: &str, uri: &str, metadata: &ParquetMetaData) {
    let cannot = "so the fixture cannot prove row-group or page pruning \
                  (specs/testing.md § Fixtures and § Correctness oracles)";
    let (Some(column_index), Some(offset_index)) =
        (metadata.column_index(), metadata.offset_index())
    else {
        panic!("{label} ({uri}) carries no column index or offset index, {cannot}");
    };
    if ROW_GROUP_SPLIT_FILES.contains(&label) {
        assert!(
            metadata.num_row_groups() >= 2,
            "{label} ({uri}) holds {} row group, {cannot}",
            metadata.num_row_groups()
        );
    }
    if !PAGE_SPLIT_FILES.contains(&label) {
        return;
    }
    let k = metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|column| column.name() == "k")
        .unwrap_or_else(|| panic!("{label} ({uri}) has no `k` column"));
    let pages = offset_index[0][k].page_locations().len();
    assert!(
        pages >= 2,
        "the first row group of {label} ({uri}) holds {pages} `k` page, {cannot}"
    );
    let ColumnIndexMetaData::INT64(k_index) = &column_index[0][k] else {
        panic!("{label} ({uri}) has no INT64 column index for `k`, {cannot}");
    };
    let first_page_max = *k_index
        .max_value(0)
        .unwrap_or_else(|| panic!("the first `k` page of {label} ({uri}) has no max"));
    let Some(Statistics::Int64(row_group_k)) = metadata.row_group(0).column(k).statistics() else {
        panic!("the first row group of {label} ({uri}) has no INT64 `k` statistics, {cannot}");
    };
    let row_group_max = *row_group_k
        .max_opt()
        .unwrap_or_else(|| panic!("the first row group of {label} ({uri}) has no `k` max"));
    assert!(
        first_page_max < K_SPLIT && row_group_max >= K_SPLIT,
        "the first row group of {label} ({uri}) must span {K_SPLIT} while its first page stays \
         below it, got page max {first_page_max} and row-group max {row_group_max}, {cannot}"
    );
}

/// Each URI names one fixture label, no label repeats, every label is present, and each file
/// holds the row groups and pages its cases need. `format` only names the copy in failures.
pub fn assert_one_shaped_file_per_label(format: &str, uris: &[String]) {
    let every_label: BTreeSet<String> = PRUNING_FILES
        .iter()
        .map(|file| file.label.to_string())
        .collect();
    let mut seen = BTreeSet::new();
    for uri in uris {
        let named = labels_named(uri);
        let [label] = named.iter().collect::<Vec<_>>()[..] else {
            panic!("{format} file {uri} must name exactly one fixture label, got {named:?}");
        };
        assert!(
            seen.insert(label.clone()),
            "{format} holds two files labelled {label}"
        );
        assert_pruning_shape(label, uri, &read_footer(uri));
    }
    assert_eq!(seen, every_label, "{format} must hold one file per label");
}

/// An expected label set is given only for a format that translates the predicate fully,
/// because a partly translated predicate is held to soundness alone (`pushdown-file-pruning`).
struct Case {
    predicate: &'static str,
    placement: Placement,
    counts: bool,
    iceberg: Option<&'static [&'static str]>,
    hive: Option<&'static [&'static str]>,
    delta: Option<&'static [&'static str]>,
}

impl Case {
    const fn new(predicate: &'static str, placement: Placement) -> Self {
        Case {
            predicate,
            placement,
            counts: false,
            iceberg: None,
            hive: None,
            delta: None,
        }
    }

    const fn counted(self) -> Self {
        Case {
            counts: true,
            ..self
        }
    }

    const fn iceberg(self, labels: &'static [&'static str]) -> Self {
        Case {
            iceberg: Some(labels),
            ..self
        }
    }

    const fn hive(self, labels: &'static [&'static str]) -> Self {
        Case {
            hive: Some(labels),
            ..self
        }
    }

    const fn delta(self, labels: &'static [&'static str]) -> Self {
        Case {
            delta: Some(labels),
            ..self
        }
    }

    fn expected(&self, format: Format) -> Option<BTreeSet<String>> {
        let labels = match format {
            Format::Iceberg => self.iceberg,
            Format::Hive => self.hive,
            Format::Delta => self.delta,
        }?;
        Some(labels.iter().map(ToString::to_string).collect())
    }
}

const ALL_FILES: &[&str] = &["a1", "a2", "b1", "b2", "n1", "n2"];

/// All-NULL columns fail comparisons. Iceberg: `!=`/`NOT IN` keep NULL partitions and all stats.
/// Delta: NULL partitions fail every comparison; `!=`/`NOT IN` prune. Hive: three-valued logic.
const CASES: &[Case] = &[
    Case::new("NOT (K < 30 AND S LIKE 'x%')", Scan).counted(),
    Case::new("NOT (S LIKE 'x%' AND K < 30)", Scan).counted(),
    Case::new("NOT (P = 'a' AND ABS(X) > 0.1)", Scan).counted(),
    Case::new("NOT (SECOND(TS, 3) > 1 AND P = 'b')", OuterWhere).counted(),
    Case::new("NOT (K < 30 AND SECOND(TS, 3) > 1)", OuterWhere).counted(),
    Case::new("NOT ((K < 30 OR P = 'a') AND S LIKE 'x%')", Scan),
    Case::new("NOT (K >= 10 AND K <= 20)", Scan)
        .iceberg(&["a2", "b1", "n1", "n2"])
        .delta(&["a2", "b1", "n1", "n2"]),
    Case::new("NOT (K > 9 AND K < 21)", Scan)
        .iceberg(&["a2", "b1", "n1", "n2"])
        .delta(&["a2", "b1", "n1", "n2"]),
    Case::new("NOT (K < 30 AND P = 'a')", Scan)
        .iceberg(ALL_FILES)
        .delta(&["a2", "b1", "b2", "n1", "n2"]),
    Case::new("NOT (P > 'a' AND P < 'c')", Scan)
        .iceberg(&["a1", "a2"])
        .hive(&["a1", "a2"])
        .delta(&["a1", "a2"]),
    Case::new("NOT (K < 10 OR S LIKE 'x%')", Scan),
    Case::new("NOT (P = 'b' OR ABS(X) > 0.1)", Scan),
    Case::new("NOT (SECOND(TS, 3) > 1 OR K > 40)", OuterWhere),
    Case::new("NOT (S LIKE 'x%' OR P = 'a')", Scan),
    Case::new("NOT (NOT (K < 30 AND S LIKE 'x%') OR P = 'b')", Scan),
    Case::new("NOT (NOT (P = 'a' AND ABS(X) > 0.1) AND K < 50)", Scan),
    Case::new("(K < 10 AND S LIKE 'x%') OR P = 'b'", Scan),
    Case::new("(P = 'a' AND SECOND(TS, 3) > 1) OR K > 40", OuterWhere),
    Case::new("NOT ((K < 30 AND S LIKE 'x%') OR P = 'b')", Scan),
    Case::new("NOT ((P = 'a' AND ABS(X) > 0.1) OR K > 40)", Scan),
    Case::new("NOT (P IN ('a', 'b'))", Scan)
        .iceberg(&["n1", "n2"])
        .hive(&[])
        .delta(&[]),
    Case::new("NOT (K IN (10, 20))", Scan)
        .iceberg(ALL_FILES)
        .delta(&["a1", "a2", "b1", "n1", "n2"]),
    Case::new("NOT (P IN ('a', 'b') OR P IS NULL)", Scan)
        .iceberg(&[])
        .hive(&[])
        .delta(&[]),
    Case::new("NOT (P BETWEEN 'b' AND 'c')", Scan)
        .iceberg(&["a1", "a2"])
        .hive(&["a1", "a2"])
        .delta(&["a1", "a2"]),
    Case::new("NOT (K BETWEEN 10 AND 20.5)", Scan),
    Case::new("NOT (P IS NULL)", Scan)
        .iceberg(&["a1", "a2", "b1", "b2"])
        .hive(&["a1", "a2", "b1", "b2"])
        .delta(&["a1", "a2", "b1", "b2"]),
    Case::new("NOT (K IS NULL)", Scan)
        .iceberg(&["a1", "a2", "b1", "n1", "n2"])
        .delta(&["a1", "a2", "b1", "n1", "n2"]),
    Case::new("NOT (P IS NOT NULL)", Scan)
        .iceberg(&["n1", "n2"])
        .hive(&["n1", "n2"])
        .delta(&["n1", "n2"]),
    Case::new("NOT (K IS NOT NULL)", Scan)
        .iceberg(&["a2", "b2"])
        .delta(&["a2", "b2"]),
    Case::new("NOT (K <= 1000)", Scan).iceberg(&[]).delta(&[]),
];

/// `None` when the case holds; otherwise every failed assertion, with the plan.
fn case_failures(conn: &mut ExaConn, format: Format, case: &Case) -> Option<String> {
    let predicate = case.predicate;
    let table = format.table();
    let select = format!("SELECT ID FROM {table} WHERE {predicate} ORDER BY ID");
    let mut failures = Vec::new();

    let rows = sorted_ids(conn, &select);
    let oracle_rows = sorted_ids(
        conn,
        &format!("SELECT ID FROM {PRUNING_ORACLE_TABLE} WHERE {predicate} ORDER BY ID"),
    );
    if rows != oracle_rows {
        failures.push(format!("returned ids {rows:?}, the oracle {oracle_rows:?}"));
    }
    if case.counts {
        let count =
            conn.query_scalar_i64(&format!("SELECT COUNT(*) FROM {table} WHERE {predicate}"));
        let oracle_count = conn.query_scalar_i64(&format!(
            "SELECT COUNT(*) FROM {PRUNING_ORACLE_TABLE} WHERE {predicate}"
        ));
        if count != oracle_count {
            failures.push(format!("COUNT(*) {count}, the oracle {oracle_count}"));
        }
    }

    let plan = isolated_pushdown_statement(conn, &select);
    let scanned = labels_named(&plan);
    let matching = oracle_labels(conn, predicate);
    if !matching.is_subset(&scanned) {
        failures.push(format!(
            "Sound: scanned {scanned:?}, but {matching:?} hold a matching row"
        ));
    }
    if let Some(expected) = case.expected(format)
        && scanned != expected
    {
        failures.push(format!(
            "Effective: scanned {scanned:?}, expected {expected:?}"
        ));
    }
    if plan.contains(SCAN_SCRIPT_NAME) == scanned.is_empty() {
        failures.push(format!(
            "the plan must name {SCAN_SCRIPT_NAME} exactly when it names a file, scanned {scanned:?}"
        ));
    }
    if !scanned.is_empty() && !case.placement.holds(&plan) {
        failures.push(format!(
            "the {:?} placement marker is missing",
            case.placement
        ));
    }

    (!failures.is_empty()).then(|| {
        format!(
            "{format:?} `{predicate}`:\n  - {}\n  plan: {plan}",
            failures.join("\n  - ")
        )
    })
}

/// Runs every case against `format`'s virtual schema, which the caller's setup has created,
/// and fails once with every failed case.
pub fn assert_cases_hold(conn: &mut ExaConn, format: Format) {
    let failures: Vec<String> = CASES
        .iter()
        .filter_map(|case| case_failures(conn, format, case))
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {} pruning cases failed:\n{}",
        failures.len(),
        CASES.len(),
        failures.join("\n")
    );
}
