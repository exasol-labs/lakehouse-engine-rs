//! Pruning case table over one fixture on Iceberg and Hive direct storage, checked against a
//! native oracle that names each row's file. FAILS (never skips) when the stack is unreachable.
#![cfg(feature = "exasol-e2e")]

mod common;

use common::e2e_harness::{
    SCAN_SCRIPT_NAME, VsProps, create_schema_and_scripts, create_virtual_schema,
    create_virtual_schema_with_password, exa_conn, install_slc, isolated_pushdown_statement,
    local_stack_s3_store, parse_int, resolve_fixture_files, split_s3_bucket_and_key, upload_so,
    value_to_string,
};
use common::exasol_ws::ExaConn;
use common::pruning_fixture::{
    K_SPLIT, PAGE_SPLIT_FILES, PRUNING_FILES, PRUNING_HIVE_BASE, PRUNING_NAMESPACE,
    PRUNING_ORACLE_TABLE, PRUNING_TABLE, ROW_GROUP_SPLIT_FILES, hive_object_uri, labels_named,
    oracle_statements, write_pruning_hive_copy,
};
use common::seed::seed_pruning_cases;
use common::stack::{
    CatalogConnectionPassword, iceberg_catalog_url, local_stack_connection_password,
    wait_for_exasol, wait_for_iceberg_catalog, wait_for_seaweedfs,
};

use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectStorePath;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};
use parquet::file::statistics::Statistics;

use std::collections::BTreeSet;
use std::sync::OnceLock;

use Placement::{OuterWhere, Scan};

const VS_ICEBERG: &str = "PRUNING_ICEBERG";
const VS_HIVE: &str = "PRUNING_HIVE";
const CONN_HIVE: &str = "PRUNING_HIVE_CREDS";

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_seaweedfs();
        wait_for_iceberg_catalog();

        runtime().block_on(async {
            seed_pruning_cases(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed the Iceberg pruning fixture")
        });
        write_pruning_hive_copy();

        install_slc();
        upload_so();
        let mut conn = exa_conn();
        for statement in oracle_statements() {
            conn.execute(&statement);
        }
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn, &VsProps::new(VS_ICEBERG, PRUNING_NAMESPACE));
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_HIVE, "")
                .with_catalog_kind("DIRECT_STORAGE")
                .with_catalog_conn_name(CONN_HIVE),
            PRUNING_HIVE_BASE,
            &hive_storage_password(),
        );
    });
}

/// No `warehouse`: the direct-storage kind rejects that field.
fn hive_storage_password() -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        warehouse: String::new(),
        ..local_stack_connection_password()
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

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
enum Format {
    Iceberg,
    Hive,
}

impl Format {
    fn table(self) -> String {
        let vs = match self {
            Format::Iceberg => VS_ICEBERG,
            Format::Hive => VS_HIVE,
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

fn read_footer(uri: &str) -> ParquetMetaData {
    let (bucket, key) = split_s3_bucket_and_key(uri);
    let store = local_stack_s3_store(bucket);
    let bytes = runtime().block_on(async {
        store
            .get(&ObjectStorePath::from(key))
            .await
            .unwrap_or_else(|e| panic!("GET {uri} from SeaweedFS: {e}"))
            .bytes()
            .await
            .unwrap_or_else(|e| panic!("read bytes of {uri}: {e}"))
    });
    let options = ReadOptionsBuilder::new().with_page_index().build();
    SerializedFileReader::new_with_options(bytes, options)
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

/// Scenario: Pruning keeps every file with a matching row for every filter shape
#[test]
fn pruning_fixture_files_hold_the_row_groups_and_pages_the_cases_need() {
    setup();

    let iceberg: Vec<String> = runtime()
        .block_on(resolve_fixture_files(PRUNING_NAMESPACE, PRUNING_TABLE))
        .into_iter()
        .map(|file| file.path)
        .collect();
    let hive: Vec<String> = PRUNING_FILES.iter().map(hive_object_uri).collect();
    let every_label: BTreeSet<String> = PRUNING_FILES
        .iter()
        .map(|file| file.label.to_string())
        .collect();

    for (format, uris) in [("Iceberg", &iceberg), ("Hive", &hive)] {
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
}

/// An expected label set is given only for a format that translates the predicate fully,
/// because a partly translated predicate is held to soundness alone (`pushdown-file-pruning`).
struct Case {
    predicate: &'static str,
    placement: Placement,
    counts: bool,
    iceberg: Option<&'static [&'static str]>,
    hive: Option<&'static [&'static str]>,
}

impl Case {
    const fn new(predicate: &'static str, placement: Placement) -> Self {
        Case {
            predicate,
            placement,
            counts: false,
            iceberg: None,
            hive: None,
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

    fn expected(&self, format: Format) -> Option<BTreeSet<String>> {
        let labels = match format {
            Format::Iceberg => self.iceberg,
            Format::Hive => self.hive,
        }?;
        Some(labels.iter().map(|label| label.to_string()).collect())
    }
}

const ALL_FILES: &[&str] = &["a1", "a2", "b1", "b2", "n1", "n2"];

/// Iceberg sets follow iceberg-rust 0.10 (`!=`/`NOT IN` keep a NULL partition and every file's
/// statistics; an all-NULL column fails comparisons). Hive sets follow three-valued logic.
const CASES: &[Case] = &[
    Case::new("NOT (K < 30 AND S LIKE 'x%')", Scan).counted(),
    Case::new("NOT (S LIKE 'x%' AND K < 30)", Scan).counted(),
    Case::new("NOT (P = 'a' AND ABS(X) > 0.1)", Scan).counted(),
    Case::new("NOT (SECOND(TS, 3) > 1 AND P = 'b')", OuterWhere).counted(),
    Case::new("NOT (K < 30 AND SECOND(TS, 3) > 1)", OuterWhere).counted(),
    Case::new("NOT ((K < 30 OR P = 'a') AND S LIKE 'x%')", Scan),
    Case::new("NOT (K >= 10 AND K <= 20)", Scan).iceberg(&["a2", "b1", "n1", "n2"]),
    Case::new("NOT (K > 9 AND K < 21)", Scan).iceberg(&["a2", "b1", "n1", "n2"]),
    Case::new("NOT (K < 30 AND P = 'a')", Scan).iceberg(ALL_FILES),
    Case::new("NOT (P > 'a' AND P < 'c')", Scan)
        .iceberg(&["a1", "a2"])
        .hive(&["a1", "a2"]),
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
        .hive(&[]),
    Case::new("NOT (K IN (10, 20))", Scan).iceberg(ALL_FILES),
    Case::new("NOT (P IN ('a', 'b') OR P IS NULL)", Scan)
        .iceberg(&[])
        .hive(&[]),
    Case::new("NOT (P BETWEEN 'b' AND 'c')", Scan)
        .iceberg(&["a1", "a2"])
        .hive(&["a1", "a2"]),
    Case::new("NOT (K BETWEEN 10 AND 20.5)", Scan),
    Case::new("NOT (P IS NULL)", Scan)
        .iceberg(&["a1", "a2", "b1", "b2"])
        .hive(&["a1", "a2", "b1", "b2"]),
    Case::new("NOT (K IS NULL)", Scan).iceberg(&["a1", "a2", "b1", "n1", "n2"]),
    Case::new("NOT (P IS NOT NULL)", Scan)
        .iceberg(&["n1", "n2"])
        .hive(&["n1", "n2"]),
    Case::new("NOT (K IS NOT NULL)", Scan).iceberg(&["a2", "b2"]),
    Case::new("NOT (K <= 1000)", Scan).iceberg(&[]),
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

fn assert_cases_hold(format: Format) {
    setup();
    let mut conn = exa_conn();
    let failures: Vec<String> = CASES
        .iter()
        .filter_map(|case| case_failures(&mut conn, format, case))
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {} pruning cases failed:\n{}",
        failures.len(),
        CASES.len(),
        failures.join("\n")
    );
}

/// Scenario: Pruning keeps every file with a matching row for every filter shape
/// Scenario: A NOT over a partly translated predicate keeps every file with a matching row
/// Scenario: A NOT over a fully translated predicate still prunes
#[test]
fn iceberg_pruning_keeps_every_file_with_a_matching_row() {
    assert_cases_hold(Format::Iceberg);
}

/// Scenario: A predicate on partition columns prunes files before their footers are read
#[test]
fn hive_partition_pruning_keeps_every_file_with_a_matching_row() {
    assert_cases_hold(Format::Hive);
}
