//! Declares and reads a column of every mapped type on each Parquet-file source and checks
//! the declared Exasol type and the returned values or the refusal against `MATRIX`.
#![cfg(feature = "exasol-e2e")]

mod common;

use common::e2e_harness::{
    VsProps, create_schema_and_scripts, create_virtual_schema, create_virtual_schema_with_password,
    exa_conn, install_slc, upload_so,
};
use common::stack::{
    CatalogConnectionPassword, iceberg_catalog_url, minio_url_internal, wait_for_exasol,
    wait_for_iceberg_catalog, wait_for_minio,
};
use common::type_matrix::{
    DIRECT_BASE, ICEBERG_NAMESPACE, Source, assert_no_mismatches, matrix_mismatches,
    seed_iceberg_tables, write_direct_storage_fixtures,
};

use std::sync::OnceLock;

const VS_DIRECT_STORAGE: &str = "TYPE_MATRIX_DS";
const VS_ICEBERG: &str = "TYPE_MATRIX_ICEBERG";
const CONN_DIRECT_STORAGE: &str = "TYPE_MATRIX_DS_CREDS";

fn direct_storage_password() -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        endpoint: minio_url_internal(),
        region: "us-east-1".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
        path_style: true,
        ..Default::default()
    }
}

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_minio();
        wait_for_iceberg_catalog();

        write_direct_storage_fixtures();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(seed_iceberg_tables(
                &iceberg_catalog_url(),
                "s3://warehouse/",
            ))
            .expect("seed the Iceberg type-matrix tables");

        install_slc();
        upload_so();
        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);

        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_DIRECT_STORAGE, "")
                .with_catalog_kind("DIRECT_STORAGE")
                .with_catalog_conn_name(CONN_DIRECT_STORAGE),
            DIRECT_BASE,
            &direct_storage_password(),
        );
        create_virtual_schema(&mut conn, &VsProps::new(VS_ICEBERG, ICEBERG_NAMESPACE));
    });
}

fn check_source(source: Source, vs_name: &str) -> Vec<String> {
    setup();
    matrix_mismatches(&mut exa_conn(), source, vs_name)
}

/// Scenario: Every mapped type declares and returns as its matrix row states on each Parquet-file source
#[test]
fn direct_storage_type_matrix_matches_every_row() {
    assert_no_mismatches(check_source(Source::DirectStorage, VS_DIRECT_STORAGE));
}

/// Scenario: Every mapped type declares and returns as its matrix row states on each Parquet-file source
#[test]
fn iceberg_type_matrix_matches_every_row() {
    assert_no_mismatches(check_source(Source::Iceberg, VS_ICEBERG));
}
