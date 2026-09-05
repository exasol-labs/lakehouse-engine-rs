//! Permanent regression coverage for the credential-exposure fix (#135, #378):
//! the CONNECTION's storage credential must never reach the generated pushdown
//! SQL, and the scan must resolve it through its own grant-gated
//! `ctx.connection()` read instead.
//!
//! # Why this binary provisions a NON-DBA virtual-schema owner
//!
//! Exasol evaluates `ACCESS ON CONNECTION ... FOR SCRIPT` against the VIRTUAL
//! SCHEMA OWNER when the script is reached through VS-rewritten pushdown SQL —
//! the only path a `SELECT ... FROM <vs>.<table>` query takes. Verified live on
//! 2025.2.1 in both directions: a `SELECT`-only user holding no connection
//! privilege at all queries the VS fine; revoking the OWNER's grant breaks that
//! same user's query; granting it to the querying user while it is revoked from
//! the owner does NOT restore it.
//!
//! Every other `exasol-e2e` binary provisions as `sys`, a DBA. A DBA holds every
//! CONNECTION implicitly, and Exasol refuses `GRANT ACCESS ON CONNECTION ... TO
//! SYS` outright (`cannot grant connections to SYS`, SQL state `42500`), so a
//! revoke test written against a `sys`-owned VS could never observe a denial —
//! it would pass on a vulnerable build. This binary therefore stands up a real
//! deployment shape: a non-DBA user that owns the CONNECTION and the Virtual
//! Schema, plus a separate least-privilege reader. The revoke half is issued
//! against the OWNER, which is what actually gates the check.
//!
//! # Why absence of the credential is a non-vacuous assertion here
//!
//! The credential values are the harness's own (`minioadmin`), read from
//! `local_stack_connection_password()` rather than hard-coded, so the assertions
//! cannot drift from what the CONNECTION actually carries. The same values were
//! captured live in `PUSHDOWN_SQL` on the pre-fix build (`"access_key":
//! "minioadmin","secret_key":"minioadmin"` inside a `"storage":{"s3":{...}}`
//! block), so their absence is a real change of state, not an empty check. Three
//! further guards keep it that way:
//!
//! 1. A POSITIVE control before every absence assertion: the plan must name the
//!    CONNECTION in the reference wire form (`"connection":{"name":...}`).
//! 2. Structural absence: the JSON keys `"access_key"`, `"secret_key"`,
//!    `"session_token"` and the inline-backend tag `"s3":{` must all be gone. A
//!    regression to an inline backend puts them straight back.
//! 3. The revoke half: if the scan were NOT resolving the CONNECTION at scan
//!    time, revoking the owner's grant could not break the query at all.
//!
//! Note `"s3":{` and not `"s3:` — `table_root` is an `s3://` URI, so the shorter
//! token is present legitimately (confirmed live).
//!
//! # What this binary does NOT assert, and why
//!
//! * **`EXA_USER_PROFILE_LAST_DAY.SQL_TEXT` and `EXA_DBA_AUDIT_SQL.SQL_TEXT`.**
//!   Neither ever carries the VS-rewritten pushdown SQL — only the user's own
//!   literal statement (verified live; audit is additionally unreachable to a
//!   `SELECT`-only user, SQL state `42500`). An absence assertion against them
//!   would pass on a vulnerable build.
//! * **The sealed vended envelope (#378).** Vending needs a catalog that vends;
//!   the local Iceberg REST fixture does not. `e2e_lakekeeper_test.rs` owns the
//!   vended path against the Lakekeeper overlay, which `make test-e2e` does not
//!   bring up. The unit-level sealed-envelope positive control lives in
//!   `pushdown_tests.rs`.
//!
//! All tests FAIL (never skip) when the stack is unavailable — per project rules.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::seed::{E2E_NAMESPACE, E2E_TABLE, SEED_ROWS_SCORE_GT_15, SEED_TOTAL_ROWS, seed_events};
use common::stack::{
    CatalogConnectionPassword, exasol_host, exasol_sql_port, iceberg_catalog_url,
    iceberg_catalog_url_internal, local_stack_connection_password, wait_for_exasol,
    wait_for_iceberg_catalog, wait_for_minio,
};

use std::sync::{Mutex, MutexGuard, OnceLock};

/// Dedicated CONNECTION name. NOT the shared `LAKEHOUSE_CATALOG_CREDS`: this
/// binary revokes grants on it, and a sibling binary's
/// `CREATE OR REPLACE CONNECTION` would drop them out from under the assertions.
const CONN_NAME: &str = "CREDEXP_CATALOG_CREDS";
/// Dedicated Virtual Schema, owned by `OWNER_USER` rather than by `sys`.
const VS_NAME: &str = "CREDEXP_VS";

/// The non-DBA principal that owns the CONNECTION and the Virtual Schema, and
/// whose grant the pushdown-path check is actually evaluated against.
const OWNER_USER: &str = "CREDEXP_OWNER";
const OWNER_PASSWORD: &str = "CredExpOwner2026x";

/// The least-privilege reader: `CREATE SESSION` plus `SELECT` on the Virtual
/// Schema, and nothing else — no connection privilege, no role, no `EXECUTE`.
const READER_USER: &str = "CREDEXP_READER";
const READER_PASSWORD: &str = "CredExpReader2026x";

static SETUP_DONE: OnceLock<()> = OnceLock::new();

/// Serializes the tests in this binary against each other.
///
/// `make test-e2e` already passes `--test-threads=1`, but this binary is the one
/// whose tests MUTATE shared server-side state — they revoke and re-grant the
/// owner's connection access on the one CONNECTION all four share. Under a bare
/// `cargo test --test e2e_credential_exposure_test` libtest would run them in
/// parallel, and a revoke in one test would surface as a spurious denial (or a
/// spurious grant) in another. Correctness here must not depend on how the
/// binary was invoked.
static SERIAL: Mutex<()> = Mutex::new(());

/// Take the serialization lock, recovering from poisoning.
///
/// A failing test panics while holding the guard, which poisons the mutex. The
/// state it mutated is already restored by then (every mutation is undone before
/// its assertions run), so the next test should report its own outcome rather
/// than a cascade of poison errors that hide the first real failure.
fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The CONNECTION password this binary provisions — the harness's own, so the
/// sentinel values the assertions grep for cannot drift from the credential the
/// CONNECTION actually carries.
fn catalog_password() -> CatalogConnectionPassword {
    local_stack_connection_password()
}

fn owner_conn() -> ExaConn {
    ExaConn::connect(
        &exasol_host(),
        exasol_sql_port(),
        OWNER_USER,
        OWNER_PASSWORD,
    )
}

fn reader_conn() -> ExaConn {
    ExaConn::connect(
        &exasol_host(),
        exasol_sql_port(),
        READER_USER,
        READER_PASSWORD,
    )
}

fn vs_table() -> String {
    format!("{VS_NAME}.{}", E2E_TABLE.to_uppercase())
}

/// `GRANT` the scan script's connection access on the OWNER, issued BY the
/// owner — it owns the CONNECTION, so it needs no DBA to change its own grants
/// (verified live).
fn grant_owner_scan_access(owner: &mut ExaConn) {
    owner.execute(&format!(
        "GRANT ACCESS ON CONNECTION {CONN_NAME} FOR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME} \
         TO {OWNER_USER}"
    ));
}

/// `REVOKE` the scan script's connection access on the OWNER, issued BY the
/// owner — the symmetric counterpart of [`grant_owner_scan_access`].
fn revoke_owner_scan_access(owner: &mut ExaConn) {
    owner.execute(&format!(
        "REVOKE ACCESS ON CONNECTION {CONN_NAME} FOR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME} \
         FROM {OWNER_USER}"
    ));
}

fn setup_e2e() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_minio();
        wait_for_iceberg_catalog();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async {
            seed_events(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed Iceberg events table");
        });

        install_slc();
        upload_so();

        let mut sys = exa_conn();
        create_schema_and_scripts(&mut sys);

        // Tear down a previous run's objects in dependency order. `DROP USER
        // ... CASCADE` takes the Virtual Schema with it but leaves the
        // CONNECTION standing (verified live), so the CONNECTION is dropped
        // explicitly — and before the owner is re-created, since a fresh owner
        // could not `CREATE OR REPLACE` a CONNECTION it does not own.
        let _ = sys.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {VS_NAME} CASCADE"));
        sys.execute(&format!("DROP USER IF EXISTS {READER_USER} CASCADE"));
        sys.execute(&format!("DROP USER IF EXISTS {OWNER_USER} CASCADE"));
        sys.execute(&format!("DROP CONNECTION IF EXISTS {CONN_NAME}"));

        // The non-DBA owner. `CREATE CONNECTION` lets it own the CONNECTION and
        // therefore issue and revoke its own script-scoped grants;
        // `CREATE VIRTUAL SCHEMA` plus `EXECUTE` on the adapter are what a real
        // non-DBA deployment needs (docs/security.md § What a non-DBA installer
        // needs).
        sys.execute(&format!(
            "CREATE USER {OWNER_USER} IDENTIFIED BY \"{OWNER_PASSWORD}\""
        ));
        sys.execute(&format!("GRANT CREATE SESSION TO {OWNER_USER}"));
        sys.execute(&format!("GRANT CREATE CONNECTION TO {OWNER_USER}"));
        sys.execute(&format!("GRANT CREATE VIRTUAL SCHEMA TO {OWNER_USER}"));
        for script in [
            ADAPTER_SCRIPT_NAME,
            SCAN_SCRIPT_NAME,
            DISTRIBUTOR_SCRIPT_NAME,
        ] {
            sys.execute(&format!(
                "GRANT EXECUTE ON SCRIPT {SCHEMA_NAME}.{script} TO {OWNER_USER}"
            ));
        }

        sys.execute(&format!(
            "CREATE USER {READER_USER} IDENTIFIED BY \"{READER_PASSWORD}\""
        ));
        sys.execute(&format!("GRANT CREATE SESSION TO {READER_USER}"));

        // The CONNECTION, its two owner grants, and the Virtual Schema, all
        // created BY the owner through the shared harness helper — so the owner
        // owns the VS and the harness's own grant step (task 7.2) is what makes
        // the scan resolvable. The helper issues the grants BEFORE
        // `CREATE VIRTUAL SCHEMA`, which is mandatory: the adapter resolves the
        // CONNECTION while the Virtual Schema is being created.
        let mut owner = owner_conn();
        create_virtual_schema_with_password(
            &mut owner,
            &VsProps::new(VS_NAME, E2E_NAMESPACE).with_catalog_conn_name(CONN_NAME),
            &iceberg_catalog_url_internal(),
            &catalog_password(),
        );
        owner.execute(&format!(
            "GRANT SELECT ON SCHEMA {VS_NAME} TO {READER_USER}"
        ));
    });
}

/// The least-privilege reader gets the SAME rows the owner does, holding no
/// connection privilege whatsoever.
///
/// This is the gate's positive half and the premise of every absence assertion
/// below: the credential really did reach MinIO (which refuses anonymous reads),
/// so the scan resolved the CONNECTION at scan time from a plan that carries no
/// credential.
#[test]
fn a_least_privilege_reader_gets_the_owners_rows_without_a_connection_grant() {
    let _serial = serial();
    setup_e2e();

    let count_sql = format!("SELECT COUNT(*) FROM {}", vs_table());
    let filtered_sql = format!("SELECT COUNT(*) FROM {} WHERE SCORE > 15.0", vs_table());

    let owner_total = owner_conn().query_scalar_i64(&count_sql);
    let owner_filtered = owner_conn().query_scalar_i64(&filtered_sql);
    assert_eq!(
        owner_total, SEED_TOTAL_ROWS as i64,
        "the owner must see every seeded row"
    );
    assert_eq!(
        owner_filtered, SEED_ROWS_SCORE_GT_15 as i64,
        "the owner must see every seeded row above the score bound"
    );

    let mut reader = reader_conn();
    assert_eq!(
        reader.query_scalar_i64(&count_sql),
        owner_total,
        "a SELECT-only reader holding NO connection privilege must get the \
         owner's own row count — the scan resolves the CONNECTION against the \
         VS OWNER's grant, not the reader's"
    );
    assert_eq!(
        reader.query_scalar_i64(&filtered_sql),
        owner_filtered,
        "a SELECT-only reader must get the owner's own filtered row count"
    );

    // The reader really is least-privilege: no connection privilege, no role.
    let mut sys = exa_conn();
    assert_eq!(
        sys.query_row_count(&format!(
            "SELECT GRANTEE FROM EXA_DBA_CONNECTION_PRIVS WHERE GRANTEE = '{READER_USER}'"
        )),
        0,
        "the reader must hold no connection privilege, or this binary's \
         revoke half proves nothing"
    );
    assert_eq!(
        sys.query_row_count(&format!(
            "SELECT GRANTED_ROLE FROM EXA_DBA_ROLE_PRIVS WHERE GRANTEE = '{READER_USER}'"
        )),
        0,
        "the reader must hold no role, or a role-carried connection grant \
         could mask the owner-scoped check"
    );
}

/// `EXPLAIN VIRTUAL`, read by the least-privilege reader itself, names the
/// CONNECTION and carries neither credential value nor any inline storage key.
///
/// The reader is the security-relevant principal: `EXPLAIN VIRTUAL` needs no
/// privilege beyond `SELECT` on the Virtual Schema, so this is exactly the read
/// #135 reports. `explain_virtual_sql` flattens all four returned columns
/// (`PUSHDOWN_ID`, `PUSHDOWN_SQL`, `PUSHDOWN_JSON`, `PUSHDOWN_INVOLVED_TABLES`),
/// so `PUSHDOWN_JSON`'s echo of the same `sql` value is covered too.
#[test]
fn the_readers_pushdown_plan_names_the_connection_and_carries_no_credential() {
    let _serial = serial();
    setup_e2e();
    let password = catalog_password();

    let plan = explain_virtual_sql(
        &mut reader_conn(),
        &format!(
            "SELECT ID, NAME, SCORE FROM {} WHERE SCORE > 15.0",
            vs_table()
        ),
    );

    // Positive controls first: a plan that failed to render, or rendered without
    // reaching the scan script, must not let the absence assertions pass.
    assert!(
        !plan.trim().is_empty(),
        "EXPLAIN VIRTUAL must return a non-empty plan"
    );
    assert!(
        plan.contains(SCAN_SCRIPT_NAME),
        "the plan must drive the scan script, got:\n{plan}"
    );
    assert!(
        plan.contains(&format!(r#""connection":{{"name":"{CONN_NAME}""#)),
        "the plan must carry the CONNECTION by NAME in the reference wire form, got:\n{plan}"
    );

    // The credential values themselves — the exact strings the pre-fix build
    // put in this same plan.
    for value in [&password.access_key, &password.secret_key] {
        assert!(
            !value.is_empty(),
            "the provisioned CONNECTION must carry a non-empty credential, or \
             the absence assertion below is vacuous"
        );
        assert!(
            !plan.contains(value.as_str()),
            "the pushdown plan must not carry the credential value {value:?}, got:\n{plan}"
        );
    }

    // Structural absence: a regression to an inline backend restores these keys
    // whatever the credential happens to be.
    for key in [
        r#""access_key""#,
        r#""secret_key""#,
        r#""session_token""#,
        r#""s3":{"#,
        r#""inline":"#,
    ] {
        assert!(
            !plan.contains(key),
            "the pushdown plan must not carry the inline-storage token {key} — \
             storage travels as a CONNECTION reference, got:\n{plan}"
        );
    }
}

/// Revoking the OWNER's scan-script connection grant breaks the READER's query
/// with the scan's own named error, carrying no credential value; re-granting
/// restores it.
///
/// The reader's privileges are untouched throughout — the owner's grant is the
/// only variable — which is what makes this the owner-scoped check and not a
/// per-user one. The grant is restored before any assertion runs, so a failing
/// assertion cannot leave the shared Virtual Schema broken for a later test.
#[test]
fn revoking_the_owners_scan_grant_denies_the_reader_without_leaking_the_credential() {
    let _serial = serial();
    setup_e2e();
    let password = catalog_password();
    let query = format!(
        "SELECT ID, NAME, SCORE FROM {} WHERE SCORE > 15.0",
        vs_table()
    );

    // Positive control: the query works before the revoke.
    assert_eq!(
        reader_conn().query_row_count(&query),
        SEED_ROWS_SCORE_GT_15 as i64,
        "the reader's query must succeed before the revoke, or the denial \
         below proves nothing"
    );

    let mut owner = owner_conn();
    revoke_owner_scan_access(&mut owner);
    let denied = reader_conn().try_execute(&query);
    // Restore BEFORE asserting: a failed assertion must not leave the VS broken.
    grant_owner_scan_access(&mut owner);

    assert_eq!(
        denied["status"].as_str(),
        Some("error"),
        "revoking the OWNER's scan grant must deny the reader's query, got: {denied}"
    );
    let msg = denied["exception"]["text"].as_str().unwrap_or("");
    assert!(
        msg.contains(CONN_NAME),
        "the denial must name the CONNECTION it could not access, got: {msg}"
    );
    assert!(
        msg.contains("GRANT ACCESS ON CONNECTION")
            && msg.contains(&format!("FOR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME}")),
        "the denial must name the grant the deployment is missing, RESOLVED to this \
         deployment's own scan script rather than a <schema> placeholder, got: {msg}"
    );
    assert!(
        msg.contains("OWNER of the virtual schema"),
        "the denial must name the VIRTUAL SCHEMA OWNER as the grantee — naming \
         the querying user instead is wrong operator guidance, got: {msg}"
    );
    for value in [&password.access_key, &password.secret_key] {
        assert!(
            !msg.contains(value.as_str()),
            "the denial must not carry the credential value {value:?}, got: {msg}"
        );
    }

    assert_eq!(
        reader_conn().query_row_count(&query),
        SEED_ROWS_SCORE_GT_15 as i64,
        "re-granting the OWNER's scan grant must restore the reader's query, \
         so the denial was the grant and nothing else"
    );
}

/// The grant that gates the reader's query is the OWNER's, not the reader's:
/// granting it to the READER while it is revoked from the OWNER still denies the
/// query.
///
/// This is the symmetric half of the proof, and the guard that keeps the
/// operator guidance in `docs/security.md` and `install.sh` honest. Without it,
/// a future Exasol version that switched to a per-querying-user check would make
/// both documents wrong with no test failing.
#[test]
fn a_grant_on_the_reader_is_no_substitute_for_the_owners_grant() {
    let _serial = serial();
    setup_e2e();
    let query = format!("SELECT COUNT(*) FROM {}", vs_table());

    let mut owner = owner_conn();
    owner.execute(&format!(
        "GRANT ACCESS ON CONNECTION {CONN_NAME} FOR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME} \
         TO {READER_USER}"
    ));
    revoke_owner_scan_access(&mut owner);
    let denied = reader_conn().try_execute(&query);
    // Restore BEFORE asserting, in both directions.
    grant_owner_scan_access(&mut owner);
    owner.execute(&format!(
        "REVOKE ACCESS ON CONNECTION {CONN_NAME} FOR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME} \
         FROM {READER_USER}"
    ));

    assert_eq!(
        denied["status"].as_str(),
        Some("error"),
        "a connection grant on the QUERYING USER must not substitute for the \
         OWNER's grant — the pushdown-path check reads the owner's privileges, \
         got: {denied}"
    );

    assert_eq!(
        reader_conn().query_scalar_i64(&query),
        SEED_TOTAL_ROWS as i64,
        "the restored owner grant must make the reader's query work again"
    );
}
