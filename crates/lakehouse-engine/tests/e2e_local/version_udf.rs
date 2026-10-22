use crate::common::e2e_harness::*;
use crate::fixtures::stack;

#[test]
fn version_udf_returns_the_compiled_crate_version() {
    stack();
    let mut conn = exa_conn();

    let resp = conn.execute(&format!(
        "SELECT {SCHEMA_NAME}.{VERSION_SCRIPT_NAME}() AS REPORTED_VERSION"
    ));
    let value = resp["responseData"]["results"][0]["resultSet"]["data"][0][0]
        .as_str()
        .unwrap_or_else(|| panic!("expected a VARCHAR scalar from LAKEHOUSE_VERSION(), got {resp}"))
        .to_string();

    assert_eq!(
        value,
        env!("CARGO_PKG_VERSION"),
        "LAKEHOUSE_VERSION() must report the compiled crate version"
    );
}
