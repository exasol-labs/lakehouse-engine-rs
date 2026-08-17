//! Pure convention test (no I/O): the SDK/SLC version has one source of truth,
//! the `exasol-udf-sdk` entry in the root `[workspace.dependencies]`, in the
//! one-line literal shape `make install-slc` and `deploy/scripts/install.sh`
//! both scrape. A stale second copy installs an SLC that rejects the `.so`.

const WORKSPACE_MANIFEST: &str = include_str!("../../../Cargo.toml");
const ENGINE_MANIFEST: &str = include_str!("../Cargo.toml");

/// Version of the SDK actually compiled in; its `build.rs` stamps the
/// fingerprint from its own `CARGO_PKG_VERSION`.
fn linked_sdk_version() -> &'static str {
    exasol_udf_sdk::abi::EXA_SDK_FINGERPRINT
        .split(':')
        .next()
        .expect("fingerprint always has a version field before the ':'")
}

/// Comment-stripped lines of `manifest` declaring `dep_name`. Anchored at line
/// start, since these manifests also cite crate names in prose.
fn declaration_lines<'m>(manifest: &'m str, dep_name: &str) -> Vec<&'m str> {
    manifest
        .lines()
        .map(|line| line.split('#').next().unwrap_or("").trim_end())
        .filter(|line| {
            line.strip_prefix(dep_name)
                .is_some_and(|rest| rest.trim_start().starts_with('='))
        })
        .collect()
}

/// Mirrors what the Makefile's `sed` and install.sh's bash regex each extract.
fn scraped_version(declaration: &str) -> Option<&str> {
    let after_key = declaration.split_once("version")?.1.split_once('"')?.1;
    after_key.split_once('"').map(|(version, _rest)| version)
}

#[test]
fn workspace_manifest_pins_the_sdk_version_scrapers_can_read() {
    let declarations = declaration_lines(WORKSPACE_MANIFEST, "exasol-udf-sdk");

    assert_eq!(
        declarations.len(),
        1,
        "the root manifest must declare `exasol-udf-sdk` on exactly one line: install.sh's \
         resolve_engine_pinned_slc_version takes the FIRST such line and stops. Found: {declarations:?}"
    );

    let version = scraped_version(declarations[0]).unwrap_or_else(|| {
        panic!(
            "keep a literal `version = \"x.y.z\"` on the `exasol-udf-sdk` line: `make install-slc` \
             and install.sh scrape it to pick the SLC, and neither resolves a workspace-inherited \
             or multi-line form. Found: {:?}",
            declarations[0]
        )
    });

    assert_eq!(
        version,
        linked_sdk_version(),
        "manifest pins {version} but the compiled-in SDK is {}, so the installers would register \
         an SLC that rejects the .so. Run `cargo update -p exasol-udf-sdk`.",
        linked_sdk_version()
    );
}

#[test]
fn engine_manifest_inherits_the_sdk_pin_instead_of_restating_it() {
    for dep_name in ["exasol-udf-sdk", "exasol-udf-macros"] {
        let declarations = declaration_lines(ENGINE_MANIFEST, dep_name);

        assert_eq!(
            declarations.len(),
            1,
            "the engine manifest must declare `{dep_name}` exactly once. Found: {declarations:?}"
        );
        assert!(
            declarations[0].contains("workspace = true"),
            "inherit `{dep_name}` with `{{ workspace = true }}`, never restate a literal version: \
             Cargo silently unifies to the higher requirement within a minor line, and resolves \
             two SDK copies into one cdylib across minor lines. Found: {:?}",
            declarations[0]
        );
    }
}
