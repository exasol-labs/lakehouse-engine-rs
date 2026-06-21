//! Pure convention test (no I/O): asserts the build documentation states that a
//! host `cargo build --release` of the UDF crate produces an artifact that
//! cannot be loaded inside Exasol.
//!
//! Covers packaging/single-so-two-entry-points scenario:
//! "Host release build of the .so is rejected by convention" — the documentation
//! MUST state that host `cargo build --release` produces an unloadable artifact.
//!
//! The documentation is embedded at compile time via `include_str!`, so this is
//! a pure test: it reads no files at runtime and needs no live services.

/// Workspace build-convention documentation, embedded at compile time.
/// Path is relative to this source file: crates/lakehouse-engine/tests -> workspace root.
const WORKSPACE_CLAUDE_MD: &str = include_str!("../../../CLAUDE.md");

#[test]
fn host_release_build_documented_unloadable() {
    let doc = WORKSPACE_CLAUDE_MD.to_lowercase();

    assert!(
        doc.contains("cargo build --release"),
        "build documentation must mention the host `cargo build --release` path"
    );
    assert!(
        doc.contains("host"),
        "build documentation must call out the host build specifically"
    );
    assert!(
        doc.contains("fails to load") || doc.contains("unloadable"),
        "build documentation must state the host-built .so does not load in Exasol"
    );
}
