//! Resolving a scan spec's storage REFERENCE into the concrete backends the
//! scan reads its files through.
//!
//! The wire spec carries no static storage credential and no vended one in
//! plaintext: it names the Exasol CONNECTION that supplies them
//! ([`ScanStorage::Connection`]) or carries a sealed envelope keyed from that
//! same CONNECTION ([`ScanStorage::Sealed`]). This module is where those become
//! a [`StorageBackend`] again — once per scan invocation, at the top of
//! `run_scan`, before any object store exists.
//!
//! Resolving BOTH sides in ONE step is the point of [`ResolvedScanStorage`].
//! The error-redaction set must be the union of every side's secrets, and code
//! holding one side's store structurally cannot assemble a set covering a side
//! it never sees — so the pair is resolved together, up front, and threaded from
//! there. [`ScanStorage`] deliberately exposes no secret accessor, so a site
//! left reading the unresolved wire value fails to compile rather than silently
//! redacting against an empty set.

use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::StorageCreds;

use crate::scan::sealed::{derive_sealed_storage_key, unseal_storage};
use crate::scan::spec::{CommonScanSpec, ScanStorage, StorageBackend};

/// The storage backends ONE scan invocation reads through: the fact side's, plus
/// the dimension side's when the spec carries a broadcast join.
///
/// The ONLY type in the scan path that exposes a secret set, and therefore the
/// single owner of the union rule every redaction feed site reads. `Debug`
/// delegates to [`StorageBackend`]'s own redacting impl, so a `{:?}` added at any
/// call site cannot print a credential.
///
/// Declared `pub` because it is a parameter of the three `pub` facade entries
/// host tests drive (`run_scan_one`, `run_raw_scan_with_session`,
/// `run_join_scan_with_session`): a `pub(crate)` type in a `pub` signature trips
/// `private_interfaces`, and no external caller could construct the argument.
#[derive(Debug)]
pub struct ResolvedScanStorage {
    primary: StorageBackend,
    join: Option<StorageBackend>,
}

impl ResolvedScanStorage {
    /// The pair a caller ALREADY holds its backends for, contacting no
    /// CONNECTION and reading no wire value.
    ///
    /// This exists for the external host tests that drive a `pub` facade entry
    /// directly over a local-file session: they construct their backends
    /// themselves and have no CONNECTION to resolve. Because it reads nothing
    /// from a spec, it can never become the site a production path resolves
    /// through — [`resolve_scan_storage`] is the only constructor that reads a
    /// CONNECTION.
    pub fn from_backends(primary: StorageBackend, join: Option<StorageBackend>) -> Self {
        Self { primary, join }
    }

    /// The fact side's resolved backend.
    pub(crate) fn primary(&self) -> &StorageBackend {
        &self.primary
    }

    /// The dimension side's resolved backend, present iff the spec carried a
    /// broadcast-join block.
    pub(crate) fn join(&self) -> Option<&StorageBackend> {
        self.join.as_ref()
    }

    /// EVERY secret value that must be stripped from an error this scan
    /// surfaces: the fact side's credentials unioned with the dimension side's.
    ///
    /// The single owner of that union rule. Each side is read through its own
    /// backend, but an error can be raised by code holding one side's store — or
    /// by the router over both — which cannot assemble a set covering a side it
    /// never sees. A second, independently maintained copy is how a
    /// dimension-side credential leaks through a fact-side-only redaction set.
    pub(crate) fn all_secret_values(&self) -> Vec<&str> {
        let mut secrets = self.primary.secret_values();
        if let Some(join) = &self.join {
            secrets.extend(join.secret_values());
        }
        secrets
    }
}

/// Resolve both of a scan spec's storage sides through the CONNECTION each one
/// references — the ONLY constructor that reads a CONNECTION.
///
/// Called ONCE per scan invocation, at the top of `run_scan`. Each side is
/// resolved independently from its own [`ScanStorage`], because a vended
/// credential is scoped to the table it was vended for and the two sides may
/// reference different CONNECTIONs.
///
/// A CONNECTION read that fails, a password that is not a JSON object, and an
/// envelope that will not open are all hard failures naming the connection and
/// the operation. There is deliberately no fallback to an inline or partial
/// credential: a scan that proceeded with an empty credential would fail later
/// with a confusing storage error, and one that proceeded with a partial
/// credential is exactly the silent degradation the reference design exists to
/// prevent.
pub(crate) fn resolve_scan_storage(
    common: &CommonScanSpec,
    ctx: &dyn UdfContext,
) -> Result<ResolvedScanStorage, UdfError> {
    let primary = resolve_side(&common.storage, ctx)?;
    let join = match &common.join {
        Some(join) => Some(resolve_side(&join.storage, ctx)?),
        None => None,
    };
    Ok(ResolvedScanStorage { primary, join })
}

/// Resolve ONE side's wire value into the backend it stands for.
fn resolve_side(storage: &ScanStorage, ctx: &dyn UdfContext) -> Result<StorageBackend, UdfError> {
    match storage {
        ScanStorage::Inline(backend) => Ok(backend.clone()),
        ScanStorage::Connection { name, allow_http } => {
            let password = connection_password(ctx, name)?;
            Ok(StorageCreds::from_json(&password_object(name, &password)?).backend(*allow_http))
        }
        ScanStorage::Sealed { name, payload } => {
            let password = connection_password(ctx, name)?;
            let key = derive_sealed_storage_key(&password);
            unseal_storage(payload, &key).map_err(|failure| {
                UdfError::User(format!(
                    "the scan cannot open the storage envelope sealed under CONNECTION \
                     '{name}' ({failure}); the expected cause is the CONNECTION's password \
                     having been rotated after this query was planned"
                ))
            })
        }
    }
}

/// Read the referenced CONNECTION's password through the scan's own grant-gated
/// `ctx.connection()` call.
///
/// The password is returned as an opaque string and never parsed here: the
/// sealed path feeds these BYTES to HKDF unparsed, which is what keeps the
/// structural exclusion of catalog-authentication fields from the scan intact —
/// no `token`, `client_id`, or `client_secret` value is ever constructed inside
/// the UDF.
///
/// A refusal names the connection, carries the host's own reason (a failed read
/// has no password to echo), and names the grant a deployment without it is
/// missing — resolved against the running script's OWN schema and name (see
/// [`script_reference`]), so the statement it prints is runnable as printed.
///
/// The grantee it names is the VIRTUAL SCHEMA's OWNER, not the querying user.
/// Verified live on Exasol 2025.2.1 in both directions: a `SELECT`-only user
/// holding no connection privilege at all queries the virtual schema
/// successfully, while revoking the grant from the OWNER breaks that same user's
/// query — and granting it to the querying user instead does NOT restore it.
/// Exasol evaluates `ACCESS ON CONNECTION ... FOR SCRIPT` against the owner when
/// the script is reached through VS-rewritten pushdown SQL, so the remedy is one
/// deployment-time statement, not one per reader. (A script invoked DIRECTLY —
/// `SELECT <schema>.LAKEHOUSE_SCAN(...)` — is checked against the session user
/// instead, but that is not how a virtual-schema query reaches this code.)
fn connection_password(ctx: &dyn UdfContext, name: &str) -> Result<String, UdfError> {
    match ctx.connection(name) {
        Ok(connection) => Ok(connection.password),
        Err(cause) => {
            let script = script_reference(ctx);
            Err(UdfError::User(format!(
                "the scan cannot access CONNECTION '{name}' ({cause}); the scan script needs \
                 GRANT ACCESS ON CONNECTION {name} FOR SCRIPT {script}, granted \
                 directly or through a role, to the OWNER of the virtual schema being queried \
                 (one deployment-time grant — a querying user needs no connection privilege)"
            )))
        }
    }
}

/// The schema-qualified script name a `FOR SCRIPT` grant has to name, taken from
/// the handshake metadata of the invocation that is actually missing the grant.
///
/// A deployment that renamed the scan script or installed it outside the default
/// schema is exactly the one whose operator cannot guess these values, so the
/// remedy quotes what this UDF reports about itself rather than a canonical
/// install. `UdfContext` answers both with the empty string on a context that does
/// not report them, and half a qualified name is not a runnable statement, so each
/// half keeps its placeholder in that case.
fn script_reference(ctx: &dyn UdfContext) -> String {
    let schema = ctx.script_schema();
    let script = ctx.script_name();
    format!(
        "{}.{}",
        if schema.is_empty() {
            UNREPORTED_SCRIPT_SCHEMA
        } else {
            schema.as_str()
        },
        if script.is_empty() {
            UNREPORTED_SCRIPT_NAME
        } else {
            script.as_str()
        }
    )
}

/// Stands in for a script schema the host did not report — a placeholder the
/// operator has to substitute, never a schema that might exist.
const UNREPORTED_SCRIPT_SCHEMA: &str = "<schema>";

/// Stands in for a script name the host did not report: the name the installer's
/// own template creates, which is the best available guess.
const UNREPORTED_SCRIPT_NAME: &str = "LAKEHOUSE_SCAN";

/// Parse a CONNECTION password into the JSON object the nine-field storage
/// projection reads, without echoing it.
///
/// A non-object password is REFUSED rather than read as an absent-everything
/// object: `StorageCreds::from_json` would answer every field empty, and a
/// credential-less backend is the partial-credential fallback this path must not
/// take.
fn password_object(name: &str, password: &str) -> Result<serde_json::Value, UdfError> {
    let refusal = || {
        UdfError::User(format!(
            "CONNECTION '{name}' password is not a JSON object, so the scan cannot derive \
             its storage credentials"
        ))
    };
    let json: serde_json::Value = serde_json::from_str(password).map_err(|_| refusal())?;
    if json.is_object() {
        Ok(json)
    } else {
        Err(refusal())
    }
}

#[cfg(test)]
#[path = "storage_ref_tests.rs"]
mod tests;
