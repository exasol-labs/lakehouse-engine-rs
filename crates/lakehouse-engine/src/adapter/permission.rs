//! The opt-in Lakekeeper permission check of a virtual schema (#415): its two properties, the
//! `USER_MAPPING` template that maps the querying Exasol user to a Lakekeeper principal, and the
//! gate that authorizes a pushdown request's tables or refuses the query. No other module names
//! the template engine.

use crate::adapter::catalog_kind::CatalogKind;
use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{
    CatalogSession, ConnectionCreds, TableReadDecision, lakekeeper_batch_check,
    lakekeeper_management_url,
};
use minijinja::{AutoEscape, Environment, UndefinedBehavior, context};
use serde_json::Value as Json;

pub(super) const PERMISSION_CHECK: &str = "PERMISSION_CHECK";
pub(super) const USER_MAPPING: &str = "USER_MAPPING";
const LAKEKEEPER: &str = "LAKEKEEPER";
/// The largest planned template renders in 19 units, a 5,000-entry lookup table in 8.
const RENDER_FUEL: u64 = 100_000;

const REQUIRES_ICEBERG_REST: &str =
    "PERMISSION_CHECK = 'LAKEKEEPER' requires the Iceberg REST catalog kind";

/// The parsed permission properties of one request. `Off` reads neither `USER_MAPPING` nor the
/// current user, so a virtual schema without `PERMISSION_CHECK` behaves exactly as before.
pub(crate) enum PermissionSettings {
    Off,
    Lakekeeper(Box<UserMapping>),
}

impl PermissionSettings {
    /// Compiles `USER_MAPPING` without rendering it, so createVirtualSchema, refresh, and
    /// setProperties reject a template syntax error without knowing any user.
    pub(crate) fn parse(props: &Json) -> Result<Self, UdfError> {
        let Some(value) = super::nonempty_str(props, PERMISSION_CHECK) else {
            return Ok(Self::Off);
        };
        if !value.eq_ignore_ascii_case(LAKEKEEPER) {
            return Err(UdfError::User(format!(
                "unrecognized '{PERMISSION_CHECK}' value '{value}'; set it to '{LAKEKEEPER}', \
                 or leave it absent to turn the permission check off"
            )));
        }
        let template = super::nonempty_str(props, USER_MAPPING).ok_or_else(|| {
            UdfError::User(format!(
                "{PERMISSION_CHECK} = '{LAKEKEEPER}' requires a '{USER_MAPPING}' template that \
                 maps the querying Exasol user to a Lakekeeper principal"
            ))
        })?;
        Ok(Self::Lakekeeper(Box::new(UserMapping::compile(template)?)))
    }

    /// Runs before the CONNECTION is read, so a wrong kind costs no request. A comparison, not a
    /// match, so a new kind is refused without an edit here.
    pub(crate) fn require_supported_kind(&self, kind: CatalogKind) -> Result<(), UdfError> {
        match self {
            Self::Lakekeeper(_) if kind != CatalogKind::IcebergRest => {
                Err(UdfError::User(REQUIRES_ICEBERG_REST.into()))
            }
            Self::Lakekeeper(_) | Self::Off => Ok(()),
        }
    }

    /// Runs once the CONNECTION names the catalog URI and before any catalog request, so a URI
    /// that derives no management URL is refused before the catalog is contacted.
    pub(crate) fn require_management_url(&self, catalog_uri: &str) -> Result<(), UdfError> {
        match self {
            Self::Lakekeeper(_) => lakekeeper_management_url(catalog_uri).map(drop),
            Self::Off => Ok(()),
        }
    }

    /// Builds a pushdown request's check from the current user, which is read only while the
    /// check is on. A refused user fails the request, so a gated request never runs unchecked.
    pub(crate) fn check_for(
        &self,
        current_user: impl FnOnce() -> Option<String>,
    ) -> Result<PermissionCheck, UdfError> {
        let Self::Lakekeeper(mapping) = self else {
            return Ok(PermissionCheck::Off);
        };
        let user = current_user()
            .filter(|user| !user.is_empty())
            .ok_or_else(no_current_user)?;
        let principal = mapping.principal_for(&user)?;
        Ok(PermissionCheck::Enforced(PermissionGate {
            user,
            principal,
        }))
    }
}

/// What one request enforces. Only a pushdown under the check is `Enforced`: createVirtualSchema,
/// refresh, and setProperties list as the CONNECTION's identity and check nothing.
pub(crate) enum PermissionCheck {
    Off,
    Enforced(PermissionGate),
}

/// A compiled `USER_MAPPING` template with one variable, `user`.
///
/// Setting the property requires ALTER on the virtual schema, so the template's author is
/// trusted, and neither the uniqueness nor the owner of a principal is checked. The checks stop
/// injection only: the user name enters as a value and is never compiled, undefined values are
/// strict so a missing lookup refuses instead of yielding a partial principal, the environment
/// has no loader so a template reaches no other, and fuel bounds a render.
pub(crate) struct UserMapping {
    environment: Environment<'static>,
}

impl UserMapping {
    fn compile(source: &str) -> Result<Self, UdfError> {
        let mut environment = Environment::new();
        environment.set_undefined_behavior(UndefinedBehavior::Strict);
        environment.set_auto_escape_callback(|_| AutoEscape::None);
        environment.set_fuel(Some(RENDER_FUEL));
        environment.set_trim_blocks(true);
        environment.set_lstrip_blocks(true);
        environment
            .add_template_owned(USER_MAPPING, source.to_string())
            .map_err(|error| {
                UdfError::User(format!(
                    "the '{USER_MAPPING}' template does not compile: {}",
                    describe(&error)
                ))
            })?;
        Ok(Self { environment })
    }

    /// The trimmed render for `user`.
    pub(crate) fn principal_for(&self, user: &str) -> Result<String, UdfError> {
        let rendered = self
            .environment
            .get_template(USER_MAPPING)
            .and_then(|template| template.render(context! { user }))
            .map_err(|error| {
                unmappable(
                    user,
                    &format!("the template fails to render: {}", describe(&error)),
                )
            })?;
        let principal = rendered.trim();
        if principal.is_empty() {
            return Err(unmappable(user, "the template renders an empty principal"));
        }
        if principal
            .chars()
            .any(|c| c.is_whitespace() || c.is_control())
        {
            return Err(unmappable(
                user,
                &format!(
                    "the template renders the principal {principal:?}, which holds a whitespace \
                     or control character"
                ),
            ));
        }
        Ok(principal.to_string())
    }
}

fn no_current_user() -> UdfError {
    UdfError::User(format!(
        "the Lakekeeper permission check refuses the query: the request names no \
         current Exasol user, so '{USER_MAPPING}' cannot map it to a Lakekeeper principal"
    ))
}

fn unmappable(user: &str, reason: &str) -> UdfError {
    UdfError::User(format!(
        "the Lakekeeper permission check refuses the query of Exasol user '{user}': \
         '{USER_MAPPING}' cannot map it to a Lakekeeper principal, because {reason}"
    ))
}

fn describe(error: &minijinja::Error) -> String {
    let mut text = error.kind().to_string();
    if let Some(detail) = error.detail() {
        text = format!("{text}: {detail}");
    }
    if let Some(line) = error.line() {
        text = format!("{text} (line {line})");
    }
    text
}

/// One request's Exasol user and the Lakekeeper principal that `USER_MAPPING` maps it to.
pub(crate) struct PermissionGate {
    user: String,
    principal: String,
}

impl PermissionGate {
    /// Asks Lakekeeper once, on the request's own session, whether the principal may read every
    /// table, and refuses the query unless each one is allowed.
    pub(crate) async fn authorize(
        &self,
        session: &CatalogSession,
        tables: &[&str],
        creds: &ConnectionCreds,
    ) -> Result<(), UdfError> {
        let decisions = lakekeeper_batch_check(session, &self.principal, tables, creds).await?;
        self.verdict(tables, &decisions)
    }

    /// A table without an allowing decision counts as denied, so an answer that covers fewer
    /// tables than the request reads never admits it.
    fn verdict(&self, tables: &[&str], decisions: &[TableReadDecision]) -> Result<(), UdfError> {
        let mut denied: Vec<&str> = Vec::new();
        for table in tables {
            let allowed = decisions
                .iter()
                .any(|decision| decision.table == *table && decision.allowed);
            if !allowed && !denied.contains(table) {
                denied.push(table);
            }
        }
        if denied.is_empty() {
            return Ok(());
        }
        let (user, principal) = (&self.user, &self.principal);
        Err(UdfError::User(format!(
            "the Lakekeeper permission check refuses the query: Exasol user '{user}', mapped by \
             '{USER_MAPPING}' to the Lakekeeper principal '{principal}', may not read {}. \
             Lakekeeper denies read_data on each, or the table does not exist. Only a Lakekeeper \
             grant that names the principal '{principal}' lets this user read a table",
            denied.join(", ")
        )))
    }
}

#[cfg(test)]
#[path = "permission_tests.rs"]
mod tests;
