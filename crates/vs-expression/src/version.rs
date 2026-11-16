/// The Exasol release a request runs on, as far as the type Exasol gives an expression depends on
/// it. Exasol 2026 types every form the typed pass covers as Exasol 2025 does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExasolVersion {
    /// Exasol 8 or older, where every TIMESTAMP holds 3 fractional digits.
    Exasol8,
    /// Exasol 2025 or a later calendar-versioned release.
    Exasol2025,
    /// A version string that names no release. Only the types both releases share apply.
    Unknown,
}

const FIRST_CALENDAR_VERSIONED_MAJOR: u32 = 2025;

impl ExasolVersion {
    /// The release of a version string such as `8.29.13` or `2025.1.16`, read from its major.
    pub fn of(version: &str) -> Self {
        match version
            .split('.')
            .next()
            .and_then(|major| major.parse::<u32>().ok())
        {
            Some(major) if major < FIRST_CALENDAR_VERSIONED_MAJOR => Self::Exasol8,
            Some(_) => Self::Exasol2025,
            None => Self::Unknown,
        }
    }
}

#[cfg(test)]
#[path = "version_tests.rs"]
mod tests;
