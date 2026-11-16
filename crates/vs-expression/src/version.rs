/// The Exasol release a request runs on, the one reader of the database version string. Exasol
/// 2026 behaves as Exasol 2025 in every respect the adapter and the scan depend on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExasolVersion {
    /// Exasol 8 or older, where every TIMESTAMP holds 3 fractional digits.
    Exasol8,
    /// Exasol 2025 or a later calendar-versioned release.
    Exasol2025,
    /// A version string that names no release. The typed pass proves only the types both releases
    /// share, while column declarations and the scan's arithmetic take it for the likelier 2025.
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
