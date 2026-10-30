//! The one conversion between raw object keys and the scan spec's wire paths, so no source
//! escapes or decodes a key its own way (`file-planning/pushdown-planning-file-encoding`).
use crate::scan::object_store::store_root_url;
use crate::scan::spec::reconstruct_abs_uri;
use datafusion::datasource::listing::ListingTableUrl;
use exasol_udf_sdk::error::UdfError;
use object_store::path::{Path as StorePath, PathPart};
use percent_encoding::{AsciiSet, utf8_percent_encode};
use url::{Position, Url};

/// What a URL parse decodes (`%`), cuts at (`#`, `?`), reads as `/` (`\` under `file://`), or
/// trims (a trailing space). Non-ASCII is always escaped.
const WIRE_ESCAPED: &AsciiSet = &AsciiSet::EMPTY
    .add(b'%')
    .add(b'#')
    .add(b'?')
    .add(b'\\')
    .add(b' ');

/// An object's store root and its raw key, never percent-encoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectLocation {
    store_root: Url,
    key: StorePath,
}

impl ObjectLocation {
    /// `location` names its key literally, as an Iceberg manifest path, a Glue location, a
    /// CONNECTION address, or a listed object key does, so nothing is decoded.
    pub(crate) fn from_raw(location: &str) -> Result<Self, UdfError> {
        let refused = |cause: String| {
            UdfError::User(format!("invalid storage location '{location}': {cause}"))
        };
        let (scheme, rest) = location
            .split_once("://")
            .filter(|(scheme, _)| !scheme.is_empty())
            .ok_or_else(|| refused("it names no scheme".to_string()))?;
        let (authority, key) = rest.split_once('/').unwrap_or((rest, ""));
        let key = key.trim_end_matches('/');
        let store_root = Url::parse(&format!("{scheme}://{authority}"))
            .map_err(|error| refused(error.to_string()))?;
        let key = StorePath::parse(key).map_err(|error| refused(error.to_string()))?;
        Ok(Self { store_root, key })
    }

    /// `url` is percent-encoded, as a Delta `add.path` or a Unity storage location is, so its
    /// path decodes exactly once.
    pub(crate) fn from_url(url: &str) -> Result<Self, UdfError> {
        let parsed = Url::parse(url)
            .map_err(|error| UdfError::User(format!("invalid storage URI '{url}': {error}")))?;
        let key = StorePath::from_url_path(parsed.path())
            .map_err(|error| UdfError::User(format!("invalid storage path in '{url}': {error}")))?;
        Ok(Self {
            store_root: store_root_url(url)?,
            key,
        })
    }

    /// Parses an absolute wire path exactly as the scan's `ListingTableUrl` does.
    pub(crate) fn parse_wire(path: &str) -> Result<Self, UdfError> {
        let url = ListingTableUrl::parse(path)
            .map_err(|error| UdfError::User(format!("invalid file URL '{path}': {error}")))?;
        Ok(Self {
            store_root: store_root_url(url.as_str())?,
            key: url.prefix().clone(),
        })
    }

    /// One scan-spec entry, relative to `table_root` or absolute.
    pub(crate) fn parse_entry(entry_path: &str, table_root: &str) -> Result<Self, UdfError> {
        Self::parse_wire(&reconstruct_abs_uri(entry_path, table_root))
    }

    /// `name` is one listed key segment, kept literally: `PathPart::from` would escape it again.
    pub(crate) fn child(&self, name: &str) -> Result<Self, UdfError> {
        let part = PathPart::parse(name).map_err(|error| {
            UdfError::User(format!(
                "invalid key segment '{name}' below '{self}': {error}"
            ))
        })?;
        Ok(self.at(self.key.clone().join(part)))
    }

    /// The object at `key` in this location's store.
    pub(crate) fn at(&self, key: StorePath) -> Self {
        Self {
            store_root: self.store_root.clone(),
            key,
        }
    }

    /// The `scheme://userinfo@host:port` root, as `ListingTableUrl::object_store()` takes it.
    pub(crate) fn store_root(&self) -> &Url {
        &self.store_root
    }

    pub(crate) fn key(&self) -> &StorePath {
        &self.key
    }

    /// The wire path relative to `root`, when this object lies strictly below it in the same
    /// store; `<root>-archive/...` or the root itself is not below it.
    pub(crate) fn relative_to(&self, root: &Self) -> Option<String> {
        if self.store_root != root.store_root {
            return None;
        }
        let mut below = self.key.prefix_match(&root.key)?.peekable();
        below.peek()?;
        Some(escape_parts(below))
    }

    /// The one escaping the scan spec carries: [`Self::parse_wire`] reads back the same raw key.
    pub(crate) fn to_wire(&self) -> String {
        format!("{}/{}", self.root_str(), escape_parts(self.key.parts()))
    }

    /// Not a trailing-`/` trim: that would also eat the `//` of an empty-host `memory://`.
    fn root_str(&self) -> &str {
        &self.store_root[..Position::BeforePath]
    }
}

/// The raw form, as a manifest or a positional-delete `file_path` names the object.
impl std::fmt::Display for ObjectLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.root_str(), self.key)
    }
}

fn escape_parts<'p>(parts: impl Iterator<Item = PathPart<'p>>) -> String {
    let mut escaped = String::new();
    for (index, part) in parts.enumerate() {
        if index > 0 {
            escaped.push('/');
        }
        escaped.extend(utf8_percent_encode(part.as_ref(), WIRE_ESCAPED));
    }
    escaped
}

#[cfg(test)]
#[path = "object_location_tests.rs"]
mod tests;
