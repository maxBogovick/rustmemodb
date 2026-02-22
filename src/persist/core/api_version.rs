/// The name of the table used to store schema versions.
pub const PERSIST_SCHEMA_REGISTRY_TABLE: &str = "__persist_schema_versions";
/// The major version of the public persistence API.
pub const PERSIST_PUBLIC_API_VERSION_MAJOR: u16 = 1;
/// The minor version of the public persistence API.
pub const PERSIST_PUBLIC_API_VERSION_MINOR: u16 = 0;
/// The patch version of the public persistence API.
pub const PERSIST_PUBLIC_API_VERSION_PATCH: u16 = 0;
/// The string representation of the public persistence API version.
pub const PERSIST_PUBLIC_API_VERSION_STRING: &str = "1.0.0";

/// Represents the version of the public persistence API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PersistPublicApiVersion {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}

impl fmt::Display for PersistPublicApiVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Returns the current public persistence API version.
pub const fn persist_public_api_version() -> PersistPublicApiVersion {
    PersistPublicApiVersion {
        major: PERSIST_PUBLIC_API_VERSION_MAJOR,
        minor: PERSIST_PUBLIC_API_VERSION_MINOR,
        patch: PERSIST_PUBLIC_API_VERSION_PATCH,
    }
}

/// Returns the default schema version for new persistence items.
pub const fn default_schema_version() -> u32 {
    1
}
