#[derive(Debug, Clone)]
pub struct Config {
    /// Whether to enable schema versioning.
    pub enable_versioning: bool,

    /// Default schema version if versioning is disabled or not passed to query.
    pub default_version: u64,
}