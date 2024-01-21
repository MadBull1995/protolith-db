pub const BUILD_INFO: BuildInfo = BuildInfo {
    date: env!("PROTOLITH_BUILD_DATE"),
    git_sha: env!("GIT_SHA"),
    profile: env!("PROFILE"),
    vendor: env!("PROTOLITH_VENDOR"),
    version: env!("PROTOLITH_VERSION"),
};

#[derive(Copy, Clone, Debug, Default, Hash, PartialEq, Eq)]
pub struct BuildInfo {
    pub date: &'static str,
    pub git_sha: &'static str,
    pub profile: &'static str,
    pub vendor: &'static str,
    pub version: &'static str,
}