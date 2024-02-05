use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    time::Duration,
};
use thiserror::Error;
use protolith_auth as auth;
use tracing::error;
use protolith_core::{
    db, meta_store, schema
};
use  protolith_admin as admin;
/// The strings used to build a configuration.
pub trait Strings {
    /// Retrieves the value for the key `key`.
    ///
    /// `key` must be one of the `ENV_` values below.
    fn get(&self, key: &str) -> Result<Option<String>, EnvError>;
}

/// An implementation of `Strings` that reads the values from environment variables.
pub struct Env;

/// Errors produced when loading a `Config` struct.
#[derive(Clone, Debug, Error)]
pub enum EnvError {
    #[error("invalid environment variable")]
    InvalidEnvVar,
    #[error("no rocks db path configured")]
    NoRocksDbPath,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ParseError {
    #[error("not a valid duration")]
    NotADuration,
    #[error("not a valid DNS domain suffix")]
    NotADomainSuffix,
    #[error("not a boolean value: {0}")]
    NotABool(
        #[from]
        #[source]
        std::str::ParseBoolError,
    ),
    #[error("not an integer: {0}")]
    NotAnInteger(
        #[from]
        #[source]
        std::num::ParseIntError,
    ),
    #[error("not a floating-point number: {0}")]
    NotAFloat(
        #[from]
        #[source]
        std::num::ParseFloatError,
    ),
    #[error("not a valid subnet mask")]
    NotANetwork,
    #[error("host is not an IP address")]
    HostIsNotAnIpAddress,
    #[error("not a valid IP address: {0}")]
    NotAnIp(
        #[from]
        #[source]
        std::net::AddrParseError,
    ),
    #[error("not a valid port range")]
    NotAPortRange,
    // #[error(transparent)]
    // AddrError(addr::Error),
    #[error("not a valid identity name")]
    NameError,
    #[error("could not read token source")]
    InvalidTokenSource,
    #[error("invalid trust anchors")]
    InvalidTrustAnchors,
    #[error("not a valid port policy: {0}")]
    InvalidPortPolicy(String),
}

// Environment variables to look at when loading the configuration
pub const ENV_DB_PATH: &str = "PROTOLITH_DB_PATH";
pub const ENV_DB_MAX_OPEN_FILES: &str = "PROTOLITH_DB_MAX_OPEN_FILES";
pub const ENV_DB_CACHE_SIZE: &str = "PROTOLITH_DB_CACHE_SIZE";
pub const ENV_METASTORE_INDEX_NAME: &str = "PROTOLITH_METASTORE_INDEX_NAME";
pub const ENV_METASTORE_SCHEMA_NAME: &str = "PROTOLITH_METASTORE_SCHEMA_NAME";
pub const ENV_METASTORE_VERSION_NAME: &str = "PROTOLITH_METASTORE_VERSION_NAME";
pub const ENV_METASTORE_USER: &str = "PROTOLITH_METASTORE_USER";
pub const ENV_SCHEMA_DEFAULT_VERSION: &str = "PROTOLITH_SCHEMA_DEFAULT_VERSION";
pub const ENV_SCHEMA_ENABLE_VERSIONING: &str = "PROTOLITH_SCHEMA_VERSIONING";
pub const ENV_ADDR: &str = "PROTOLITH_ADDR";
pub const ENV_USER: &str = "PROTOLITH_USER";
pub const ENV_PASS: &str = "PROTOLITH_PASS";
const ENV_SHUTDOWN_GRACE_PERIOD: &str = "PROTOLITH_SHUTDOWN_GRACE_PERIOD";
const ENV_DATABASE: &str = "PROTOLITH_DATABASE";
const ENV_DB_DROP_ON_SHUTDOWN: &str = "PROTOLITH_DESTROY_ON_SHUTDOWN";
const ENV_DB_DESCRIPTOR_FILE_NAME: &str = "PROTOLITH_DB_DESCRIPTOR_NAME";
const ENV_DEFAULT_DB_DESCRIPTOR_PATH: &str = "PROTOLITH_DEFAULT_DB_DESCRIPTOR";

// Default values for various configuration fields
const DEFAULT_DB_MAX_OPEN_FILES: i32 = 1000;
const DEFAULT_DB_CACHE_SIZE: usize = 50 * 1024 * 1024 * 1024; // 1GB in bytes
const DEFAULT_INDEX_CF_NAME: &str = "index";
const DEFAULT_SCHEMA_CF_NAME: &str = "schema";
const DEFAULT_SCHEMA_VERSIONS_CF_NAME: &str = "schema_versions";
const DEFAULT_USER_CF_NAME: &str = "user";
const DEFAULT_ADDR: &str = "0.0.0.0:5678";
const DEFAULT_DB_DESCRIPTOR: &str = "/usr/src/bin/protolith-db/descriptor.bin";
const DEFAULT_USER: &str = "protolith";
const DEFAULT_PASS: &str = "protolith";

// 2 minutes seems like a reasonable amount of time to wait for connections to close...
const DEFAULT_SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(2 * 60);
const DEFAULT_SCHEMA_VERSION: u64 = 1;
const DEFAULT_DATABASE: &str = "protolith";
const DEFAULT_DESCRIPTOR_NAME: &str = "DESCRIPTOR";
/// Load a `App` by reading ENV variables.
pub fn parse_config<S: Strings>(strings: &S) -> Result<super::Config, EnvError> {
    // Parse all the environment variables. `parse` will log any errors so
    // defer returning any errors until all of them have been parsed.
    let shutdown_grace_period = parse(strings, ENV_SHUTDOWN_GRACE_PERIOD, parse_duration);
    let cache_size = parse(strings, ENV_DB_CACHE_SIZE, parse_number);
    let max_open_files = parse(strings, ENV_DB_MAX_OPEN_FILES, parse_number);
    let index_cf_name = parse(strings, ENV_METASTORE_INDEX_NAME, parse_string);
    let schema_cf_name = parse(strings, ENV_METASTORE_SCHEMA_NAME, parse_string);
    let schema_versions_cf_name = parse(strings, ENV_METASTORE_VERSION_NAME, parse_string);
    let user_cf_name = parse(strings, ENV_METASTORE_USER, parse_string);
    let default_version = parse(strings, ENV_SCHEMA_DEFAULT_VERSION, parse_number);
    let schema_versioning = parse(strings, ENV_SCHEMA_ENABLE_VERSIONING, parse_bool);
    let database = parse(strings, ENV_DATABASE, parse_string);
    let addr = parse(strings, ENV_ADDR, parse_socket_addr);
    let drop_on_shutdown = parse(strings, ENV_DB_DROP_ON_SHUTDOWN, parse_bool);
    let descriptor_file_name = parse(strings, ENV_DB_DESCRIPTOR_FILE_NAME, parse_string);
    let database_descriptor_path = parse(strings, ENV_DEFAULT_DB_DESCRIPTOR_PATH, parse_pathbuf);
    let user = parse(strings, ENV_USER, parse_string);
    let password = parse(strings, ENV_PASS, parse_string);
    
    let drop_on_shutdown =  drop_on_shutdown?.unwrap_or(false);
    let user = user?.unwrap_or(DEFAULT_USER.to_owned());
    let password = password?.unwrap_or(DEFAULT_PASS.to_owned());

    let db = {
        let descriptor_file_name = descriptor_file_name?.unwrap_or(DEFAULT_DESCRIPTOR_NAME.to_string());
        let db_path = parse_rocks_db_path(strings, ENV_DB_PATH)?;
        let cache_size = cache_size?.unwrap_or(DEFAULT_DB_CACHE_SIZE);
        let max_open_files = max_open_files?.unwrap_or(DEFAULT_DB_MAX_OPEN_FILES);
        db::Config {
            db_path,
            cache_size,
            max_open_files,
            descriptor_file_name,
        }
    };

    let database = database?.unwrap_or(DEFAULT_DATABASE.to_string());

    let meta_store = {
        let index_cf_name = index_cf_name?.unwrap_or(DEFAULT_INDEX_CF_NAME.to_string());
        let schema_cf_name = schema_cf_name?.unwrap_or(DEFAULT_SCHEMA_CF_NAME.to_string());
        let schema_versions_cf_name = schema_versions_cf_name?.unwrap_or(DEFAULT_SCHEMA_VERSIONS_CF_NAME.to_string());
        let user_cf_name = user_cf_name?.unwrap_or(DEFAULT_USER_CF_NAME.to_string());

        meta_store::Config {
            index_cf_name,
            schema_cf_name,
            schema_versions_cf_name,
            user_cf_name,
            default_db: database.clone(),
        }
    };

    let schema = {
        let default_version = default_version?.unwrap_or(DEFAULT_SCHEMA_VERSION);
        let enable_versioning = schema_versioning?.unwrap_or(false);
        schema::Config {
            default_version,
            enable_versioning,
        }
    };

    let admin = {
        admin::Config {

        }
    };

    let addr = addr?.unwrap_or(DEFAULT_ADDR.parse().unwrap());
    let database_descriptor_path = database_descriptor_path?.unwrap_or(PathBuf::from(DEFAULT_DB_DESCRIPTOR));
    let auth = {
        auth::Config {
            password,
            user,
            meta_store: meta_store.clone()
        }
    };
    Ok(super::Config {
        addr,
        db,
        admin,
        auth,
        meta_store,
        schema,
        default_database: (database, database_descriptor_path),
        destroy_on_shutdown: drop_on_shutdown,
        shutdown_grace_period: shutdown_grace_period?.unwrap_or(DEFAULT_SHUTDOWN_GRACE_PERIOD),
    })
}

#[allow(unused)]
fn convert_attributes_string_to_map(attributes: String) -> HashMap<String, String> {
    attributes
        .lines()
        .filter_map(|line| {
            let mut parts = line.splitn(2, '=');
            parts.next().and_then(move |key| {
                parts.next().map(move |val|
                // Trim double quotes in value, present by default when attached through k8s downwardAPI
                (key.to_string(), val.trim_matches('"').to_string()))
            })
        })
        .collect()
}


// === impl Env ===

impl Strings for Env {
    fn get(&self, key: &str) -> Result<Option<String>, EnvError> {
        use std::env;

        match env::var(key) {
            Ok(value) => Ok(Some(value)),
            Err(env::VarError::NotPresent) => Ok(None),
            Err(env::VarError::NotUnicode(_)) => {
                error!("{key} is not encoded in Unicode");
                Err(EnvError::InvalidEnvVar)
            }
        }
    }
}

impl Env {
    pub fn try_config(&self) -> Result<super::Config, EnvError> {
        parse_config(self)
    }
}

// === Parsing ===

fn parse_socket_addr(s: &str) -> Result<SocketAddr, ParseError> {
    s.parse().map_err(Into::into)
}

fn parse_bool(s: &str) -> Result<bool, ParseError> {
    s.parse().map_err(Into::into)
}

fn parse_string(s: &str) -> Result<String, ParseError> {
    Ok(s.to_owned())
}

fn parse_rocks_db_path<S: Strings>(s: &S, base: &str) -> Result<PathBuf, EnvError> {
    let path_str = parse(s, base, parse_string)?;

    match path_str {
        Some(path) => match PathBuf::from_str(&path) {
            Err(_) => Err(EnvError::InvalidEnvVar),
            Ok(pb) => Ok(pb)
        },
        _ => {
            error!("{base} must be specified");
            Err(EnvError::NoRocksDbPath)
        }
    }
}

fn parse_number<T>(s: &str) -> Result<T, ParseError>
where
    T: FromStr,
    ParseError: From<T::Err>,
{
    s.parse().map_err(Into::into)
}

fn parse_pathbuf(s: &str) -> Result<PathBuf, ParseError> {
    Ok(PathBuf::from(s))
}

fn parse_duration(s: &str) -> Result<Duration, ParseError> {
    use regex::Regex;

    let re = Regex::new(r"^\s*(\d+)(ms|s|m|h|d)?\s*$").expect("duration regex");

    let cap = re.captures(s).ok_or(ParseError::NotADuration)?;

    let magnitude = parse_number(&cap[1])?;
    match cap.get(2).map(|m| m.as_str()) {
        None if magnitude == 0 => Ok(Duration::from_secs(0)),
        Some("ms") => Ok(Duration::from_millis(magnitude)),
        Some("s") => Ok(Duration::from_secs(magnitude)),
        Some("m") => Ok(Duration::from_secs(magnitude * 60)),
        Some("h") => Ok(Duration::from_secs(magnitude * 60 * 60)),
        Some("d") => Ok(Duration::from_secs(magnitude * 60 * 60 * 24)),
        _ => Err(ParseError::NotADuration),
    }
}

// fn parse_socket_addr(s: &str) -> Result<SocketAddr, ParseError> {
//     match parse_addr(s)? {
//         Addr::Socket(a) => Ok(a),
//         _ => {
//             error!("Expected IP:PORT; found: {s}");
//             Err(ParseError::HostIsNotAnIpAddress)
//         }
//     }
// }

// fn parse_addr(s: &str) -> Result<Addr, ParseError> {
//     Addr::from_str(s).map_err(|e| {
//         error!("Not a valid address: {s}");
//         // ParseError::AddrError(e)
//     })
// }

pub(super) fn parse<T, Parse>(
    strings: &dyn Strings,
    name: &str,
    parse: Parse,
) -> Result<Option<T>, EnvError>
where
    Parse: FnOnce(&str) -> Result<T, ParseError>,
{
    match strings.get(name)? {
        Some(ref s) => {
            let r = parse(s).map_err(|parse_error| {
                error!("{name}={s:?} is not valid: {parse_error:?}");
                EnvError::InvalidEnvVar
            })?;
            Ok(Some(r))
        }
        None => Ok(None),
    }
}
