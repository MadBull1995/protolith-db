#![deny(rust_2018_idioms, clippy::disallowed_methods, clippy::disallowed_types)]
#![forbid(unsafe_code)]

pub mod env;
mod build_info;
pub mod signals;
mod layer;
use layer::{MetadataLayer, TracingLayer, SessionLayer};
pub use build_info::BUILD_INFO;
use engine::{ProtolithDbEngine, service::ProtolithEngineService, Admin as _};
use protolith_core::{error::Error, api::{DescriptorPool, prost::bytes::Bytes}};
use tracing::{debug, error, info, warn};
use std::{time::{Duration, Instant}, collections::{HashMap, HashSet}, sync::{Arc, Mutex}, net::SocketAddr, path::{PathBuf, Path}, fs::{self, File}, io::{BufReader, Read, Write}};
use drain;
pub use protolith_core::{
    db,
    meta_store,
    schema,
    trace,
};
use protolith_admin as admin;
use protolith_engine as engine;
use protolith_auth as auth;
use tonic::transport::Server;
pub const EX_USAGE: i32 = 64;


pub struct App {
    addr: SocketAddr,
    admin: admin::Admin<ProtolithDbEngine>,
    auth: auth::Auth<ProtolithDbEngine>,
    drain: drain::Signal,
    engine: ProtolithDbEngine,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    destroy_on_shutdown: bool,
}

const SESSIONS_FILE: &str = "/Users/amitshmulevitch/rusty-land/protolith-db/sessions";

#[derive(Debug, Clone)]
pub struct Session {
    user_id: String,
    last_accessed: Instant,  // Use this to expire sessions
}

#[derive(Debug, Clone)]
pub struct Config {
    pub db: db::Config,
    admin: admin::Config,
    auth: auth::Config,
    meta_store: meta_store::Config,
    schema: schema::Config,
    addr: SocketAddr,
    pub default_database: (String, PathBuf),
    pub destroy_on_shutdown: bool,
    pub shutdown_grace_period: Duration,
}

impl Config {
    pub fn try_from_env() -> Result<Self, env::EnvError> {
        env::Env.try_config()
    }
}

impl Config {
    pub async fn build(
        self,
        _trace: trace::Handle,
    ) -> Result<App, Error> {
        let Config {
            db,
            admin,
            auth,
            meta_store,
            schema,
            default_database,
            addr,
            destroy_on_shutdown,
            ..
        } = self;

        info!("Building App");

        let (drain_tx, drain_rx) = drain::channel();

        debug!(config = ?db, "Building RocksDB Embedded Server");
        let mut dbs = HashMap::new();
        
        let existing_databases = find_rocksdb_databases(db.db_path.as_path());
        let mut folder_set = HashSet::new();
        folder_set.insert((default_database.0, default_database.1));
        
        for database in existing_databases.iter() {
            if database != &meta_store.default_db {
                let descriptor_path = db.db_path.join(database).join(db.descriptor_file_name.clone());
                folder_set.insert((database.to_string(), descriptor_path));
            }
        }

        let merged_databases: Vec<(String, PathBuf)> = folder_set.into_iter().collect();
        for (db_name, descriptor_path) in merged_databases {
            let f = File::open(descriptor_path.clone());
            
            let f = {
                match f {
                    Err(e) => {
                        warn!(db = ?db_name, path = ?descriptor_path, err = ?e, "failed to locate protobuf descriptor file in");
                        Bytes::from(vec![])
                    },
                    Ok(f) => {
                        let mut reader = BufReader::new(f);
                        let mut buffer = Vec::new();
                        reader.read_to_end(&mut buffer)?;
                        Bytes::from(buffer)
                    }
                }
            };
            let pool = DescriptorPool::decode(f).unwrap();
            let rocksdb = db.clone().build(db_name.clone(), meta_store.clone(), schema.clone(), pool)?;
            dbs.insert(db_name,  rocksdb);
        }
        
        let engine = engine::ProtolithDbEngine::new(db, meta_store.clone(), schema.clone(), dbs.clone());
        // let meta_store = meta_store.build(dbs.clone());
        debug!(config = ?schema, "Building Schema");

        debug!(config = ?admin, "Building Admin Service");
        let engine_arc = Arc::new(engine.clone());
        let admin = admin.build(engine_arc.clone(), drain_rx.clone())?;
        let mut auth = auth.build(engine_arc).await?;
        match load_sessions(SESSIONS_FILE) {
            Ok(sess) => auth.set_sessions(sess).await,
            Err(err) => warn!(error = ?err, "not loaded any sessions")
        }
        info!(?addr, "Serving protolith db instance at");
        Ok(App {
            admin,
            addr,
            engine,
            auth,
            destroy_on_shutdown,
            drain: drain_tx,
            sessions: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

impl App {

    pub  fn spawn(self) -> drain::Signal {
        let Self { 
            admin,
            addr,
            drain,
            engine,
            destroy_on_shutdown,
            sessions,
            auth,
            ..
        } = self;
        
        let max_message_size = 50 * 1024 * 1024;
        let admin_service = admin.clone().service(max_message_size);
        let mut engine = engine.clone();
        let engine_service = ProtolithEngineService::new(engine.clone()).service();
        let auth_arc = Arc::new(auth);
        let session_layer = SessionLayer::new(auth_arc.clone());
        let auth_service = auth_arc.service(max_message_size);
        let layer = tower::ServiceBuilder::new()
            // .timeout(Duration::from_secs(30))
            .layer(TracingLayer)
            .layer(MetadataLayer)
            .layer(session_layer)
            .into_inner();
        let server = Server::builder()
            .layer(layer)
            .add_service(admin_service)
            .add_service(engine_service)
            .add_service(auth_service)
            .serve_with_shutdown(addr, async move {
                let release = admin.drain.clone().signaled().await;
                info!("starting RocksDB shutdown");
                if destroy_on_shutdown {
                    let dbs = engine.list_databases().await.expect("failed to get an updated database list");
                    let mut to_destroy = Vec::with_capacity(dbs.databases.len());
                    for db in dbs.databases {
                        to_destroy.push(db.name);
                    }
                    for db in to_destroy {
                        warn!(db = ?db, "Destroying");
                        engine.destroy_db(&db).await.expect("destroying database");
                    }
                } else {
                    let sessions = auth_arc.sessions().await;
                    println!("{:?}", sessions);
                    save_sessions(&sessions, SESSIONS_FILE).expect("presisting sessions");
                }
                drop(release)
            });

        tokio::spawn(async move {
            if let Err(e) = server.await {
                eprintln!("Server error: {:?}", e);
            }
        });
        drain
    }
}

// Function to check if a directory contains a RocksDB database
fn contains_rocksdb<P: AsRef<Path>>(dir: P) -> bool {
    let rocksdb_files = ["IDENTITY", "CURRENT"];
    
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Ok(file_name) = entry.file_name().into_string() {
                if rocksdb_files.contains(&file_name.as_str()) {
                    return true;
                }
            }
        }
    }
    false
}

// Function to iterate over directories and find RocksDB databases
fn find_rocksdb_databases<P: AsRef<Path>>(path: P) -> Vec<String> {
    let mut databases = Vec::new();

    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() && contains_rocksdb(&path) {
                if let Some(folder_name) = path.file_name() {
                    if let Some(folder_name_str) = folder_name.to_str() {
                        databases.push(folder_name_str.to_string());
                    }
                }
            }
        }
    }

    databases
}



fn save_sessions(sessions: &HashMap<String, auth::Session>, file_path: &str) -> Result<(), std::io::Error> {
    // Serialize the session map to a JSON string
    let serialized = serde_json::to_string(sessions)?;

    // Write the JSON string to a file
    let mut file = std::fs::File::create(file_path)?;
    file.write_all(serialized.as_bytes())?;
    Ok(())
}

fn load_sessions(file_path: &str) -> Result<HashMap<String, auth::Session>, std::io::Error> {
    // Read the entire file content
    let mut file = std::fs::File::open(file_path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    // Deserialize the JSON string to a SessionMap
    let sessions = serde_json::from_str(&contents)?;
    Ok(sessions)
}