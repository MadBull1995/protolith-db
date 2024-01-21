pub mod env;
mod build_info;
pub mod signals;
mod layer;

use hyper::Request;
use layer::{MetadataLayer, TracingLayer};
use tokio::{sync::{mpsc, oneshot, watch}, signal};
pub use build_info::BUILD_INFO;
use engine::{Engine, ProtolithDbEngine, service::ProtolithEngineService};
use protolith_core::{error::Error, api::{protolith::services::v1::admin_service_server::{AdminServiceServer, AdminService}, DescriptorPool, prost::bytes::Bytes}, db::RocksDb};
use tracing::{debug, info, warn, Instrument, info_span};
use std::{time::Duration, collections::{HashMap, HashSet}, sync::Arc, net::SocketAddr, path::{PathBuf, Path}, fs::{self, File}, io::{BufReader, Read}};
use drain;
pub use protolith_core::{
    db,
    meta_store,
    schema,
    trace,
};
use protolith_admin as admin;
use protolith_engine as engine;
use tonic::{transport::Server, Status};
pub const EX_USAGE: i32 = 64;


pub struct App {
    addr: SocketAddr,
    admin: admin::Admin<ProtolithDbEngine>,
    drain: drain::Signal,
    engine: ProtolithDbEngine,
    destroy_on_shutdown: bool,
}

#[derive(Debug, Clone)]
pub struct Config {
    db: db::Config,
    admin: admin::Config,
    meta_store: meta_store::Config,
    schema: schema::Config,
    addr: SocketAddr,
    pub databases: Vec<String>,
    pub destroy_on_shutdown: bool,
    pub shutdown_grace_period: Duration,
}

impl Config {
    pub fn try_from_env() -> Result<Self, env::EnvError> {
        env::Env.try_config()
    }
}

impl Config {
    pub fn build(
        self,
        trace: trace::Handle,
    ) -> Result<App, Error> {
        let Config {
            db,
            admin,
            meta_store,
            schema,
            databases,
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
        for database in existing_databases.iter().chain(databases.iter()) {
            let descriptor_path = db.db_path.join(database).join("descriptor.bin");
            folder_set.insert((database.to_string(), descriptor_path));
        }
        let merged_databases: Vec<(String, PathBuf)> = folder_set.into_iter().collect();
        for (db_name, descriptor_path) in merged_databases {
            let f = File::open(descriptor_path.clone());
            let f = {
                match f {
                    Err(e) => {
                        warn!(db = ?db_name, err = ?e, "failed to locate protobuf descriptor file in");
                        Bytes::from(vec![])
                    },
                    Ok(f) => {
                        let mut reader = BufReader::new(f);
                        let mut buffer = Vec::new();
                        reader.read_to_end(&mut buffer)?;
                        let buf = Bytes::from(buffer);
                        buf
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

        let admin = admin.build(Arc::new(engine.clone()), drain_rx.clone())?;
        info!(?addr, "Serving protolith db instance at");
        Ok(App {
            admin,
            addr,
            engine,
            destroy_on_shutdown,
            drain: drain_tx,
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
            ..
        } = self;

        let admin_service = admin.clone().service();
        let mut engine = engine.clone();
        let engine_service = ProtolithEngineService::new(engine.clone()).service();
        let destroy_on_shutdown = destroy_on_shutdown;
        let layer = tower::ServiceBuilder::new()
            // .timeout(Duration::from_secs(30))
            .layer(MetadataLayer::default())
            .layer(TracingLayer)
            .into_inner();
        let server = Server::builder()
            .layer(layer)
            .add_service(admin_service)
            .add_service(engine_service)
            .serve_with_shutdown(addr, async move {
                let release = admin.drain.clone().signaled().await;
                if destroy_on_shutdown {
                    debug!("starting RocksDB shutdown destruction");
                    let dbs = engine.list_databases().await.expect("failed to get an updated database list");
                    let mut to_destroy = Vec::with_capacity(dbs.databases.len());
                    for db in dbs.databases {
                        to_destroy.push(db.name);
                    }
                    for db in to_destroy {
                        warn!(db = ?db, "Destroying");
                        engine.destroy_db(&db).await.expect("destroying database");
                    }
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
    let rocksdb_files = vec!["IDENTITY", "CURRENT"];
    
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
