use std::fs;
use std::io::{Read, BufReader};
use std::{collections::HashMap, future::Future, sync::Arc};
pub mod service;
pub mod client;
use protolith_core::api::DescriptorPool;
use protolith_core::api::prost::Message;
use protolith_core::api::prost::bytes::Bytes;
use protolith_core::api::prost_wkt_types::Any;
use protolith_core::schema;
use rocksdb::{AsColumnFamilyRef, BlockBasedOptions, ColumnFamilyDescriptor, Options, DB};
use service::ProtolithEngineService;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
mod error;
pub use error::{EngineError, OpError};
use protolith_core::api::protolith::core::v1::Collection;


use protolith_core::{
    api::{
        protolith::{
            self,
            core::v1::{Database},
            services::v1::{CreateDatabaseResponse, ListDatabasesResponse, CreateCollectionResponse},
            types::v1::{ApiOp, Op, OpStatus},
        },
        DynamicMessage, MessageDescriptor,
    },
    db,
    error::{Error, Result},
    meta_store,
};

// pub fn main() {
//     let path = "_path_for_rocksdb_storage_with_cfs";
//     let mut cf_opts = Options::default();
//     let mut opts = BlockBasedOptions::default();
//     // opts.set_bl
//     cf_opts.set_log_level(rocksdb::LogLevel::Debug);
//     cf_opts.set_max_write_buffer_number(16);
//     let cf = ColumnFamilyDescriptor::new("cf1", cf_opts);
//     let pool = get_protolith_file_descriptor();
//     let ext = pool
//         .get_extension_by_name("protolith.annotation.v1.collection")
//         .unwrap();
//     // dbg!(ext);
//     let msg = pool
//         .get_message_by_name("protolith.test.v1.MyCollection")
//         .unwrap();
//     let messages = pool
//         .all_messages()
//         .filter(|m| m.full_name().starts_with("protolith"));
//     // println!("{:?}", messages.collect::<Vec<MessageDescriptor>>());
//     let exts = msg.extensions();
//     for ref ext in msg.options().extensions() {
//         dbg!(ext);
//         let collection_message_desc = ext.1.as_message().unwrap();

//         // let ext_msg_desc = ext.0.parent_message();
//         // let msg = DynamicMessage::new(ext_msg_desc.unwrap());
//         dbg!(collection_message_desc.get_field_by_name("name"));
//     }
//     for field in msg.fields() {
//         for ref field_ext in field.options().extensions() {
//             dbg!(field_ext);
//             let collection_message_desc = field_ext.1.as_message().unwrap();
//             dbg!(collection_message_desc);
//         }
//     }
//     let msg_ext = msg.get_extension_by_full_name(ext.full_name());
//     dbg!(msg_ext);

//     let mut db_opts = Options::default();
//     db_opts.create_missing_column_families(true);
//     db_opts.create_if_missing(true);
//     {
//         let db = DB::open_cf_descriptors(&db_opts, path, vec![cf]).unwrap();
//         let cf_handle = db.cf_handle("cf1").unwrap();
//         let key: &[u8; 6] = b"my key";
//         let value = b"my val";
//         db.put_cf(cf_handle, key, value).unwrap();

//         let cfs = DB::list_cf(&Options::default(), path).unwrap();
//         println!("{:?}", cfs);
//         let values = db.get_pinned_cf(&cf_handle, key).unwrap().unwrap();
//         println!("{:?}", values.as_ref())
//     }
//     let _ = DB::destroy(&db_opts, path);
// }

pub trait Metadata {
    fn version(&self) -> &str;
}

pub trait Admin {
    fn list_databases(
        &self,
    ) -> impl Future<Output = Result<ListDatabasesResponse, EngineError>> + Send;
    fn create_database(
        &self,
        name: String,
        fd_descriptor_set: Vec<u8>,
    ) -> impl Future<Output = Result<CreateDatabaseResponse, EngineError>> + Send;
    fn create_collection(
        &self,
        database: String,
        name: String,
        key: String,
        version: u64,
    ) -> impl Future<Output = Result<CreateCollectionResponse, EngineError>> + Send;
}

pub trait Engine: Admin + Metadata + Sync + Send + 'static {
    fn insert(
        &self,
        database: String,
        message: Any,
    ) -> impl Future<Output = Result<String, EngineError>> + Send;
    fn get(
        &self,
        database: String,
        collection: String,
        key: &[u8],
    ) -> impl Future<Output = Result<Any, EngineError>> + Send;
    fn list(
        &self,
        database: String,
        collection: String,
    ) -> impl Future<Output = Result<Vec<Any>, EngineError>> + Send;
}



pub type DatabasesMap = HashMap<String, db::RocksDb>;

#[derive(Clone)]
pub struct ProtolithDbEngine {
    pub db_config: db::Config,
    pub meta_store_config: meta_store::Config,
    pub schema_config: schema::Config,
    inner: Arc<Mutex<Inner>>,
}

#[derive(Clone)]
pub struct Inner {
    dbs: DatabasesMap,
}

impl Metadata for ProtolithDbEngine {
    fn version(&self) -> &str {
        const VERSION: &str = env!("CARGO_PKG_VERSION");
        VERSION
    }
}

impl Admin for ProtolithDbEngine {
    async fn create_database(&self, name: String, fd_descriptor: Vec<u8>) -> Result<CreateDatabaseResponse, EngineError> {
        let mut inner = self.inner.lock().await;
        if inner.dbs.contains_key(&name) {
            error!("database {name} already exists");
            Err(EngineError::OpError(OpError::DatabaseAlreadyExists(name)))
        } else {
            let buf = Bytes::from(fd_descriptor.clone());
            let pool = DescriptorPool::decode(buf).unwrap();
            let db: db::RocksDb = self
                .db_config
                .clone()
                .build(name.clone(), self.meta_store_config.clone(), self.schema_config.clone(), pool)
                .map_err(|e| EngineError::Internal(e))?;
            inner.dbs.insert(name.clone(), db);
            
            let descriptor_path = self.db_config.db_path.join(name.clone()).join(self.db_config.descriptor_file_name.clone());
            fs::write(descriptor_path, fd_descriptor).unwrap();
            Ok(CreateDatabaseResponse {
                name: name.clone(),
                op: Some(ApiOp {
                    r#type: Op::Create.into(),
                    description: format!("created database {}", name),
                    status: OpStatus::Success.into(),

                })
            })
        }
    }

    async fn list_databases(
        &self,
    ) -> Result<ListDatabasesResponse, EngineError> {
        let inner = self.inner.lock().await;
        let mut dbs = Vec::with_capacity(inner.dbs.len());
        for (name, db) in &inner.dbs {
            let collections = db
                .get_collections()
                .map_err(|e| EngineError::Internal(e))?;
            dbs.push(Database {
                name: name.to_string(),
                collections: collections,
                path: db.path.to_string_lossy().to_string()
            })
        }
        Ok(ListDatabasesResponse {
            databases: dbs
        })
    }

    async fn create_collection(&self, database: String, collection: String, key: String, version: u64) -> Result<CreateCollectionResponse, EngineError> {
        let mut inner = self.inner.lock().await;
        let db = inner.dbs.get_mut(&database).unwrap();
        match db.get_collection(collection.clone()) {
            Err(_) => {
                let schema = db.create_schema(collection.clone(), key, version).unwrap();
                info!(schema = ?schema, "created new collection");
                Ok(CreateCollectionResponse {
                    database: database.clone(),
                    name: collection.clone(),
                    op: Some(ApiOp {
                        description: format!("created collection {} on database {}", database, collection),
                        r#type: Op::Create.into(),
                        status: OpStatus::Success.into(),
                    })
                })
            },
            Ok(_) => {
                Err(EngineError::OpError(OpError::CollectionAlreadyExists(database, collection)))
            }
        }
    }
}

impl Engine for ProtolithDbEngine {
    
    async fn list(
        &self,
        database: String,
        collection: String
    ) -> Result<Vec<Any>, EngineError> {
        
        let inner = self.inner.lock().await;
        let db = inner.dbs.get(&database);
        match db {
            None => Err(EngineError::OpError(OpError::DatabaseNotFound(database))),
            Some(db) => { 
                let rep = db.list(collection.clone())
                    .map_err(|_e| EngineError::OpError(OpError::CollectionNotFound(database ,collection)));
                rep
            }
        }
    }

    async fn insert(
            &self,
            database: String,
            message: Any,
    ) -> Result<String, EngineError> {
        let inner = self.inner.lock().await;
        let db = inner.dbs.get(&database);
        match db {
            None => Err(EngineError::OpError(OpError::DatabaseNotFound(database))),
            Some(db) => { 
                let rep = db.insert(message).map_err(|err| EngineError::OpError(OpError::KeyAlreadyExists(err.into())));
                rep
            }
        }
    }

    async fn get(
        &self,
        database: String,
        collection: String,
        key: &[u8],
    ) -> Result<Any, EngineError> {
        let inner = self.inner.lock().await;
        if let Some(db) = inner.dbs.get(&database) {
            let schema = db.get_schema(collection.clone())
                .map_err(|e| EngineError::OpError(OpError::CollectionNotFound(collection.clone(), database)))?;
            let value = db.get(collection.clone(), key).unwrap();
            Ok(value)

        } else {
            return Err(EngineError::OpError(OpError::DatabaseNotFound(database)))
        }
    }
}

impl ProtolithDbEngine {
    pub fn new(
        db_config: db::Config,
        meta_store_config: meta_store::Config,
        schema_config: schema::Config,
        dbs: DatabasesMap,
    ) -> Self {
        let inner = Arc::new(Mutex::new(Inner { dbs }));

        Self {
            inner,
            db_config,
            schema_config,
            meta_store_config,
        }
    }

    // Method to destroy a specific database
    pub async fn destroy_db(&mut self, db_name: &str) -> Result<(), String> {
        let mut inner = self.inner.lock().await;

        // Check if the database exists
        if let Some(db) = inner.dbs.remove(db_name) {
            // Drop the db to close it
            drop(db);

            let base_path = &self.db_config.db_path;
            // Construct the path to the database
            let db_path = base_path.join(db_name);

            // Attempt to destroy the database
            match DB::destroy(&Options::default(), &db_path) {
                Ok(_) => {
                    let pb_descriptor_file = self.db_config.db_path.join(db_name);
                    let removed_file = fs::remove_dir_all(pb_descriptor_file.clone());
                    match removed_file {
                        Err(err) => {
                            error!(file = ?pb_descriptor_file, error = ?err, "Failed to remove protobuf descriptor")
                        },
                        Ok(_) => debug!(file = ?pb_descriptor_file, "Removed protobuf descriptor")
                    }
                    Ok(())
                },
                Err(e) => Err(format!("Failed to destroy the database: {}", e)),
            }
        } else {
            Err(format!("Database '{}' not found", db_name))
        }
    }

    pub async fn get_databse_collections(&self, db_name: &str ) -> Result<Vec<Collection>, Error> {
        let inner = self.inner.lock().await;
        if let Some(db_engine) = inner.dbs.get(db_name) {
            let collections = db_engine.get_collections().unwrap();
            Ok(collections)
        } else {
            Err(Box::new(EngineError::OpError(OpError::DatabaseNotFound(db_name.to_owned()))))
        }
    } 

}