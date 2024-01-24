use std::{collections::HashMap, sync::Arc, time};

use protolith_api::{protolith::{
    metastore::v1::{SchemaVersion, Schema},
    core::v1::Collection
}, pbjson_types::Timestamp};
use protolith_error::{Result, Error};
use rocksdb::{DB, IteratorMode};
use tracing::{error, debug, info};
use protolith_api::prost::Message;
use crate::{db::RocksDb, schema};

#[derive(Debug, Clone)]
pub struct MetaStore {
    pub(crate) schema_versions: String,
    pub(crate) index: String,
    pub(crate) schema: String,
    pub(crate) schema_config: schema::Config,
    collections: Vec<Collection>,
    users: Vec<()>,
    db: Arc<DB>,
    cache: HashMap<String, Schema>,
}

#[derive(Debug, Clone)]
pub struct Config {
    /// Column family name for storing schema metadata.
    pub schema_cf_name: String,

    /// Column family name for storing index metadata.
    pub index_cf_name: String,

    /// Column family name for storing schema version metadata.
    pub schema_versions_cf_name: String,
}

#[derive(Debug, Clone)]
pub struct Client {
    
}

impl Config {
    pub fn build(
        self,
        db: Arc<DB>,
        mut collections: Vec<Collection>,
        schema: schema::Config,
    ) -> Result<MetaStore, Error> {
        
        let Self {
            index_cf_name,
            schema_cf_name,
            schema_versions_cf_name
        } = self;
        let mut cache = HashMap::new();
        for mut collection in collections.clone() {
            debug!(versioned = ?schema.enable_versioning, "Building schema");
            if schema.enable_versioning {
                cache.insert(collection.clone().full_name, handle_versioned_schema(
                    schema_cf_name.clone(),
                    schema_versions_cf_name.clone(),
                    db.clone(),
                    &mut collection
                ));
            } else {
                cache.insert(collection.clone().full_name, handle_no_version_schema(schema_cf_name.clone(), db.clone(), &mut collection));
            }
            
        }
        
        Ok(MetaStore {
            collections,
            users: Vec::new(),
            index: index_cf_name,
            schema: schema_cf_name,
            schema_versions: schema_versions_cf_name,
            schema_config: schema,
            db,
            cache,
        })
    }

    
    
}

impl MetaStore {
    pub fn create_user(&self) -> Result<(), Error> {
        
        Ok(())
    }

    pub fn create_schema(&mut self, mut collection_schema: Collection) -> Result<Schema, Error> {
        let schema = handle_no_version_schema(self.schema.clone(), self.db.clone(), &mut collection_schema);
        self.cache.insert(collection_schema.full_name, schema.clone());
        Ok(schema)
    }

    pub fn get_schema(&self, collection: String) -> Result<Schema, Error> {
        
        if self.schema_config.enable_versioning {
            todo!()
        } else {
            if let Some(schema) = self.cache.get(&collection) {
                return Ok(schema.clone())
            } else {
                let schema_cf = self.db.cf_handle(&self.schema).unwrap();
                let key: Vec<u8> = format!("{}:{}", collection, self.schema_config.default_version).into_bytes();
                let schema = self.db.get_cf(schema_cf, key)?;
                if let Some(schema) = schema {
                    let schema = deserialize_schema(&schema); // Implement schema deserialization
                    return Ok(schema)
                } else {
                    Err(format!("schema {} not found", collection).into())
                }
            }
        }
    }
}

fn parse_schema_version_from_key(key: &[u8]) -> Result<u64, Error> {
    let key_str = std::str::from_utf8(key).map_err(|_| "Invalid UTF-8 sequence")?;
    let parts: Vec<&str> = key_str.split(':').collect();
    // Ensure that the key contains exactly two parts
    if parts.len() != 2 {
        return Err("Key does not contain exactly two parts separated by ':'".into());
    }

    // Return the two parts as a tuple
    Ok(parts[1].parse().unwrap())
}

fn handle_versioned_schema(schema_cf_name: String, schema_versions_cf_name: String, db: Arc<DB>, collection: &mut Collection) -> Schema {
    let prefix = collection.clone().full_name.into_bytes();
    let schema_handle = db.cf_handle(&schema_cf_name).unwrap();
    let schema_versions_handle = db.cf_handle(&schema_versions_cf_name).unwrap();
    let iter_mod = IteratorMode::From(&prefix, rocksdb::Direction::Forward);
    let mut iter = db.iterator_cf(schema_versions_handle, iter_mod);
    let mut latest = 0;
    for schema_ver in iter {
        match schema_ver {
            Err(err) => error!("{}", err),
            Ok((key, _)) => {
                let ver = parse_schema_version_from_key(&key).unwrap();
                if ver > latest {
                    latest = ver;
                }
            }
        }
    }

    if latest == 0 {
        // Handling new schema
        let schema_id = collection.clone().full_name;
        latest+=1;
        let key = format!("{}:{}", schema_id, latest);
        let schema_ver = SchemaVersion {
            schema_id: schema_id.clone(),
            version_number: latest,
            is_current: true,
            creation_timestamp: None
        };
        for idx in &mut collection.indexes {
            idx.schema_id += &format!(":{}", latest);
        }
        let schema = Schema {
            schema_id,
            schema_version: latest,
            schema_definition: collection.encode_to_vec(),
            ..Default::default()
        };

        let mut buf_ver = vec![];
        let mut buf_schema = vec![];
        schema_ver.encode(&mut buf_ver).unwrap();
        schema.encode(&mut buf_schema).unwrap();
        db.put_cf(schema_versions_handle, &key, buf_ver).unwrap();
        db.put_cf(schema_handle, &key, buf_schema).unwrap();
        info!(collection = ?key, version = ?latest, "Created new versioned schema:");
        return schema
    } else {
        // Handling updating version for schema
        todo!()

    }
}

fn handle_no_version_schema(schema_cf_name: String, db: Arc<DB>, collection: &mut Collection) -> Schema{
    let schema_cf_handle = db.cf_handle(&schema_cf_name).unwrap();
    let schema_id = &collection.full_name;
    // TODO: Handle default version from Env
    let default_ver = 1;
    for idx in &mut collection.indexes {
        idx.schema_id += &format!(":{}", default_ver);
    }
    let key = format!("{}:{}", schema_id, default_ver).into_bytes();
    let schema = Schema {
        schema_id: schema_id.to_string(),
        schema_version: default_ver,
        schema_definition: collection.encode_to_vec(),
        ..Default::default()
    };

    let mut buf_schema = vec![];
    schema.encode(&mut buf_schema).unwrap();
    db.put_cf(schema_cf_handle, key.clone(), buf_schema).unwrap();
    info!(collection = ?schema_id, version = ?default_ver, "Created new schema:");
    schema
}


fn deserialize_schema(schema_bytes: &[u8]) -> Schema {
    Schema::decode(schema_bytes).expect("Failed to decode Schema")
}