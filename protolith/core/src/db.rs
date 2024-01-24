use rocksdb::{Options, ColumnFamilyDescriptor, Cache, BlockBasedOptions, WriteBatch, IteratorMode, DBIterator, Error as RocksDbError};

use std::{path::PathBuf, sync::Arc, collections::HashMap, default};
use protolith_api::{protolith::{
    core::v1::{Collection, Field},
    metastore::v1::{SchemaVersion, Schema, Index}, annotation::v1::IndexType
}, DescriptorPool, prost::bytes::Bytes, pbjson_types::{FieldDescriptorProto, field_descriptor_proto}, prost_wkt_types::{Any, Struct}};
use protolith_error::Error;
use thiserror::Error as tError;
use crate::{meta_store::{self, MetaStore}, schema};
use tracing::{debug, info, warn, error};
pub use rocksdb::DB;
use protolith_api::prost::Message;
use prost_reflect::{Kind, DynamicMessage, MessageDescriptor};

#[derive(Debug, Clone, tError)]
pub enum CoreError {
    #[error("{0}")]
    SchemaNotExists(String),
    #[error("key {1} already exists on collection {0}.")]
    KeyAlreadyExists(String, String),
    #[error("internal error: {0}")]
    Internal(String)
}

#[derive(Debug, Clone)]
pub struct Config {
    pub db_path: PathBuf,
    pub cache_size: usize,
    pub max_open_files: i32,
    pub descriptor_file_name: String,
}

impl Config {
    pub fn build(
        self,
        name: String,
        meta_store: meta_store::Config,
        schema: schema::Config,
        pool: DescriptorPool,
    ) -> Result<RocksDb, Error> {
        let mut db_opts = Options::default();
        let mut block_db_opts = BlockBasedOptions::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_max_open_files(self.max_open_files);

        let lru_cache = Cache::new_lru_cache(self.cache_size);
        block_db_opts.set_block_cache(&lru_cache);
        // Get the default column families
        let cloned_metastore = meta_store.clone();
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new("default", Options::default()),
            ColumnFamilyDescriptor::new(cloned_metastore.schema_cf_name, Options::default()),
            ColumnFamilyDescriptor::new(cloned_metastore.index_cf_name, Options::default()),
            ColumnFamilyDescriptor::new(cloned_metastore.schema_versions_cf_name, Options::default()),
        ];

        db_opts.set_block_based_table_factory(&block_db_opts);
        let path =self.db_path.join(&name);
        let existing_cf_names = match DB::list_cf(&Options::default(), &path) {
            Err(e) => None,
            Ok(list_cf) => Some(list_cf)
        };

        let existing_cf_names = match existing_cf_names {
            None => {
                debug!(db = ?name, "First init");
                vec![]
            },
            Some(cf_names) => {
                debug!(db = ?name, cfs = ?cf_names, "Existing column families for");
                cf_names
            }
        };
        
        // Filter out any column families that already exist
        let new_cf_descriptors: Vec<ColumnFamilyDescriptor> = cf_descriptors.into_iter().filter(|cf_desc| {
            !existing_cf_names.contains(&cf_desc.name().to_string())
        }).collect();

        // Combine existing and new column families
        let mut combined_cf_descriptors = existing_cf_names.iter().map(|cf_name| {
            ColumnFamilyDescriptor::new(cf_name, Options::default())
        }).collect::<Vec<_>>();
        combined_cf_descriptors.extend(new_cf_descriptors);


        // Process schema
        let mut collections = Vec::new();
        if let Some(collection_ext) = pool
            .get_extension_by_name("protolith.annotation.v1.collection") {
            let key_ext = pool.get_extension_by_name("protolith.annotation.v1.key").unwrap();
            for msg in pool.all_messages() {
                if msg.options().has_extension(&collection_ext) {
                    let fields = msg
                        .fields()
                        .map(|f| Field { 
                            name: f.name().to_string(),
                            r#type: Some(parse_field_type(f.kind()))
                        })
                        .collect();
                    let indexes = msg
                        .fields()
                        .filter(|f| f.options().has_extension(&key_ext))
                        .map(|f| Index {
                            index_id: format!("{}:{}", msg.full_name(), f.name()),
                            schema_id: msg.full_name().to_string(),
                            field_name: f.name().to_string(),
                            index_type: IndexType::Key.into(),
                            ..Default::default()
                        }).collect();
                    collections.push(Collection {
                        name: msg.name().to_owned(),
                        full_name: msg.full_name().to_owned(),
                        descriptor: msg.descriptor_proto().encode_to_vec(),
                        fields,
                        indexes,
                    })
                }
            }
        }

        for collection in &collections {
            let cf_descriptors = parse_collection_to_cf(collection.clone());
            debug!(collection = ?collection.full_name, index_cfs =? cf_descriptors.len(), "Building CFs for");
            combined_cf_descriptors.extend(cf_descriptors);
        }

        let db = DB::open_cf_descriptors(&db_opts, &path, combined_cf_descriptors)?;
        
        debug!(db = ?name, config = ?meta_store, "Building Metastore");
        let db = Arc::new(db);
        
        let meta_store = meta_store.clone().build(db.clone(), collections, schema).unwrap();
        Ok(RocksDb {
            db,
            name,
            path,
            opts: db_opts,
            meta_store: meta_store,
            pool,
        })
    }
}

#[derive(Clone)]
pub struct RocksDb {
    db: Arc<DB>,
    pub name: String,
    pub path: PathBuf,
    pub opts: Options,
    meta_store: MetaStore,
    pool: DescriptorPool,
}

#[derive(Debug, Clone, PartialEq, Eq, tError)]
pub enum DBError {

    #[error("Failed to get cf handle for {0}")]
    InvalidColumnFamily(String),

}

impl RocksDb {
    
    pub fn get(&self, collection: String, key: &[u8]) -> Result<Any, Error> {
        let cf = self.db.cf_handle("default").unwrap();
        let value = self.db.get_cf(cf, key).unwrap();

        let message_desc = self.pool.get_message_by_name(&collection).unwrap();
        
        let buf = Bytes::from(value.unwrap());
        let dynamic_message = DynamicMessage::decode(message_desc, buf).unwrap();
        let mut buf = Vec::new();
        dynamic_message.encode(&mut buf).unwrap();
        let any = Any { 
            type_url: format!("type.googleapis.com/{}", collection),
            value: buf
        };
        Ok(any)
    }

    pub fn get_schema(&self, collection: String) -> Result<Schema, Error> {
        let schema = self.meta_store.get_schema(collection)?;
        Ok(schema)
    }

    pub fn create_schema(&mut self, collection: String, key: String, version: u64) -> Result<Schema, Error> {
        self.meta_store.create_schema(Collection { 
            name: collection.clone(),
            full_name: collection.clone(),
            fields: vec![],
            indexes: vec![
                Index {
                    field_name: key.clone(),
                    schema_id: format!("{}:{}", collection, version),
                    index_type: IndexType::Key.into(),
                    index_id: format!("{}:{}", collection, key),
                    ..Default::default()
                }
            ],
            ..Default::default()
        })
    }
    
    pub fn get_collection(&self, collection: String) -> Result<Collection, Error> {
        let schema = self.meta_store.get_schema(collection)?;
        let buf = Bytes::from(schema.schema_definition);
        let col = Collection::decode(buf).unwrap();
        Ok(col)
    }

    pub fn get_collections(&self) -> Result<Vec<Collection>, Error> {
        let mut collections = Vec::new();
        // Retrieve handle for the schema_versions column family
        let versions_cf_handle = self.db.cf_handle(&self.meta_store.schema_versions)
            .ok_or_else(|| DBError::InvalidColumnFamily(self.meta_store.schema_versions.clone()))?;
        // Retrieve handle for the schema column family
        let schema_cf_handle = self.db.cf_handle(&self.meta_store.schema)
            .ok_or_else(|| DBError::InvalidColumnFamily(self.meta_store.schema.clone()))?;
        // Retrieve handle for the index column family
        let index_cf_handle = self.db.cf_handle(&self.meta_store.index)
            .ok_or_else(|| DBError::InvalidColumnFamily(self.meta_store.index.clone()))?;
        if self.meta_store.schema_config.enable_versioning {

            // Seek for latest version of the schemas
            let mut latest_versions = std::collections::HashMap::new();
            // let iter_mode = IteratorMode::From((), ())
            let mut iter = self.db.iterator_cf(versions_cf_handle, IteratorMode::Start);
            for key_value in iter {
            match key_value {
                Err(e) => error!("{}", e.into_string()),
                Ok(key_value) => {
                    let (key, value) = key_value;
                    let (schema, version) = parse_schema_version_id(&key).expect("Failed to read schema version key");
                    if deserialize_schema_version(&value).is_current {
                        // let schema_key = format!("{schema}:{version}").as_bytes();
                        latest_versions.insert(schema, version);
                    }
                }
            }
            }

            // We return fast here since the DB isnt populated yet
            if latest_versions.is_empty() {
                return Ok(vec![])
            }

            // Retrieve schemas by the cf schema and collect them
            for (schema_id, version) in latest_versions {
            let key = make_schema_key(&schema_id, &version); // Implement this based on your key structure
            if let Some(schema_bytes) = self.db.get_cf(schema_cf_handle, &key)? {
                let schema = deserialize_schema(&schema_bytes); // Implement schema deserialization
                collections.push(Collection {
                    full_name: schema_id,
                    ..Default::default()
                });
            }
            }

            // Collect for each schema the indexes related to it via cf index
            for collection in &mut collections {
                let collection_key_prefix = collection.full_name.clone().into_bytes();
                let iter_mod = IteratorMode::From(&collection_key_prefix, rocksdb::Direction::Forward);
                let mut iter = self.db.iterator_cf(index_cf_handle, iter_mod);
                for key_value in iter {
                    match key_value {
                        Err(err) => error!("{}", err.into_string()),
                        Ok((key, value)) => {
                            let schema_id = parse_schema_id_from_key(&key).unwrap(); // Implement this
                            if schema_id == collection.full_name {
                                let index_info = deserialize_index_info(&value); // Implement this
                                collection.indexes.push(index_info);
                            }

                            if !schema_id.starts_with(&collection.full_name) {
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            let mut iter = self.db.iterator_cf(schema_cf_handle, IteratorMode::Start);
            for schema in iter {
                match schema {
                    Err(err) => error!("{}", err),
                    Ok((schema_id, schema)) => {
                        let schema_id = parse_schema_id_from_key(&schema_id).unwrap();
                        let schema = deserialize_schema(&schema); // Implement schema deserialization
                        let buf = Bytes::from(schema.schema_definition);
                        let mut collection = Collection::decode(buf).unwrap();
                        let collection_key_prefix = schema_id.into_bytes();
                        let iter_mod = IteratorMode::From(&collection_key_prefix, rocksdb::Direction::Forward);
                        let mut iter = self.db.iterator_cf(index_cf_handle, iter_mod);
                        for idx in iter {
                            match idx {
                                Err(err) => error!("{}", err.into_string()),
                                Ok((key, value)) => {
                                    let schema_id = parse_schema_id_from_key(&key).unwrap(); // Implement this
                                    if schema_id == collection.full_name {
                                        let index_info = deserialize_index_info(&value); // Implement this
                                        collection.indexes.push(index_info);
                                    }
        
                                    if !schema_id.starts_with(&collection.full_name) {
                                        break;
                                    }
                                }
                            } 
                        }
                        collections.push(collection);

                    }
                }
            }
        }
        
       
        Ok(collections)
    }

    pub fn insert(&self, message: Any) -> Result<String, CoreError> {
        let message_name = message.type_url.split("/").collect::<Vec<&str>>()[1];
        let message_desc = self.pool.get_message_by_name(&message_name).unwrap();
        
        let buf = Bytes::from(message.clone().value);
        let dynamic_message = DynamicMessage::decode(message_desc, buf).unwrap();
        
        let cf = self.db.cf_handle("default").unwrap();
        let schema: Schema = self.meta_store.get_schema(message_name.to_owned())
            .map_err(|e| CoreError::SchemaNotExists(e.to_string()))?;
        let buf = Bytes::from(schema.schema_definition);
        let col = Collection::decode(buf).unwrap();
        let idx = col.indexes.iter().find(|key| key.index_type()==IndexType::Key).unwrap();
        let binding = dynamic_message.get_field_by_name(&idx.field_name).unwrap();
        let idx_field = binding.as_ref();
        let key = format!("{}:{}", message_name, idx_field.as_str().unwrap());
        debug!(collection = ?message_name, key = ?key, bytes = ?message.value.len(), "insert");
        let exist = self.db.get_pinned_cf(cf, key.clone().into_bytes())
            .map_err(|e| CoreError::Internal(e.into_string()))?;
        match exist {
            None => {
                self.db.put_cf(cf, key.into_bytes(), message.value).unwrap();
                Ok(message_name.to_owned())
            },
            Some(_) => Err(CoreError::KeyAlreadyExists(message_name.to_owned(),key).into())
        }
    }

    pub fn list(
        &self,
        collection: String,
    ) -> Result<Vec<Any>, Error> {
        debug!(db = self.name.clone(), collection = collection);
        let message_desc = self.pool.get_message_by_name(&collection).unwrap();
        let mut data = Vec::new();
        let prefix = collection.clone().into_bytes();
        let cf_handle = self.db.cf_handle("default").unwrap();
        let iter_mode = IteratorMode::From(&prefix, rocksdb::Direction::Forward);
        let iter = self.db.iterator_cf(cf_handle, iter_mode);
        for i in iter {
            match i {
                Ok(item) => {
                    if !item.0.starts_with(&prefix) {
                        break;
                    } 
        
                    let buf = Bytes::from(item.1);
                    let dynamic_message = DynamicMessage::decode(message_desc.clone(), buf).unwrap();
                    let mut buf = Vec::new();
                    dynamic_message.encode(&mut buf).unwrap();
                    let any = Any { 
                        type_url: format!("type.googleapis.com/{}", collection),
                        value: buf
                    };
                    data.push(any)
                },
                Err(e) => error!(err = ?e, "failed to iter item")
            }
        }
        Ok(data)
    }
}

fn deserialize_schema_version(schema_version_bytes: &[u8]) -> SchemaVersion {
    SchemaVersion::decode(schema_version_bytes).expect("Failed to decode SchemaVersion")
}

fn deserialize_schema(schema_bytes: &[u8]) -> Schema {
    Schema::decode(schema_bytes).expect("Failed to decode Schema")
}

fn deserialize_index_info(index_bytes: &[u8]) -> Index {
    Index::decode(index_bytes).expect("Failed to decode Index")
}

fn parse_schema_version_id(key: &[u8]) -> Result<(String, u64), Error> {
    let key_str = std::str::from_utf8(key).map_err(|_| "Invalid UTF-8 sequence")?;
    let parts: Vec<&str> = key_str.split(':').collect();
    // Ensure that the key contains exactly two parts
    if parts.len() != 2 {
        return Err("Key does not contain exactly two parts separated by ':'".into());
    }
    Ok((parts[0].to_string(), parts[1].parse().unwrap()))
}

fn make_schema_key<'a>(schema_id: &'a str, version: &'a u64) -> Vec<u8> {
    format!("{}:{}", schema_id, version).into_bytes()
}

fn parse_schema_id_from_key(key:&[u8]) -> Result<String, Error> {
    let key_str = std::str::from_utf8(key).map_err(|_| "Invalid UTF-8 sequence")?;
    let parts: Vec<&str> = key_str.split(':').collect();
    // Ensure that the key contains exactly two parts
    if parts.len() != 2 {
        return Err("Key does not contain exactly two parts separated by ':'".into());
    }
    Ok(parts[0].to_string())
}


fn parse_collection_to_cf(collection: Collection) -> Vec<ColumnFamilyDescriptor> {
    let mut idx_cfs = Vec::new();
    for idx in collection.indexes {
        let cf_name = format!("{}:{}", collection.full_name, idx.field_name);
        idx_cfs.push(ColumnFamilyDescriptor::new(cf_name, Options::default()));
    }

    idx_cfs
}

fn parse_field_type(kind: Kind ) -> i32 {
    match kind {
        Kind::Double => field_descriptor_proto::Type::Double.into(),
        Kind::Float => field_descriptor_proto::Type::Float.into(),
        Kind::Int64 => field_descriptor_proto::Type::Int64.into(),
        Kind::Uint64 => field_descriptor_proto::Type::Uint64.into(),
        Kind::Int32 => field_descriptor_proto::Type::Int32.into(),
        Kind::Fixed64 => field_descriptor_proto::Type::Fixed64.into(),
        Kind::Fixed32 => field_descriptor_proto::Type::Fixed32.into(),
        Kind::Bool => field_descriptor_proto::Type::Bool.into(),
        Kind::String => field_descriptor_proto::Type::String.into(),
        Kind::Message(_) => field_descriptor_proto::Type::Message.into(),
        Kind::Bytes => field_descriptor_proto::Type::Bytes.into(),
        Kind::Uint32 => field_descriptor_proto::Type::Uint32.into(),
        Kind::Enum(e) => field_descriptor_proto::Type::Enum.into(),
        Kind::Sfixed32 => field_descriptor_proto::Type::Sfixed32.into(),
        Kind::Sfixed64 => field_descriptor_proto::Type::Sfixed64.into(),
        Kind::Sint32 => field_descriptor_proto::Type::Sint32.into(),
        Kind::Sint64 => field_descriptor_proto::Type::Sint64.into(),
    }
}