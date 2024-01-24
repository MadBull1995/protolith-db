use std::path::PathBuf;
use admin::Client;
use protolith_core::{
    api::{protolith::{
        services::v1::{CreateDatabaseResponse, ListDatabasesResponse, InsertResponse},
        types::v1::{Op, OpStatus, ApiOp}, test::v1::{MyCollection, NotCollection, OtherCollection},
    }, prost_wkt_types::Any, prost::Message},
    error::{Error, Result}, meta_store,
};
use protolith_engine::client::{self as engine, Collection};
use tonic::transport::{Endpoint, Channel};

use protolith_admin as admin;
use tower::ServiceBuilder;



#[macro_export]
/// Creates a model to query from a compiled protobuf struct
/// 
/// # Example
/// ```no_run
/// use protolith_client::{ProtolithDb, create_model};
/// use path::to::package::MyMessage;
/// 
/// const ADDR: &str = "http://localhost:5678";
/// let protolith = ProtolithDb::connect(ADDR)
/// let db = protlith.db("my_new_db");
/// 
/// create_model!(MyMessage, MyMessageModel, String);
/// let my_message_model = MyMessageModel::new(db);
/// 
/// my_message_model.insert(MyMessage{
///     ..Default::default()
/// }).await;
/// ```
macro_rules! create_model {
    ($proto_struct_name:ident, $model_name:ident, $key_type:ty) => {
        #[derive(Debug, Clone)]
        struct $model_name {
            db: protolith_engine::client::Client,
        }

        impl protolith_engine::client::Collection for $model_name {
            type Key = $key_type;
            type Message = $proto_struct_name;

            async fn list(&mut self) -> Result<Vec<protolith_engine::client::Response<$proto_struct_name>>, Box<dyn std::error::Error + Send + Sync>> {
                self.db.list::<Self>().await
            }

            async fn insert(&mut self, msg: Self::Message) -> Result<protolith_core::api::protolith::services::v1::InsertResponse, Box<dyn std::error::Error + Send + Sync>> {
                self.db.insert::<Self>(msg).await
            }

            async fn get(&mut self, key: Key<Self::Key>) -> Result<protolith_engine::client::Response<$proto_struct_name>, Box<dyn std::error::Error + Send + Sync>> {
                self.db.get::<Self>(&key).await
            }
        }

        impl $model_name {
            #[allow(unused)]
            pub fn new(db: protolith_engine::client::Client) -> Self {
                Self {
                    db
                }
            }
        }

    };
}

#[derive(Debug, Clone)]
pub struct ProtolithDb {
    admin: admin::Client,
    meta_store: meta_store::Client,
    channel: Channel,
}

impl ProtolithDb {
    pub async fn connect(addr: &'static str) -> Result<Self, Error> {
        let channel = Endpoint::from_static(&addr)
            .connect()
            .await?;
        
        Ok(Self {
            channel: channel.clone(),
            admin: admin::Client::new(channel.clone()),
            meta_store: meta_store::Client {  },
        })
    }

    pub fn db(&self, database: &str) -> Result<engine::Client, Error> {
        let client = engine::Client::new(self.channel.clone(), database.to_owned());
        Ok(client)
    }

    // pub fn with_model<M>(&self, models: &str) -> M::Model
    // where
    //     M: ModelFactory<Model = M>
    // {
    //     // let client = engine::Client::new(self.channel.clone(), database.to_owned());

    //     M::model(m, db)
    // }

    // pub async fn collection(&self, databse: &str, collection: &str) -> Result<impl Collection, Error> {
    //     let client = engine::Client::new(self.channel.clone(), database.to_owned());
    //     let collection = client.collection(collection);
    //     Ok(collection)
    // }

    pub async fn create_database(&mut self, database: &str, fd_path: PathBuf) -> Result<CreateDatabaseResponse, Error> {
        self.admin.create_database(database, fd_path).await
    }
    
    pub async fn list_databases(&mut self) -> Result<ListDatabasesResponse, Error> {
        self.admin.list_databases().await
    }
}

#[cfg(test)]
mod test {
    use std::{str::FromStr, error::Error};

    use super::*;
    use protolith_core::{api::protolith::test::v1::{MyCollection, OtherCollection}, Key};
    use protolith_macros::Collection;
    use tokio;
    
    pub const ADDR: &str = "http://localhost:5678";

    #[tokio::test]
    async fn test_list_databases() {
        let mut protolithdb = ProtolithDb::connect(ADDR).await.expect("unable to connect");
        let dbs = protolithdb.list_databases().await.unwrap();
        // assert_eq!(dbs.databases.len(), 1);
        dbg!(dbs);
        let rep = protolithdb
            .create_database("my_new_db", PathBuf::from_str("/Users/amitshmulevitch/rusty-land/protolith-db/descriptor.bin").unwrap())
            .await
            .unwrap();
        println!("{:?}", rep);
        let dbs = protolithdb.list_databases().await.unwrap();
        dbg!(dbs);
    }

    #[tokio::test]
    async fn test_insert() {
        let mut protolithdb: ProtolithDb = ProtolithDb::connect(ADDR).await.expect("unable to connect");
        let mut db = protolithdb.db("protolith").unwrap();
        
        create_model!(MyCollection, Collection1Model, String);
        let mut model_1 = Collection1Model::new(db.clone());

        create_model!(OtherCollection, Collection2Model, String);
        let mut model_2 = Collection2Model::new(db.clone());

        create_model!(NotCollection, Collection3Model, String);
        let mut model_3 = Collection3Model::new(db);

        let msg = MyCollection {
            id: "some_id".to_string(),
            name: "some cool message".to_string()
        };
        model_1.insert(msg).await;
        
        let msg = MyCollection {
            id: "some_other_id".to_string(),
            name: "some other message".to_string()
        };
        model_1.insert(msg).await;

        let msg = OtherCollection {
            some_key: "some_other_id".to_string(),
            data: "some other message".to_string()
        };
        model_2.insert(msg).await;

        let rep = model_3.insert(NotCollection {
            ..Default::default()
        }).await;

        dbg!(rep);
    }

    #[tokio::test]
    async fn test_get() {
        let mut protolithdb: ProtolithDb = ProtolithDb::connect(ADDR).await.expect("unable to connect");
        let mut db = protolithdb.db("protolith").unwrap();
        create_model!(MyCollection, CollectionModel, &'static str);
        let mut model = CollectionModel::new(db);
        let rep = model.get(Key::new("some_id")).await.unwrap();
        dbg!(rep.into_inner());   
    }

    #[tokio::test]
    async fn test_iter() {
        let mut protolithdb: ProtolithDb = ProtolithDb::connect(ADDR).await.expect("unable to connect");
        let mut db = protolithdb.db("protolith").unwrap();
        create_model!(MyCollection, CollectionModel, String);
        let mut model = CollectionModel::new(db);

        let rep = model.list().await.unwrap();
        // db.get("", "").await;
        for i in rep {
            dbg!(i.into_inner().unwrap());
            
        }
    }

    use protolith_core::collection::Wrapper;
    use serde_json;
    use serde::{Deserialize, Serialize};
    use protolith_core::api::prost_wkt_types::Struct;

    #[tokio::test]
    async fn test_collection_struct() {
        #[derive(Debug, Clone, Deserialize, Serialize, Collection)]
        struct MyStruct {
            id: String,
            data: Vec<String>,
        }

        impl MyStruct {
            pub fn new(id:String) -> Self {
                Self {
                    id,
                    data: vec![], 
                }
            }
        }

        let s = MyStruct::new("Hello world".to_string());
        let s = s.serialize();
        dbg!(MyStruct::into_struct(s));
    }
}

// trait ModelFactory {
//     type Model: Collection;
//     fn model(m: &Self, db: Client) -> Self::Model;
// }