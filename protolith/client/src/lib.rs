use std::{path::PathBuf, fs::{File, self}};
use admin::Client;
use std::io::Write;
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
use protolith_auth as auth;
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
    session: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ConnectionOpt {
    addr: &'static str,
    username: String,
    password: String,
    clear_sessions: bool,
}

impl Default for ConnectionOpt {
    fn default() -> Self {
        ConnectionOpt {
            addr: "http://0.0.0.0:5678",
            password: "protolith".to_owned(),
            username: "protolith".to_owned(),
            clear_sessions: false,
        }
    }
}

impl ProtolithDb {
    pub async fn connect(connection_opt: ConnectionOpt) -> Result<Self, Error> {
        let channel = Endpoint::from_static(&connection_opt.addr)
            .connect()
            .await?;
        let home = env!("HOME");
        let path = format!("{}/{}", home, "protolith_session.txt");
        let cloned_path = path.clone();
        let cloned_chan = channel.clone();
        let login = || async move {
            // If the file doesn't exist or can't be read, authenticate and write the session to the file
            let mut auth = auth::Client::new(cloned_chan.clone());
            let s = auth.login(&connection_opt.username, &connection_opt.password).await.unwrap();
            let session = s.session;

            // Write the session to the file
            let mut file = File::create(cloned_path).unwrap();
            write!(file, "{}", session).unwrap();

            session
        };
        // Try to read the session from the file
        let session = if connection_opt.clear_sessions {
            login().await
        } else {
            match fs::read_to_string(path.clone()) {
                Ok(contents) => contents,
                Err(_) => {
                    login().await
                }
            }
        };
        
        Ok(Self {
            channel: channel.clone(),
            admin: admin::Client::new(channel.clone(), session.clone()),
            meta_store: meta_store::Client {  },
            session: Some(session),
        })
    }

    pub fn db(&self, database: &str) -> Result<engine::Client, Error> {
        if let Some(session) = &self.session {
            let client = engine::Client::new(self.channel.clone(), database.to_owned(), session.to_string());
            Ok(client)
        } else {
            Err("Must have a session before interacting with db".into())
        }
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
    pub const USER: &str = "protolith";
    pub const PASS: &str = "protolith";
    
    #[tokio::test]
    async fn test_list_databases() {
        let conn_opt = ConnectionOpt::default();
        let mut protolithdb = ProtolithDb::connect(conn_opt).await.expect("unable to connect");
        let dbs = protolithdb.list_databases().await.unwrap();
        // assert_eq!(dbs.databases.len(), 1);
        let rep = protolithdb
            .create_database("my_new_db", PathBuf::from_str("/Users/amitshmulevitch/rusty-land/protolith-db/descriptor.bin").unwrap())
            .await
            .unwrap();
        println!("{:?}", rep);
        let dbs = protolithdb.list_databases().await.unwrap();
    }

    #[tokio::test]
    async fn test_insert() {
        let conn_opt = ConnectionOpt::default();
        let protolithdb: ProtolithDb = ProtolithDb::connect(conn_opt)
            .await
            .expect("unable to connect");
        let db = protolithdb.db("protolith").unwrap();
        
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
        let conn_opt = ConnectionOpt::default();
        let mut protolithdb: ProtolithDb = ProtolithDb::connect(conn_opt).await.expect("unable to connect");
        let mut db = protolithdb.db("protolith").unwrap();
        create_model!(MyCollection, CollectionModel, &'static str);
        let mut model = CollectionModel::new(db);
        let rep = model.get(Key::new("some_id")).await.unwrap();
    }

    #[tokio::test]
    async fn test_iter() {
        let conn_opt = ConnectionOpt::default();
        let mut protolithdb: ProtolithDb = ProtolithDb::connect(conn_opt).await.expect("unable to connect");
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

        // let s = MyStruct::new("Hello world".to_string());
        // let s = s.serialize();
        // MyStruct::insert(s);
        // dbg!(MyStruct::into_struct(s));
    }
}

// trait ModelFactory {
//     type Model: Collection;
//     fn model(m: &Self, db: Client) -> Self::Model;
// }