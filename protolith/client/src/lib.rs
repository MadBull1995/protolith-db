use std::path::PathBuf;

use protolith_core::{
    api::{protolith::{
        services::v1::{CreateDatabaseResponse, ListDatabasesResponse, InsertResponse},
        types::v1::{Op, OpStatus, ApiOp},
    }, prost_wkt_types::Any},
    error::{Error, Result}, meta_store,
};
use protolith_engine::client as engine;
use tonic::transport::{Endpoint, Channel};

use protolith_admin as admin;
use tower::ServiceBuilder;
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

    pub async fn db(&self, database: &str) -> Result<engine::Client, Error> {
        let client = engine::Client::new(self.channel.clone(), database.to_owned());
        Ok(client)
    }

    pub async fn get_database(&mut self, name: &str) -> Result<(), Error> {
        // let req = GetDatabaseRequest {
        //     name: name.to_string()
        // };

        // let raw_db = self.admin.get_database(req).await;

        // let 
        Ok(())
    }

    pub async fn create_database(&mut self, database: &str, fd_path: PathBuf) -> Result<CreateDatabaseResponse, Error> {
        self.admin.create_database(database, fd_path).await
    }
    
    pub async fn list_databases(&mut self) -> Result<ListDatabasesResponse, Error> {
        self.admin.list_databases().await
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use super::*;
    use protolith_core::{api::protolith::test::v1::MyCollection, Key};
    use tokio;
    
    pub const ADDR: &str = "http://localhost:5678";

    #[tokio::test]
    async fn test_list_databases() {
        let mut protolithdb = ProtolithDb::connect(ADDR).await.expect("unable to connect");
        let dbs = protolithdb.list_databases().await.unwrap();
        assert_eq!(dbs.databases.len(), 1);
        dbg!(dbs);
        let rep = protolithdb
            .create_database("my_new_db", PathBuf::from_str("/Users/amitshmulevitch/rusty-land/protolith-db/descriptors.bin").unwrap())
            .await
            .unwrap();
        println!("{:?}", rep);
        let dbs = protolithdb.list_databases().await.unwrap();
        dbg!(dbs);
    }

    #[tokio::test]
    async fn test_insert() {
        let mut protolithdb: ProtolithDb = ProtolithDb::connect(ADDR).await.expect("unable to connect");
        let mut db = protolithdb.db("my_new_db").await.unwrap();
        let msg = MyCollection {
            id: "some_id".to_string(),
            name: "some cool message".to_string()
        };
        let rep = db.insert(msg).await;
        // db.get("", "").await;
        println!("{:?}", rep);
    }

    #[tokio::test]
    async fn test_get() {
        let mut protolithdb: ProtolithDb = ProtolithDb::connect(ADDR).await.expect("unable to connect");
        let mut db = protolithdb.db("my_new_db").await.unwrap();
        
        let rep = db.get("protolith.test.v1.MyCollection".to_string(), &Key::new("some_id")).await.unwrap();
        // db.get("", "").await;
        if let Some(data) = rep.data {
            let msg = data.unpack_as(MyCollection::default()).unwrap();

            println!("{:?}", msg.id);
            
        }
    }
}