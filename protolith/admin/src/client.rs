use std::{path::PathBuf, fs::File, io::{BufReader, Read}};

use protolith_api::service::MetadataSvc;
use protolith_engine::EngineError;
use protolith_error::{Result, Error};
use tonic::{transport::Channel, Request, Status, IntoRequest, service::interceptor::InterceptedService, Response};
pub use protolith_api::{protolith::services::v1::{admin_service_client::AdminServiceClient, CreateDatabaseResponse, CreateDatabaseRequest, ListDatabasesResponse}, pbjson_types::Empty};
use tracing::{debug, info};
#[derive(Debug, Clone)]
pub struct Client {
    admin_client: AdminServiceClient<MetadataSvc>
}


impl Client {
    
    pub fn new(channel: Channel) -> Self {
        const VERSION: Option<&str> = option_env!("CARGO_PKG_VERSION");
        
        let channel = tower::ServiceBuilder::new()
            // .layer(tonic::service::interceptor(user_agent_interceptor))
            .layer_fn(|service| MetadataSvc::new(service, VERSION.unwrap().to_owned()))
            .service(channel);
        let admin_client = AdminServiceClient::new(channel);
        Self {
            admin_client,
        }
    }

    pub async fn create_database(
        &mut self,
        name: &str,
        fd_path: PathBuf
    ) -> Result<CreateDatabaseResponse, Error> {
        
        let f = File::open(fd_path.clone())?;
        let mut reader = BufReader::new(f);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;
        
        let response = self
            .admin_client
            .create_database(CreateDatabaseRequest {
                name: name.to_owned(),
                file_descriptor_set: buffer,
            }.into_request()).await?;
        let md = response.metadata();
        debug!(metadata = ?md, "Incoming metadata");
        Ok(response.into_inner())
    }

    pub async fn list_databases(
        &mut self,
    ) -> Result<ListDatabasesResponse, Error> {
        let response = self
            .admin_client
            .list_databases(Empty::default())
            .await?;
        Ok(response.into_inner())
    }

}