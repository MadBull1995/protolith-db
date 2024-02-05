use std::{
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
};

use protolith_api::service::MetadataSvc;
pub use protolith_api::{
    pbjson_types::Empty,
    protolith::services::v1::{
        admin_service_client::AdminServiceClient, CreateDatabaseRequest, CreateDatabaseResponse,
        ListDatabasesResponse,
    },
};
use protolith_error::{Error, Result};
use tonic::{transport::Channel, IntoRequest};
use tracing::debug;

#[derive(Debug, Clone)]
pub struct Client {
    admin_client: AdminServiceClient<MetadataSvc>,
    session: String,
}

impl Client {
    pub fn new(channel: Channel, session: String) -> Self {
        const VERSION: Option<&str> = option_env!("CARGO_PKG_VERSION");

        let channel = tower::ServiceBuilder::new()
            .layer_fn(|service| MetadataSvc::new(service, VERSION.unwrap().to_owned()))
            .service(channel);
        let admin_client = AdminServiceClient::new(channel);
        Self {
            admin_client,
            session,
        }
    }

    pub async fn create_database(
        &mut self,
        name: &str,
        fd_path: PathBuf,
    ) -> Result<CreateDatabaseResponse, Error> {
        let f = File::open(fd_path.clone())?;
        let mut reader = BufReader::new(f);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;
        let mut request = CreateDatabaseRequest {
            name: name.to_owned(),
            file_descriptor_set: buffer,
        }
        .into_request();
        request
            .metadata_mut()
            .insert("protolith-session", self.session.parse().unwrap())
            .unwrap();
        let response = self.admin_client.create_database(request).await?;
        let md = response.metadata();
        debug!(metadata = ?md, "Incoming metadata");
        Ok(response.into_inner())
    }

    pub async fn list_databases(&mut self) -> Result<ListDatabasesResponse, Error> {
        let mut request = Empty::default().into_request();
        request
            .metadata_mut()
            .insert("protolith-session", self.session.parse().unwrap())
            .unwrap();

        let response = self.admin_client.list_databases(request).await?;
        Ok(response.into_inner())
    }
}
