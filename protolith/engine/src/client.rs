use protolith_core::{
    Key,
    api::{
        prost::Message,
        prost_wkt_types::{Any, MessageSerde},
        protolith::services::v1::{
            engine_service_client::EngineServiceClient, InsertRequest, InsertResponse, GetRequest, GetResponse,
        },
        service::MetadataSvc, pbjson_types::Value,
    },
    error::Error,
};
use tonic::{transport::Channel, IntoRequest};

#[derive(Debug, Clone)]
pub struct Client {
    engine_client: EngineServiceClient<MetadataSvc>,
    database: String,
}

impl Client {
    pub fn new(channel: Channel, database: String) -> Self {
        const VERSION: Option<&str> = option_env!("CARGO_PKG_VERSION");

        let channel = tower::ServiceBuilder::new()
            // .layer(tonic::service::interceptor(user_agent_interceptor))
            .layer_fn(|service| MetadataSvc::new(service, VERSION.unwrap().to_owned()))
            .service(channel);
        let engine_client = EngineServiceClient::new(channel);
        Self {
            engine_client,
            database,
        }
    }

    pub async fn get<T>(&mut self, collection: String, key: &Key<T>) -> Result<GetResponse, Error> 
    where
        T: serde::Serialize,
        T: 'static,
    {
        let value = self.engine_client.get(
            GetRequest {
                database: self.database.clone(),
                collection: collection,
                key: Some(key.as_value())
            }.into_request()
        ).await?;
        Ok(value.into_inner())
    }

    pub async fn insert<M: Message>(&mut self, message: M) -> Result<InsertResponse, Error>
    where
        M: MessageSerde + Default,
    {
        let any = Any::try_pack(message)?;
        let rep = self
            .engine_client
            .insert(
                InsertRequest {
                    database: self.database.clone(),
                    data: Some(any),
                }
                .into_request(),
            )
            .await?;
        Ok(rep.into_inner())
    }
}
