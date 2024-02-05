use std::{fmt::Debug, future::Future, marker::PhantomData};

use protolith_core::{
    api::{
        prost::{Message, Name},
        prost_wkt_types::{Any, MessageSerde},
        protolith::services::v1::{
            engine_service_client::EngineServiceClient, GetRequest, InsertRequest,
            InsertResponse, ListRequest,
        },
        service::MetadataSvc,
    },
    error::Error,
    Key,
};
use tonic::{transport::Channel, IntoRequest};

#[derive(Debug, Clone)]
pub struct Client {
    engine_client: EngineServiceClient<MetadataSvc>,
    database: String,
    session: String,
}

impl Client {
    pub fn new(channel: Channel, database: String, session: String) -> Self {
        const VERSION: Option<&str> = option_env!("CARGO_PKG_VERSION");

        let channel = tower::ServiceBuilder::new()
            // .layer(tonic::service::interceptor(user_agent_interceptor))
            .layer_fn(|service| MetadataSvc::new(service, VERSION.unwrap().to_owned()))
            .service(channel);
        let engine_client = EngineServiceClient::new(channel);
        Self {
            engine_client,
            database,
            session,
        }
    }

    pub async fn list<C>(&mut self) -> Result<Vec<Response<C::Message>>, Error>
    where
        C: Collection,
        C::Message: Name,
    {
        let collection = C::Message::full_name();
        let mut request = ListRequest {
            database: self.database.clone(),
            collection: collection.clone(),
        }
        .into_request();
        request
            .metadata_mut()
            .insert("protolith-session", self.session.parse().unwrap());
        let list = self.engine_client.list(request).await?;
        let l = list.into_inner();
        let mut data_list = Vec::with_capacity(l.data.len());
        for d in l.data {
            let rep = Response {
                collection: collection.clone(),
                data: d,
                _marker: PhantomData,
            };
            data_list.push(rep);
        }

        Ok(data_list)
    }

    pub async fn get<C>(&mut self, key: &Key<C::Key>) -> Result<Response<C::Message>, Error>
    where
        C: Collection,
        C::Key: serde::Serialize + 'static,
        C::Message: Message + Name + Default,
    {
        let collection = C::Message::full_name();
        let mut request = GetRequest {
            database: self.database.clone(),
            collection: collection,
            key: Some(key.as_value()),
        }
        .into_request();

        request
            .metadata_mut()
            .insert("protolith-session", self.session.parse().unwrap());
        let value = self.engine_client.get(request).await?;
        let rep = value.into_inner();
        let data = if let Some(data) = rep.data {
            data
        } else {
            Any {
                ..Default::default()
            }
        };
        let rep = Response {
            collection: rep.collection,
            data,
            _marker: PhantomData,
        };
        Ok(rep)
    }

    pub async fn insert<C>(&mut self, message: C::Message) -> Result<InsertResponse, Error>
    where
        C: Collection,
        C::Message: MessageSerde + Default,
    {
        let any = Any::try_pack(message)?;
        let mut request = InsertRequest {
            database: self.database.clone(),
            data: Some(any),
        }
        .into_request();
        request
            .metadata_mut()
            .insert("protolith-session", self.session.parse().unwrap());
        let rep = self.engine_client.insert(request).await?;
        Ok(rep.into_inner())
    }
}

#[derive(Debug)]
pub struct Response<T> {
    collection: String,
    data: Any,
    _marker: PhantomData<T>,
}

impl<T> Response<T>
where
    T: Message + Default,
{
    pub fn into_inner(self) -> Result<T, Error> {
        let bytes = self.data.value; // Assuming `Any` follows prost_types::Any structure.
        T::decode(bytes.as_slice()) // Decode the bytes into the desired type.
            .map_err(|e| e.into()) // Convert decoding error into a Box<dyn Error>.
    }
}

pub trait Collection {
    type Key: Debug + 'static;
    type Message: Debug + 'static;

    fn list(&mut self) -> impl Future<Output = Result<Vec<Response<Self::Message>>, Error>>;
    fn insert(&mut self, msg: Self::Message)
        -> impl Future<Output = Result<InsertResponse, Error>>;
    fn get(
        &mut self,
        key: Key<Self::Key>,
    ) -> impl Future<Output = Result<Response<Self::Message>, Error>>;
}
