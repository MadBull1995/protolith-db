use protolith_core::api::{
    pbjson_types::value::Kind,
    protolith::{
        services::v1::{
            engine_service_server::{self, EngineService},
            GetRequest, GetResponse, InsertRequest, InsertResponse, ListRequest, ListResponse,
        },
        types::v1::{ApiOp, Op, OpStatus},
    },
};
use tracing::debug;

use crate::Engine;
use tonic::{Request, Response, Status};
pub struct ProtolithEngineService<E: Engine> {
    engine: E,
}

type EngineServiceType<E> = engine_service_server::EngineServiceServer<ProtolithEngineService<E>>;

impl<E: Engine> ProtolithEngineService<E> {
    pub fn new(e: E) -> Self {
        Self { engine: e }
    }

    pub fn service(self) -> EngineServiceType<E> {
        engine_service_server::EngineServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl<E: Engine> EngineService for ProtolithEngineService<E> {
    async fn list(&self, request: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        let req = request.into_inner();
        let database = req.database;
        let collection = req.collection;

        let data = self
            .engine
            .list(database, collection.clone())
            .await
            .map_err(|err| match err {
                crate::EngineError::Internal(e) => Status::internal(e.to_string()),
                crate::EngineError::OpError(op) => match op {
                    crate::OpError::DatabaseNotFound(e) => Status::not_found(e),
                    crate::OpError::KeyAlreadyExists(e) => Status::already_exists(e.to_string()),
                    _ => unreachable!(),
                },
            })?;
        let length = data.len();
        Ok(Response::new(ListResponse {
            collection: collection.clone(),
            data,
            op: Some(ApiOp {
                description: format!(
                    "list {} successfully. {} items fetched.",
                    collection, length
                ),
                status: OpStatus::Success.into(),
                r#type: Op::List.into(),
            }),
        }))
    }

    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        let req = request.into_inner();
        if let Some(any) = req.data {
            let collection =
                self.engine
                    .insert(req.database, any)
                    .await
                    .map_err(|err| match err {
                        crate::EngineError::Internal(e) => Status::internal(e.to_string()),
                        crate::EngineError::OpError(op) => match op {
                            crate::OpError::DatabaseNotFound(e) => Status::not_found(e),
                            crate::OpError::KeyAlreadyExists(e) => {
                                Status::already_exists(e.to_string())
                            }
                            _ => unreachable!(),
                        },
                    })?;
            Ok(Response::new(InsertResponse {
                collection,
                ..Default::default()
            }))
        } else {
            Err(Status::invalid_argument(
                "Must pass a valid Any type message",
            ))
        }
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        if let Some(key) = &req.key {
            let key = match &key.kind {
                Some(Kind::NumberValue(n)) => format!("{}:{}", req.collection, n),
                Some(Kind::StringValue(s)) => {
                    req.collection.to_string() + ":" + &s.trim_matches('"')
                }
                _ => todo!(),
            };
            let value = self
                .engine
                .get(
                    req.database,
                    req.collection.clone(),
                    &key.clone().into_bytes(),
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            debug!(collection = ?req.collection.clone(), key = ?key, bytes = ?value.value.len(), "get");
            return Ok(Response::new(GetResponse {
                collection: req.collection,
                data: Some(value),
                ..Default::default()
            }));
        } else {
            return Err(Status::invalid_argument(
                "key must be parsable type to binary",
            ));
        };
    }
}
