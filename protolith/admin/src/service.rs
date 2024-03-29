use std::sync::Arc;

use protolith_api::{
    pbjson_types::Empty,
    protolith::services::v1::{
        admin_service_server::AdminService, CreateCollectionRequest, CreateCollectionResponse,
        CreateDatabaseRequest, CreateDatabaseResponse, ListDatabasesResponse,
    },
};
use tracing::{info_span, Instrument, Span};

use protolith_engine::Engine;
use tonic::{Request, Response, Status};

pub enum AdminRequest {
    ListDatabase,
    CreateDatabase(CreateDatabaseRequest),
}

#[derive(Debug, Clone)]
pub struct ProtolithAdminService<E: Engine> {
    engine: Arc<E>,
}

impl<E: Engine> ProtolithAdminService<E> {
    pub fn build_client_request_span(&self, request: AdminRequest) -> Span {
        match request {
            AdminRequest::ListDatabase => info_span!("handling_list_database"),
            AdminRequest::CreateDatabase(req) => info_span!("handling_create_database", req.name),
        }
    }
}

impl<E: Engine> ProtolithAdminService<E> {
    pub fn new(engine: Arc<E>) -> Self {
        Self { engine }
    }
}

#[tonic::async_trait]
impl<E: Engine> AdminService for ProtolithAdminService<E> {
    async fn create_database(
        &self,
        request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        let req = request.into_inner();
        let span = self.build_client_request_span(AdminRequest::CreateDatabase(req.clone()));
        let db_response = self
            .engine
            .create_database(req.name, req.file_descriptor_set)
            .instrument(span)
            .await;

        match db_response {
            Err(e) => match e {
                protolith_engine::EngineError::Internal(err) => Err(Status::internal(format!(
                    "some internal error occured: {:?}",
                    err
                ))),
                protolith_engine::EngineError::OpError(err) => match err {
                    protolith_engine::OpError::DatabaseAlreadyExists(e) => {
                        Err(Status::already_exists(e))
                    }
                    _ => unreachable!(),
                },
            },
            Ok(rep) => Ok(Response::new(rep)),
        }
    }

    async fn create_or_replace_database(
        &self,
        request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        let _req = request.into_inner();

        Ok(Response::new(CreateDatabaseResponse::default()))
    }

    async fn list_databases(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<ListDatabasesResponse>, Status> {
        let span = self.build_client_request_span(AdminRequest::ListDatabase);
        let resp = self.engine.list_databases().instrument(span).await;
        match resp {
            Err(e) => Err(Status::internal(e.to_string())),
            Ok(databases) => Ok(Response::new(databases)),
        }
    }

    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        let req = request.into_inner();
        let rep = self
            .engine
            .create_collection(req.database, req.collection, req.key, 1)
            .await
            .map_err(|err| match err {
                protolith_engine::EngineError::Internal(err) => {
                    Status::internal(format!("{}", err))
                }
                protolith_engine::EngineError::OpError(op_err) => match op_err {
                    protolith_engine::OpError::DatabaseNotFound(err) => Status::not_found(err),
                    protolith_engine::OpError::CollectionAlreadyExists(db, col) => {
                        Status::already_exists(format!("{} already exists on {}", col, db))
                    }
                    _ => unreachable!(),
                },
            })?;
        Ok(Response::new(rep))
    }
}
