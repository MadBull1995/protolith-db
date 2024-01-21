use std::sync::Arc;

use protolith_api::{protolith::{
    services::v1::{
        CreateDatabaseRequest,
        CreateDatabaseResponse,
        ListDatabasesResponse,
        admin_service_server::AdminService
    }
}, pbjson_types::Empty};
use tracing::{info, Instrument, span, info_span, Span};

use protolith_engine::Engine;
use protolith_error::{is_caused_by, cause_ref};
use tonic::{Status, Response, Request, IntoRequest, metadata::MetadataMap};

enum AdminRequest {
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
            AdminRequest::CreateDatabase(req) => info_span!("handling_create_database", req.name)
        }
    }
}

impl<E: Engine> ProtolithAdminService<E> {
    pub fn new(engine: Arc<E>) -> Self {
        Self {
            engine,
        }
    }
}

#[tonic::async_trait]
impl<E: Engine> AdminService for ProtolithAdminService<E> {
    async fn create_database(&self, request: Request<CreateDatabaseRequest>) -> Result<Response<CreateDatabaseResponse>, Status> {
        let req = request.into_inner();
        let span = self.build_client_request_span(AdminRequest::CreateDatabase(req.clone()));
        let db_response = self.engine.create_database(req.name, req.file_descriptor_set).instrument(span).await;

        match db_response {
            Err(e) => {
                match e {
                    protolith_engine::EngineError::Internal(err) => Err(Status::internal(format!("some internal error occured: {:?}", err))),
                    protolith_engine::EngineError::OpError(err) => {
                        match err {
                            protolith_engine::OpError::DatabaseAlreadyExists(e)=>Err(Status::already_exists(e)),
                            _ => unreachable!()
                        }
                    },
                }
            },
            Ok(rep) => Ok(Response::new(rep)),
        }
    }

    async fn create_or_replace_database(&self, request: Request<CreateDatabaseRequest>) -> Result<Response<CreateDatabaseResponse>, Status> {
        let req = request.into_inner();

        Ok(
            Response::new(CreateDatabaseResponse::default())
        )
    }

    async fn list_databases(&self, request: Request<Empty>) -> Result<Response<ListDatabasesResponse>, Status> {
        let span = self.build_client_request_span(AdminRequest::ListDatabase);
        let resp = self.engine.list_databases().instrument(span).await;
        match resp {
            Err(e) => Err(Status::internal(e.to_string())),
            Ok(databases) => Ok(Response::new(databases)),
        }
        

    }
}