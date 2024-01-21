use std::{time::Duration, collections::HashMap, sync::Arc};
mod client;
mod service;

use protolith_engine::{Engine, ProtolithDbEngine};
use protolith_error::Error;
pub use client::{Client, AdminServiceClient};
use protolith_api::protolith::services::v1::admin_service_server;
pub use service::ProtolithAdminService;
type AdminServiceType<E: Engine> = admin_service_server::AdminServiceServer<ProtolithAdminService<E>>;

#[derive(Debug, Clone)]
pub struct Config {

}

impl Config {
    pub fn build<E: Engine>(self, engine: Arc<E>, drain: drain::Watch ) -> Result<Admin<E>, Error> {
        Ok(Admin {
            admin: ProtolithAdminService::new(engine),
            drain
        })
    }
}

#[derive(Debug, Clone)]
pub struct Admin<E: Engine> {
    admin: ProtolithAdminService<E>,
    pub drain: drain::Watch,
}

impl<E: Engine> Admin<E> {
    pub fn service(self) -> AdminServiceType<E> {
        admin_service_server::AdminServiceServer::new(self.admin)
    }
}