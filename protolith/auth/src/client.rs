pub use protolith_api::protolith::services::v1::auth_service_client::AuthServiceClient;
use protolith_api::{
    protolith::services::v1::{LoginRequest, LoginResponse},
    service::MetadataSvc,
};
use protolith_error::{Error, Result};
use tonic::{transport::Channel, IntoRequest};

#[derive(Debug, Clone)]
pub struct Client {
    auth_client: AuthServiceClient<MetadataSvc>,
}

impl Client {
    pub fn new(channel: Channel) -> Self {
        const VERSION: Option<&str> = option_env!("CARGO_PKG_VERSION");

        let channel = tower::ServiceBuilder::new()
            .layer_fn(|service| MetadataSvc::new(service, VERSION.unwrap().to_owned()))
            .service(channel);
        let auth_client = AuthServiceClient::new(channel);
        Self { auth_client }
    }

    pub async fn login(&mut self, username: &str, password: &str) -> Result<LoginResponse, Error> {
        let request = LoginRequest {
            password: username.to_owned(),
            username: password.to_owned(),
        }
        .into_request();
        // request.metadata_mut().insert("protolith-session", self.session.parse().unwrap()).unwrap();
        let response = self.auth_client.login(request).await?;
        Ok(response.into_inner())
    }
}
