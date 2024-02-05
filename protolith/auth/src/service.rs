use crate::ProtolithAuth;
use protolith_core::api::protolith::services::v1::{
    auth_service_server::AuthService, LoginRequest, LoginResponse,
};
use protolith_engine::Engine;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl<E: Engine> AuthService for ProtolithAuth<E> {
    async fn login(
        &self,
        request: Request<LoginRequest>,
    ) -> Result<Response<LoginResponse>, Status> {
        let req = request.into_inner();
        let session = self
            .engine
            .login_user(req.username.clone(), req.password)
            .await
            .map_err(|e| Status::not_found(e.to_string()))?;
        let mut sessions = self.sessions.lock().unwrap();
        sessions.insert(
            session.clone(),
            crate::Session {
                username: req.username,
            },
        );
        println!("{:?}", sessions);
        Ok(Response::new(LoginResponse { session }))
    }
}
