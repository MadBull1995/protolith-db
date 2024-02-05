mod client;
mod service;
pub use client::Client;
use protolith_api::protolith::services::v1::auth_service_server;
use protolith_core::{error::Error, meta_store};
use protolith_engine::Engine;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use thiserror::Error as thisError;
use tracing::info;

#[derive(Debug, Clone, PartialEq, Eq, thisError)]
pub enum AuthError {
    #[error("user {0} not exists")]
    UserNotExists(String),
}

#[derive(Debug, Clone)]
pub struct Config {
    pub meta_store: meta_store::Config,
    pub user: String,
    pub password: String,
}

impl Config {
    pub async fn build<E: Engine>(self, engine: Arc<E>) -> Result<Auth<E>, Error> {
        engine.create_user(self.user, self.password).await?;
        Ok(Auth {
            auth: ProtolithAuth::new(engine, self.meta_store.clone()),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Auth<E: Engine> {
    auth: ProtolithAuth<E>,
}

type AuthServiceType<E> = auth_service_server::AuthServiceServer<ProtolithAuth<E>>;

impl<E: Engine> Auth<E>
where
    E: Clone,
{
    pub fn get_session(&self, session: String) -> Option<Session> {
        self.auth.get_session(session)
    }

    pub async fn sessions(&self) -> HashMap<String, Session> {
        let sessions = self.auth.sessions();
        sessions
    }

    pub async fn set_sessions(&mut self, sessions: HashMap<String, Session>) {
        info!(sessions = ?sessions, "loading sessions");
        self.auth.sessions = Arc::new(Mutex::new(sessions));
    }

    pub fn service(&self, max_message_size: usize) -> AuthServiceType<E> {
        auth_service_server::AuthServiceServer::new(self.auth.clone())
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size)
    }
}

#[derive(Debug, Clone)]
pub struct ProtolithAuth<E: Engine> {
    engine: Arc<E>,
    metastore: meta_store::Config,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
}

impl<E: Engine> ProtolithAuth<E> {
    pub fn new(engine: Arc<E>, metastore: meta_store::Config) -> Self {
        Self {
            engine,
            metastore,
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn create_session(&mut self, session_id: String, username: String) {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.insert(session_id.clone(), Session { username: username });
    }

    pub fn get_session(&self, session: String) -> Option<Session> {
        let sessions = self.sessions.lock().unwrap();
        sessions.clone().get(&session).cloned()
    }

    pub fn sessions(&self) -> HashMap<String, Session> {
        let sessions = self.sessions.lock().unwrap();
        sessions.clone()
    }

    pub async fn login_user(
        &mut self,
        username: String,
        password: String,
    ) -> Result<String, AuthError> {
        let session = self
            .engine
            .login_user(username.clone(), password)
            .await
            .map_err(|_e| AuthError::UserNotExists(username.clone()))?;
        self.create_session(session.clone(), username);
        Ok(session)
    }

    pub fn create_user(&self, username: String, password: String) -> Result<Session, AuthError> {
        self.engine.create_user(username.clone(), password);
        Ok(Session { username })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Session {
    username: String,
}
