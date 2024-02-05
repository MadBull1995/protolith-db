use protolith_auth::Auth;
use protolith_core::api::service::HEADER_USER_AGENT;
use protolith_engine::ProtolithDbEngine;
use tower::{Layer, Service};
use tracing::{error, trace};
use std::{task::{Context, Poll}, fmt::Debug};
use tonic::{body::BoxBody, Status};
use hyper::header::HeaderValue;
use std::sync::Arc;
use futures_util::future::BoxFuture;
use hyper::Body;

use crate::BUILD_INFO;


#[derive(Debug, Clone, Default)]
pub struct MetadataLayer;

impl<S> Layer<S> for MetadataLayer {
    type Service = Metadata<S>;

    fn layer(&self, service: S) -> Self::Service {
        Metadata { inner: service }
    }
}

#[derive(Debug, Clone)]
pub struct Metadata<S> {
    inner: S,
}

pub const HEADER_PROTOLITH_KEY: &str = "protolith-version";

impl<S> Service<hyper::Request<Body>> for Metadata<S>
where
    S: Service<hyper::Request<Body>, Response = hyper::Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<Body>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let mut response = inner.call(req).await?;
            response.headers_mut().append(
                HEADER_PROTOLITH_KEY, 
                HeaderValue::from_static(BUILD_INFO.version)
            ); 
           Ok(response)
        })
    }
}

#[derive(Debug, Clone)]
pub struct TracingLayer;

impl<S> Layer<S> for TracingLayer {
    type Service = TracingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TracingService { inner }
    }
}

#[derive(Debug, Clone)]
pub struct TracingService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<hyper::Request<ReqBody>> for TracingService<S>
where
    S: Service<hyper::Request<ReqBody>, Response = hyper::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
    <S as Service<hyper::Request<ReqBody>>>::Error: std::fmt::Debug,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<ReqBody>) -> Self::Future {
        let uri = req.uri().clone();
        let clone = self.inner.clone();
        let headers = req.headers().clone();
        let ua = headers.get(HEADER_USER_AGENT).unwrap_or(&hyper::header::HeaderValue::from_static("unknown")).clone();
        let session = headers.get("protolith-session").unwrap_or(&hyper::header::HeaderValue::from_static("unknown")).clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            trace!(path = ?uri.path(), ua = ?ua, session = ?session, "request:");
            let response = inner.call(req).await;
            if let Err(e) = &response {
                error!(path = ?uri.path(), ua = ?ua, error = ?e,"Error processing request:");
            }
            response
        })
    }
}


#[derive(Clone)]
pub struct SessionLayer {
    auth: Arc<Auth<ProtolithDbEngine>>,
}

impl SessionLayer {
    pub fn new(auth: Arc<Auth<ProtolithDbEngine>>) -> Self {
        SessionLayer {
            auth,
        }
    }
}

impl<S> Layer<S> for SessionLayer {
    type Service = SessionSvc<S>;

    fn layer(&self, service: S) -> Self::Service {
        SessionSvc { inner: service, auth: self.auth.clone() }
    }
}

#[derive(Clone)]
pub struct SessionSvc<S> {
    inner: S,
    auth: Arc<Auth<ProtolithDbEngine>>, 
}

pub const HEADER_PROTOLITH_SESSION: &str = "protolith-session";

impl<S> Service<hyper::Request<Body>> for SessionSvc<S>
where
    S: Service<hyper::Request<Body>, Response = hyper::Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    <S as Service<hyper::Request<hyper::Body>>>::Error: From<Status>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<Body>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        
        let session_token = extract_session_token_from_metadata(&req);
        let auth = self.auth.clone();
        Box::pin(async move {
            if req.uri().path() == "/protolith.services.v1.AuthService/Login" {
                let fut = inner.call(req)
                    .await?;
                Ok(fut)
            } else {
                let session = auth.get_session(session_token.clone());
                let sessions = auth.sessions().await;
                println!("interceptor: {:?}", sessions);
                if let Some(_session) = session {
                    let fut = inner.call(req)
                        .await?;
                    Ok(fut)
                } else {
                    Err(Status::unauthenticated(format!("session {:?} is not exists", session_token)).into())
                }
            }
        })
    }
}


fn extract_session_token_from_metadata(req: &hyper::Request<Body>) -> String {
    if let Some(token) = req.headers().get(HEADER_PROTOLITH_SESSION) {
        token.to_str().unwrap().to_owned()
    } else {
        "unknown".to_string()
    }
}