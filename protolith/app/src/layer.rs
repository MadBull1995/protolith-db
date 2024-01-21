use protolith_core::api::service::HEADER_USER_AGENT;
use tower::{Layer, Service};
use tracing::{error, trace};
use std::{task::{Context, Poll}, fmt::Debug};
use tonic::{body::BoxBody, transport::Server, Request, Response, Status};
use hyper::{http::{HeaderMap, header::HeaderName}, header::{HeaderValue, InvalidHeaderValue}};
use std::sync::Arc;
use futures_util::future::BoxFuture;
use std::pin::Pin;
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
                HeaderValue::from_static(&BUILD_INFO.version)
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
        let method = req.method().clone();
        let uri = req.uri().clone();
        let clone = self.inner.clone();
        let headers = req.headers().clone();
        let ua = headers.get(HEADER_USER_AGENT).unwrap_or(&hyper::header::HeaderValue::from_static("unknown")).clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            trace!(path = ?uri.path(), ua = ?ua, "request:");
            let response = inner.call(req).await;
            match &response {
                Err(e) => error!(path = ?uri.path(), ua = ?ua, error = ?e,"Error processing request:"),
                _ => {},
            }
            response
        })
    }
}