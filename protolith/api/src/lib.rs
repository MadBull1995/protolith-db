mod api {
    include!(concat!(env!("OUT_DIR"), "/mod.rs"));
}
pub use pbjson_types;
pub use prost;
pub use prost_wkt_types;
pub use api::protolith;
pub use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};

pub mod service {
    pub const HEADER_USER_AGENT: &str = "protolith-user-agent";
    // use hyper::http::{Request, Response};
    use hyper::header::HeaderValue;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tonic::body::BoxBody;
    use hyper::Body;
    use tonic::transport::Channel;
    use tower::Service;


    #[derive(Debug, Clone)]
    pub struct MetadataSvc {
        inner: Channel,
        version: String,
    }

    impl MetadataSvc {
        pub fn new(inner: Channel, version: String) -> Self {
            MetadataSvc { inner, version }
        }
    }

    impl Service<hyper::Request<BoxBody>> for MetadataSvc {
        type Response = hyper::Response<Body>;
        type Error = Box<dyn std::error::Error + Send + Sync>;
        #[allow(clippy::type_complexity)]
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx).map_err(Into::into)
        }

        fn call(&mut self, mut req: hyper::Request<BoxBody>) -> Self::Future {
            // This is necessary because tonic internally uses `tower::buffer::Buffer`.
            // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
            // for details on why this is necessary
            let clone = self.inner.clone();
            let mut inner = std::mem::replace(&mut self.inner, clone);
            req.headers_mut().insert(HEADER_USER_AGENT, HeaderValue::from_str(&format!("protolith@rust/{}", self.version)).unwrap());
            Box::pin(async move {
                // Do extra async work here...
                let response = inner.call(req).await?;

                Ok(response)
            })
        }
    }
}


