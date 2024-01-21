use protolith_app::{
    Config,
    trace,
    BUILD_INFO,
    EX_USAGE, signals,
    db
};
use tracing::{debug, info, warn};

#[tokio::main]
async fn main() {
    
    let trace = match trace::Settings::from_env().init() {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Invalid logging configuration: {}", e);
            std::process::exit(EX_USAGE);
        }
    };

    info!(
        "{profile} {version} ({sha}) by {vendor} on {date}",
        date = BUILD_INFO.date,
        sha = BUILD_INFO.git_sha,
        version = BUILD_INFO.version,
        profile = BUILD_INFO.profile,
        vendor = BUILD_INFO.vendor,
    );

    // Load configuration from the environment without binding ports.
    let config = match Config::try_from_env() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Invalid configuration: {}", e);
            std::process::exit(EX_USAGE);
        }
    };

    let shutdown_grace_period = config.shutdown_grace_period;
    // let databases = config.databases;
    let mut app = match config
        .build(trace)
    {
        Ok(app) => app,
        Err(e) => {
            eprintln!("Initialization failure: {}", e);
            std::process::exit(1);
        }
    };
    
    let drain = app.spawn();
    
    tokio::select! {
        _ = signals::shutdown() => {
            tracing::info!("Received shutdown signal");
        }
        // _ = shutdown_rx.recv() => {
        //     tracing::info!("Received shutdown via admin interface");
        // }
    };
    
    match tokio::time::timeout(shutdown_grace_period, drain.drain()).await {
        Ok(()) => debug!("Shutdown completed gracefully"),
        Err(_) => warn!(
            "Graceful shutdown did not complete in {shutdown_grace_period:?}, terminating now"
        ),
    }
}
