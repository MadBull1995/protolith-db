#![deny(rust_2018_idioms, clippy::disallowed_methods, clippy::disallowed_types)]
#![forbid(unsafe_code)]

use protolith_app::{
    Config,
    trace,
    BUILD_INFO,
    EX_USAGE, signals,
};
use protolith_macros::*;
use serde::{Serialize, Deserialize};
use tracing::{info, warn};
use protolith_core::collection::Wrapper;
use protolith_core::api::prost_wkt_types::Struct;
#[derive(Serialize, Deserialize, Collection, Debug)]
struct MyStruct {
    test: String
}

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

    // if !user_exists(app_user) {
    //     info!("User '{}' not found. Running setup script...", app_user);
    //     run_setup_script(app_user, config.db.db_path.to_str().unwrap()).expect("setup");
    // } else {
    //     info!("User '{}' found. Proceeding with the application...", app_user);
    //     // Proceed with the rest of your application
    // }

    let shutdown_grace_period = config.shutdown_grace_period;
    // let databases = config.databases;
    let app = match config
        .build(trace).await
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
        Ok(()) => info!("Shutdown completed gracefully"),
        Err(_) => warn!(
            "Graceful shutdown did not complete in {shutdown_grace_period:?}, terminating now"
        ),
    }

    println!("Bye Bye :)");
}


// 