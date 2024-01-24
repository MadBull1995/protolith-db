
use std::path::PathBuf;
use std::{io, env};
use std::process::{Command, Stdio};

use users::{get_user_by_name, os::unix::UserExt};


use protolith_app::{
    Config,
    trace,
    BUILD_INFO,
    EX_USAGE, signals,
    db
};
use protolith_macros::*;
use serde::{Serialize, Deserialize};
use tracing::{debug, info, warn, error};
use protolith_core::collection::Wrapper;
use protolith_core::api::prost_wkt_types::Struct;
#[derive(Serialize, Deserialize, Collection, Debug)]
struct MyStruct {
    test: String
}

#[tokio::main]
async fn main() {
    let app_user = "protolithdb";

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
        Ok(()) => info!("Shutdown completed gracefully"),
        Err(_) => warn!(
            "Graceful shutdown did not complete in {shutdown_grace_period:?}, terminating now"
        ),
    }

    println!("Bye Bye :)");
}


fn run_setup_script(app_user: &str, app_dir: &str) -> io::Result<()> {
    debug!(dir = ?env::current_dir().unwrap(), "Current ");

    let current_dir = env::current_dir().expect("Failed to get current directory");
    
    // Construct the path to the script
    let script_name = if cfg!(target_os = "windows") {
        "setup.bat"
    } else {
        "setup.sh"
    };
    let script_path = PathBuf::from(current_dir)
        .join("protolith-db") // Adjust this path as needed
        .join("src")
        .join(script_name);
    // Check if the script exists
    if !script_path.exists() {
        error!("Script not found: {:?}", script_path);
        return Err(io::Error::new(io::ErrorKind::NotFound, "Script not found"));
    }
    debug!(script_path = ?script_path, app_user = ?app_user, app_dir = ?app_dir, "Running setup script");
    let mut child = Command::new(script_path)
        .arg(app_user) // Pass app_user as the first argument
        .arg(app_dir) 
        .output();

    match child {
        Ok(output) => {
            if !output.status.success() {
                error!("Setup script failed to run");
                error!("Stdout: {}", String::from_utf8_lossy(&output.stdout));
                error!("Stderr: {}", String::from_utf8_lossy(&output.stderr));
            } else {
                info!("Setup script ran successfully");
            }
        },
        Err(e) => {
            error!("Failed to execute setup script: {}", e);
            return Err(e);
        }
    }
    

    Ok(())
}

fn user_exists(username: &str) -> bool {
    get_user_by_name(username).is_some()
}