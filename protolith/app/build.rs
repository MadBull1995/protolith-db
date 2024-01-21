use std::process::Command;

fn set_env(name: &str, cmd: &mut Command) {
    let value = match cmd.output() {
        Ok(output) => String::from_utf8(output.stdout).unwrap(),
        Err(err) => {
            println!("cargo:warning={}", err);
            "".to_string()
        }
    };
    println!("cargo:rustc-env={}={}", name, value);
}

fn version() -> String {
    if let Ok(v) = std::env::var("PROTOLITH_VERSION") {
        if !v.is_empty() {
            if semver::Version::parse(&v).is_err() {
                panic!("PROTOLITH_VERSION must be semver");
            }
            return v;
        }
    }

    "0.0.0-dev".to_string()
}

fn vendor() -> String {
    std::env::var("PROTOLITH_VENDOR").unwrap_or_default()
}

fn main() {
    set_env(
        "GIT_SHA",
        Command::new("git").args(["rev-parse", "--short", "HEAD"]),
    );

    // Capture the ISO 8601 formatted UTC time.
    set_env(
        "PROTOLITH_BUILD_DATE",
        Command::new("date").args(["-u", "+%Y-%m-%dT%H:%M:%SZ"]),
    );

    println!("cargo:rustc-env=PROTOLITH_VERSION={}", version());
    println!("cargo:rustc-env=PROTOLITH_VENDOR={}", vendor());

    let profile = std::env::var("PROFILE").expect("PROFILE must be set");
    println!("cargo:rustc-env=PROFILE={profile}");
}