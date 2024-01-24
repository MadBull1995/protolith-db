use std::env;

fn main() {
    let db_path = if cfg!(target_os = "windows") {
        // System-wide Path for Windows
        r"C:\ProgramData\ProtolithDB\db".to_string()
    } else {
        // System-wide Path for macOS
        "_protolith_db".to_string()
    };

    // Set the PROTOLITH_DB_PATH environment variable
    println!("cargo:rustc-env=PROTOLITH_DB_PATH={}", db_path);
}
