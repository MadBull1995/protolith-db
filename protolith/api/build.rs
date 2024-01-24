use glob::glob;
use std::io;
use std::path::PathBuf;
use std::env;
use prost_wkt_build::*;

fn main() -> io::Result<()> {
    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    let descriptor_file = out.join("descriptor.bin");
    let test_descriptor_path = PathBuf::from("../../descriptor.bin");
    let protos: Vec<PathBuf> = glob("../../api/protolith/**/v1/*.proto")
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    // let mut config = prost_build::Config::new();
    // config.disable_comments(["."]);
    tonic_build::configure()
        .type_attribute(
            ".",
            "#[derive(serde::Serialize,serde::Deserialize)]"
        )
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(descriptor_file.clone())
        .compile_well_known_types(true)
        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
        .extern_path(".google.protobuf", "::pbjson_types")
        .include_file("mod.rs")
        // .skip_protoc_run()
        .compile(
            &protos,
            &[
                "../../api",
            ],
        )?;
    
    let descriptor_bytes =
        std::fs::read(descriptor_file)
        .unwrap();
    std::fs::write(test_descriptor_path, descriptor_bytes.clone());
    let descriptor =
        FileDescriptorSet::decode(&descriptor_bytes[..])
        .unwrap();

    prost_wkt_build::add_serde(out, descriptor);
    Ok(())
}