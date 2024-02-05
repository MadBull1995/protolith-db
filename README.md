# ProtolithDB

> Work In Progress

A Protobuf Database.
ProtolithDB is protobuf-centric database using RocksDB as storage engine.

# Usage

## Installation
To install ProtolithDB you can use cargo:

### Cargo

```bash
cargo install protolith-db
```

Then you can run the instance:
```bash
protolithdb --port 5678 --user admin --password admin
```
> See [Config](#configurations) section for more details on optional arguments to be passed when setting up the ProtolithDB instance, and all the Enviroment variables availabe.

### Manual
You can also manual install with cloning this repo and build the project locally
```bash
git clone https://github.com/MadBull1995/protolith-db.git . \
    && cd protolith-db \
    && cargo build
```

## Schema Annotation
ProtolithDB main focus is to store Protobuf messages, it seems natural to also annotate and interact with ProtolithDB Engine within the protobuf definitions.

For this we use a common feature of Protobuf which called `Custom Options`, specificlly we are using most of the time the following options that are provided withing Protobuf implemantation:
- `MessageOptions` - To annotate collection specific data
- `FieldOptions` - To annotate field (indexes) data

### Setup

To annotate your protobuf files you will need to import the Protolith-DB repository protobuf files under [api](./api) folder.

You can run the following command in your root directory of you protobuf project (assuming your other protobuf files located on `/protos`):
```bash
git submodule add https://github.com/MadBull1995/protolith-db.git /protos
```

Then you can easily import them as long as you will remember to include the library `protolith` under `/protos/protlith-db/api`

```proto
syntax = "proto3";

package Foo;

import "protolith/annotation/v1/annotation.proto";

message Bar {
    option (protolith.annotation.v1.collection) = {
        name: "Bar"
    };

    string id = 1 [(protolith.annotation.v1.key) = {}];
}
```

This schema annotation is having the following affect when you will create a ProtolithDB with this schema:
1. It will "tell" ProtolithDB Engine that you have collection based on the Protobuf schema of your `Foo.Bar` message.
2. The `Foo.Bar` message is indexed by the field `Foo.Bar.id` which is string value and should be unique.

### Compile your schema

Lets examine the following project setup for example:
```bash
./SomeProject
├── Cargo.toml
├── build.rs
├── protos
│   ├── protolith-db # The cloned protolith repo
│   │   └── api
│   └── my_package.proto
└── src
    └── main.rs

```

If you followed the setup you should have the protolith-db repo cloned into your `protos` firectory whcih wrap all protobuf library in used with this project.

Then we want to compile your own protobuf files with linking the existing `protolith-db` definitions as follow:

__Rust__:
The most straight forward way to compile the protobuf is to use build script:
```rust,build.rs
use glob::glob;
use std::io;
use std::path::PathBuf;
use std::env;
use prost_wkt_build::*;

fn main() -> io::Result<()> {
    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    let descriptor_file = out.join("descriptor.bin");
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
        .compile(
            &protos,
            &[
                "./protos",
                "./protos/protolith-db/api",
            ],
        )?;
    
    let descriptor_bytes =
        std::fs::read(descriptor_file)
        .unwrap();
    let descriptor =
        FileDescriptorSet::decode(&descriptor_bytes[..])
        .unwrap();

    prost_wkt_build::add_serde(out, descriptor);
    Ok(())
}
```


__Python__:
```
protoc -o=descriptor.bin -I=protos -I=protos/protolith-db/api --python_out=. ./protos/my_package.proto
```
