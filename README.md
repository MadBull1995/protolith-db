# ProtolithDB

> Work In Progress

A Protobuf Database.
ProtolithDB is protobuf-centric database using RocksDB as storage engine.

# Usage

## Installation
To install ProtolithDB you can use cargo:

### Cargo

```bash
cargo install protolith_db
```

Then you can run the instance:
```bash
protolithdb --port 5678 --user admin --password admin
```
> See [Config](#configurations) section for more details on optional arguments to be passed when setting up the ProtolithDB instance, and all the Enviroment variables availabe.

### Manual
You can also manual install with cloning this repo and build the project locally
```bash
git clone https://github.com/MadBull1995/protolith-db.git . && cd protolith-db && cargo build
```

## Schema Annotation
ProtlithDB main focus is to store Protobuf messages, it seems natural to also annotate and interact with ProtolithDB Engine within the protobuf definitions.

For this we use a common feature of Protobuf which called `Custom Options`, specificlly we are using most of the time the following options that are provided withing Protobuf implemantation:
- `MessageOptions` - To annotate collection specific data
- `FieldOptions` - To annotate field (indexes) data

### Setup

To annotate your protobuf files you will need to import the Protolith-DB repository protobuf files under [api](./api) folder.

You can run the following command in your root directory of you protobuf project (assuming your other protobuf files located on `/protos`):
```bash
git submodule add https://github.com/MadBull1995/protolith-db.git /protos
```

Then you can easilyy import them as long as you will remember to include the library `protolith` under `/protos/protlith-db/api`

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


