# Stage 1: Building the binary
FROM rust:1.75 as builder

# create a new empty shell project
RUN USER=root cargo new --bin protolith-db-build
WORKDIR /usr/src/protolith-db-build

# Install necessary packages
RUN apt-get update && \
    apt-get remove -y libpq5 && \
    apt-get install -y protobuf-compiler libpq-dev libclang-dev clang && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Copy workspace files and build
COPY ./Cargo.toml ./Cargo.lock ./
COPY ./protolith ./protolith
COPY ./api ./api
COPY ./protolith-db ./protolith-db
COPY ./descriptor.bin ./descriptor.bin

# this build step will cache dependencies
RUN cargo build --package protolith-db --release
RUN rm -r protolith-db/src/*.rs protolith api

# our final base
FROM ubuntu:latest
WORKDIR /usr/src/protolith-db
RUN apt-get update

# Copy the binary from the builder stage
COPY --from=builder /usr/src/protolith-db-build/target/release/protolith-db /usr/local/bin/protolith-db

# Expose the port the gRPC server listens on
EXPOSE 5678

# # Create a directory for the RocksDB data and set it as a volume
RUN mkdir /data

# Set environment variables if needed
ENV PROTOLITH_DB_PATH=/data
# ENV PROTOLITH_ADDR=localhost:5678
# Run the binary
ENTRYPOINT ["protolith-db"]
