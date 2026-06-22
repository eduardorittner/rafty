# Stage 1: Build the Rust workspace
FROM rust:1.85-slim as builder

# Install protoc since prost needs it to compile message.proto
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/rafty

# Copy the whole workspace
COPY . .

# Build the release binary for the docker-scenario crate
RUN cargo build --release -p docker-scenario

# Stage 2: Minimal runner image
FROM debian:bookworm-slim

WORKDIR /usr/local/bin

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/src/rafty/target/release/docker-scenario .

EXPOSE 9001
EXPOSE 8080

ENTRYPOINT ["./docker-scenario"]
