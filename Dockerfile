FROM mcr.microsoft.com/azurelinux/base/rust:1.75.0-8-azl3.0.20240727 AS builder

RUN mkdir /build
WORKDIR /build
COPY ./ .

RUN tdnf install -y openssl-devel pkgconfig
RUN cargo build

FROM ubuntu:24.04 AS runtime
WORKDIR /build

RUN apt-get update && apt-get install -y libssl3 ca-certificates
COPY --from=builder /build/target/debug/examples/spawn_partition_receiver ./
RUN chmod +x /build/spawn_partition_receiver
ENTRYPOINT ["/build/spawn_partition_receiver"]