FROM rust:1.80 AS builder

RUN mkdir /build
WORKDIR /build
COPY ./ .

RUN cargo build

FROM rust:1.80 AS runtime
WORKDIR /build

COPY --from=builder /build/target/debug/examples/spawn_partition_receiver ./
RUN chmod +x /build/spawn_partition_receiver
ENTRYPOINT ["/build/spawn_partition_receiver"]