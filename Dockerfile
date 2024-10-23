FROM rust:1.80 AS builder

RUN mkdir /build
WORKDIR /build
COPY ./ .

RUN cargo build

FROM alpine
WORKDIR /build

COPY --from=builder /build/target/debug/examples/spawn_multiple_consumer ./
ENTRYPOINT ["/build/spawn_multiple_consumer"]