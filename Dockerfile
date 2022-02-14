ARG RUST_VERSION="1.58"
FROM rust:${RUST_VERSION}-buster AS builder

WORKDIR /app
COPY ./Cargo.toml ./Cargo.lock /app/
COPY ./src/ /app/src/

RUN \
  cargo build --release


FROM gcr.io/distroless/cc

ARG PROJECT_NAME="file-kafka-loader-rs"

COPY --from=builder /app/target/release/${PROJECT_NAME} /

ENTRYPOINT [ "/${PROJECT_NAME}" ]
