#!/usr/bin/env bash

HERE=$(readlink -f $(dirname -- "$0"))
PROJECT_NAME=$(basename -- "$HERE")

export F_ROOT_DIR="$HERE/data/root"
export F_STREAM_DIR="$HERE/data/streams"
export F_REJECT_DIR="$HERE/data/reject"
export F_KAFKA_URL="kafka:///?bootstrap.servers=127.0.0.1:9092&transactional.id=test&enable.idempotence=true&compression.codec=zstd"
export F_SCHEMA_REGISTRY_URL="http://127.0.0.1:8085"
export F_WORKERS="2"
export F_LOG_LEVEL="Debug"
export F_METRICS_PORT="9091"

export RUST_BACKTRACE="1"

cargo build && "$HERE/target/debug/$PROJECT_NAME" run