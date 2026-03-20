#!/bin/sh
export API_GRPC_ADDR=127.0.0.1:50051
export API_HTTP_ADDR=127.0.0.1:3000
export API_AUTO_MIGRATE=true
export API_DATABASE_URL=postgresql://postgres:postgrespassword@127.0.0.1:5432/postgres
export OTLP_ENDPOINT=http://alloy:4317
export RUST_LOG=info

./target/release/sp1-cluster-api
