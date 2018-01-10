#!/usr/bin/env bash
cd "$(dirname "$0")"
python -m grpc_gcp_benchmark.spanner_benchmark "$@"
