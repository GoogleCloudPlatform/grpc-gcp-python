#!/usr/bin/env bash
cd "$(dirname "$0")"
python -m grpc_gcp_test.benchmark.spanner_benchmark "$@"
