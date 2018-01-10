#!/usr/bin/env bash
cd "$(dirname "$0")"
python -m grpc_gcp_test.spanner_test
