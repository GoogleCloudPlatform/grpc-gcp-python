#!/usr/bin/env bash
cd "$(dirname "$0")"
python -m grpc_gcp_test.integration.spanner_test
