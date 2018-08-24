#!/usr/bin/env bash
cd "$(dirname "$0")"
python -m unittest discover grpc_gcp_test/unit/ -p "*_test.py"
