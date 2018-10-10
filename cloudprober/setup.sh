#!/usr/bin/env bash

cd "$(dirname "$0")"
pip install --upgrade pip
./grpc-gcp-python/src/setup.sh
pip uninstall grpc-gcp-prober -y
pip install -rrequirements.txt
./codegen.sh
pip install .

