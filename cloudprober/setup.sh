#!/usr/bin/env bash
cd "$(dirname "$0")"

# install grpc-gcp
../src/setup.sh

# install grpc-gcp-prober
cd "$(dirname "$0")"
pip install --upgrade pip
pip uninstall grpc-gcp-prober -y
pip install -rrequirements.txt
./codegen.sh
pip install .
