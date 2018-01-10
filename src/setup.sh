#!/usr/bin/env bash
cd "$(dirname "$0")"
rm -rf build
rm -rf dist
rm -rf *.egg-info
pip uninstall grpcio-gcp -y
pip install -rrequirements.txt
cp -f ../template/version.py version.py
python -m grpc_tools.protoc -I. --python_out=grpc_gcp grpc_gcp.proto
python setup.py sdist bdist_wheel
pip install dist/grpcio-gcp-*.tar.gz