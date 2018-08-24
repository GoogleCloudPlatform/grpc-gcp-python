#!/usr/bin/env bash
cd "$(dirname "$0")"
rm -rf google
pip uninstall grpcio-gcp-test -y
pip install -rrequirements.txt
cp -f ../template/version.py version.py
./codegen.sh
pip install .
