#!/usr/bin/env bash

# Generate gRPC source code using protocols downloaded from googleapis/googleapis

git clone https://github.com/googleapis/googleapis.git

rm -rf google
for p in $(find googleapis/google/ -type f -name '*.proto'); do
    python \
    -m grpc_tools.protoc \
    -I googleapis/ \
    --python_out=. \
    --grpc_python_out=. \
    "${p}"
done

for d in $(find google -type d); do
  touch "${d}/__init__.py"
done

cp -f template/__init__.py google

rm -rf googleapis
