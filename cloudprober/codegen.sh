#!/usr/bin/env bash
cd "$(dirname "$0")"
rm -rf google
for p in $(find ../third_party/googleapis/google -type f -name '*.proto'); do
    python \
    -m grpc_tools.protoc \
    -I ../third_party/googleapis/ \
    --python_out=. \
    --grpc_python_out=. \
    "${p}"
done

for d in $(find google -type d); do
  touch "${d}/__init__.py"
done

cp -f template/__init__.py google