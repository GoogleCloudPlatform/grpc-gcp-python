# gRPC for GCP extensions

Copyright 2018
[The gRPC Authors](https://github.com/grpc/grpc/blob/master/AUTHORS)

## About This Repository

This repo is created to support GCP specific extensions for gRPC. To use the extension features, please refer to [grpcio-gcp](src).

This repo also contains supporting infrastructures such as end2end tests and benchmarks for accessing cloud APIs with gRPC client libraries.

## Testing

Download from github

```sh
 $ git clone https://github.com/GoogleCloudPlatform/grpc-gcp-python.git
 $ cd grpc-gcp-python
 $ git submodule update --init --recursive
```

Install the gRPC-GCP extension

```sh
 $ src/setup.sh
```

Install the gRPC-GCP extension test & benchmark suite

```sh
 $ tests/setup.sh
```

Run end to end integration tests.

```sh
 $ tests/integration.sh
```

Run end to end benchmark with gRPC-GCP extension.

```sh
 $ tests/benchmark.sh --gcp
```

Run end to end benchmark without gRPC-GCP extension. For comparison purspose.

```sh
 $ tests/benchmark.sh
```
