gRPC for GCP extensions
=======================

Copyright 2018
[The gRPC Authors](https://github.com/grpc/grpc/blob/master/AUTHORS)


# Installation

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
 $ test/setup.sh
```

Run end to end integration tests.

```sh
 $ test/test.sh
```

Run end to end benchmark with gRPC-GCP extension.

```sh
 $ test/benchmark.sh --gcp
```

Run end to end benchmark without gRPC-GCP extension. For comparison purspose.

```sh
 $ test/benchmark.sh
```