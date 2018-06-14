gRPC-GCP Python
===============

Package for gRPC-GCP Python.

Installation
------------

gRPC-GCP Python is available wherever gRPC is available.

From PyPI
~~~~~~~~~

If you are installing locally...

::

  $ pip install grpcio-gcp

Else system wide (on Ubuntu)...

::

  $ sudo pip install grpcio-gcp

Usage
-----

Create a config file (e.g. ``spanner.grpc.config``) defining API configuration,
with ChannelPoolConfig and MethodConfig.

::

  channel_pool: {
    max_size: 10
    max_concurrent_streams_low_watermark: 1
  }
  method: {
    name: "/google.spanner.v1.Spanner/CreateSession"
    affinity: {
      command: BIND
      affinity_key: "name"
    }
  }
  method: {
    name: "/google.spanner.v1.Spanner/GetSession"
    affinity: {
      command: BOUND
      affinity_key: "name"
    }
  }
  method: {
    name: "/google.spanner.v1.Spanner/DeleteSession"
    affinity: {
      command: UNBIND
      affinity_key: "name"
    }
  }

Load configuration file to ApiConfig object.

.. code-block:: python

  import google.protobuf.text_format

  config = grpc_gcp.grpc_gcp_pb2.ApiConfig()
  google.protobuf.text_format.Merge(
      pkg_resources.resource_string(__name__, 'spanner.grpc.config'),
      config)

Create channel pool using grpc_gcp.

.. code-block:: python

  import grpc_gcp
  import grpc

  credentials = grpc.ssl_channel_credentials()
  # Add api config key-value pair to options
  options = [(grpc_gcp.GRPC_GCP_CHANNEL_ARG_API_CONFIG, config)]
  channel_pool = grpc_gcp.secure_channel(target, credentials, options)

The generated channel pool is a wrapper of the original grpc.Channel,
and has the same APIs.
