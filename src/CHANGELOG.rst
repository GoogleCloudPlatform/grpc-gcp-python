Changelog
=========

v0.2.1
------

- Apply RLock for Channel class.
- Fix corner cases for async future calls.
- Apply unit tests of grpc channel to grpc_gcp channel.

v0.2.0
------

- Added ``grpc_gcp.api_config_from_text_pb``: Create an instance of ApiConfig given a protocal message string defining api configurations.

v0.1.1
------

Initial release with core functionalities.

- ``grpc_gcp.secure_channel``: Create a secure channel object with connection management.
