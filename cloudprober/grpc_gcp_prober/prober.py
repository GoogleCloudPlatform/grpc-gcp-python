"""Main method of the cloudprober as an entrypoint to execute probes."""

import argparse
import sys
import traceback
import firestore_probes
import grpc
import grpc_gcp
import pkg_resources
import spanner_probes
from stackdriver_util import StackdriverUtil
from google import auth
import google.auth.transport.grpc as transport_grpc
from google.auth.transport.requests import Request
from google.cloud.firestore_v1beta1.proto import firestore_pb2_grpc
import google.protobuf.text_format
from google.cloud.spanner_v1.proto import spanner_pb2_grpc

_OAUTH_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'

_SPANNER_TARGET = 'spanner.googleapis.com'
_FIRESTORE_TARGET = 'firestore.googleapis.com'


def _get_args():
  """Retrieves arguments passed in while executing the main method.

  Returns:
    An object containing all the values for each argument parsed in.

  Raises:
    NotImplementedError: An error occurred when api does not match any records.
  """
  parser = argparse.ArgumentParser()
  parser.add_argument('--api', type=str, help='define the cloud api to probe')
  parser.add_argument('--extension',
                      type=bool,
                      help='options to use grpc-gcp extension')
  return parser.parse_args()


def _secure_authorized_channel(credentials,
                               request,
                               target,
                               ssl_credentials=None,
                               **kwargs):
  metadata_plugin = transport_grpc.AuthMetadataPlugin(credentials, request)

  # Create a set of grpc.CallCredentials using the metadata plugin.
  google_auth_credentials = grpc.metadata_call_credentials(metadata_plugin)

  if ssl_credentials is None:
    ssl_credentials = grpc.ssl_channel_credentials()

  # Combine the ssl credentials and the authorization credentials.
  composite_credentials = grpc.composite_channel_credentials(
      ssl_credentials, google_auth_credentials)

  return grpc_gcp.secure_channel(target, composite_credentials, **kwargs)


def _get_stub_channel(target, use_extension=False):
  cred, _ = auth.default([_OAUTH_SCOPE])
  if not use_extension:
    return _secure_authorized_channel(cred, Request(), target)
  config = grpc_gcp.api_config_from_text_pb(
      pkg_resources.resource_string(__name__, 'spanner.grpc.config'))
  options = [(grpc_gcp.API_CONFIG_CHANNEL_ARG, config)]
  return _secure_authorized_channel(cred, Request(), target, options=options)


def _execute_probe(api, use_extension=False):
  """Execute a probe function given certain Cloud api and probe name.

  Args:
    api: the name of the api provider, e.g. "spanner", "firestore".
    use_extension: option to use grpc-gcp extension when creating channel.

  Raises:
    NotImplementedError: An error occurred when api does not match any records.
  """
  util = StackdriverUtil(api)

  if api == 'spanner':
    channel = _get_stub_channel(_SPANNER_TARGET, use_extension)
    stub = spanner_pb2_grpc.SpannerStub(channel)
    probe_functions = spanner_probes.PROBE_FUNCTIONS
  elif api == 'firestore':
    channel = _get_stub_channel(_FIRESTORE_TARGET)
    stub = firestore_pb2_grpc.FirestoreStub(channel)
    probe_functions = firestore_probes.PROBE_FUNCTIONS
  else:
    raise NotImplementedError('gRPC prober is not implemented for %s !' % api)

  total = len(probe_functions)
  success = 0

  # Execute all probes for given api
  for probe_name in probe_functions:
    probe_function = probe_functions[probe_name]
    try:
      probe_function(stub)
      success += 1
    except Exception:  # pylint: disable=broad-except
      # report any kind of exception to Stackdriver
      util.report_error(traceback.format_exc())

  if success == total:
    util.set_success(True)

  # Summarize metrics
  util.output_metrics()

  # Fail this probe if any function fails
  if success != total:
    sys.exit(1)


if __name__ == '__main__':
  args = _get_args()
  _execute_probe(args.api, args.extension)
